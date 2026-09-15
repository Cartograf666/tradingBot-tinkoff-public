import { performance } from 'node:perf_hooks';
import type { RecordedEventKind } from './market-recording.js';

export interface SubscriptionAcknowledgment {
  key: string;
  success: boolean;
}

export interface CaptureMarketStreamOptions {
  openStream: (signal: AbortSignal) => AsyncIterable<unknown>;
  record: (kind: RecordedEventKind, payload: unknown, connectionEpoch: number) => void;
  expectedSubscriptions: string[];
  acknowledgments: (response: unknown) => SubscriptionAcknowledgment[];
  durationMs: number;
  subscriptionTimeoutMs?: number;
  heartbeatTimeoutMs?: number;
  tickIntervalMs?: number;
  maxReconnects?: number;
  backoffMs?: number[];
  signal?: AbortSignal;
}

export interface CaptureMarketStreamResult {
  reason: 'duration' | 'aborted' | 'max_reconnects';
  epochs: number;
  connectAttempts: number;
  responses: number;
  acknowledgments: number;
  disconnects: number;
  gaps: number;
  ticks: number;
}

type StopReason = 'duration' | 'aborted';
type ConnectionFailure = 'open_error' | 'stream_error' | 'stream_ended' | 'subscription_failed'
  | 'subscription_timeout' | 'heartbeat_timeout' | 'acknowledgment_error';

interface FailureDetails {
  reason: ConnectionFailure;
  code?: number;
}

interface StreamResult {
  type: 'next';
  result: IteratorResult<unknown>;
}

interface StreamError {
  type: 'error';
  error: unknown;
}

const DEFAULT_SUBSCRIPTION_TIMEOUT_MS = 5_000;
const DEFAULT_HEARTBEAT_TIMEOUT_MS = 15_000;
const DEFAULT_TICK_INTERVAL_MS = 1_000;
const DEFAULT_MAX_RECONNECTS = 5;
const DEFAULT_BACKOFF_MS = [250, 1_000, 2_500, 5_000];

function positiveFinite(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${label} must be a positive safe integer`);
  return value;
}

function nonNegativeInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${label} must be a non-negative safe integer`);
  return value;
}

function numericErrorCode(error: unknown): number | undefined {
  if (!error || typeof error !== 'object') return undefined;
  const code = (error as { code?: unknown }).code;
  return typeof code === 'number' && Number.isFinite(code) ? code : undefined;
}

function failurePayload(details: FailureDetails): { reason: ConnectionFailure; code?: number } {
  return details.code === undefined ? { reason: details.reason } : { reason: details.reason, code: details.code };
}

function detachIterator(iterator: AsyncIterator<unknown> | null): void {
  if (!iterator?.return) return;
  try {
    void Promise.resolve(iterator.return()).catch(() => undefined);
  } catch {
    // The connection is already abandoned; never let iterator cleanup block shutdown.
  }
}

function raceWithExternalAbort<T>(candidates: Promise<T>[], signal?: AbortSignal): Promise<T | 'abort'> {
  if (!signal) return Promise.race(candidates);
  if (signal.aborted) return Promise.resolve('abort');
  return new Promise<T | 'abort'>((resolve, reject) => {
    let settled = false;
    const cleanup = (): void => signal.removeEventListener('abort', onAbort);
    const settle = (value: T | 'abort'): void => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(value);
    };
    const onAbort = (): void => settle('abort');
    signal.addEventListener('abort', onAbort, { once: true });
    if (signal.aborted) {
      onAbort();
      return;
    }
    Promise.race(candidates).then(settle, (error: unknown) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(error);
    });
  });
}

export async function captureMarketStream(options: CaptureMarketStreamOptions): Promise<CaptureMarketStreamResult> {
  const durationMs = positiveFinite(options.durationMs, 'durationMs');
  const subscriptionTimeoutMs = positiveFinite(
    options.subscriptionTimeoutMs ?? DEFAULT_SUBSCRIPTION_TIMEOUT_MS,
    'subscriptionTimeoutMs',
  );
  const heartbeatTimeoutMs = positiveFinite(
    options.heartbeatTimeoutMs ?? DEFAULT_HEARTBEAT_TIMEOUT_MS,
    'heartbeatTimeoutMs',
  );
  const tickIntervalMs = positiveFinite(options.tickIntervalMs ?? DEFAULT_TICK_INTERVAL_MS, 'tickIntervalMs');
  const maxReconnects = nonNegativeInteger(options.maxReconnects ?? DEFAULT_MAX_RECONNECTS, 'maxReconnects');
  const backoffMs = options.backoffMs ?? DEFAULT_BACKOFF_MS;
  if (backoffMs.length === 0 || backoffMs.some((value) => !Number.isSafeInteger(value) || value < 0)) {
    throw new Error('backoffMs must contain non-negative safe integer delays');
  }
  const expected = new Set(options.expectedSubscriptions);
  if (expected.size !== options.expectedSubscriptions.length
    || options.expectedSubscriptions.some((key) => key.length === 0)) {
    throw new Error('expectedSubscriptions must contain unique non-empty keys');
  }

  const startedAt = performance.now();
  const deadline = startedAt + durationMs;
  let nextTickIndex = 1;
  let nextTickAt = startedAt + tickIntervalMs;
  let currentController: AbortController | null = null;
  let currentEpoch = 0;
  let recordFailure: unknown = null;
  let responses = 0;
  let acknowledgmentCount = 0;
  let connectAttempts = 0;
  let disconnects = 0;
  let gaps = 0;
  let ticks = 0;
  let stopReason: StopReason | null = null;

  const record = (kind: RecordedEventKind, payload: unknown): void => {
    try {
      options.record(kind, payload, currentEpoch);
    } catch (error) {
      recordFailure = error;
      currentController?.abort();
      throw error;
    }
  };

  const emitDueTicks = (now: number): void => {
    if (nextTickAt > now || nextTickAt > deadline) return;
    const skippedIntervals = Math.max(0, Math.floor((now - nextTickAt) / tickIntervalMs));
    nextTickIndex += skippedIntervals + 1;
    // Anchor every deadline: repeated floating-point addition can put the last tick beyond the capture end.
    nextTickAt = startedAt + nextTickIndex * tickIntervalMs;
    record('tick', { elapsedMs: Math.max(0, Math.round(now - startedAt)), skippedIntervals });
    ticks += 1;
  };

  const nextStopReason = (now = performance.now()): StopReason | null => {
    if (options.signal?.aborted) return 'aborted';
    if (now >= deadline) return 'duration';
    return null;
  };

  const waitUntil = async (target: number): Promise<StopReason | null> => {
    while (true) {
      const immediate = nextStopReason();
      if (immediate) return immediate;
      const now = performance.now();
      if (now >= target) return null;
      const wakeAt = Math.min(target, deadline, nextTickAt);
      let timer: NodeJS.Timeout | undefined;
      const timerPromise = new Promise<'timer'>((resolve) => {
        timer = setTimeout(() => resolve('timer'), Math.min(60_000, Math.max(0, wakeAt - now)));
      });
      const outcome = await raceWithExternalAbort<'timer'>([timerPromise], options.signal);
      if (timer) clearTimeout(timer);
      if (outcome === 'abort') return 'aborted';
      const afterWait = performance.now();
      emitDueTicks(afterWait);
      const stopped = nextStopReason(afterWait);
      if (stopped) return stopped;
    }
  };

  try {
    while (!stopReason) {
      stopReason = nextStopReason();
      if (stopReason) break;

      currentEpoch += 1;
      connectAttempts += 1;
      currentController = new AbortController();
      record('connect_attempt', { attempt: connectAttempts });
      let iterator: AsyncIterator<unknown> | null = null;
      let failure: FailureDetails | null = null;
      let pendingNext: Promise<StreamResult | StreamError> | null = null;
      const connectedAt = performance.now();
      let lastResponseAt = connectedAt;
      const subscriptionDeadline = connectedAt + subscriptionTimeoutMs;
      const pendingSubscriptions = new Set(expected);

      try {
        iterator = options.openStream(currentController.signal)[Symbol.asyncIterator]();
      } catch (error) {
        failure = { reason: 'open_error', code: numericErrorCode(error) };
      }

      while (!failure && !stopReason && iterator) {
        const beforeWait = performance.now();
        emitDueTicks(beforeWait);
        stopReason = nextStopReason(beforeWait);
        if (stopReason) break;
        if (!pendingNext) {
          pendingNext = Promise.resolve()
            .then(() => iterator!.next())
            .then<StreamResult>((result) => ({ type: 'next', result }))
            .catch<StreamError>((error: unknown) => ({ type: 'error', error }));
        }

        const now = performance.now();
        const heartbeatDeadline = lastResponseAt + heartbeatTimeoutMs;
        const wakeAt = Math.min(
          deadline,
          nextTickAt,
          heartbeatDeadline,
          pendingSubscriptions.size > 0 ? subscriptionDeadline : Number.POSITIVE_INFINITY,
        );
        let timer: NodeJS.Timeout | undefined;
        const timerPromise = new Promise<'timer'>((resolve) => {
          timer = setTimeout(() => resolve('timer'), Math.min(60_000, Math.max(0, wakeAt - now)));
        });
        const outcome = await raceWithExternalAbort<StreamResult | StreamError | 'timer'>([
          pendingNext,
          timerPromise,
        ], options.signal);
        if (timer) clearTimeout(timer);

        if (outcome === 'abort') {
          stopReason = 'aborted';
          break;
        }
        if (outcome === 'timer') {
          const afterWait = performance.now();
          emitDueTicks(afterWait);
          stopReason = nextStopReason(afterWait);
          if (stopReason) break;
          if (pendingSubscriptions.size > 0 && afterWait >= subscriptionDeadline) {
            record('subscription_timeout', { pendingCount: pendingSubscriptions.size });
            failure = { reason: 'subscription_timeout' };
          } else if (afterWait >= heartbeatDeadline) {
            record('heartbeat_timeout', { silentForMs: Math.max(0, afterWait - lastResponseAt) });
            failure = { reason: 'heartbeat_timeout' };
          }
          continue;
        }

        pendingNext = null;
        if (outcome.type === 'error') {
          failure = { reason: 'stream_error', code: numericErrorCode(outcome.error) };
          break;
        }
        if (outcome.result.done) {
          failure = { reason: 'stream_ended' };
          break;
        }

        // Raw responses are durable before any decoding, acknowledgment filtering or deduplication.
        record('response', outcome.result.value);
        responses += 1;
        lastResponseAt = performance.now();
        emitDueTicks(lastResponseAt);
        let acknowledgments: SubscriptionAcknowledgment[];
        try {
          acknowledgments = options.acknowledgments(outcome.result.value);
          if (!Array.isArray(acknowledgments)
            || acknowledgments.some((item) => !item || typeof item.key !== 'string' || typeof item.success !== 'boolean')) {
            throw new Error('Invalid acknowledgment decoder result');
          }
        } catch {
          failure = { reason: 'acknowledgment_error' };
          break;
        }
        for (const acknowledgment of acknowledgments) {
          if (!expected.has(acknowledgment.key)) continue;
          if (!acknowledgment.success) {
            failure = { reason: 'subscription_failed' };
            break;
          }
          if (pendingSubscriptions.delete(acknowledgment.key)) acknowledgmentCount += 1;
        }
      }

      currentController.abort();
      detachIterator(iterator);
      currentController = null;
      if (stopReason) break;
      if (!failure) continue;

      disconnects += 1;
      record('disconnect', failurePayload(failure));
      const retriesUsed = currentEpoch - 1;
      const canReconnect = retriesUsed < maxReconnects;
      const reconnectDelay = canReconnect ? backoffMs[Math.min(retriesUsed, backoffMs.length - 1)] : null;
      gaps += 1;
      record('gap', { ...failurePayload(failure), reconnectInMs: reconnectDelay });
      if (!canReconnect) break;
      stopReason = await waitUntil(performance.now() + reconnectDelay!);
    }

    const reason: CaptureMarketStreamResult['reason'] = stopReason ?? 'max_reconnects';
    record('stop', { reason });
    return {
      reason,
      epochs: currentEpoch,
      connectAttempts,
      responses,
      acknowledgments: acknowledgmentCount,
      disconnects,
      gaps,
      ticks,
    };
  } catch (error) {
    currentController?.abort();
    if (recordFailure) throw recordFailure;
    throw error;
  } finally {
    currentController?.abort();
  }
}
