import { ClientError, Status } from 'nice-grpc';

export type MetadataOperation = 'shareBy' | 'tradingSchedules' | 'getTradingStatuses';
export type MetadataFailureClass = 'TIMEOUT' | 'UNAVAILABLE' | 'RESOURCE_EXHAUSTED' | 'TERMINAL' | 'CANCELLED' | 'DEADLINE';
export interface MetadataDiagnostic {
  operation: MetadataOperation; attempt: number; code: number | null; classification: MetadataFailureClass;
  retry: boolean;
}
export interface MetadataRetryRuntime {
  now(): number;
  setTimer(callback: () => void, milliseconds: number): unknown;
  clearTimer(timer: unknown): void;
}
const runtime: MetadataRetryRuntime = {
  now: Date.now,
  setTimer: (callback, milliseconds) => setTimeout(callback, milliseconds),
  clearTimer: timer => clearTimeout(timer as NodeJS.Timeout),
};
export class MetadataRequestFailure extends Error {
  constructor(readonly diagnostic: MetadataDiagnostic) {
    super('Metadata request failed');
    this.name = 'MetadataRequestFailure';
  }
  get code(): number | null { return this.diagnostic.code; }
}

export function classifyMetadataError(error: unknown): { code: number | null; classification: MetadataFailureClass } {
  const code = error && typeof error === 'object' && 'code' in error && typeof error.code === 'number'
    && Number.isFinite(error.code) ? error.code : null;
  // nice-grpc propagates the AbortSignal reason unchanged. Node's TimeoutError has code 23.
  if (error instanceof DOMException && error.name === 'TimeoutError') return { code, classification: 'TIMEOUT' };
  if (error instanceof ClientError) {
    if (error.code === Status.DEADLINE_EXCEEDED) return { code, classification: 'TIMEOUT' };
    if (error.code === Status.RESOURCE_EXHAUSTED) return { code, classification: 'RESOURCE_EXHAUSTED' };
    // An HTTP access refusal can be surfaced as gRPC UNAVAILABLE; it is not a transient network failure.
    if (error.code === Status.UNAVAILABLE && !/Received HTTP status code (?:401|403)\b/.test(error.details)) {
      return { code, classification: 'UNAVAILABLE' };
    }
  }
  return { code, classification: 'TERMINAL' };
}

/** A single absolute clock bound shared by metadata setup and streaming. Dispose always removes the timer/listener. */
export function captureDeadlineSignal(deadlineMs: number | undefined, signal?: AbortSignal,
  clock: MetadataRetryRuntime = runtime): { signal: AbortSignal; dispose(): void } {
  if (deadlineMs !== undefined && !Number.isFinite(deadlineMs)) throw new Error('Invalid capture deadline');
  const controller = new AbortController();
  let timer: unknown;
  const onAbort = () => controller.abort(signal?.reason);
  signal?.addEventListener('abort', onAbort, { once: true });
  if (signal?.aborted) onAbort();
  const expire = () => {
    if (deadlineMs === undefined || controller.signal.aborted) return;
    const remaining = deadlineMs - clock.now();
    if (remaining <= 0) controller.abort(new DOMException('Capture deadline has passed', 'TimeoutError'));
    else timer = clock.setTimer(expire, Math.min(remaining, 2_147_483_647));
  };
  expire();
  return { signal: controller.signal, dispose: () => {
    if (timer !== undefined) clock.clearTimer(timer);
    signal?.removeEventListener('abort', onAbort);
  } };
}

function abortable<T>(operation: () => Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => { cleanup(); reject(signal.reason); };
    const cleanup = () => signal.removeEventListener('abort', onAbort);
    signal.addEventListener('abort', onAbort, { once: true });
    // Promise handlers remain attached even if cancellation wins, so a late SDK rejection is never unhandled.
    Promise.resolve().then(() => { signal.throwIfAborted(); return operation(); }).then(
      value => { cleanup(); resolve(value); }, error => { cleanup(); reject(error); });
  });
}

export interface MetadataRetryOptions {
  operation: MetadataOperation;
  signal?: AbortSignal;
  deadlineMs?: number;
  maxAttempts?: number;
  attemptTimeoutMs?: number;
  backoffMs?: readonly number[];
  onDiagnostic?: (diagnostic: MetadataDiagnostic) => void;
  runtime?: MetadataRetryRuntime;
}

export async function retryMetadata<T>(request: (signal: AbortSignal) => Promise<T>, options: MetadataRetryOptions): Promise<T> {
  const clock = options.runtime ?? runtime;
  const maxAttempts = options.maxAttempts ?? 4, attemptTimeoutMs = options.attemptTimeoutMs ?? 10_000;
  const backoffMs = options.backoffMs ?? [1_000, 2_000, 5_000];
  if (!Number.isSafeInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 20
    || !Number.isSafeInteger(attemptTimeoutMs) || attemptTimeoutMs < 1
    || !backoffMs.length || backoffMs.some(ms => !Number.isSafeInteger(ms) || ms < 0)
    || (options.deadlineMs !== undefined && !Number.isFinite(options.deadlineMs))) throw new Error('Invalid metadata retry bounds');
  // Standalone calls also have a finite retry budget. Its exhaustion is a transient request timeout;
  // only the caller's absolute capture deadline means the chunk window itself has expired.
  const budgetMs = maxAttempts * attemptTimeoutMs + Array.from({ length: maxAttempts - 1 },
    (_, index) => backoffMs[Math.min(index, backoffMs.length - 1)]).reduce((sum, ms) => sum + ms, 0);
  const deadlineMs = Math.min(options.deadlineMs ?? Infinity, clock.now() + budgetMs);
  const terminal = (attempt: number, classification: MetadataFailureClass, code: number | null = null): never => {
    const diagnostic = { operation: options.operation, attempt, code, classification, retry: false };
    options.onDiagnostic?.(diagnostic);
    throw new MetadataRequestFailure(diagnostic);
  };
  const captureExpired = () => options.deadlineMs !== undefined && clock.now() >= options.deadlineMs;
  const expired = (attempt: number): never => captureExpired()
    ? terminal(attempt, 'DEADLINE') : terminal(attempt, 'TIMEOUT', 23);
  const check = (attempt: number) => {
    if (options.signal?.aborted) terminal(attempt, 'CANCELLED');
    if (clock.now() >= deadlineMs) expired(attempt);
  };
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    check(attempt);
    const attemptDeadlineMs = Math.min(deadlineMs, clock.now() + attemptTimeoutMs);
    const bounded = captureDeadlineSignal(attemptDeadlineMs, options.signal, clock);
    let failure: { code: number | null; classification: MetadataFailureClass };
    try {
      const response = await abortable(() => request(bounded.signal), bounded.signal);
      // A response arriving at the deadline does not extend capture or defeat a simultaneous user cancellation.
      if (options.signal?.aborted) terminal(attempt, 'CANCELLED');
      if (clock.now() >= deadlineMs) expired(attempt);
      if (clock.now() >= attemptDeadlineMs) throw new DOMException('Metadata attempt timed out', 'TimeoutError');
      return response;
    } catch (error) {
      if (error instanceof MetadataRequestFailure) throw error;
      failure = options.signal?.aborted ? { code: null, classification: 'CANCELLED' }
        : captureExpired() ? { code: null, classification: 'DEADLINE' }
          : bounded.signal.aborted ? { code: 23, classification: 'TIMEOUT' } : classifyMetadataError(error);
    } finally { bounded.dispose(); }
    const transient = ['TIMEOUT', 'UNAVAILABLE', 'RESOURCE_EXHAUSTED'].includes(failure.classification);
    const delay = backoffMs[Math.min(attempt - 1, backoffMs.length - 1)];
    const retry = transient && attempt < maxAttempts && clock.now() + delay < deadlineMs;
    const diagnostic = { operation: options.operation, attempt, ...failure, retry };
    options.onDiagnostic?.(diagnostic);
    if (!retry) throw new MetadataRequestFailure(diagnostic);
    const waiting = captureDeadlineSignal(deadlineMs, options.signal, clock);
    let timer: unknown;
    try {
      await abortable(() => new Promise<void>(resolve => { timer = clock.setTimer(resolve, delay); }), waiting.signal);
    } catch {
      if (options.signal?.aborted) terminal(attempt, 'CANCELLED');
      expired(attempt);
    } finally {
      if (timer !== undefined) clock.clearTimer(timer);
      waiting.dispose();
    }
  }
  throw new Error('Unreachable metadata retry state');
}
