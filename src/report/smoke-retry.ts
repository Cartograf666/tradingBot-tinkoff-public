const MAX_ATTEMPTS = 3;
const RETRY_DELAY_MS = 15_000;
const TOTAL_BUDGET_MS = 8 * 60_000;
const MINIMUM_PROBE_WINDOW_MS = 90_000;

export type SmokeCheckEvent =
  | { type: 'attempt'; attempt: number }
  | { type: 'quality-rejected'; attempt: number; reasons: string[] }
  | { type: 'retry'; attempt: number; delayMs: number }
  | { type: 'passed'; attempt: number }
  | { type: 'stopped'; attempt: number; reason: SmokeRetryError['reason'] }
  | { type: 'fatal'; attempt: number; stage: StudyStageError['stage'] | 'unknown' };

export class SmokeQualityError extends Error {
  readonly reasons: string[];

  constructor(reasons: string[]) {
    super('Smoke quality check rejected the recording');
    this.name = 'SmokeQualityError';
    this.reasons = [...reasons];
  }
}

export class StudyStageError extends Error {
  readonly stage: 'recording' | 'quality' | 'replay' | 'private-archive';

  constructor(stage: StudyStageError['stage']) {
    super(`Smoke ${stage} stage failed`);
    this.name = 'StudyStageError';
    this.stage = stage;
  }
}

export class SmokeRetryError extends Error {
  readonly reason: 'QUALITY_RETRIES_EXHAUSTED' | 'CHECK_WINDOW_EXPIRED';

  constructor(reason: SmokeRetryError['reason']) {
    super(reason === 'QUALITY_RETRIES_EXHAUSTED'
      ? 'Smoke quality retries exhausted'
      : 'Smoke check window expired');
    this.name = 'SmokeRetryError';
    this.reason = reason;
  }
}

export interface SmokeRetryOptions {
  signal: AbortSignal;
  deadlineMs?: number;
  now?: () => number;
  wait?: (delayMs: number, signal: AbortSignal) => Promise<void>;
  onEvent?: (event: SmokeCheckEvent) => void;
}

function abortError(): Error {
  const error = new Error('Operation aborted');
  error.name = 'AbortError';
  return error;
}

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) throw abortError();
}

function finiteNow(now: () => number): number {
  const value = now();
  if (!Number.isFinite(value)) throw new Error('Invalid smoke retry clock');
  return value;
}

function defaultWait(delayMs: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(abortError());
      return;
    }
    const timeout = setTimeout(done, delayMs);
    const onAbort = () => {
      clearTimeout(timeout);
      cleanup();
      reject(abortError());
    };
    const cleanup = () => signal.removeEventListener('abort', onAbort);
    function done(): void {
      cleanup();
      resolve();
    }
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

/** Runs a short quality gate without relaxing the gate or retrying infrastructure failures. */
export async function runSmokeWithRetries<T>(
  probe: (deadlineMs: number) => Promise<T>,
  options: SmokeRetryOptions,
): Promise<T> {
  const now = options.now ?? Date.now;
  const startedAt = finiteNow(now);
  if (options.deadlineMs !== undefined && !Number.isFinite(options.deadlineMs)) {
    throw new Error('Invalid smoke retry deadline');
  }
  const deadlineMs = Math.min(options.deadlineMs ?? Infinity, startedAt + TOTAL_BUDGET_MS);
  const wait = options.wait ?? defaultWait;
  const onEvent = options.onEvent ?? (() => undefined);

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1) {
    throwIfAborted(options.signal);
    if (deadlineMs - finiteNow(now) < MINIMUM_PROBE_WINDOW_MS) {
      const failure = new SmokeRetryError('CHECK_WINDOW_EXPIRED');
      onEvent({ type: 'stopped', attempt, reason: failure.reason });
      throw failure;
    }
    onEvent({ type: 'attempt', attempt });
    let result: T;
    try {
      result = await probe(deadlineMs);
    } catch (error) {
      throwIfAborted(options.signal);
      if (!(error instanceof SmokeQualityError)) {
        onEvent({ type: 'fatal', attempt, stage: error instanceof StudyStageError ? error.stage : 'unknown' });
        throw error;
      }
      onEvent({ type: 'quality-rejected', attempt, reasons: [...error.reasons] });
      if (attempt === MAX_ATTEMPTS) {
        const failure = new SmokeRetryError('QUALITY_RETRIES_EXHAUSTED');
        onEvent({ type: 'stopped', attempt, reason: failure.reason });
        throw failure;
      }
      if (deadlineMs - finiteNow(now) < RETRY_DELAY_MS + MINIMUM_PROBE_WINDOW_MS) {
        const failure = new SmokeRetryError('CHECK_WINDOW_EXPIRED');
        onEvent({ type: 'stopped', attempt, reason: failure.reason });
        throw failure;
      }
      onEvent({ type: 'retry', attempt, delayMs: RETRY_DELAY_MS });
      await wait(RETRY_DELAY_MS, options.signal);
      throwIfAborted(options.signal);
      continue;
    }
    throwIfAborted(options.signal);
    if (finiteNow(now) > deadlineMs) {
      const failure = new SmokeRetryError('CHECK_WINDOW_EXPIRED');
      onEvent({ type: 'stopped', attempt, reason: failure.reason });
      throw failure;
    }
    onEvent({ type: 'passed', attempt });
    return result;
  }
  throw new Error('Smoke retry loop terminated unexpectedly');
}
