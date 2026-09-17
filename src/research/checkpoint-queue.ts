/** A bounded, sequential uploader. Acquisition never waits for network I/O. */
export class CheckpointQueueError extends Error {
  constructor(readonly category: 'CHECKPOINT_OVERFLOW' | 'CHECKPOINT_UPLOAD_FAILED' | 'CHECKPOINT_TIMEOUT') {
    super(category);
    this.name = 'CheckpointQueueError';
  }
}

export interface CheckpointQueueOptions<T> {
  upload: (item: T, signal: AbortSignal) => Promise<void>;
  onFailure: (error: CheckpointQueueError) => void;
  maxPending?: number;
  uploadTimeoutMs?: number;
}

export class CheckpointQueue<T> {
  private readonly options: CheckpointQueueOptions<T>;
  private readonly maxPending: number;
  private readonly uploadTimeoutMs: number;
  private readonly waiting: T[] = [];
  private active = false;
  private activeController: AbortController | null = null;
  private closed = false;
  private failure: CheckpointQueueError | null = null;
  private readonly idleWaiters = new Set<() => void>();

  constructor(options: CheckpointQueueOptions<T>) {
    this.options = options;
    this.maxPending = options.maxPending ?? 2;
    this.uploadTimeoutMs = options.uploadTimeoutMs ?? 120_000;
    for (const [label, value] of [['maxPending', this.maxPending], ['uploadTimeoutMs', this.uploadTimeoutMs]] as const) {
      if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${label} must be a positive safe integer`);
    }
  }

  get pending(): number { return this.waiting.length + Number(this.active); }
  throwIfFailed(): void { if (this.failure) throw this.failure; }

  private fail(error: CheckpointQueueError): void {
    if (this.failure) return;
    this.failure = error;
    this.waiting.length = 0;
    this.activeController?.abort(error);
    try { this.options.onFailure(error); } catch { /* The original queue failure still rejects acquisition/drain. */ }
    this.notifyIdle();
  }

  enqueue(item: T): void {
    this.throwIfFailed();
    if (this.closed) throw new Error('Checkpoint queue is closed');
    if (this.pending >= this.maxPending) {
      this.fail(new CheckpointQueueError('CHECKPOINT_OVERFLOW'));
      this.throwIfFailed();
    }
    this.waiting.push(item);
    this.startNext();
  }

  private notifyIdle(): void {
    if (!this.failure && this.pending !== 0) return;
    for (const waiter of this.idleWaiters) waiter();
    this.idleWaiters.clear();
  }

  private startNext(): void {
    if (this.active || this.failure || this.waiting.length === 0) { this.notifyIdle(); return; }
    const item = this.waiting.shift()!;
    this.active = true;
    const controller = new AbortController();
    this.activeController = controller;
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new CheckpointQueueError('CHECKPOINT_TIMEOUT')), this.uploadTimeoutMs);
    });
    let onAbort: (() => void) | undefined;
    const aborted = new Promise<never>((_, reject) => {
      onAbort = () => reject(this.failure ?? new CheckpointQueueError('CHECKPOINT_UPLOAD_FAILED'));
      controller.signal.addEventListener('abort', onAbort, { once: true });
    });
    void Promise.race([Promise.resolve().then(() => this.options.upload(item, controller.signal)), timeout, aborted])
      .catch((error: unknown) => this.fail(error instanceof CheckpointQueueError ? error
        : new CheckpointQueueError('CHECKPOINT_UPLOAD_FAILED')))
      .finally(() => {
        if (timer) clearTimeout(timer);
        if (onAbort) controller.signal.removeEventListener('abort', onAbort);
        this.active = false;
        this.activeController = null;
        this.startNext();
      });
  }

  async drain(timeoutMs = this.uploadTimeoutMs): Promise<void> {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) throw new Error('drain timeout must be a positive safe integer');
    this.closed = true;
    this.throwIfFailed();
    if (this.pending === 0) return;
    let timer: NodeJS.Timeout | undefined;
    let waiter: (() => void) | undefined;
    try {
      await Promise.race([
        new Promise<void>(resolve => { waiter = resolve; this.idleWaiters.add(resolve); }),
        new Promise<never>((_, reject) => { timer = setTimeout(() => {
          const error = new CheckpointQueueError('CHECKPOINT_TIMEOUT');
          this.fail(error); reject(error);
        }, timeoutMs); }),
      ]);
      this.throwIfFailed();
    } finally {
      if (timer) clearTimeout(timer);
      if (waiter) this.idleWaiters.delete(waiter);
    }
  }
}
