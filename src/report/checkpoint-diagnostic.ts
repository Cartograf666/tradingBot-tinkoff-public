/** Safe, finite diagnostics for checkpoint storage. Never retain CLI output or paths. */
export type CheckpointStorageStage = 'RELEASE_DISCOVERY' | 'RELEASE_CREATE' | 'ASSET_INVENTORY'
  | 'ASSET_UPLOAD' | 'ASSET_VERIFY' | 'OPERATION_UPDATE' | 'ARCHIVE_CREATE' | 'CHECKPOINT_PREPARE';
export type CheckpointStorageCategory = 'HTTP_TRANSIENT' | 'HTTP_PERMANENT' | 'TIMEOUT'
  | 'TRANSPORT_TRANSIENT' | 'ABORTED' | 'IDENTITY_CONFLICT' | 'REMOTE_INVALID' | 'UNKNOWN';
export interface CheckpointStorageDiagnostic {
  storageStage: CheckpointStorageStage;
  storageCategory: CheckpointStorageCategory;
  httpStatus: number | null;
  retryable: boolean;
}

export class CheckpointStorageFailure extends Error {
  constructor(readonly diagnostic: CheckpointStorageDiagnostic) {
    super(`${diagnostic.storageStage}:${diagnostic.storageCategory}`);
    this.name = 'CheckpointStorageFailure';
  }
}

export function checkpointStorageFailure(error: unknown, storageStage: CheckpointStorageStage,
  signal?: AbortSignal): CheckpointStorageFailure {
  if (error instanceof CheckpointStorageFailure) return error;
  if (signal?.aborted) return new CheckpointStorageFailure({ storageStage, storageCategory: 'ABORTED', httpStatus: null, retryable: false });
  const value = error && typeof error === 'object' ? error as Record<string, unknown> : {};
  const output = [value.stdout, value.stderr].filter(Buffer.isBuffer).map(part => (part as Buffer).toString('utf8')).join('\n');
  const match = /\bHTTP\s+(\d{3})\b/i.exec(output);
  const httpStatus = match ? Number(match[1]) : null;
  const code = typeof value.code === 'string' ? value.code : '';
  let storageCategory: CheckpointStorageCategory = 'UNKNOWN', retryable = false;
  if (code === 'ABORT_ERR') storageCategory = 'ABORTED';
  else if (code === 'ETIMEDOUT' || code === 'ESOCKETTIMEDOUT') { storageCategory = 'TIMEOUT'; retryable = true; }
  else if (['ECONNRESET', 'EPIPE', 'ENETUNREACH', 'EHOSTUNREACH', 'EAI_AGAIN'].includes(code)) {
    storageCategory = 'TRANSPORT_TRANSIENT'; retryable = true;
  } else if (httpStatus !== null) {
    retryable = [408, 429, 500, 502, 503, 504].includes(httpStatus);
    storageCategory = retryable ? 'HTTP_TRANSIENT' : 'HTTP_PERMANENT';
  }
  return new CheckpointStorageFailure({ storageStage, storageCategory, httpStatus, retryable });
}

export async function checkpointStorageStep<T>(storageStage: CheckpointStorageStage,
  action: () => Promise<T> | T, signal?: AbortSignal): Promise<T> {
  try { signal?.throwIfAborted(); return await action(); }
  catch (error) { throw checkpointStorageFailure(error, storageStage, signal); }
}
