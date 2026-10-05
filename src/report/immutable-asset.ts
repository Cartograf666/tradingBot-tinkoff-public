/** A release upload is a commit: an uncertain response must be reconciled before retrying. */
import { CheckpointStorageFailure, checkpointStorageFailure, type CheckpointStorageDiagnostic } from './checkpoint-diagnostic.js';

export interface ImmutableAsset { id: number; name: string; size: number; state: 'uploaded' | 'starter'; digest?: string }
const failure = (storageCategory: CheckpointStorageDiagnostic['storageCategory'],
  storageStage: CheckpointStorageDiagnostic['storageStage']) => new CheckpointStorageFailure({
  storageStage, storageCategory, httpStatus: null, retryable: false,
});
async function waitForInventory(signal?: AbortSignal): Promise<void> {
  signal?.throwIfAborted();
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => { signal?.removeEventListener('abort', abort); resolve(); }, 200);
    const abort = () => { clearTimeout(timer); reject(signal?.reason ?? failure('ABORTED', 'ASSET_INVENTORY')); };
    signal?.addEventListener('abort', abort, { once: true });
  });
}
export async function confirmImmutableAsset(
  expected: { name: string; bytes: number; sha256: string },
  io: { find(): Promise<ImmutableAsset | undefined>; upload(): Promise<void>; downloadedHash(asset: ImmutableAsset): Promise<string> },
  signal?: AbortSignal,
): Promise<ImmutableAsset> {
  const verify = async (asset: ImmutableAsset) => {
    if (asset.state !== 'uploaded') throw failure('REMOTE_INVALID', 'ASSET_VERIFY');
    if (asset.name !== expected.name || asset.size !== expected.bytes
      || (asset.digest && asset.digest !== `sha256:${expected.sha256}`)) throw failure('IDENTITY_CONFLICT', 'ASSET_VERIFY');
    if (!asset.digest && await boundedRead(() => io.downloadedHash(asset), 'ASSET_VERIFY') !== expected.sha256) {
      throw failure('IDENTITY_CONFLICT', 'ASSET_VERIFY');
    }
    return asset;
  };
  const boundedRead = async <T>(read: () => Promise<T>, storageStage: CheckpointStorageDiagnostic['storageStage']): Promise<T> => {
    for (let attempt = 0; ; attempt += 1) {
      signal?.throwIfAborted();
      try { return await read(); }
      catch (error) {
        const safe = checkpointStorageFailure(error, storageStage, signal);
        if (!safe.diagnostic.retryable || attempt >= 2) throw safe;
        await waitForInventory(signal);
      }
    }
  };
  const find = () => boundedRead(io.find, 'ASSET_INVENTORY');
  let lastUploadFailure: CheckpointStorageFailure | undefined;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    signal?.throwIfAborted();
    const existing = await find();
    if (existing) return verify(existing);
    let uploadFailure: unknown;
    try { await io.upload(); } catch (error) {
      uploadFailure = checkpointStorageFailure(error, 'ASSET_UPLOAD', signal);
      lastUploadFailure = uploadFailure as CheckpointStorageFailure;
    }
    signal?.throwIfAborted();
    // A lost upload response may hide a committed asset; observe inventory first.
    for (let observation = 0; observation < 3; observation += 1) {
      const confirmed = await find();
      if (confirmed) return verify(confirmed);
      if (observation < 2) await waitForInventory(signal);
    }
    if (!uploadFailure) throw failure('REMOTE_INVALID', 'ASSET_INVENTORY');
    if (!(uploadFailure instanceof CheckpointStorageFailure) || !uploadFailure.diagnostic.retryable) throw uploadFailure;
    if (attempt < 2) await waitForInventory(signal);
  }
  throw lastUploadFailure ?? failure('REMOTE_INVALID', 'ASSET_UPLOAD');
}
