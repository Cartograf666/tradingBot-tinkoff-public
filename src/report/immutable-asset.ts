/** A release upload is a commit: an uncertain response must be reconciled before retrying. */
export interface ImmutableAsset { id: number; name: string; size: number; digest?: string }
export async function confirmImmutableAsset(
  expected: { name: string; bytes: number; sha256: string },
  io: { find(): Promise<ImmutableAsset | undefined>; upload(): Promise<void>; downloadedHash(asset: ImmutableAsset): Promise<string> },
  signal?: AbortSignal,
): Promise<ImmutableAsset> {
  const verify = async (asset: ImmutableAsset) => {
    if (asset.name !== expected.name || asset.size !== expected.bytes
      || (asset.digest && asset.digest !== `sha256:${expected.sha256}`)) throw new Error('Immutable asset identity conflict');
    if (!asset.digest && await io.downloadedHash(asset) !== expected.sha256) throw new Error('Immutable asset content conflict');
    return asset;
  };
  for (let attempt = 0; attempt < 3; attempt += 1) {
    signal?.throwIfAborted();
    const existing = await io.find();
    if (existing) return verify(existing);
    let failed = false;
    try { await io.upload(); } catch { failed = true; }
    signal?.throwIfAborted();
    const confirmed = await io.find();
    if (confirmed) return verify(confirmed);
    if (!failed) throw new Error('Successful upload absent from release inventory');
  }
  throw new Error('Release upload failed after bounded reconciliation');
}
