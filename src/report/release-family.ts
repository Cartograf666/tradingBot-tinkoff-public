import { STUDY_RELEASE_TAG } from '../research/study-protocol.js';
import { setTimeout as pause } from 'node:timers/promises';
import { checkpointStorageFailure, CheckpointStorageFailure } from './checkpoint-diagnostic.js';
import { confirmImmutableAsset, type ImmutableAsset } from './immutable-asset.js';

export const RELEASE_ASSET_LIMIT = 1000;
export interface PhysicalRelease { id: number; tag: string; ordinal: number }
export interface FamilyInventory { releases: PhysicalRelease[]; assets: ImmutableAsset[];
  slots: Map<number, number> }
const fail = (stage: 'RELEASE_DISCOVERY' | 'RELEASE_CREATE' | 'ASSET_INVENTORY' | 'ASSET_VERIFY',
  category: 'IDENTITY_CONFLICT' | 'REMOTE_INVALID') => new CheckpointStorageFailure({
  storageStage: stage, storageCategory: category, httpStatus: null, retryable: false,
});

export function physicalReleaseTag(ordinal: number): string {
  if (!Number.isSafeInteger(ordinal) || ordinal < 1 || ordinal > 9999) throw fail('RELEASE_CREATE', 'REMOTE_INVALID');
  return ordinal === 1 ? STUDY_RELEASE_TAG : `${STUDY_RELEASE_TAG}-part-${String(ordinal).padStart(4, '0')}`;
}

/** Check the entire authenticated draft inventory; reserved near-miss tags fail closed. */
export function parsePhysicalReleases(raw: unknown[]): PhysicalRelease[] {
  const releases: PhysicalRelease[] = [], ordinals = new Set<number>(), ids = new Set<number>();
  for (const value of raw) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw fail('RELEASE_DISCOVERY', 'REMOTE_INVALID');
    const item = value as Record<string, unknown>, tag = item.tag_name;
    if (typeof tag !== 'string' || !tag.startsWith(STUDY_RELEASE_TAG)) continue;
    const ordinal = tag === STUDY_RELEASE_TAG ? 1
      : /^market-study-archive-v1-part-([0-9]{4})$/.exec(tag)?.[1];
    const number = typeof ordinal === 'string' ? Number(ordinal) : ordinal ?? 0;
    if (!Number.isSafeInteger(number) || number < 1 || physicalReleaseTag(number) !== tag
      || item.draft !== true || !Number.isSafeInteger(item.id) || Number(item.id) <= 0
      || ordinals.has(number) || ids.has(Number(item.id))) throw fail('RELEASE_DISCOVERY', 'IDENTITY_CONFLICT');
    ordinals.add(number); ids.add(Number(item.id));
    releases.push({ id: Number(item.id), tag, ordinal: number });
  }
  releases.sort((a, b) => a.ordinal - b.ordinal);
  for (let index = 0; index < releases.length; index += 1) {
    if (releases[index].ordinal !== index + 1) throw fail('RELEASE_DISCOVERY', 'IDENTITY_CONFLICT');
  }
  return releases;
}

export interface ReleaseFamilyIO {
  releases(): Promise<PhysicalRelease[]>;
  assets(release: PhysicalRelease): Promise<ImmutableAsset[]>;
  create(tag: string): Promise<void>;
  upload(release: PhysicalRelease): Promise<void>;
  downloadedHash(asset: ImmutableAsset): Promise<string>;
}

const visibilityDelaysMs = [0, 1_000, 3_000, 6_000] as const;

/** A successful create can precede its appearance in the paginated inventory. Read only; never create twice. */
async function confirmCreatedRelease(io: ReleaseFamilyIO, ordinal: number, tag: string, signal?: AbortSignal,
  wait: (ms: number, signal?: AbortSignal) => Promise<void> = async (ms, active) => {
    await pause(ms, undefined, { signal: active });
  }): Promise<boolean> {
  for (const delay of visibilityDelaysMs) {
    signal?.throwIfAborted();
    if (delay) await wait(delay, signal);
    const inventory = await familyInventory(io, signal);
    if (inventory.releases.some(release => release.ordinal === ordinal && release.tag === tag)) return true;
  }
  return false;
}

export async function familyInventory(io: Pick<ReleaseFamilyIO, 'releases' | 'assets'>,
  signal?: AbortSignal): Promise<FamilyInventory> {
  signal?.throwIfAborted();
  const releases = await io.releases(), assets: ImmutableAsset[] = [], slots = new Map<number, number>();
  const names = new Set<string>(), ids = new Map<number, ImmutableAsset>();
  const ordinals = new Set<number>(), releaseIds = new Set<number>();
  for (const release of releases) {
    signal?.throwIfAborted();
    if (ordinals.has(release.ordinal) || releaseIds.has(release.id) || !Number.isSafeInteger(release.id)
      || release.id <= 0 || release.tag !== physicalReleaseTag(release.ordinal)) {
      throw fail('RELEASE_DISCOVERY', 'IDENTITY_CONFLICT');
    }
    ordinals.add(release.ordinal); releaseIds.add(release.id);
    const rows = await io.assets(release);
    if (rows.length > RELEASE_ASSET_LIMIT) throw fail('ASSET_INVENTORY', 'REMOTE_INVALID');
    slots.set(release.id, rows.length);
    for (const asset of rows) {
      if (!Number.isSafeInteger(asset.id) || asset.id <= 0 || !asset.name || !Number.isSafeInteger(asset.size)
        || asset.size < 0 || !['uploaded', 'starter'].includes(asset.state)
        || names.has(asset.name) || ids.has(asset.id)) throw fail('ASSET_INVENTORY', 'IDENTITY_CONFLICT');
      names.add(asset.name); ids.set(asset.id, asset); assets.push(asset);
    }
  }
  for (let index = 1; index <= releases.length; index += 1) {
    if (!ordinals.has(index)) throw fail('RELEASE_DISCOVERY', 'IDENTITY_CONFLICT');
  }
  return { releases, assets, slots };
}

/** One pinned destination per upload. Rollover follows proven fullness, never a timeout. */
export async function confirmFamilyAsset(expected: { name: string; bytes: number; sha256: string },
  io: ReleaseFamilyIO, signal?: AbortSignal,
  waitForVisibility?: (ms: number, signal?: AbortSignal) => Promise<void>): Promise<ImmutableAsset> {
  let rollover = 0;
  while (rollover < 3) {
    signal?.throwIfAborted();
    const inventory = await familyInventory(io, signal);
    const known = inventory.assets.find(asset => asset.name === expected.name);
    if (known) return confirmImmutableAsset(expected, { find: async () => {
      const current = await familyInventory(io, signal);
      return current.assets.find(asset => asset.name === expected.name);
    }, upload: async () => { throw fail('ASSET_VERIFY', 'IDENTITY_CONFLICT'); }, downloadedHash: io.downloadedHash }, signal);
    const selected = inventory.releases.at(-1);
    if (!selected || inventory.slots.get(selected.id) === RELEASE_ASSET_LIMIT) {
      const ordinal = selected ? selected.ordinal + 1 : 1;
      const tag = physicalReleaseTag(ordinal);
      let createError: unknown;
      try { await io.create(tag); }
      catch (error) { createError = error; }
      if (createError) {
        const failure = checkpointStorageFailure(createError, 'RELEASE_CREATE', signal);
        if (!failure.diagnostic.retryable && ![409, 422].includes(failure.diagnostic.httpStatus ?? 0)) throw failure;
      }
      // Only a confirmed create or an ambiguous transient/concurrent result can
      // enter bounded read-only reconciliation. Auth and unknown failures stop here.
      if (!await confirmCreatedRelease(io, ordinal, tag, signal, waitForVisibility)) {
        if (createError) throw createError;
        throw fail('RELEASE_CREATE', 'REMOTE_INVALID');
      }
      rollover++; continue;
    }
    // The destination stays pinned once an upload begins. A 409/422 response is
    // ambiguous even if another writer filled the release meanwhile.
    return confirmImmutableAsset(expected, {
      find: async () => (await familyInventory(io, signal)).assets.find(asset => asset.name === expected.name),
      upload: async () => {
        try { await io.upload(selected); }
        catch (error) { throw checkpointStorageFailure(error, 'ASSET_UPLOAD', signal); }
      }, downloadedHash: io.downloadedHash,
    }, signal);
  }
  throw fail('RELEASE_CREATE', 'REMOTE_INVALID');
}
