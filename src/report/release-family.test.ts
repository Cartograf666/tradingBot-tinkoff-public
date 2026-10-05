import assert from 'node:assert/strict';
import test from 'node:test';
import { CheckpointStorageFailure } from './checkpoint-diagnostic.js';
import { confirmFamilyAsset, familyInventory, parsePhysicalReleases, physicalReleaseTag,
  type PhysicalRelease, type ReleaseFamilyIO } from './release-family.js';
import { listReleaseFamily, releaseAssetsIn } from './market-study.js';
import type { ImmutableAsset } from './immutable-asset.js';

const expected = { name: 'checkpoint-000031.tar.gz', bytes: 42, sha256: 'a'.repeat(64) };
const match = (id: number): ImmutableAsset => ({ id, name: expected.name, size: expected.bytes,
  state: 'uploaded', digest: `sha256:${expected.sha256}` });
const old = (count: number): ImmutableAsset[] => Array.from({ length: count }, (_, index) => ({
  id: index + 1, name: `old-${index + 1}.tar.gz`, size: index === 0 ? 0 : 1,
  state: index === 0 ? 'starter' : 'uploaded', digest: `sha256:${'b'.repeat(64)}`,
}));
function fake(initialCount: number, extra: Array<{ release: PhysicalRelease; assets: ImmutableAsset[] }> = []) {
  const releases: PhysicalRelease[] = [{ id: 1, tag: physicalReleaseTag(1), ordinal: 1 }, ...extra.map(row => row.release)];
  const rows = new Map<number, ImmutableAsset[]>([[1, old(initialCount)], ...extra.map(row => [row.release.id, row.assets] as const)]);
  const created: string[] = [], uploads: number[] = [];
  const io: ReleaseFamilyIO = {
    releases: async () => [...releases], assets: async release => [...(rows.get(release.id) ?? [])],
    create: async tag => { created.push(tag); const ordinal = releases.length + 1;
      assert.equal(tag, physicalReleaseTag(ordinal)); releases.push({ id: ordinal, tag, ordinal }); rows.set(ordinal, []); },
    upload: async release => { uploads.push(release.id); rows.get(release.id)!.push(match(1001)); },
    downloadedHash: async () => expected.sha256,
  };
  return { io, releases, rows, created, uploads };
}

test('physical release names are canonical private draft family; reserved near misses and duplicate identities fail', () => {
  assert.equal(physicalReleaseTag(1), 'market-study-archive-v1');
  assert.equal(physicalReleaseTag(2), 'market-study-archive-v1-part-0002');
  const base = { id: 1, tag_name: physicalReleaseTag(1), draft: true };
  const part = { id: 2, tag_name: physicalReleaseTag(2), draft: true };
  assert.deepEqual(parsePhysicalReleases([part, base]).map(row => row.ordinal), [1, 2]);
  for (const bad of [{ ...part, tag_name: 'market-study-archive-v1-part-02' },
    { ...part, tag_name: 'market-study-archive-v1-part-0001' }, { ...part, draft: false },
    { ...part, id: 1 }, { ...part, tag_name: physicalReleaseTag(3) }]) {
    assert.throws(() => parsePhysicalReleases([base, bad]), /IDENTITY_CONFLICT/);
  }
  assert.throws(() => parsePhysicalReleases([base, part, part]), /IDENTITY_CONFLICT/);
});

test('slot 999 stays in base; slot 1000 rolls to private part 0002 with old ids intact', async () => {
  const before = fake(999);
  assert.deepEqual(await confirmFamilyAsset(expected, before.io), match(1001));
  assert.deepEqual(before.uploads, [1]); assert.deepEqual(before.created, []);
  assert.equal(before.rows.get(1)?.length, 1000);

  const full = fake(1000);
  assert.deepEqual(await confirmFamilyAsset(expected, full.io), match(1001));
  assert.deepEqual(full.created, [physicalReleaseTag(2)]); assert.deepEqual(full.uploads, [2]);
  const inventory = await familyInventory(full.io);
  assert.equal(inventory.assets.length, 1001); assert.equal(inventory.assets[0].id, 1);
  assert.equal(inventory.assets.at(-1)?.id, 1001);
  assert.equal(inventory.slots.get(1), 1000); assert.equal(inventory.slots.get(2), 1);
});

test('created private shard becomes visible after bounded read-only inventory retries', async () => {
  const state = fake(1000), read = state.io.releases;
  let hiddenReads = 2;
  state.io.releases = async () => {
    const releases = await read();
    return state.created.length && hiddenReads-- > 0 ? releases.filter(item => item.ordinal === 1) : releases;
  };
  const delays: number[] = [];
  assert.deepEqual(await confirmFamilyAsset(expected, state.io, undefined, async ms => { delays.push(ms); }), match(1001));
  assert.deepEqual(state.created, [physicalReleaseTag(2)], 'never create the same draft twice');
  assert.deepEqual(state.uploads, [2], 'upload only after the exact draft is visible');
  assert.deepEqual(delays, [1_000, 3_000]);
});

test('ambiguous create response is reconciled by reads, while permanently missing draft fails closed', async () => {
  const recovered = fake(1000), create = recovered.io.create, read = recovered.io.releases;
  let hiddenReads = 1;
  recovered.io.create = async tag => { await create(tag); throw new CheckpointStorageFailure({
    storageStage: 'RELEASE_CREATE', storageCategory: 'TIMEOUT', httpStatus: null, retryable: true }); };
  recovered.io.releases = async () => {
    const releases = await read();
    return recovered.created.length && hiddenReads-- > 0 ? releases.filter(item => item.ordinal === 1) : releases;
  };
  assert.deepEqual(await confirmFamilyAsset(expected, recovered.io, undefined, async () => {}), match(1001));
  assert.deepEqual(recovered.created, [physicalReleaseTag(2)]); assert.deepEqual(recovered.uploads, [2]);

  const absent = fake(1000), actual = absent.io.releases;
  absent.io.releases = async () => (await actual()).filter(item => item.ordinal === 1);
  let reads = 0;
  const inventory = absent.io.releases;
  absent.io.releases = async () => { reads++; return inventory(); };
  await assert.rejects(confirmFamilyAsset(expected, absent.io, undefined, async () => {}), /REMOTE_INVALID/);
  assert.equal(reads, 5, 'one initial inventory plus four visibility reads');
  assert.deepEqual(absent.created, [physicalReleaseTag(2)]); assert.deepEqual(absent.uploads, []);
});

test('auth and unclassified create errors never become permission by observing a concurrent draft', async () => {
  for (const category of ['HTTP_PERMANENT', 'UNKNOWN'] as const) {
    const state = fake(1000), create = state.io.create;
    let reads = 0;
    const list = state.io.releases;
    state.io.releases = async () => { reads++; return list(); };
    state.io.create = async tag => { await create(tag); throw new CheckpointStorageFailure({
      storageStage: 'RELEASE_CREATE', storageCategory: category,
      httpStatus: category === 'HTTP_PERMANENT' ? 403 : null, retryable: false }); };
    await assert.rejects(confirmFamilyAsset(expected, state.io, undefined, async () => {}), /RELEASE_CREATE/);
    assert.equal(reads, 1, 'permanent create failure has no visibility polling');
    assert.deepEqual(state.created, [physicalReleaseTag(2)]); assert.deepEqual(state.uploads, []);
  }
});

test('abort during shard visibility wait prevents further reads and all uploads', async () => {
  const state = fake(1000), read = state.io.releases, controller = new AbortController();
  let reads = 0;
  state.io.releases = async () => { reads++; const releases = await read();
    return state.created.length ? releases.filter(item => item.ordinal === 1) : releases; };
  await assert.rejects(confirmFamilyAsset(expected, state.io, controller.signal, async () => { controller.abort();
    controller.signal.throwIfAborted(); }));
  assert.equal(reads, 2, 'initial and immediate confirmation reads only');
  assert.deepEqual(state.created, [physicalReleaseTag(2)]); assert.deepEqual(state.uploads, []);
});

test('family inventory rejects 1001 in one release, duplicate names across shards, and malformed slots', async () => {
  await assert.rejects(familyInventory(fake(1001).io), /REMOTE_INVALID/);
  const part: PhysicalRelease = { id: 2, tag: physicalReleaseTag(2), ordinal: 2 };
  const duplicated = fake(1, [{ release: part, assets: [{ id: 1002, name: 'old-1.tar.gz', size: 1, state: 'uploaded' }] }]);
  await assert.rejects(familyInventory(duplicated.io), /IDENTITY_CONFLICT/);
  const sameId = fake(1, [{ release: part, assets: [{ id: 1, name: 'different.tar.gz', size: 1, state: 'uploaded' }] }]);
  await assert.rejects(familyInventory(sameId.io), /IDENTITY_CONFLICT/);
  const invalid = fake(1); invalid.rows.get(1)![0].size = -1;
  await assert.rejects(familyInventory(invalid.io), /IDENTITY_CONFLICT/);
  const unknown = fake(1); (unknown.rows.get(1)![0] as { state: string }).state = 'pending';
  await assert.rejects(familyInventory(unknown.io), /IDENTITY_CONFLICT/);
});

test('same-name starter occupies a slot but can never confirm an archive', async () => {
  const state = fake(0);
  state.rows.get(1)!.push({ ...match(77), state: 'starter' });
  assert.equal((await familyInventory(state.io)).slots.get(1), 1);
  await assert.rejects(confirmFamilyAsset(expected, state.io), /REMOTE_INVALID/);
  assert.deepEqual(state.uploads, []); assert.deepEqual(state.created, []);
});

test('409/422 after the selected release fills cannot switch shards or create another upload', async () => {
  for (const status of [409, 422]) {
    const state = fake(999);
    state.io.upload = async release => {
      state.uploads.push(release.id);
      state.rows.get(1)!.push({ id: 1000, name: 'other-writer.tar.gz', size: 1, state: 'uploaded' });
      throw new CheckpointStorageFailure({ storageStage: 'ASSET_UPLOAD', storageCategory: 'HTTP_PERMANENT',
        httpStatus: status, retryable: false });
    };
    await assert.rejects(confirmFamilyAsset(expected, state.io), error => {
      assert.ok(error instanceof CheckpointStorageFailure);
      assert.equal(error.diagnostic.httpStatus, status); return true;
    });
    assert.deepEqual(state.uploads, [1]); assert.deepEqual(state.created, []);
  }
});

test('unknown upload result stays pinned and never rotates or clobbers a different shard', async () => {
  const state = fake(999);
  state.io.upload = async release => { state.uploads.push(release.id); throw new CheckpointStorageFailure({
    storageStage: 'ASSET_UPLOAD', storageCategory: 'TIMEOUT', httpStatus: null, retryable: true }); };
  await assert.rejects(confirmFamilyAsset(expected, state.io), /TIMEOUT/);
  assert.deepEqual(state.uploads, [1, 1, 1]); assert.deepEqual(state.created, []);
});

test('abort prevents family traversal or mutation', async () => {
  const state = fake(1000), controller = new AbortController(); controller.abort();
  await assert.rejects(confirmFamilyAsset(expected, state.io, controller.signal));
  assert.deepEqual(state.created, []); assert.deepEqual(state.uploads, []);
});

test('GitHub adapter paginates full release and asset inventories including zero-byte slots', async () => {
  const unrelated = Array.from({ length: 99 }, (_, index) => ({ id: 10000 + index, tag_name: `unrelated-${index}`, draft: false }));
  const releases = [...unrelated, { id: 1, tag_name: physicalReleaseTag(1), draft: true },
    { id: 2, tag_name: physicalReleaseTag(2), draft: true }];
  const releaseCalls: string[] = [];
  const physical = await listReleaseFamily('owner/private', undefined, async args => {
    releaseCalls.push(args[1]);
    const page = Number(/[?&]page=(\d+)/.exec(args[1])?.[1]);
    return releases.slice((page - 1) * 100, page * 100);
  });
  assert.deepEqual(physical.map(item => item.id), [1, 2]); assert.equal(releaseCalls.length, 2);
  const assetRows = old(1000);
  const assetCalls: string[] = [];
  const rows = await releaseAssetsIn('owner/private', physical[0], undefined, async args => {
    assetCalls.push(args[1]); const page = Number(/[?&]page=(\d+)/.exec(args[1])?.[1]);
    return assetRows.slice((page - 1) * 100, page * 100);
  });
  assert.equal(rows.length, 1000); assert.equal(rows[0].size, 0); assert.equal(assetCalls.length, 11);
  assert.equal(rows[0].state, 'starter');
  await assert.rejects(releaseAssetsIn('owner/private', physical[0], undefined, async args => {
    const page = Number(/[?&]page=(\d+)/.exec(args[1])?.[1]);
    return [...assetRows, { id: 1001, name: 'overflow', size: 1, state: 'uploaded' }].slice((page - 1) * 100, page * 100);
  }), /capacity/);
  await assert.rejects(releaseAssetsIn('owner/private', physical[0], undefined, async () => [
    { id: 1, name: 'unknown.tar.gz', size: 1, state: 'pending' },
  ]), /metadata/);
});
