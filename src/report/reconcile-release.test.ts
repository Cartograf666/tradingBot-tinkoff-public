import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { migrateRemoteStudyAttemptBudget, reconcileReleaseAssets } from './market-study.js';
import { acceptStudyChunk, acceptStudyDay, beginStudyAttempt, createStudyLedger, migrateStudyAttemptBudget,
  type StudyChunkReceipt, type StudyDayReceipt, type StudyLedger } from '../research/study-state.js';
import { hashStudyValue, planStudyBlock, STUDY_60_ATTEMPT_PROTOCOL_HASH, STUDY_PROTOCOL_HASH } from '../research/study-protocol.js';
import { selectStudyDayInputs, studyDayInputHash } from '../research/study-day-inputs.js';

const date = '2026-09-14', hex = (digit: string) => digit.repeat(64);
const bounds = { sessionDate: date, mainStart: `${date}T06:00:00.000Z`, mainEnd: `${date}T15:55:00.000Z` };
function ledgerWithFullDay(): StudyLedger {
  let ledger = createStudyLedger();
  for (const [block, run] of [['early', 1], ['late', 2]] as const) {
    ledger = beginStudyAttempt(ledger, { runId: String(run), runAttempt: 1, sessionDate: date,
      block, mode: 'COUNTED', startedAt: `${date}T05:50:00.000Z` });
    const plan = planStudyBlock(date, bounds.mainStart, bounds.mainEnd, block,
      Date.parse(`${date}T${block === 'early' ? '05' : '10'}:50:00.000Z`))!;
    for (const part of plan.chunks) {
      const assetId = block === 'early' ? part.index : part.index + 10;
      ledger = acceptStudyChunk(ledger, { schemaVersion: 1, protocolHash: STUDY_PROTOCOL_HASH, assetId,
        assetName: `existing-${assetId}.tar.gz`, assetDigest: `sha256:${hex('a')}`, assetBytes: 10,
        archiveSha256: hex('b'), releaseTag: 'market-study-archive-v1', attemptId: `${run}:1`,
        chunkId: `${date}:${block}:${part.index}`, sessionDate: date, phase: 'DEVELOPMENT', block,
        chunkIndex: part.index, plannedStart: part.plannedStart, plannedEnd: part.plannedEnd,
        runId: `raw-${run}`, manifestHash: hex('c'), recordingHash: hex('d'), replayConfigHash: hex('e'),
        simulatorHashes: { source: hex('f') }, quality: block === 'late' && part.index === 1 ? 'INSUFFICIENT_DATA' : 'PASS',
        uploadedAt: `${date}T12:00:00.000Z` });
    }
  }
  return beginStudyAttempt(ledger, { runId: '3', runAttempt: 1, sessionDate: date,
    block: 'late', mode: 'COUNTED', startedAt: `${date}T11:05:00.000Z` });
}
function retry(ledger: StudyLedger): StudyChunkReceipt {
  return { ...ledger.chunks.find(item => item.chunkId === `${date}:late:1`)!,
    assetId: 30, assetName: `study-${date}-late-01-retry.tar.gz`, attemptId: '3:1',
    quality: 'PASS', uploadedAt: `${date}T15:58:00.000Z` };
}
function reportDraft(ledger: StudyLedger) {
  const receipts = selectStudyDayInputs(ledger, bounds).receipts;
  return { schemaVersion: 1 as const, protocolHash: STUDY_PROTOCOL_HASH,
    dayId: `${STUDY_PROTOCOL_HASH}:${date}`, sessionDate: date, phase: 'DEVELOPMENT' as const,
    mainStart: bounds.mainStart, mainEnd: bounds.mainEnd, chunkAssetIds: receipts.map(item => item.assetId),
    canonicalInputHash: studyDayInputHash(receipts), replayConfigHash: hex('e'), simulatorHashes: { source: hex('f') },
    quality: { status: 'PASS' as const, recordedShare: .995, expectedTicks: 35_700, observedTicks: 35_520,
      perInstrument: ['SBER', 'GAZP', 'MAGN', 'VKCO', 'SMLT', 'AFKS'].map(ticker => ({ ticker, usableShare: .9 })) },
    results: [], finalizedAt: `${date}T16:00:00.000Z` };
}
type Asset = { id: number; name: string; size: number; state: 'uploaded' | 'starter'; digest: string; archive: string };
function archive(root: string, id: number, name: string, receiptName: string, draft: unknown): Asset {
  const directory = path.join(root, `asset-${id}`); mkdirSync(directory);
  writeFileSync(path.join(directory, receiptName), JSON.stringify(draft));
  const file = path.join(root, name); execFileSync('tar', ['-czf', file, '-C', root, path.basename(directory)]);
  const bytes = readFileSync(file);
  return { id, name, size: bytes.length, state: 'uploaded',
    digest: `sha256:${createHash('sha256').update(bytes).digest('hex')}`, archive: file };
}

function fixture(t: TestContext, ledger: StudyLedger, options: {
  day: boolean; retry: boolean; retiredRetry?: boolean; duplicateDay?: boolean; reportSource?: StudyLedger;
  starterDay?: boolean; raceLedger?: StudyLedger; publicRepository?: boolean }) {
  const root = mkdtempSync(path.join(tmpdir(), 'study-release-recovery-'));
  const oldPath = process.env.PATH, oldFixture = process.env.RECOVERY_TEST_FIXTURE;
  t.after(() => { process.env.PATH = oldPath;
    if (oldFixture === undefined) delete process.env.RECOVERY_TEST_FIXTURE;
    else process.env.RECOVERY_TEST_FIXTURE = oldFixture;
    rmSync(root, { recursive: true, force: true }); });
  const assets: Asset[] = [];
  if (options.retry) {
    const { assetId: _id, assetDigest: _digest, assetBytes: _size, archiveSha256: _hash, ...draft } = retry(ledger);
    assets.push(archive(root, 30, `study-${date}-late-01-retry.tar.gz`, 'study-chunk-receipt.json',
      { ...draft, protocolHash: STUDY_PROTOCOL_HASH }));
  }
  if (options.retiredRetry) {
    const { assetId: _id, assetDigest: _digest, assetBytes: _size, archiveSha256: _hash, ...draft } = retry(ledger);
    assets.push(archive(root, 29, `study-${date}-late-01-old-retry.tar.gz`, 'study-chunk-receipt.json',
      { ...draft, protocolHash: STUDY_60_ATTEMPT_PROTOCOL_HASH, assetName: `study-${date}-late-01-old-retry.tar.gz` }));
  }
  if (options.day) assets.push(archive(root, 50, `study-day-${date}-first.tar.gz`, 'study-day-receipt.json', reportDraft(options.reportSource ?? ledger)));
  if (options.starterDay) assets.find(asset => asset.id === 50)!.state = 'starter';
  if (options.duplicateDay) assets.push(archive(root, 51, `study-day-${date}-second.tar.gz`, 'study-day-receipt.json',
    { ...reportDraft(options.reportSource ?? ledger), finalizedAt: `${date}T16:01:00.000Z` }));
  const fixtureFile = path.join(root, 'fixture.json');
  writeFileSync(fixtureFile, JSON.stringify({ assets, serial: 0, raceLedger: options.raceLedger,
    publicRepository: options.publicRepository, files: { 'study-ledger.json': {
    content: Buffer.from(JSON.stringify(ledger)).toString('base64'), sha: 'initial' } } }));
  const bin = path.join(root, 'bin'); mkdirSync(bin);
  const shim = path.join(bin, 'gh');
  writeFileSync(shim, `#!${process.execPath}\nconst fs=require('node:fs');const p=process.env.RECOVERY_TEST_FIXTURE,f=JSON.parse(fs.readFileSync(p));const a=process.argv.slice(2),u=a.find(x=>x.startsWith('/repos/'))||'';const fail=(n)=>{process.stderr.write('gh: failure (HTTP '+n+')');process.exit(1)};
if(u==='/repos/owner/private'){process.stdout.write(JSON.stringify({private:!f.publicRepository,default_branch:'main'}));process.exit(0)}
if(u.includes('/git/ref/heads/')){process.stdout.write(JSON.stringify({object:{sha:'base'}}));process.exit(0)}
if(u.includes('/releases?')){process.stdout.write(JSON.stringify([{id:9,draft:true,tag_name:'market-study-archive-v1'}]));process.exit(0)}
if(u.includes('/releases/9/assets?')){process.stdout.write(JSON.stringify(f.assets.map(({archive,...x})=>x)));process.exit(0)}
if(a.includes('Accept: application/octet-stream')){const asset=f.assets.find(x=>x.id===Number(u.split('/').at(-1)));if(!asset)fail(404);process.stdout.write(fs.readFileSync(asset.archive));process.exit(0)}
const m=/\\/contents\\/(.+?)(?:\\?ref=.*)?$/.exec(u);if(m){const key=m[1];if(a.includes('PUT')){const input=JSON.parse(fs.readFileSync(a[a.indexOf('--input')+1]));if(key==='study-ledger.json'&&f.raceLedger){f.files[key]={content:Buffer.from(JSON.stringify(f.raceLedger)).toString('base64'),sha:'raced'};delete f.raceLedger;fs.writeFileSync(p,JSON.stringify(f));fail(409)}const previous=f.files[key];if(previous&&input.sha!==previous.sha)fail(409);f.files[key]={content:input.content,sha:'commit-'+(++f.serial)};fs.writeFileSync(p,JSON.stringify(f));process.stdout.write('{}');process.exit(0)}const v=f.files[key];if(!v)fail(404);process.stdout.write(JSON.stringify(v));process.exit(0)}fail(404);\n`);
  chmodSync(shim, 0o755);
  process.env.PATH = `${bin}${path.delimiter}${oldPath}`;
  process.env.RECOVERY_TEST_FIXTURE = fixtureFile;
}

function exhaustedPredecessor(): StudyLedger {
  let ledger = createStudyLedger();
  for (let index = 1; index <= 60; index += 1) ledger = beginStudyAttempt(ledger, {
    runId: String(index), runAttempt: 1, sessionDate: date, block: 'early', mode: 'COUNTED',
    startedAt: `${date}T05:50:00.000Z`,
  });
  ledger.protocolHash = STUDY_60_ATTEMPT_PROTOCOL_HASH;
  return ledger;
}

test('remote migration retries SHA conflict against the latest old ledger, then is idempotent', async t => {
  const old = exhaustedPredecessor(), raced = structuredClone(old);
  raced.attempts[0]!.status = 'FAILED';
  fixture(t, old, { day: false, retry: false, raceLedger: raced });
  const migrated = await migrateRemoteStudyAttemptBudget('owner/private');
  assert.equal(migrated.attempts[0]!.status, 'FAILED');
  assert.equal(migrated.attemptBudgetMigration?.sourceLedgerHash, hashStudyValue(raced));
  const again = await migrateRemoteStudyAttemptBudget('owner/private');
  assert.deepEqual(again, migrated);
  const state = JSON.parse(readFileSync(process.env.RECOVERY_TEST_FIXTURE!, 'utf8'));
  const committed = JSON.parse(Buffer.from(state.files['study-ledger.json'].content, 'base64').toString()) as StudyLedger;
  assert.deepEqual(committed, migrated);
  assert.equal(state.serial, 2, 'one ledger commit plus derived README; retry made no writes');
});

test('migration refuses a public repository before writing state', async t => {
  fixture(t, exhaustedPredecessor(), { day: false, retry: false, publicRepository: true });
  await assert.rejects(migrateRemoteStudyAttemptBudget('owner/private'), /private repository/);
  const state = JSON.parse(readFileSync(process.env.RECOVERY_TEST_FIXTURE!, 'utf8'));
  assert.equal(state.serial, 0);
});

test('recovery processes a confirmed PASS retry before an obsolete uploaded day report', async t => {
  fixture(t, ledgerWithFullDay(), { day: true, retry: true });
  const recovered = await reconcileReleaseAssets('owner/private');
  assert.equal(recovered.days.length, 0);
  assert.equal(recovered.chunks.length, 21);
  assert.equal(recovered.canonicalChunks[`${date}:late:1`], 30);
  assert.equal((await reconcileReleaseAssets('owner/private')).chunks.length, 21, 'repeat is idempotent');
});

test('recovery accepts an immutable full-day report when no earlier PASS retry exists', async t => {
  fixture(t, ledgerWithFullDay(), { day: true, retry: false });
  const recovered = await reconcileReleaseAssets('owner/private');
  assert.equal(recovered.days.length, 1);
  assert.equal(recovered.days[0]!.chunkAssetIds.at(10), 11);
  assert.equal((await reconcileReleaseAssets('owner/private')).days.length, 1);
});

test('starter receipt is skipped, never accepted, and does not block other uploaded receipts', async t => {
  fixture(t, ledgerWithFullDay(), { day: true, retry: true, starterDay: true });
  const recovered = await reconcileReleaseAssets('owner/private');
  assert.equal(recovered.chunks.length, 21);
  assert.equal(recovered.canonicalChunks[`${date}:late:1`], 30);
  assert.equal(recovered.days.length, 0);
});

test('retired uploaded receipt is deferred while a new-protocol receipt recovers', async t => {
  let old = ledgerWithFullDay();
  for (let index = 4; index <= 60; index += 1) old = beginStudyAttempt(old, {
    runId: String(index), runAttempt: 1, sessionDate: date, block: 'late',
    mode: 'COUNTED', startedAt: `${date}T11:05:00.000Z`,
  });
  old.protocolHash = STUDY_60_ATTEMPT_PROTOCOL_HASH;
  for (const receipt of old.chunks) receipt.protocolHash = STUDY_60_ATTEMPT_PROTOCOL_HASH;
  const migrated = migrateStudyAttemptBudget(old, '2026-10-05T10:00:00.000Z');
  fixture(t, migrated, { day: false, retry: true, retiredRetry: true });
  const recovered = await reconcileReleaseAssets('owner/private');
  assert.equal(recovered.chunks.length, 21);
  assert.equal(recovered.chunks.at(-1)?.protocolHash, STUDY_PROTOCOL_HASH);
  assert.equal(recovered.chunks.some(receipt => receipt.assetId === 29), false);
  assert.equal(recovered.canonicalChunks[`${date}:late:1`], 30);
});

test('a starter-only day report stays absent from the full-day denominator', async t => {
  fixture(t, ledgerWithFullDay(), { day: true, retry: false, starterDay: true });
  const recovered = await reconcileReleaseAssets('owner/private');
  assert.equal(recovered.chunks.length, 20);
  assert.equal(recovered.days.length, 0);
});

test('two uploaded day reports preserve the first accepted report without poisoning later recovery', async t => {
  fixture(t, ledgerWithFullDay(), { day: true, retry: false, duplicateDay: true });
  const recovered = await reconcileReleaseAssets('owner/private');
  assert.equal(recovered.days.length, 1);
  assert.equal(recovered.days[0]!.reportAssetId, 50);
  assert.equal((await reconcileReleaseAssets('owner/private')).days[0]!.reportAssetId, 50);
});

test('a retry already committed makes the older day report obsolete without poisoning recovery', async t => {
  const initial = ledgerWithFullDay();
  fixture(t, acceptStudyChunk(initial, retry(initial)), { day: true, retry: false, reportSource: initial });
  const recovered = await reconcileReleaseAssets('owner/private');
  assert.equal(recovered.days.length, 0);
  assert.equal(recovered.canonicalChunks[`${date}:late:1`], 30);
  assert.equal((await reconcileReleaseAssets('owner/private')).days.length, 0);
});

test('late confirmed retry after committed day is audited without changing scientific inputs', async t => {
  const base = ledgerWithFullDay(), draft = reportDraft(base);
  const day = { ...draft, reportAssetId: 50, reportAssetName: `study-day-${date}-first.tar.gz`,
    reportAssetDigest: `sha256:${hex('a')}`, reportAssetBytes: 10, reportArchiveSha256: hex('b') } as StudyDayReceipt;
  fixture(t, acceptStudyDay(base, day), { day: false, retry: true });
  const recovered = await reconcileReleaseAssets('owner/private');
  assert.equal(recovered.days.length, 1);
  assert.equal(recovered.chunks.length, 21);
  assert.equal(recovered.canonicalChunks[`${date}:late:1`], undefined);
});
