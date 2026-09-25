import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { CONTINUOUS_CAPTURE_POLICY_HASH, CONTINUOUS_CAPTURE_POLICY_V2_HASH, materializeStudyInputs, reconcileContinuousBlocks } from './market-study.js';
import { STUDY_RELEASE_TAG } from '../research/study-protocol.js';
import { beginStudyAttempt, createStudyLedger, type StudyChunkReceipt } from '../research/study-state.js';
import { replayConfigHash, replaySourceHashes } from './replay-orderbook.js';
import type { ObservationInstrument, ObservationManifest } from '../research/market-observation.js';
import type { RecordedEvent, RecordedEventKind } from '../research/market-recording.js';
const sha = (data: Buffer) => createHash('sha256').update(data).digest('hex');

test('two scientific references restore one verified raw run, without duplicate replay inputs', async t => {
  const root = mkdtempSync(path.join(tmpdir(), 'continuous-reference-test-'));
  const originalPath = process.env.PATH, originalFixture = process.env.CONTINUOUS_TEST_FIXTURE;
  t.after(() => { process.env.PATH = originalPath;
    if (originalFixture === undefined) delete process.env.CONTINUOUS_TEST_FIXTURE; else process.env.CONTINUOUS_TEST_FIXTURE = originalFixture;
    rmSync(root, { recursive: true, force: true }); });
  const raw = path.join(root, 'raw-run'); mkdirSync(raw);
  const manifest = Buffer.from(JSON.stringify({ runId: 'one-run', recording: { sha256: 'a'.repeat(64) } }));
  writeFileSync(path.join(raw, 'manifest.json'), manifest);
  const makeAsset = (directory: string, name: string, id: number) => {
    const archive = path.join(root, name);
    execFileSync('tar', ['-czf', archive, '-C', root, path.basename(directory)]);
    const bytes = readFileSync(archive);
    return { id, name, size: bytes.length, digest: `sha256:${sha(bytes)}`, archive };
  };
  const sourceAsset = makeAsset(raw, 'continuous-block-one.tar.gz', 100);
  const source = { assetId: 100, assetName: sourceAsset.name, assetBytes: sourceAsset.size,
    assetDigest: sourceAsset.digest, archiveSha256: sourceAsset.digest.slice(7) };
  const identity = { runId: 'one-run', manifestHash: sha(manifest), recordingHash: 'a'.repeat(64) };
  const assets = [sourceAsset], receipts: StudyChunkReceipt[] = [];
  for (let index = 1; index <= 2; index++) {
    const view = path.join(root, `view-${index}`); mkdirSync(view);
    const name = `study-2026-09-17-early-0${index}-1-1.tar.gz`;
    writeFileSync(path.join(view, 'continuous-reference.json'), JSON.stringify({ schemaVersion: 1, kind: 'CONTINUOUS_WINDOW',
      acquisitionPolicyHash: index === 1 ? CONTINUOUS_CAPTURE_POLICY_HASH : CONTINUOUS_CAPTURE_POLICY_V2_HASH, source, ...identity }));
    writeFileSync(path.join(view, 'study-chunk-receipt.json'), JSON.stringify(identity));
    const asset = makeAsset(view, name, 100 + index); assets.push(asset);
    receipts.push({ ...identity, assetId: asset.id, assetName: name, assetBytes: asset.size,
      assetDigest: asset.digest, archiveSha256: asset.digest.slice(7) } as StudyChunkReceipt);
  }
  const bin = path.join(root, 'bin'); mkdirSync(bin);
  const shim = path.join(bin, 'gh');
  writeFileSync(shim, `#!${process.execPath}\nconst fs = require('node:fs');\nconst f = JSON.parse(fs.readFileSync(process.env.CONTINUOUS_TEST_FIXTURE));\nconst endpoint = process.argv.find(a => a.startsWith('/repos/'));\nif (/\\/releases\\?/.test(endpoint)) process.stdout.write(JSON.stringify([{id: 9, draft:true, tag_name:f.tag}]));\nelse if (/\\/releases\\/9\\/assets\\?/.test(endpoint)) process.stdout.write(JSON.stringify(f.assets));\nelse { const id = Number(endpoint.split('/').at(-1)); const asset = f.assets.find(a => a.id === id); if(!asset) process.exit(2); fs.appendFileSync(f.log, id+'\\n'); process.stdout.write(fs.readFileSync(asset.archive)); }\n`);
  chmodSync(shim, 0o755);
  const fixture = path.join(root, 'fixture.json'), log = path.join(root, 'downloads.log');
  writeFileSync(fixture, JSON.stringify({ tag: STUDY_RELEASE_TAG, assets, log }));
  process.env.PATH = `${bin}${path.delimiter}${originalPath}`; process.env.CONTINUOUS_TEST_FIXTURE = fixture;
  const workspace = path.join(root, 'restore'); mkdirSync(workspace);
  const directories = await materializeStudyInputs('owner/private', receipts, workspace);
  assert.equal(directories.length, 1);
  assert.deepEqual(readFileSync(path.join(directories[0], 'manifest.json')), manifest);
  assert.equal(readFileSync(log, 'utf8').split('\n').filter(line => line === '100').length, 1, 'source downloaded once for both views');
  const bad = { ...receipts[0], manifestHash: 'b'.repeat(64) };
  const other = path.join(root, 'bad'); mkdirSync(other);
  await assert.rejects(materializeStudyInputs('owner/private', [bad], other), /identity differs/);
});

test('report recovery retries a partial raw block, commits every window, marks completion and then performs no writes', async t => {
  const root = mkdtempSync(path.join(tmpdir(), 'continuous-reconcile-test-'));
  const originalPath = process.env.PATH, originalFixture = process.env.CONTINUOUS_TEST_FIXTURE;
  t.after(() => { process.env.PATH = originalPath;
    if (originalFixture === undefined) delete process.env.CONTINUOUS_TEST_FIXTURE; else process.env.CONTINUOUS_TEST_FIXTURE = originalFixture;
    rmSync(root, { recursive: true, force: true }); });
  const date = '2026-09-14', start = Date.parse(`${date}T06:00:00.000Z`), iso = (offset: number) => new Date(start + offset).toISOString();
  const instruments: ObservationInstrument[] = ['SBER', 'GAZP', 'MAGN', 'VKCO', 'SMLT', 'AFKS'].map((ticker, index) => ({
    ticker, instrumentId: `id-${index}`, uid: `uid-${index}`, figi: `figi-${index}`, classCode: 'TQBR', lot: 1,
    currency: 'RUB', name: ticker, exchange: 'MOEX', sector: 'test',
  }));
  const plans = [{ index: 1, plannedStart: iso(0), plannedEnd: iso(10_000) },
    { index: 2, plannedStart: iso(10_000), plannedEnd: iso(20_000) }];
  const runId = '11111111-1111-4111-8111-111111111111', attemptId = '900:1';
  const frames: Array<[RecordedEventKind, number, unknown]> = [
    ['connect_attempt', 0, {}],
    ['response', 1, { observationOrigin: 'UNARY_GET_TRADING_STATUSES', response: { tradingStatuses: instruments.map(i => ({ instrumentUid: i.uid, tradingStatus: 5 })) } }],
    ['response', 2, {
      subscribeOrderBookResponse: { orderBookSubscriptions: instruments.map(i => ({ instrumentUid: i.uid, depth: 20, orderBookType: 1, subscriptionStatus: 1 })) },
      subscribeTradesResponse: { tradeSource: 1, tradeSubscriptions: instruments.map(i => ({ instrumentUid: i.uid, subscriptionStatus: 1 })) },
      subscribeInfoResponse: { infoSubscriptions: instruments.map(i => ({ instrumentUid: i.uid, subscriptionStatus: 1 })) },
    }],
  ];
  for (let second = 0; second < 20; second++) {
    const at = second * 1_000 + 100;
    for (const instrument of instruments) frames.push(['response', at, { orderbook: { instrumentUid: instrument.uid,
      figi: instrument.figi, time: iso(at), isConsistent: true, depth: 20, orderBookType: 1,
      bids: [{ price: { units: 100, nano: 0 }, quantity: 100 }], asks: [{ price: { units: 101, nano: 0 }, quantity: 100 }] } }]);
    frames.push(['tick', second * 1_000 + 500, {}]);
  }
  frames.push(['stop', 20_000, { reason: 'duration' }]);
  const events: RecordedEvent[] = frames.map(([kind, at, payload], index) => ({ schemaVersion: 1, runId, sequence: index + 1,
    connectionEpoch: 1, receivedAt: iso(at), monotonicOffsetNs: String(at * 1_000_000), kind, payload }));
  const bytes = Buffer.from(events.map(event => JSON.stringify(event)).join('\n') + '\n');
  const raw = path.join(root, 'raw-run'); mkdirSync(raw);
  const manifest: ObservationManifest = { schemaVersion: 1, runId, createdAt: iso(0), completedAt: iso(20_000), status: 'COMPLETE',
    endpoint: 'sandbox', source: 'exchange', instruments, intervals: [{ exchange: 'MOEX', type: 'regular_trading_session_main', start: iso(-1_000), end: iso(21_000) }],
    scheduleFetchedAt: iso(0), codeHashes: {}, notes: [], settings: { durationMs: 20_000, depth: 20, budgetRub: 4_000,
      commissionRate: .0005, maxBookAgeMs: 2_000, maxFutureSkewMs: 1_000, sampleIntervalMs: 1_000, maxBytes: 1_000_000,
      fsyncEveryMs: 1_000, subscriptionTimeoutMs: 10_000, heartbeatTimeoutMs: 20_000, maxReconnects: 5, session: 'main' },
    capture: { reason: 'duration', epochs: 1, responses: events.filter(event => event.kind === 'response').length },
    recording: { events: events.length, bytes: bytes.length, sha256: sha(bytes) } };
  writeFileSync(path.join(raw, 'manifest.json'), JSON.stringify(manifest)); writeFileSync(path.join(raw, 'events.ndjson'), bytes);
  const plan = { sessionDate: date, block: 'early' as const, mainStart: iso(0), mainEnd: iso(20_000), ownedStart: iso(0), ownedEnd: iso(20_000),
    prepareAt: iso(-10_000), captureNotBefore: iso(0), latenessMs: 0, chunks: plans };
  writeFileSync(path.join(raw, 'continuous-block.json'), JSON.stringify({ schemaVersion: 1, kind: 'CONTINUOUS_BLOCK',
    acquisitionPolicyHash: CONTINUOUS_CAPTURE_POLICY_HASH, attemptId, diagnosticOnly: false, plan, phase: 'DEVELOPMENT',
    replayConfigHash: replayConfigHash(), simulatorHashes: replaySourceHashes(process.cwd()), checkpoints: [] }));
  const rawArchive = path.join(root, `continuous-block-${runId}.tar.gz`);
  execFileSync('tar', ['-czf', rawArchive, '-C', root, path.basename(raw)]);
  const rawBytes = readFileSync(rawArchive), assetsDir = path.join(root, 'assets'); mkdirSync(assetsDir);
  const storedRaw = path.join(assetsDir, path.basename(rawArchive)); writeFileSync(storedRaw, rawBytes);
  let ledger = beginStudyAttempt(createStudyLedger(), { runId: '900', runAttempt: 1, sessionDate: date, block: 'early', mode: 'COUNTED', startedAt: iso(0) });
  const operation = { schemaVersion: 1, attemptId, plan, startedAt: iso(0), updatedAt: iso(20_000), state: 'PROCESSING', failure: null,
    captureFormat: 'continuous-v2', checkpointIntervalMs: 300_000, recordingStoppedAt: iso(20_000), parts: [] };
  const fixture = path.join(root, 'fixture.json'), log = path.join(root, 'actions.log');
  const encode = (value: unknown) => Buffer.from(`${JSON.stringify(value, null, 2)}\n`).toString('base64');
  writeFileSync(fixture, JSON.stringify({ root, nextId: 101, failPattern: '-02-', failCount: 3, log,
    assets: [{ id: 100, name: path.basename(rawArchive), size: rawBytes.length, digest: `sha256:${sha(rawBytes)}`, archive: storedRaw }],
    files: { 'study-ledger.json': { content: encode(ledger), sha: 'sha-ledger-0' },
      'operations/900-1.json': { content: encode(operation), sha: 'sha-operation-0' } } }));
  const bin = path.join(root, 'bin'); mkdirSync(bin); const shim = path.join(bin, 'gh');
  writeFileSync(shim, `#!${process.execPath}\nconst fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const file=process.env.CONTINUOUS_TEST_FIXTURE;let f=JSON.parse(fs.readFileSync(file));const a=process.argv.slice(2);const save=()=>fs.writeFileSync(file,JSON.stringify(f));const endpoint=a.find(x=>x.startsWith('/repos/'))||'';
const fail=(n)=>{process.stderr.write('gh: failure (HTTP '+n+')');process.exit(1)};
if(a[0]==='release'&&a[1]==='upload'){const src=a[3],name=path.basename(src);if(f.failCount>0&&name.includes(f.failPattern)){f.failCount--;fs.appendFileSync(f.log,'FAIL '+name+'\\n');save();process.exit(1)}const dst=path.join(f.root,'assets',name);fs.copyFileSync(src,dst);const b=fs.readFileSync(dst),id=f.nextId++;f.assets.push({id,name,size:b.length,digest:'sha256:'+crypto.createHash('sha256').update(b).digest('hex'),archive:dst});fs.appendFileSync(f.log,'UPLOAD '+name+'\\n');save();process.exit(0)}
if(a.includes('Accept: application/octet-stream')){const id=Number(endpoint.split('/').at(-1)),asset=f.assets.find(x=>x.id===id);if(!asset)fail(404);process.stdout.write(fs.readFileSync(asset.archive));process.exit(0)}
if(/\\/releases\\?/.test(endpoint)){process.stdout.write(JSON.stringify([{id:9,draft:true,tag_name:'${STUDY_RELEASE_TAG}'}]));process.exit(0)}
if(/\\/releases\\/9\\/assets\\?/.test(endpoint)){process.stdout.write(JSON.stringify(f.assets.map(({archive,...x})=>x)));process.exit(0)}
if(endpoint==='/repos/owner/private'){process.stdout.write(JSON.stringify({private:true,default_branch:'main'}));process.exit(0)}
if(/\\/git\\/ref\\/heads\\/(observation-state|main)$/.test(endpoint)){process.stdout.write(JSON.stringify({object:{sha:'base'}}));process.exit(0)}
const m=/\\/contents\\/(.+?)(?:\\?ref=.*)?$/.exec(endpoint);if(m){const key=m[1];if(a.includes('PUT')){const input=a[a.indexOf('--input')+1],p=JSON.parse(fs.readFileSync(input));const old=f.files[key];if((old&&p.sha!==old.sha)||(!old&&p.sha))fail(409);const sha='sha-'+(++f.nextId);f.files[key]={content:p.content,sha};fs.appendFileSync(f.log,'PUT '+key+'\\n');save();process.stdout.write('{}');process.exit(0)}const v=f.files[key];if(!v)fail(404);process.stdout.write(JSON.stringify(v));process.exit(0)}
fail(404);\n`);
  chmodSync(shim, 0o755); process.env.PATH = `${bin}${path.delimiter}${originalPath}`; process.env.CONTINUOUS_TEST_FIXTURE = fixture;
  const workspace1 = path.join(root, 'restore-1'); mkdirSync(workspace1);
  const firstResult = await reconcileContinuousBlocks('owner/private', workspace1);
  let state = JSON.parse(readFileSync(fixture, 'utf8'));
  assert.equal(firstResult, 1, JSON.stringify({ assets: state.assets.map((item: { name: string }) => item.name),
    files: Object.keys(state.files), log: readFileSync(log, 'utf8') }));
  assert.equal(state.files['continuous-completed/100.json'], undefined);
  ledger = JSON.parse(Buffer.from(state.files['study-ledger.json'].content, 'base64').toString()); assert.equal(ledger.chunks.length, 1);
  const workspace2 = path.join(root, 'restore-2'); mkdirSync(workspace2);
  assert.equal(await reconcileContinuousBlocks('owner/private', workspace2), 0);
  state = JSON.parse(readFileSync(fixture, 'utf8')); assert.ok(state.files['continuous-completed/100.json']);
  ledger = JSON.parse(Buffer.from(state.files['study-ledger.json'].content, 'base64').toString()); assert.equal(ledger.chunks.length, 2);
  const savedOperation = JSON.parse(Buffer.from(state.files['operations/900-1.json'].content, 'base64').toString());
  assert.equal(savedOperation.state, 'FINISHED'); assert.equal(savedOperation.parts.length, 2);
  const actionsBefore = readFileSync(log, 'utf8');
  const workspace3 = path.join(root, 'restore-3'); mkdirSync(workspace3);
  assert.equal(await reconcileContinuousBlocks('owner/private', workspace3), 0);
  assert.equal(readFileSync(log, 'utf8'), actionsBefore, 'completion marker makes the next recovery a write-free no-op');
});
