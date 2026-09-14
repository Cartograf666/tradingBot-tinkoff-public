import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { assessStudyChunk, ensureStateBranch, ledgerReadme, runAfterBlockCheck, sandboxDiscoveryFailure, waitUntil } from './market-study.js';
import { createStudyLedger, type StudyDayReceipt } from '../research/study-state.js';

function http(status: number): Error & { stderr: Buffer } {
  return Object.assign(new Error(`HTTP ${status}`), { stderr: Buffer.from(`gh: failure (HTTP ${status})`) });
}

test('sandbox discovery diagnostics keep only numeric RPC status and fixed labels', () => {
  assert.deepEqual(sandboxDiscoveryFailure({ code: 16, message: 'private-token', details: 'private-response' }),
    { stage: 'sandbox-discovery', grpcCode: 16, reason: 'UNAUTHENTICATED', transport: 'UNCLASSIFIED' });
  assert.deepEqual(sandboxDiscoveryFailure({ code: 'private-token' }),
    { stage: 'sandbox-discovery', grpcCode: null, reason: 'DISCOVERY_FAILED', transport: 'UNCLASSIFIED' });
  assert.equal(sandboxDiscoveryFailure({ code: 14 }).reason, 'UNAVAILABLE');
  assert.equal(sandboxDiscoveryFailure({ code: 14, details: 'Received HTTP status code 403 private-token' }).transport, 'HTTP_403');
  assert.equal(sandboxDiscoveryFailure({ code: 14, details: 'Name resolution failed private-token' }).transport, 'DNS');
  assert.doesNotMatch(JSON.stringify(sandboxDiscoveryFailure({ code: 14, details: 'SSL private-token' })), /private-token/);
});

test('state branch bootstrap creates from exact default SHA and tolerates only a confirmed concurrent race', async () => {
  const calls: string[][] = [];
  let targetReads = 0;
  await ensureStateBranch('owner/private', async args => {
    calls.push(args);
    const endpoint = args.find(value => value.startsWith('/repos/')) ?? '';
    if (endpoint === '/repos/owner/private') return { private: true, default_branch: 'main' };
    if (endpoint.endsWith('/git/ref/heads/observation-state')) {
      targetReads += 1;
      if (targetReads === 1) throw http(404);
      return { object: { sha: 'base' } };
    }
    if (endpoint.endsWith('/git/ref/heads/main')) return { object: { sha: 'exact-default-sha' } };
    if (endpoint.endsWith('/git/refs')) throw http(422);
    throw new Error(`Unexpected call ${args.join(' ')}`);
  });
  const create = calls.find(args => args.includes('/repos/owner/private/git/refs'))!;
  assert.ok(create.includes('sha=exact-default-sha'));
  assert.equal(targetReads, 2, 'a concurrent 422 is accepted only after confirming the target ref');
});

test('branch bootstrap does not reinterpret auth or network failures as a missing branch', async () => {
  await assert.rejects(ensureStateBranch('owner/private', async args => {
    if (args[1] === '/repos/owner/private') return { private: true, default_branch: 'main' };
    throw http(503);
  }), /HTTP 503/);
});

test('short tail chunk can pass, while a frame before its planned boundary fails closed', () => {
  const directory = mkdtempSync(path.join(tmpdir(), 'study-quality-'));
  try {
    writeFileSync(path.join(directory, 'manifest.json'), JSON.stringify({ source: 'exchange',
      capture: { reason: 'duration' }, instruments: ['SBER', 'GAZP', 'MAGN', 'VKCO', 'SMLT', 'AFKS'].map(ticker => ({ ticker })) }));
    const phase = { phase: 'regular_trading_session_main', usableShareOfObservedScheduledTicks: 0.9 };
    writeFileSync(path.join(directory, 'summary.json'), JSON.stringify({ complete: true,
      firstReceivedAt: '2026-09-14T15:50:00.100Z', lastReceivedAt: '2026-09-14T15:54:54.900Z',
      exchangeSamples: 1, allSubscriptionsAcknowledged: true,
      expectedSubscriptions: Array.from({ length: 18 }, (_, index) => `ack-${index}`),
      samplingCoverage: { recordedShare: 0.995 },
      groups: ['SBER', 'GAZP', 'MAGN', 'VKCO', 'SMLT', 'AFKS'].map(ticker => ({ ticker, source: 'EXCHANGE', phases: [phase] })) }));
    const bounds = { plannedStart: '2026-09-14T15:50:00.000Z', plannedEnd: '2026-09-14T15:55:00.000Z' };
    assert.equal(assessStudyChunk(directory, bounds).status, 'PASS');
    const summary = JSON.parse(readFileSync(path.join(directory, 'summary.json'), 'utf8'));
    summary.firstReceivedAt = '2026-09-14T15:49:59.999Z';
    writeFileSync(path.join(directory, 'summary.json'), JSON.stringify(summary));
    const failed = assessStudyChunk(directory, bounds);
    assert.equal(failed.status, 'INSUFFICIENT_DATA');
    assert.equal(failed.checks.plannedBounds, false);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('a failed previous chunk cannot make the next capture start before its fixed boundary', async () => {
  const controller = new AbortController(), started = Date.now();
  await waitUntil(started + 20, controller.signal);
  assert.ok(Date.now() >= started + 15);
});

test('automatic block capture waits for session start AND the completed smoke/archive check', async () => {
  const calls: string[] = [], notBefore = Date.now() + 20;
  let releaseProbe!: () => void, notifyProbe!: () => void;
  const probeStarted = new Promise<void>(resolve => { notifyProbe = resolve; });
  const checkComplete = new Promise<void>(resolve => { releaseProbe = resolve; });
  const work = runAfterBlockCheck(notBefore, new AbortController().signal, async () => {
    assert.ok(Date.now() >= notBefore);
    calls.push('probe'); notifyProbe();
    await checkComplete; calls.push('archive-confirmed');
  }, async () => { calls.push('capture'); return 7; });
  await probeStarted;
  assert.deepEqual(calls, ['probe']);
  releaseProbe();
  assert.equal(await work, 7);
  assert.deepEqual(calls, ['probe', 'archive-confirmed', 'capture']);
});

test('failed quality, replay or archive check prevents all canonical collection', async () => {
  for (const stage of ['quality', 'replay', 'archive']) {
    let captured = false;
    await assert.rejects(runAfterBlockCheck(Date.now(), new AbortController().signal,
      async () => { throw new Error(stage); }, async () => { captured = true; }), new RegExp(stage));
    assert.equal(captured, false);
  }
});

test('cancel before or during automatic smoke cannot continue to canonical collection', async () => {
  for (const abortBefore of [true, false]) {
    const controller = new AbortController();
    let probed = false, captured = false;
    if (abortBefore) controller.abort();
    await assert.rejects(runAfterBlockCheck(Date.now(), controller.signal,
      async () => { probed = true; controller.abort(); }, async () => { captured = true; }), /abort/i);
    assert.equal(probed, !abortBefore);
    assert.equal(captured, false);
  }
});

test('README performance aggregate excludes finalized days that failed the quality gate', () => {
  const ledger = createStudyLedger();
  const base = { schemaVersion: 1, protocolHash: ledger.protocolHash, phase: 'DEVELOPMENT',
    mainStart: '2026-09-14T06:00:00.000Z', mainEnd: '2026-09-14T15:55:00.000Z', chunkAssetIds: [1],
    canonicalInputHash: 'a'.repeat(64), replayConfigHash: 'b'.repeat(64), simulatorHashes: { source: 'c'.repeat(64) },
    reportAssetId: 1, reportAssetName: 'day.tar.gz', reportAssetDigest: `sha256:${'d'.repeat(64)}`,
    reportAssetBytes: 1, reportArchiveSha256: 'e'.repeat(64), finalizedAt: '2026-09-14T16:00:00.000Z' };
  ledger.days = [
    { ...base, dayId: 'pass', sessionDate: '2026-09-14', quality: { status: 'PASS', recordedShare: 1,
      expectedTicks: 1, observedTicks: 1, perInstrument: [] }, results: [{ name: 'baseline', entries: 2, feesRub: 3, unresolvedPositions: 0, netPnlRub: 4 }] },
    { ...base, dayId: 'rejected', sessionDate: '2026-09-15', quality: { status: 'INSUFFICIENT_DATA', recordedShare: .5,
      expectedTicks: 1, observedTicks: 1, perInstrument: [] }, results: [{ name: 'baseline', entries: 1000, feesRub: 1000, unresolvedPositions: 10, netPnlRub: 1000 }] },
  ] as StudyDayReceipt[];
  const markdown = ledgerReadme(ledger, 'owner/private');
  assert.match(markdown, /\| DEVELOPMENT \| baseline \| 1 \| 2 \| 4\.00 \| 3\.00 \| 0 \|/);
  assert.match(markdown, /Rejected finalized days: 1/);
  assert.doesNotMatch(markdown, /1000\.00/);
});

test('workflow keeps schedules activation-gated and action versions immutable', () => {
  const workflow = readFileSync(path.resolve('.github/workflows/market-study.yml'), 'utf8');
  assert.match(workflow, /github\.event_name == 'schedule' && vars\.MARKET_STUDY_ENABLED == 'true'/);
  assert.match(workflow, /github\.event\.schedule == '50 5 \* \* 1-5' && 'early'.*inputs\.mode == 'campaign' && inputs\.block/);
  assert.match(workflow, /cancel-in-progress: false/);
  assert.match(workflow, /actions\/checkout@11d5960a326750d5838078e36cf38b85af677262/);
  assert.match(workflow, /actions\/setup-node@49933ea5288caeca8642d1e84afbd3f7d6820020/);
  assert.doesNotMatch(workflow, /TINKOFF_API_TOKEN_PROD/);
});
