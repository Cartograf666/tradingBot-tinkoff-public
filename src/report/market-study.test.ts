import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { assessStudyChunk, diagnosticStopAt, ensureStateBranch, findDraftRelease, ledgerReadme, runAfterBlockCheck, sandboxDiscoveryFailure, studyBlockRecorded, studyChunkRecorded, waitUntil } from './market-study.js';
import { boundedCaptureDuration } from './record-market.js';
import { createStudyLedger, type StudyChunkReceipt, type StudyDayReceipt } from '../research/study-state.js';
import { planStudyBlock, STUDY_RELEASE_TAG } from '../research/study-protocol.js';

function http(status: number): Error & { stderr: Buffer } {
  return Object.assign(new Error(`HTTP ${status}`), { stderr: Buffer.from(`gh: failure (HTTP ${status})`) });
}

test('diagnostic morning capture is finite, stops before afternoon preparation and requires an open session', () => {
  const start = '2026-09-15T06:00:00Z', end = '2026-09-15T15:54:59Z';
  assert.equal(diagnosticStopAt(Date.parse('2026-09-15T08:17:00Z'), start, end), Date.parse('2026-09-15T10:50:00Z'));
  assert.equal(diagnosticStopAt(Date.parse('2026-09-15T08:17:00Z'), start, '2026-09-15T09:00:00Z'), Date.parse('2026-09-15T09:00:00Z'));
  for (const now of ['2026-09-15T05:59:00Z', '2026-09-15T10:49:00Z', '2026-09-15T10:50:00Z', '2026-09-15T16:00:00Z']) {
    assert.throws(() => diagnosticStopAt(Date.parse(now), start, end));
  }
});

test('metadata preparation cannot push the market stream past its absolute deadline', () => {
  assert.equal(boundedCaptureDuration(1_800_000, 1_000), 1_800_000);
  assert.equal(boundedCaptureDuration(1_800_000, 5_000, 65_000), 60_000);
  assert.equal(boundedCaptureDuration(60_000, 5_000, 200_000), 60_000);
  assert.throws(() => boundedCaptureDuration(60_000, 65_000, 65_000));
  assert.throws(() => boundedCaptureDuration(60_000, 66_000, 65_000));
  assert.throws(() => boundedCaptureDuration(60_000, 1_000, NaN));
});

test('archive lookup finds an authenticated draft beyond the first release page without by-tag lookup', async () => {
  const calls: string[][] = [];
  const result = await findDraftRelease('owner/private', async args => {
    calls.push(args);
    assert.doesNotMatch(args[1], /\/tags\//);
    return args[1].endsWith('page=1') ? Array.from({ length: 100 }, (_, id) => ({ id: id + 1, tag_name: `other-${id}` }))
      : [{ id: 123, tag_name: STUDY_RELEASE_TAG, draft: true }];
  });
  assert.deepEqual(result, { id: 123 });
  assert.equal(calls.length, 2);
  assert.equal(await findDraftRelease('owner/private', async () => []), null);
});

test('archive lookup rejects published, malformed or ambiguous archive destinations', async () => {
  const draft = { id: 123, tag_name: STUDY_RELEASE_TAG, draft: true };
  for (const releases of [[{ ...draft, draft: false }], [{ ...draft, id: 0 }], [draft, { ...draft, id: 124 }], {}]) {
    await assert.rejects(findDraftRelease('owner/private', async () => releases));
  }
  await assert.rejects(findDraftRelease('owner/private', async () => { throw http(403); }), /HTTP 403/);
});

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

test('retry skips only confirmed canonical chunks with the same date, block and planned bounds', () => {
  const plan = planStudyBlock('2026-09-15', '2026-09-15T06:00:00Z', '2026-09-15T15:54:59Z', 'early', Date.parse('2026-09-15T05:50:00Z'))!;
  const ledger = createStudyLedger();
  for (const chunk of plan.chunks) {
    ledger.chunks.push({ assetId: chunk.index, sessionDate: plan.sessionDate, block: plan.block,
      chunkIndex: chunk.index, plannedStart: chunk.plannedStart, plannedEnd: chunk.plannedEnd } as StudyChunkReceipt);
    ledger.canonicalChunks[`${plan.sessionDate}:early:${chunk.index}`] = chunk.index;
  }
  assert.equal(studyBlockRecorded(ledger, plan), true);
  delete ledger.canonicalChunks[`${plan.sessionDate}:early:2`];
  assert.equal(studyBlockRecorded(ledger, plan), false);
  assert.equal(studyChunkRecorded(ledger, plan, plan.chunks[0]), true);
  assert.equal(studyChunkRecorded(ledger, plan, plan.chunks[1]), false);
  assert.equal(studyBlockRecorded(ledger, { ...plan, sessionDate: '2026-09-16' }), false);
  assert.equal(studyBlockRecorded(ledger, { ...plan, block: 'late' }), false);
  ledger.chunks[0].plannedStart = '2026-09-15T06:00:01Z';
  assert.equal(studyChunkRecorded(ledger, plan, plan.chunks[0]), false);
});

test('every retry schedule and manual capture map to the same block and concurrency group', () => {
  const workflow = readFileSync(path.resolve('.github/workflows/market-study.yml'), 'utf8');
  const groupExpression = /group: market-study-\$\{\{ (.*?) \}\}/.exec(workflow)![1];
  const blockExpression = /STUDY_BLOCK: \$\{\{ (.*?) \}\}/.exec(workflow.split('  campaign:')[1])![1];
  // These workflow expressions use only JS-compatible property access, ==, && and ||.
  const evaluate = (expression: string, event: string, schedule: string, mode: string, block: string) =>
    Function('github', 'inputs', `return (${expression});`)({ event_name: event, event: { schedule } }, { mode, block });
  for (const [schedule, block] of [
    ['50,55 5 * * 1-5', 'early'], ['0,5,10,15 6 * * 1-5', 'early'],
    ['50,55 10 * * 1-5', 'late'], ['0,5,10,15 11 * * 1-5', 'late'],
  ]) {
    assert.ok(workflow.includes(`cron: '${schedule}'`));
    assert.equal(evaluate(groupExpression, 'schedule', schedule, '', ''), block);
    assert.equal(evaluate(blockExpression, 'schedule', schedule, '', ''), block);
  }
  for (const block of ['early', 'late']) {
    assert.equal(evaluate(groupExpression, 'workflow_dispatch', '', 'campaign', block), block);
    assert.equal(evaluate(groupExpression, 'workflow_dispatch', '', 'arm', block), block);
    assert.equal(evaluate(blockExpression, 'workflow_dispatch', '', 'campaign', block), block);
  }
  assert.equal(evaluate(groupExpression, 'workflow_dispatch', '', 'smoke', 'early'), 'smoke');
  assert.equal(evaluate(groupExpression, 'workflow_dispatch', '', 'observe', 'early'), 'observe');
});

test('prepared capture cannot run after preparation fails or is cancelled, while regular capture tolerates skipped preparation', () => {
  const workflow = readFileSync(path.resolve('.github/workflows/market-study.yml'), 'utf8');
  const expression = /  campaign:\n    needs: prepare\n    if: >-\n([\s\S]*?)    runs-on:/.exec(workflow)![1].trim();
  const evaluate = (event: string, mode: string, result: string, cancelled: boolean, enabled = 'true', trusted = true) =>
    Function('github', 'inputs', 'needs', 'vars', 'cancelled', 'format', `return (${expression});`)(
      { event_name: event, event: { repository: { private: false, default_branch: 'main' } }, ref: trusted ? 'refs/heads/main' : 'refs/pull/1/merge' },
      { mode }, { prepare: { result } }, { MARKET_STUDY_ENABLED: enabled }, () => cancelled, (_: string, value: string) => `refs/heads/${value}`);
  assert.equal(evaluate('schedule', '', 'skipped', false), true);
  assert.equal(evaluate('workflow_dispatch', 'campaign', 'skipped', false), true);
  assert.equal(evaluate('workflow_dispatch', 'arm', 'success', false), true);
  for (const result of ['failure', 'cancelled', 'skipped']) assert.equal(evaluate('workflow_dispatch', 'arm', result, false), false);
  assert.equal(evaluate('workflow_dispatch', 'arm', 'success', true), false);
  assert.equal(evaluate('workflow_dispatch', 'arm', 'success', false, 'false'), false);
  assert.equal(evaluate('workflow_dispatch', 'arm', 'success', false, 'true', false), false);
  assert.doesNotMatch(workflow.split('  prepare:')[1].split('  campaign:')[0], /secrets\./);
});

test('workflow keeps schedules activation-gated and action versions immutable', () => {
  const workflow = readFileSync(path.resolve('.github/workflows/market-study.yml'), 'utf8');
  assert.match(workflow, /github\.event_name == 'schedule' && vars\.MARKET_STUDY_ENABLED == 'true'/);
  assert.match(workflow, /cancel-in-progress: false/);
  assert.match(workflow, /actions\/checkout@11d5960a326750d5838078e36cf38b85af677262/);
  assert.match(workflow, /actions\/setup-node@49933ea5288caeca8642d1e84afbd3f7d6820020/);
  assert.doesNotMatch(workflow, /TINKOFF_API_TOKEN_PROD/);
});
