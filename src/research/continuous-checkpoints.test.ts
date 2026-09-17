import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, unlinkSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { prepareContinuousCheckpoint, restoreContinuousCheckpoints } from './continuous-checkpoints.js';
import { RecordingWriter, scanRecording, type ClosedRecordingSegment, type RecordedEvent } from './market-recording.js';
import type { ObservationManifest } from './market-observation.js';
import { defaultSimulationConfig, OrderBookSimulator } from './order-book-simulator.js';
import { replayRecording } from '../report/replay-orderbook.js';

function manifest(runId: string): ObservationManifest {
  return {
    schemaVersion: 1, runId, createdAt: '2026-09-17T09:00:00.000Z', status: 'RECORDING', endpoint: 'sandbox', source: 'exchange',
    instruments: [{ ticker: 'A', instrumentId: 'a', uid: 'a', figi: 'figi-a', classCode: 'TQBR', lot: 10, currency: 'RUB', name: 'A', exchange: 'MOEX', sector: 'test' }],
    intervals: [{ exchange: 'MOEX', type: 'regular_trading_session_main', start: '2026-09-17T06:00:00.000Z', end: '2026-09-17T15:00:00.000Z' }],
    scheduleFetchedAt: '2026-09-17T09:00:00.000Z', settings: { durationMs: 10_000, depth: 20, budgetRub: 4_000,
      commissionRate: .0005, maxBookAgeMs: 2_000, maxFutureSkewMs: 1_000, sampleIntervalMs: 1_000, maxBytes: 1_000_000,
      fsyncEveryMs: 0, subscriptionTimeoutMs: 1_000, heartbeatTimeoutMs: 10_000, maxReconnects: 2, segmentMaxBytes: 430 },
    codeHashes: { recorder: 'a'.repeat(64) }, notes: ['checkpoint test'],
  };
}

function setup(t: test.TestContext) {
  const root = mkdtempSync(path.join(os.tmpdir(), 'continuous-checkpoints-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const runId = 'continuous-run', directory = path.join(root, 'raw'), closed: ClosedRecordingSegment[] = [];
  const writer = new RecordingWriter({ path: path.join(directory, 'events.ndjson'), runId, segmentMaxBytes: 430, fsyncEveryMs: 0,
    onSegmentClosed: segment => closed.push(segment) });
  writer.append('connect_attempt', { attempt: 1 }, 1);
  writer.append('response', { subscription: 'ok', data: 'x'.repeat(80) }, 1);
  writer.append('disconnect', { reason: 'stream_ended' }, 1);
  writer.append('gap', { reason: 'stream_ended' }, 1);
  writer.append('connect_attempt', { attempt: 2 }, 2);
  writer.append('response', { subscription: 'ok', data: 'y'.repeat(80) }, 2);
  writer.append('stop', { reason: 'duration' }, 2);
  const recording = writer.close();
  assert.ok(closed.length > 1);
  const base = manifest(runId), final: ObservationManifest = { ...base, status: 'COMPLETE', recording, capture: { reason: 'duration', epochs: 2, responses: 2 } };
  return { root, directory, closed, base, final, recording };
}

test('checkpoint archives preserve exact segments, reconnect events, and full terminal reconstruction', async t => {
  const { root, directory, closed, base, final, recording } = setup(t);
  const checkpoints: string[] = [];
  let previous: string | null = null;
  for (const segment of closed) {
    const prepared = prepareContinuousCheckpoint(directory, segment, { attempt: 1, protocol: 'test' }, previous,
      path.join(root, `checkpoint-${segment.index}`), base);
    checkpoints.push(prepared.directory); previous = prepared.checkpointHash;
  }
  const restored = await restoreContinuousCheckpoints(checkpoints, path.join(root, 'complete'), final);
  assert.equal(restored.complete, true);
  assert.equal(restored.recording.sha256, recording.sha256);
  assert.equal(restored.recording.events, recording.events);
  const original = await scanRecording(path.join(directory, 'events.ndjson'), undefined, recording.segments);
  const recoveredEvents: RecordedEvent[] = [];
  await scanRecording(path.join(restored.directory, 'events.ndjson'), event => { recoveredEvents.push(event); }, recording.segments);
  assert.equal(original.sha256, restored.recording.sha256);
  assert.deepEqual(recoveredEvents.map(event => [event.kind, event.connectionEpoch]), [
    ['connect_attempt', 1], ['response', 1], ['disconnect', 1], ['gap', 1], ['connect_attempt', 2], ['response', 2], ['stop', 2],
  ]);
  for (const segment of closed) assert.deepEqual(
    readFileSync(path.join(directory, segment.file)),
    readFileSync(path.join(restored.directory, segment.file)),
  );
});

test('checkpoint recovery rejects missing, duplicate, corrupt, and conflicting checkpoints; prefix stays non-complete', async t => {
  const { root, directory, closed, base, final } = setup(t);
  assert.throws(() => prepareContinuousCheckpoint(directory, { ...closed[0], file: '../events.ndjson' }, undefined, null,
    path.join(root, 'unsafe'), base), /unsafe/);
  const one = prepareContinuousCheckpoint(directory, closed[0], { block: 'early' }, null, path.join(root, 'one'), base);
  const partial = await restoreContinuousCheckpoints([one.directory], path.join(root, 'partial'));
  assert.equal(partial.complete, false);
  assert.equal(partial.manifest.status, 'RECORDING');
  assert.equal(partial.recording.hasStop, false);
  assert.equal(readFileSync(path.join(partial.directory, 'events.ndjson')).includes(Buffer.from('"stop"')), false);

  await assert.rejects(restoreContinuousCheckpoints([one.directory, one.directory], path.join(root, 'duplicate')), /chain/);
  unlinkSync(path.join(one.directory, closed[0].file));
  await assert.rejects(restoreContinuousCheckpoints([one.directory], path.join(root, 'missing')), /does not exist/);

  const clean = setup(t);
  const first = prepareContinuousCheckpoint(clean.directory, clean.closed[0], undefined, null, path.join(clean.root, 'first'), clean.base);
  const second = prepareContinuousCheckpoint(clean.directory, clean.closed[1], undefined, first.checkpointHash, path.join(clean.root, 'second'), clean.base);
  const descriptorPath = path.join(second.directory, 'checkpoint.json');
  writeFileSync(descriptorPath, `${readFileSync(descriptorPath, 'utf8')} `);
  await assert.rejects(restoreContinuousCheckpoints([first.directory, second.directory], path.join(clean.root, 'corrupt')), /canonical/);

  const mismatch = setup(t);
  const m1 = prepareContinuousCheckpoint(mismatch.directory, mismatch.closed[0], undefined, null, path.join(mismatch.root, 'm1'), mismatch.base);
  const changed = { ...mismatch.base, settings: { ...mismatch.base.settings, depth: 10 } };
  const m2 = prepareContinuousCheckpoint(mismatch.directory, mismatch.closed[1], undefined, m1.checkpointHash, path.join(mismatch.root, 'm2'), changed);
  await assert.rejects(restoreContinuousCheckpoints([m1.directory, m2.directory], path.join(mismatch.root, 'conflict'), final), /metadata conflicts/);
});

function sandboxBook(at: string, bid: number, ask: number) {
  return { orderbook: { instrumentUid: 'a', figi: 'figi-a', time: at, isConsistent: true, depth: 20, orderBookType: 1,
    bids: [{ price: { units: bid, nano: 0 }, quantity: 100 }], asks: [{ price: { units: ask, nano: 0 }, quantity: 100 }] }, pad: 'x'.repeat(180) };
}

async function simulateAcrossSegments(directory: string, recording: NonNullable<ObservationManifest['recording']>, source: ObservationManifest) {
  const simulator = new OrderBookSimulator(source, defaultSimulationConfig('momentum', {
    warmupMs: 0, latencyMs: 0, closeBeforeEndMs: 0, maxEntries: 1, maxHoldingMs: 99_999,
    takeProfitBps: 99_999, stopLossBps: 99_999, signalHook: () => true,
  }));
  await scanRecording(path.join(directory, 'events.ndjson'), event => { simulator.consume(event); }, recording.segments);
  return simulator.finish();
}

test('restored checkpoints replay identically through coverage and the simulator without a segment-boundary reset', async t => {
  const root = mkdtempSync(path.join(os.tmpdir(), 'continuous-checkpoint-replay-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const directory = path.join(root, 'raw'), runId = 'simulated-continuous', closed: ClosedRecordingSegment[] = [];
  const writer = new RecordingWriter({ path: path.join(directory, 'events.ndjson'), runId, segmentMaxBytes: 2_000, fsyncEveryMs: 0,
    onSegmentClosed: segment => closed.push(segment) });
  const now = new Date(), intervalStart = new Date(now.getTime() - 60_000).toISOString(), intervalEnd = new Date(now.getTime() + 120_000).toISOString();
  const base = manifest(runId);
  base.createdAt = now.toISOString(); base.intervals = [{ exchange: 'MOEX', type: 'regular_trading_session_main', start: intervalStart, end: intervalEnd }];
  base.settings = { ...base.settings, durationMs: 60_000, segmentMaxBytes: 2_000 };
  writer.append('connect_attempt', { attempt: 1 }, 1);
  writer.append('response', { observationOrigin: 'UNARY_GET_TRADING_STATUSES', response: { tradingStatuses: [{ instrumentUid: 'a', tradingStatus: 5 }] }, pad: 'x'.repeat(500) }, 1);
  writer.append('response', {
    subscribeOrderBookResponse: { orderBookSubscriptions: [{ instrumentUid: 'a', depth: 20, orderBookType: 1, subscriptionStatus: 1 }] },
    subscribeTradesResponse: { tradeSource: 1, tradeSubscriptions: [{ instrumentUid: 'a', subscriptionStatus: 1 }] },
    subscribeInfoResponse: { infoSubscriptions: [{ instrumentUid: 'a', subscriptionStatus: 1 }] }, pad: 'x'.repeat(500),
  }, 1);
  writer.append('response', sandboxBook(new Date().toISOString(), 99, 100), 1);
  writer.append('response', sandboxBook(new Date().toISOString(), 109, 110), 1);
  writer.append('stop', { reason: 'duration' }, 1);
  const recording = writer.close();
  assert.ok(recording.segments && closed.length > 2, 'fixture must cross several closed raw segments');
  const final: ObservationManifest = { ...base, status: 'COMPLETE', recording, capture: { reason: 'duration', epochs: 1, responses: 4 } };
  const finalPath = path.join(directory, 'manifest.json'); writeFileSync(finalPath, JSON.stringify(final));

  const checkpoints: string[] = []; let previous: string | null = null;
  for (const segment of closed) {
    const prepared = prepareContinuousCheckpoint(directory, segment, { session: 'main', block: 'early' }, previous,
      path.join(root, `checkpoint-${segment.index}`), base);
    checkpoints.push(prepared.directory); previous = prepared.checkpointHash;
  }
  const recovered = await restoreContinuousCheckpoints(checkpoints, path.join(root, 'restored'), finalPath);
  assert.equal(recovered.complete, true);

  const baselineReport = await replayRecording(directory, path.join(root, 'baseline-replay'));
  const restoredReport = await replayRecording(recovered.directory, path.join(root, 'restored-replay'));
  assert.equal(restoredReport.dataset.datasetHash, baselineReport.dataset.datasetHash);
  assert.deepEqual(restoredReport.dataset.inputs.map(input => input.recordingSha256), baselineReport.dataset.inputs.map(input => input.recordingSha256));
  assert.deepEqual(restoredReport.quality, baselineReport.quality);
  assert.deepEqual(restoredReport.results, baselineReport.results);
  assert.deepEqual(readFileSync(path.join(root, 'baseline-replay', 'fills.json')), readFileSync(path.join(root, 'restored-replay', 'fills.json')));

  const baselineSimulation = await simulateAcrossSegments(directory, recording, final);
  const restoredSimulation = await simulateAcrossSegments(recovered.directory, recording, recovered.manifest);
  assert.equal(baselineSimulation.fills.length, 1, 'signal before a boundary fills from the later raw book');
  assert.deepEqual(restoredSimulation, baselineSimulation);
});
