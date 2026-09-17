import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appendFileSync, mkdtempSync, readFileSync, symlinkSync, unlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  RecordingWriter,
  scanRecording,
  scanUnfinalizedRecording,
  type RecordedEvent,
  type RecordingSegmentSummary,
} from './market-recording.js';

function temporaryPath(name = 'recording.ndjson'): string {
  return path.join(mkdtempSync(path.join(tmpdir(), 'market-recording-')), name);
}

test('writer is exclusive and scan reproduces event count, bytes and hash', async () => {
  const recordingPath = temporaryPath();
  const writer = new RecordingWriter({ path: recordingPath, runId: 'run-1', fsyncEveryMs: 0 });
  assert.throws(() => new RecordingWriter({ path: recordingPath, runId: 'other' }), /EEXIST/);
  writer.append('connect_attempt', { attempt: 1 }, 1);
  writer.append('response', { trade: { id: 'same', price: 100 } }, 1);
  writer.append('response', { trade: { id: 'same', price: 100 } }, 1);
  writer.append('stop', { reason: 'duration' }, 1);
  assert.throws(() => writer.append('tick', {}, 1), /final event/);
  const written = writer.close();
  assert.deepEqual(writer.close(), written);
  assert.throws(() => writer.append('tick', {}, 1), /closed/);

  const events: RecordedEvent[] = [];
  const scanned = await scanRecording(recordingPath, (event) => { events.push(event); });
  assert.equal(scanned.events, 4);
  assert.equal(scanned.bytes, written.bytes);
  assert.equal(scanned.sha256, written.sha256);
  assert.equal(scanned.sha256, createHash('sha256').update(readFileSync(recordingPath)).digest('hex'));
  assert.equal(scanned.truncatedTail, false);
  assert.equal(scanned.hasStop, true);
  assert.equal(scanned.runId, 'run-1');
  assert.deepEqual(events.map((event) => event.sequence), [1, 2, 3, 4]);
  assert.deepEqual(events[1].payload, events[2].payload);
});

test('scan reports a partial final line but rejects corrupt newline-terminated middle data', async () => {
  const partialPath = temporaryPath('partial.ndjson');
  const writer = new RecordingWriter({ path: partialPath, runId: 'partial' });
  writer.append('response', { value: 1 }, 1);
  writer.close();
  appendFileSync(partialPath, '{"schemaVersion":1,"runId":"partial"');
  const partialEvents: RecordedEvent[] = [];
  const partial = await scanRecording(partialPath, (event) => { partialEvents.push(event); });
  assert.equal(partial.events, 1);
  assert.equal(partialEvents.length, 1);
  assert.equal(partial.truncatedTail, true);
  assert.equal(partial.hasStop, false);

  const corruptPath = temporaryPath('corrupt.ndjson');
  const validLine = `${JSON.stringify(partialEvents[0])}\n`;
  writeFileSync(corruptPath, `${validLine}{"broken":}\n${validLine}`);
  await assert.rejects(scanRecording(corruptPath), /line 2.*invalid JSON/);
});

test('scan validates sequence, run identity and monotonic offsets', async () => {
  const recordingPath = temporaryPath();
  const base = {
    schemaVersion: 1,
    runId: 'run-a',
    connectionEpoch: 1,
    receivedAt: '2026-09-13T10:00:00.000Z',
    kind: 'tick',
    payload: {},
  };
  writeFileSync(recordingPath, [
    JSON.stringify({ ...base, sequence: 1, monotonicOffsetNs: '10' }),
    JSON.stringify({ ...base, runId: 'run-b', sequence: 2, monotonicOffsetNs: '9' }),
    '',
  ].join('\n'));
  await assert.rejects(scanRecording(recordingPath), /runId/);

  writeFileSync(recordingPath, [
    JSON.stringify({ ...base, sequence: 1, monotonicOffsetNs: '10' }),
    JSON.stringify({ ...base, sequence: 3, monotonicOffsetNs: '11' }),
    '',
  ].join('\n'));
  await assert.rejects(scanRecording(recordingPath), /sequence must equal 2/);

  writeFileSync(recordingPath, [
    JSON.stringify({ ...base, sequence: 1, monotonicOffsetNs: '10' }),
    JSON.stringify({ ...base, sequence: 2, monotonicOffsetNs: '9' }),
    '',
  ].join('\n'));
  await assert.rejects(scanRecording(recordingPath), /backwards/);
});

test('maxBytes fails explicitly without silently dropping an event', async () => {
  const recordingPath = temporaryPath();
  const writer = new RecordingWriter({ path: recordingPath, runId: 'bounded', maxBytes: 180 });
  assert.throws(() => writer.append('response', { payload: 'x'.repeat(500) }, 1), /maxBytes/);
  const summary = writer.close();
  assert.deepEqual(summary, {
    events: 0,
    bytes: 0,
    sha256: createHash('sha256').update('').digest('hex'),
  });
  assert.equal((await scanRecording(recordingPath)).events, 0);
});

function writeSegmentedRecording(name = 'events.ndjson') {
  const recordingPath = temporaryPath(name);
  const writer = new RecordingWriter({
    path: recordingPath,
    runId: 'segmented-run',
    maxBytes: 20_000,
    segmentMaxBytes: 400,
    fsyncEveryMs: 0,
  });
  for (let index = 0; index < 5; index += 1) {
    writer.append('response', { index, raw: 'x'.repeat(90) }, 1);
  }
  writer.append('stop', { reason: 'duration' }, 1);
  const summary = writer.close();
  assert.ok(summary.segments);
  assert.ok(summary.segments.length > 1);
  return { recordingPath, summary, segments: summary.segments };
}

function concatenatedHash(recordingPath: string, segments: readonly RecordingSegmentSummary[]): string {
  const directory = path.dirname(recordingPath);
  const hash = createHash('sha256');
  for (const segment of segments) hash.update(readFileSync(path.join(directory, segment.file)));
  return hash.digest('hex');
}

test('segmented writer rolls over without resetting global sequence or losing raw responses', async () => {
  const { recordingPath, summary, segments } = writeSegmentedRecording();
  assert.deepEqual(segments.map((segment) => segment.file), [
    'events.ndjson',
    'events-000002.ndjson',
    'events-000003.ndjson',
    'events-000004.ndjson',
    'events-000005.ndjson',
    'events-000006.ndjson',
  ]);
  assert.equal(segments.reduce((total, segment) => total + segment.events, 0), summary.events);
  assert.equal(segments.reduce((total, segment) => total + segment.bytes, 0), summary.bytes);
  assert.equal(concatenatedHash(recordingPath, segments), summary.sha256);

  const events: RecordedEvent[] = [];
  const scanned = await scanRecording(recordingPath, (event) => { events.push(event); }, segments);
  assert.equal(scanned.events, summary.events);
  assert.equal(scanned.bytes, summary.bytes);
  assert.equal(scanned.sha256, summary.sha256);
  assert.deepEqual(events.map((event) => event.sequence), [1, 2, 3, 4, 5, 6]);
  assert.deepEqual(events.filter((event) => event.kind === 'response').map((event) => event.payload), [
    { index: 0, raw: 'x'.repeat(90) },
    { index: 1, raw: 'x'.repeat(90) },
    { index: 2, raw: 'x'.repeat(90) },
    { index: 3, raw: 'x'.repeat(90) },
    { index: 4, raw: 'x'.repeat(90) },
  ]);
  assert.equal(scanned.hasStop, true);
  assert.equal(scanned.truncatedTail, false);
});

test('segmented scan rejects reordered, missing, omitted, tampered and unsafe segments', async () => {
  const { recordingPath, segments } = writeSegmentedRecording();
  const reordered = [...segments];
  [reordered[0], reordered[1]] = [reordered[1], reordered[0]];
  await assert.rejects(scanRecording(recordingPath, undefined, reordered), /out-of-order filename/);
  await assert.rejects(scanRecording(recordingPath, undefined, segments.slice(0, -1)), /omits/);
  await assert.rejects(scanRecording(recordingPath, undefined, [
    { ...segments[0], file: '../events.ndjson' },
    ...segments.slice(1),
  ]), /unsafe/);
  await assert.rejects(scanRecording(recordingPath, undefined, [
    { ...segments[0], sha256: '0'.repeat(64) },
    ...segments.slice(1),
  ]), /does not match/);

  const missingPath = path.join(path.dirname(recordingPath), segments.at(-1)!.file);
  unlinkSync(missingPath);
  await assert.rejects(scanRecording(recordingPath, undefined, segments), /does not exist/);

  const corrupt = writeSegmentedRecording('corrupt.ndjson');
  const middle = corrupt.segments[1];
  writeFileSync(path.join(path.dirname(corrupt.recordingPath), middle.file), '{"broken":}\n');
  await assert.rejects(scanRecording(corrupt.recordingPath, undefined, corrupt.segments), /invalid JSON/);
});

test('segmented scan permits a partial tail only in the final declared segment', async () => {
  const { recordingPath, summary, segments } = writeSegmentedRecording();
  const directory = path.dirname(recordingPath);
  const lastPath = path.join(directory, segments.at(-1)!.file);
  appendFileSync(lastPath, '{"schemaVersion":1');
  const finalBytes = readFileSync(lastPath);
  const adjusted = segments.map((segment, index) => index === segments.length - 1 ? {
    ...segment,
    bytes: finalBytes.length,
    sha256: createHash('sha256').update(finalBytes).digest('hex'),
  } : segment);
  const scanned = await scanRecording(recordingPath, undefined, adjusted);
  assert.equal(scanned.events, summary.events);
  assert.equal(scanned.truncatedTail, true);
  assert.equal(scanned.hasStop, true);

  const earlier = writeSegmentedRecording('earlier.ndjson');
  const firstPath = path.join(path.dirname(earlier.recordingPath), earlier.segments[0].file);
  appendFileSync(firstPath, '{"partial":');
  const firstBytes = readFileSync(firstPath);
  const earlierAdjusted = earlier.segments.map((segment, index) => index === 0 ? {
    ...segment,
    bytes: firstBytes.length,
    sha256: createHash('sha256').update(firstBytes).digest('hex'),
  } : segment);
  await assert.rejects(scanRecording(earlier.recordingPath, undefined, earlierAdjusted), /partial tail before the final/);
});

test('rotation collision preserves the existing next segment and reports completed segments', () => {
  const recordingPath = temporaryPath('collision.ndjson');
  const writer = new RecordingWriter({ path: recordingPath, runId: 'collision', segmentMaxBytes: 400 });
  writer.append('response', { raw: 'x'.repeat(90) }, 1);
  const collisionPath = path.join(path.dirname(recordingPath), 'collision-000002.ndjson');
  writeFileSync(collisionPath, 'existing-owner');
  assert.throws(() => writer.append('response', { raw: 'x'.repeat(90) }, 1), /EEXIST/);
  assert.equal(readFileSync(collisionPath, 'utf8'), 'existing-owner');
  const summary = writer.close();
  assert.equal(summary.events, 1);
  assert.equal(summary.segments?.length, 1);
  assert.equal(summary.segments?.[0].file, 'collision.ndjson');
});

test('maxBytes remains a total cap across segments and an oversize segment event is rejected', async () => {
  const recordingPath = temporaryPath('total.ndjson');
  const writer = new RecordingWriter({
    path: recordingPath,
    runId: 'total-cap',
    maxBytes: 650,
    segmentMaxBytes: 300,
  });
  let accepted = 0;
  while (true) {
    try {
      writer.append('response', { accepted, raw: 'x'.repeat(20) }, 1);
      accepted += 1;
    } catch (error) {
      assert.match(String(error), /maxBytes/);
      break;
    }
  }
  const summary = writer.close();
  assert.equal(summary.events, accepted);
  assert.ok(summary.bytes <= 650);
  assert.ok(summary.segments && summary.segments.length > 1);
  const scanned = await scanRecording(recordingPath, undefined, summary.segments);
  assert.equal(scanned.events, accepted);
  assert.equal(scanned.hasStop, false);

  const oversizePath = temporaryPath('oversize.ndjson');
  const oversize = new RecordingWriter({ path: oversizePath, runId: 'oversize', segmentMaxBytes: 180 });
  assert.throws(() => oversize.append('response', { raw: 'x'.repeat(500) }, 1), /segmentMaxBytes/);
  assert.equal(oversize.close().events, 0);
});

function writeUnfinalizedRecording(name = 'events.ndjson') {
  const recordingPath = temporaryPath(name);
  const writer = new RecordingWriter({
    path: recordingPath,
    runId: 'crash-run',
    segmentMaxBytes: 400,
    fsyncEveryMs: 0,
  });
  for (let index = 0; index < 4; index += 1) {
    writer.append('response', { index, raw: 'x'.repeat(90) }, 2);
  }
  writer.close();
  return recordingPath;
}

test('unfinalized recovery discovers every contiguous crash-left segment', async () => {
  const recordingPath = writeUnfinalizedRecording();
  const events: RecordedEvent[] = [];
  const recovered = await scanUnfinalizedRecording(recordingPath, (event) => { events.push(event); });
  assert.equal(recovered.events, 4);
  assert.equal(recovered.hasStop, false);
  assert.equal(recovered.truncatedTail, false);
  assert.equal(recovered.runId, 'crash-run');
  assert.ok(recovered.segments && recovered.segments.length >= 2);
  assert.deepEqual(events.map((event) => event.sequence), [1, 2, 3, 4]);
  assert.equal(concatenatedHash(recordingPath, recovered.segments), recovered.sha256);

  const onePath = temporaryPath('one-segment.ndjson');
  const oneWriter = new RecordingWriter({ path: onePath, runId: 'one', segmentMaxBytes: 10_000 });
  oneWriter.append('response', { value: 1 }, 1);
  oneWriter.close();
  assert.deepEqual((await scanUnfinalizedRecording(onePath)).segments?.map((segment) => segment.file), [
    'one-segment.ndjson',
  ]);
});

test('unfinalized recovery rejects gaps and path escapes, and permits only a final partial tail', async () => {
  const partialPath = writeUnfinalizedRecording('partial-recovery.ndjson');
  const initiallyRecovered = await scanUnfinalizedRecording(partialPath);
  const last = initiallyRecovered.segments!.at(-1)!;
  appendFileSync(path.join(path.dirname(partialPath), last.file), '{"partial":');
  const partial = await scanUnfinalizedRecording(partialPath);
  assert.equal(partial.events, 4);
  assert.equal(partial.truncatedTail, true);

  const gapPath = writeUnfinalizedRecording('gap.ndjson');
  const gapInventory = await scanUnfinalizedRecording(gapPath);
  assert.ok(gapInventory.segments && gapInventory.segments.length >= 3);
  unlinkSync(path.join(path.dirname(gapPath), gapInventory.segments[1].file));
  await assert.rejects(scanUnfinalizedRecording(gapPath), /sequence has a gap/);

  const escapePath = temporaryPath('escape.ndjson');
  const escapeWriter = new RecordingWriter({ path: escapePath, runId: 'escape' });
  escapeWriter.append('response', { value: 1 }, 1);
  escapeWriter.close();
  const outsidePath = temporaryPath('outside.ndjson');
  writeFileSync(outsidePath, 'outside');
  symlinkSync(outsidePath, path.join(path.dirname(escapePath), 'escape-000002.ndjson'));
  await assert.rejects(scanUnfinalizedRecording(escapePath), /escapes its recording directory/);
});

test('unfinalized recovery rejects segment inventory changes during its streaming scan', async () => {
  const recordingPath = writeUnfinalizedRecording('moving.ndjson');
  let created = false;
  await assert.rejects(scanUnfinalizedRecording(recordingPath, () => {
    if (created) return;
    created = true;
    writeFileSync(path.join(path.dirname(recordingPath), 'moving-000005.ndjson'), '');
  }), /inventory changed/);
});

test('scan rejects an unterminated line before buffering it without bound', async () => {
  const recordingPath = temporaryPath();
  writeFileSync(recordingPath, Buffer.alloc(4 * 1024 * 1024 + 1, 0x78));
  await assert.rejects(scanRecording(recordingPath), /line 1 exceeds/);
});

test('temporal checkpoints retain exact continuous raw events and publish only immutable nonempty files', async t => {
  let monotonic = 0n;
  t.mock.method(process.hrtime, 'bigint', () => monotonic);
  const recordingPath = temporaryPath('temporal.ndjson');
  const closed: import('./market-recording.js').ClosedRecordingSegment[] = [];
  const capturedBytes: Buffer[] = [];
  const writer = new RecordingWriter({ path: recordingPath, runId: 'continuous', checkpointIntervalMs: 100,
    onSegmentClosed: segment => {
      assert.equal(Object.isFrozen(segment), true);
      closed.push(segment);
      capturedBytes.push(readFileSync(path.join(path.dirname(recordingPath), segment.file)));
    } });
  writer.append('connect_attempt', { attempt: 1 }, 1);
  monotonic = 99_000_000n; writer.append('response', { ack: true }, 1);
  assert.equal(closed.length, 0);
  monotonic = 100_000_000n; writer.append('tick', {}, 1);
  monotonic = 250_000_000n; writer.append('response', { book: 'fresh' }, 1);
  writer.append('stop', { reason: 'duration' }, 1);
  const summary = writer.close();
  assert.equal(closed.length, 3);
  assert.deepEqual(closed.map(segment => [segment.firstSequence, segment.lastSequence]), [[1, 2], [3, 3], [4, 5]]);
  const events: RecordedEvent[] = [];
  const scanned = await scanRecording(recordingPath, event => { events.push(event); }, summary.segments);
  assert.equal(scanned.sha256, summary.sha256);
  assert.equal(createHash('sha256').update(Buffer.concat(capturedBytes)).digest('hex'), summary.sha256);
  assert.deepEqual(events.map(event => event.kind), ['connect_attempt', 'response', 'tick', 'response', 'stop']);
  assert.deepEqual(events.map(event => event.connectionEpoch), [1, 1, 1, 1, 1]);
  for (let index = 0; index < closed.length; index += 1) {
    assert.deepEqual(readFileSync(path.join(path.dirname(recordingPath), closed[index].file)), capturedBytes[index]);
  }
});

test('an empty recording emits no phantom checkpoint and a callback failure retains durable bytes', () => {
  const emptyPath = temporaryPath('empty-checkpoint.ndjson');
  const empty = new RecordingWriter({ path: emptyPath, runId: 'empty', checkpointIntervalMs: 10,
    onSegmentClosed: () => assert.fail('Empty checkpoint was emitted') });
  assert.equal(empty.close().events, 0);
  const recordingPath = temporaryPath('failed-checkpoint.ndjson');
  const writer = new RecordingWriter({ path: recordingPath, runId: 'failure', segmentMaxBytes: 400,
    onSegmentClosed: () => { throw new Error('storage failed'); } });
  writer.append('response', { raw: 'x'.repeat(90) }, 1);
  assert.throws(() => writer.append('response', { raw: 'x'.repeat(90) }, 1), /storage failed/);
  const summary = writer.close();
  assert.equal(summary.events, 1);
  assert.equal(summary.segments?.length, 1);
  assert.equal(summary.bytes, readFileSync(recordingPath).length);
});
