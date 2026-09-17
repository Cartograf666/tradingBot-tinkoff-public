import { createHash } from 'node:crypto';
import { copyFileSync, existsSync, mkdirSync, readFileSync, realpathSync, writeFileSync, constants as fsConstants } from 'node:fs';
import path from 'node:path';
import {
  recordedEventKinds,
  scanRecording,
  type ClosedRecordingSegment,
  type RecordedEvent,
  type RecordingSegmentSummary,
} from './market-recording.js';
import type { ObservationManifest } from './market-observation.js';

const sha256Pattern = /^[0-9a-f]{64}$/;
const decimalInteger = /^(0|[1-9]\d*)$/;
const eventKinds = new Set<string>(recordedEventKinds);

export interface ContinuousCheckpointBase {
  schemaVersion: 1;
  kind: 'CONTINUOUS_RECORDING_BASE';
  runId: string;
  createdAt: string;
  endpoint: string;
  source: ObservationManifest['source'];
  instruments: ObservationManifest['instruments'];
  intervals: ObservationManifest['intervals'];
  scheduleFetchedAt: string | null;
  settings: ObservationManifest['settings'];
  codeHashes: Record<string, string>;
  notes: string[];
  nextSession?: ObservationManifest['nextSession'];
}

export interface ContinuousCheckpointDescriptor {
  schemaVersion: 1;
  kind: 'CONTINUOUS_CHECKPOINT';
  streamRunId: string;
  segment: ClosedRecordingSegment;
  previousCheckpointHash: string | null;
  metadataHash: string;
  closedAt: string;
  context?: Record<string, unknown>;
}

export interface PreparedContinuousCheckpoint {
  directory: string;
  descriptorPath: string;
  checkpointHash: string;
  descriptor: ContinuousCheckpointDescriptor;
}

export interface RestoredContinuousCheckpoints {
  directory: string;
  complete: boolean;
  checkpointHashes: string[];
  manifest: ObservationManifest;
  recording: Awaited<ReturnType<typeof scanRecording>>;
}

function hash(bytes: Buffer | string): string {
  return createHash('sha256').update(bytes).digest('hex');
}

function canonical(value: unknown): string {
  if (value === undefined || typeof value === 'function' || typeof value === 'symbol' || typeof value === 'bigint') {
    throw new Error('Checkpoint JSON contains a non-serializable value');
  }
  if (typeof value === 'number' && !Number.isFinite(value)) throw new Error('Checkpoint JSON contains a non-finite number');
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  const object = value as Record<string, unknown>;
  return `{${Object.keys(object).sort().map(key => `${JSON.stringify(key)}:${canonical(object[key])}`).join(',')}}`;
}

function jsonClone<T>(value: T): T {
  return JSON.parse(canonical(value)) as T;
}

function validIso(value: unknown, label: string): asserts value is string {
  if (typeof value !== 'string' || !value.endsWith('Z') || !Number.isFinite(Date.parse(value))) throw new Error(`${label} is invalid`);
}

function positive(value: unknown, label: string): asserts value is number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0) throw new Error(`${label} must be a positive safe integer`);
}

function expectedSegmentFile(index: number): string {
  return index === 1 ? 'events.ndjson' : `events-${String(index).padStart(6, '0')}.ndjson`;
}

function safeFile(directory: string, name: unknown, expected?: string): string {
  if (typeof name !== 'string' || path.basename(name) !== name || name === '.' || name === '..' || (expected && name !== expected)) {
    throw new Error('Checkpoint segment filename is unsafe or out of order');
  }
  const root = realpathSync(directory);
  const candidate = path.join(root, name);
  if (!existsSync(candidate)) throw new Error(`Checkpoint segment does not exist: ${name}`);
  const resolved = realpathSync(candidate);
  if (path.dirname(resolved) !== root) throw new Error(`Checkpoint segment escapes its directory: ${name}`);
  return resolved;
}

function stableBase(manifest: ObservationManifest): ContinuousCheckpointBase {
  if (manifest.schemaVersion !== 1 || !manifest.runId || !manifest.endpoint || !manifest.source) throw new Error('Checkpoint requires a valid observation manifest');
  return jsonClone({
    schemaVersion: 1,
    kind: 'CONTINUOUS_RECORDING_BASE',
    runId: manifest.runId,
    createdAt: manifest.createdAt,
    endpoint: manifest.endpoint,
    source: manifest.source,
    instruments: manifest.instruments,
    intervals: manifest.intervals,
    scheduleFetchedAt: manifest.scheduleFetchedAt,
    settings: manifest.settings,
    codeHashes: manifest.codeHashes,
    notes: manifest.notes,
    ...(manifest.nextSession === undefined ? {} : { nextSession: manifest.nextSession }),
  });
}

function verifyEvent(value: unknown, sequence: number, runId: string, previousOffset: bigint): RecordedEvent {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Checkpoint event is invalid');
  const event = value as Partial<RecordedEvent>;
  if (event.schemaVersion !== 1 || event.runId !== runId || event.sequence !== sequence
    || typeof event.connectionEpoch !== 'number' || !Number.isSafeInteger(event.connectionEpoch) || event.connectionEpoch < 0
    || !eventKinds.has(String(event.kind)) || !Object.hasOwn(event, 'payload')) throw new Error('Checkpoint event identity is invalid');
  validIso(event.receivedAt, 'Checkpoint event receipt time');
  if (typeof event.monotonicOffsetNs !== 'string' || !decimalInteger.test(event.monotonicOffsetNs)) throw new Error('Checkpoint event monotonic offset is invalid');
  if (BigInt(event.monotonicOffsetNs) < previousOffset) throw new Error('Checkpoint event moves backwards in monotonic time');
  return event as RecordedEvent;
}

function verifySegment(directory: string, segment: ClosedRecordingSegment, runId: string): { bytes: Buffer; first: RecordedEvent; last: RecordedEvent } {
  positive(segment.index, 'Checkpoint segment index');
  positive(segment.events, 'Checkpoint segment events');
  positive(segment.bytes, 'Checkpoint segment bytes');
  positive(segment.firstSequence, 'Checkpoint first sequence');
  positive(segment.lastSequence, 'Checkpoint last sequence');
  if (segment.lastSequence - segment.firstSequence + 1 !== segment.events || !sha256Pattern.test(segment.sha256)) throw new Error('Checkpoint segment sequence or hash is invalid');
  validIso(segment.firstReceivedAt, 'Checkpoint first receipt time'); validIso(segment.lastReceivedAt, 'Checkpoint last receipt time');
  if (!decimalInteger.test(segment.firstMonotonicOffsetNs) || !decimalInteger.test(segment.lastMonotonicOffsetNs)) throw new Error('Checkpoint segment monotonic range is invalid');
  const file = safeFile(directory, segment.file, expectedSegmentFile(segment.index));
  const bytes = readFileSync(file);
  if (bytes.length !== segment.bytes || hash(bytes) !== segment.sha256 || bytes.length === 0 || bytes.at(-1) !== 0x0a) throw new Error('Checkpoint segment bytes do not match descriptor');
  const lines = bytes.toString('utf8').split('\n'); lines.pop();
  if (lines.length !== segment.events) throw new Error('Checkpoint segment event count does not match descriptor');
  let previousOffset = -1n, first: RecordedEvent | undefined, last: RecordedEvent | undefined, stopped = false;
  for (const [index, line] of lines.entries()) {
    if (!line || stopped) throw new Error('Checkpoint segment contains an invalid terminal event');
    let parsed: unknown;
    try { parsed = JSON.parse(line); } catch { throw new Error('Checkpoint segment contains invalid JSON'); }
    const event = verifyEvent(parsed, segment.firstSequence + index, runId, previousOffset);
    previousOffset = BigInt(event.monotonicOffsetNs); first ??= event; last = event; stopped ||= event.kind === 'stop';
  }
  if (!first || !last || first.sequence !== segment.firstSequence || last.sequence !== segment.lastSequence
    || first.receivedAt !== segment.firstReceivedAt || last.receivedAt !== segment.lastReceivedAt
    || first.monotonicOffsetNs !== segment.firstMonotonicOffsetNs || last.monotonicOffsetNs !== segment.lastMonotonicOffsetNs) {
    throw new Error('Checkpoint segment boundaries do not match descriptor');
  }
  return { bytes, first, last };
}

function parseCheckpoint(input: string): { directory: string; descriptor: ContinuousCheckpointDescriptor; bytes: Buffer; base: ContinuousCheckpointBase } {
  const descriptorPath = input.endsWith('.json') ? path.resolve(input) : path.join(path.resolve(input), 'checkpoint.json');
  const directory = path.dirname(descriptorPath), raw = readFileSync(descriptorPath);
  let descriptor: ContinuousCheckpointDescriptor;
  try { descriptor = JSON.parse(raw.toString('utf8')) as ContinuousCheckpointDescriptor; } catch { throw new Error('Checkpoint descriptor contains invalid JSON'); }
  if (canonical(descriptor) !== raw.toString('utf8')) throw new Error('Checkpoint descriptor is not immutable canonical JSON');
  if (descriptor.schemaVersion !== 1 || descriptor.kind !== 'CONTINUOUS_CHECKPOINT' || !descriptor.streamRunId
    || !(descriptor.previousCheckpointHash === null || sha256Pattern.test(descriptor.previousCheckpointHash))
    || !sha256Pattern.test(descriptor.metadataHash)) throw new Error('Checkpoint descriptor is invalid');
  validIso(descriptor.closedAt, 'Checkpoint closed time');
  const baseRaw = readFileSync(path.join(directory, 'base-manifest.json'));
  if (hash(baseRaw) !== descriptor.metadataHash) throw new Error('Checkpoint base manifest hash does not match descriptor');
  let base: ContinuousCheckpointBase;
  try { base = JSON.parse(baseRaw.toString('utf8')) as ContinuousCheckpointBase; } catch { throw new Error('Checkpoint base manifest contains invalid JSON'); }
  if (canonical(base) !== baseRaw.toString('utf8') || base.schemaVersion !== 1 || base.kind !== 'CONTINUOUS_RECORDING_BASE'
    || base.runId !== descriptor.streamRunId) throw new Error('Checkpoint base manifest is invalid');
  return { directory, descriptor, bytes: raw, base };
}

/** Copies one fsynced segment verbatim into an immutable checkpoint archive. */
export function prepareContinuousCheckpoint(
  directory: string,
  segment: ClosedRecordingSegment,
  context: Record<string, unknown> | undefined,
  previousCheckpointHash: string | null,
  outputDir: string,
  manifest: ObservationManifest,
): PreparedContinuousCheckpoint {
  if (!(previousCheckpointHash === null || sha256Pattern.test(previousCheckpointHash))) throw new Error('Previous checkpoint hash is invalid');
  const sourceDirectory = realpathSync(directory), base = stableBase(manifest);
  const verified = verifySegment(sourceDirectory, segment, base.runId);
  if (Date.parse(segment.lastReceivedAt) > Date.now() + 60_000) throw new Error('Checkpoint segment receipt time is implausibly future');
  mkdirSync(outputDir, { recursive: false, mode: 0o700 });
  const destination = realpathSync(outputDir);
  const baseBytes = Buffer.from(canonical(base));
  const descriptor: ContinuousCheckpointDescriptor = jsonClone({
    schemaVersion: 1,
    kind: 'CONTINUOUS_CHECKPOINT',
    streamRunId: base.runId,
    segment: jsonClone(segment),
    previousCheckpointHash,
    metadataHash: hash(baseBytes),
    closedAt: new Date().toISOString(),
    ...(context === undefined ? {} : { context: jsonClone(context) }),
  });
  writeFileSync(path.join(destination, 'base-manifest.json'), baseBytes, { flag: 'wx', mode: 0o600 });
  copyFileSync(safeFile(sourceDirectory, segment.file, expectedSegmentFile(segment.index)), path.join(destination, segment.file), fsConstants.COPYFILE_EXCL);
  const descriptorBytes = Buffer.from(canonical(descriptor));
  writeFileSync(path.join(destination, 'checkpoint.json'), descriptorBytes, { flag: 'wx', mode: 0o600 });
  // A final read proves that the archive carries the exact source bytes, not a re-encoded event stream.
  if (!readFileSync(path.join(destination, segment.file)).equals(verified.bytes)) throw new Error('Checkpoint copy changed segment bytes');
  return { directory: destination, descriptorPath: path.join(destination, 'checkpoint.json'), checkpointHash: hash(descriptorBytes), descriptor };
}

function finalManifestInput(value: ObservationManifest | string | undefined): { manifest: ObservationManifest; bytes: Buffer } | undefined {
  if (value === undefined) return undefined;
  const bytes = typeof value === 'string' ? readFileSync(value) : Buffer.from(canonical(value));
  let manifest: ObservationManifest;
  try { manifest = JSON.parse(bytes.toString('utf8')) as ObservationManifest; } catch { throw new Error('Final manifest contains invalid JSON'); }
  return { manifest, bytes };
}

/** Restores an ordered checkpoint prefix. It never invents ACKs, gaps, stops, or receipt timestamps. */
export async function restoreContinuousCheckpoints(
  checkpointPaths: readonly string[],
  outputDir: string,
  finalManifest?: ObservationManifest | string,
): Promise<RestoredContinuousCheckpoints> {
  if (!checkpointPaths.length) throw new Error('At least one checkpoint is required');
  const parsed = checkpointPaths.map(parseCheckpoint);
  const metadataHash = parsed[0].descriptor.metadataHash, runId = parsed[0].descriptor.streamRunId;
  let expectedHash: string | null = null, previousLast: RecordedEvent | undefined;
  const segments: RecordingSegmentSummary[] = [], checkpointHashes: string[] = [];
  for (const [position, checkpoint] of parsed.entries()) {
    const descriptor = checkpoint.descriptor, checkpointHash = hash(checkpoint.bytes);
    if (descriptor.streamRunId !== runId || descriptor.metadataHash !== metadataHash || canonical(checkpoint.base) !== canonical(parsed[0].base)) {
      throw new Error('Checkpoint metadata conflicts with the stream');
    }
    if (descriptor.previousCheckpointHash !== expectedHash || descriptor.segment.index !== position + 1) throw new Error('Checkpoint chain is missing, duplicated, or out of order');
    const verified = verifySegment(checkpoint.directory, descriptor.segment, runId);
    if (previousLast && (verified.first.sequence !== previousLast.sequence + 1
      || Date.parse(verified.first.receivedAt) < Date.parse(previousLast.receivedAt)
      || BigInt(verified.first.monotonicOffsetNs) < BigInt(previousLast.monotonicOffsetNs))) throw new Error('Checkpoint event sequence, epoch time, or receipt time is not continuous');
    if (verified.first.kind === 'stop' && position !== parsed.length - 1) throw new Error('Checkpoint stop event appears before the final segment');
    if (previousLast?.kind === 'stop') throw new Error('Checkpoint contains events after stop');
    previousLast = verified.last; expectedHash = checkpointHash; checkpointHashes.push(checkpointHash);
    segments.push({ file: descriptor.segment.file, events: descriptor.segment.events, bytes: descriptor.segment.bytes, sha256: descriptor.segment.sha256 });
  }
  mkdirSync(outputDir, { recursive: false, mode: 0o700 });
  const destination = realpathSync(outputDir);
  for (const checkpoint of parsed) {
    copyFileSync(safeFile(checkpoint.directory, checkpoint.descriptor.segment.file, checkpoint.descriptor.segment.file),
      path.join(destination, checkpoint.descriptor.segment.file), fsConstants.COPYFILE_EXCL);
  }
  const recordingPath = path.join(destination, 'events.ndjson');
  const recording = await scanRecording(recordingPath, undefined, segments);
  const supplied = finalManifestInput(finalManifest);
  let complete = false;
  let manifest: ObservationManifest;
  if (supplied) {
    const final = supplied.manifest;
    const last = previousLast;
    if (final.schemaVersion !== 1 || final.status !== 'COMPLETE' || final.runId !== runId || !final.recording
      || !last || last.kind !== 'stop' || !final.recording.segments
      || canonical(stableBase(final)) !== canonical(parsed[0].base)
      || final.recording.segments.length !== segments.length
      || final.recording.events !== recording.events || final.recording.bytes !== recording.bytes || final.recording.sha256 !== recording.sha256
      || final.recording.segments.some((segment, index) => canonical(segment) !== canonical(segments[index]))) {
      throw new Error('Final manifest does not prove this complete checkpoint chain');
    }
    const reason = (last.payload as { reason?: unknown } | null)?.reason;
    if ((reason !== 'duration' && reason !== 'aborted') || final.capture?.reason !== reason) throw new Error('Final manifest stop semantics are not complete');
    manifest = final;
    writeFileSync(path.join(destination, 'manifest.json'), supplied.bytes, { flag: 'wx', mode: 0o600 });
    complete = true;
  } else {
    const base = parsed[0].base;
    manifest = {
      schemaVersion: 1, runId: base.runId, createdAt: base.createdAt, status: 'RECORDING', endpoint: base.endpoint,
      source: base.source, instruments: base.instruments, intervals: base.intervals, scheduleFetchedAt: base.scheduleFetchedAt,
      settings: base.settings, codeHashes: base.codeHashes, notes: base.notes,
      ...(base.nextSession === undefined ? {} : { nextSession: base.nextSession }),
    };
    writeFileSync(path.join(destination, 'manifest.json'), Buffer.from(canonical(manifest)), { flag: 'wx', mode: 0o600 });
  }
  return { directory: destination, complete, checkpointHashes, manifest, recording };
}
