import { createHash, type Hash } from 'node:crypto';
import {
  closeSync,
  createReadStream,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readdirSync,
  realpathSync,
  statSync,
  writeSync,
} from 'node:fs';
import path from 'node:path';

export const recordedEventKinds = [
  'response', 'connect_attempt', 'disconnect', 'gap', 'subscription_timeout',
  'heartbeat_timeout', 'tick', 'stop',
] as const;
export type RecordedEventKind = (typeof recordedEventKinds)[number];

export interface RecordedEvent {
  schemaVersion: 1;
  runId: string;
  sequence: number;
  connectionEpoch: number;
  receivedAt: string;
  monotonicOffsetNs: string;
  kind: RecordedEventKind;
  payload: unknown;
}

export interface RecordingWriterOptions {
  path: string;
  runId: string;
  maxBytes?: number;
  segmentMaxBytes?: number;
  fsyncEveryMs?: number;
}

export interface RecordingSegmentSummary {
  file: string;
  events: number;
  bytes: number;
  sha256: string;
}

export interface RecordingSummary {
  events: number;
  bytes: number;
  sha256: string;
  segments?: RecordingSegmentSummary[];
}

export interface RecordingScanSummary extends RecordingSummary {
  truncatedTail: boolean;
  hasStop: boolean;
  runId: string | null;
}

const DEFAULT_MAX_BYTES = 256 * 1024 * 1024;
const DEFAULT_FSYNC_EVERY_MS = 1_000;
const MAX_RECORDING_LINE_BYTES = 4 * 1024 * 1024;
const decimalInteger = /^(0|[1-9]\d*)$/;
const sha256Pattern = /^[0-9a-f]{64}$/;
const kindSet = new Set<string>(recordedEventKinds);

function positiveInteger(value: number, label: string, allowZero = false): number {
  if (!Number.isSafeInteger(value) || (allowZero ? value < 0 : value <= 0)) {
    throw new Error(`${label} must be a ${allowZero ? 'non-negative' : 'positive'} safe integer`);
  }
  return value;
}

function segmentPath(recordingPath: string, index: number): string {
  if (index === 1) return recordingPath;
  const extension = path.extname(recordingPath);
  const stem = extension ? recordingPath.slice(0, -extension.length) : recordingPath;
  return `${stem}-${String(index).padStart(6, '0')}${extension}`;
}

export class RecordingWriter {
  private descriptor: number | null = null;
  private readonly recordingPath: string;
  private readonly runId: string;
  private readonly maxBytes: number;
  private readonly segmentMaxBytes: number | null;
  private readonly fsyncEveryMs: number;
  private readonly startedAtNs = process.hrtime.bigint();
  private globalHash: Hash | null = createHash('sha256');
  private segmentHash: Hash | null = null;
  private lastFsyncAt = Date.now();
  private sequence = 0;
  private byteCount = 0;
  private segmentIndex = 1;
  private segmentByteCount = 0;
  private segmentEventCount = 0;
  private readonly segmentSummaries: RecordingSegmentSummary[] = [];
  private closed = false;
  private stopped = false;
  private failed: unknown = null;
  private summary: RecordingSummary | null = null;

  constructor(options: RecordingWriterOptions) {
    if (!options.path) throw new Error('Recording path is required');
    if (!options.runId.trim()) throw new Error('Recording runId is required');
    this.recordingPath = path.resolve(options.path);
    this.runId = options.runId;
    this.maxBytes = positiveInteger(options.maxBytes ?? DEFAULT_MAX_BYTES, 'maxBytes');
    this.segmentMaxBytes = options.segmentMaxBytes === undefined
      ? null
      : positiveInteger(options.segmentMaxBytes, 'segmentMaxBytes');
    this.fsyncEveryMs = positiveInteger(options.fsyncEveryMs ?? DEFAULT_FSYNC_EVERY_MS, 'fsyncEveryMs', true);
    mkdirSync(path.dirname(this.recordingPath), { recursive: true });
    this.openSegment();
  }

  private openSegment(): void {
    this.descriptor = openSync(segmentPath(this.recordingPath, this.segmentIndex), 'wx', 0o600);
    this.segmentHash = createHash('sha256');
    this.segmentByteCount = 0;
    this.segmentEventCount = 0;
    this.lastFsyncAt = Date.now();
  }

  private finishSegment(): unknown {
    if (this.descriptor === null || this.segmentHash === null) return undefined;
    let finishError: unknown;
    try { fsyncSync(this.descriptor); } catch (error) { finishError = error; }
    try { closeSync(this.descriptor); } catch (error) { finishError ??= error; }
    this.descriptor = null;
    this.segmentSummaries.push({
      file: path.basename(segmentPath(this.recordingPath, this.segmentIndex)),
      events: this.segmentEventCount,
      bytes: this.segmentByteCount,
      sha256: this.segmentHash.digest('hex'),
    });
    this.segmentHash = null;
    return finishError;
  }

  private rotate(): void {
    const finishError = this.finishSegment();
    if (finishError) throw finishError;
    this.segmentIndex += 1;
    this.openSegment();
  }

  append(kind: RecordedEventKind, payload: unknown, connectionEpoch: number): void {
    if (this.closed) throw new Error('Recording writer is closed');
    if (this.failed) throw this.failed;
    if (this.stopped) throw new Error('Recording stop event must be the final event');
    if (!kindSet.has(kind)) throw new Error(`Unsupported recording event kind: ${kind}`);
    positiveInteger(connectionEpoch, 'connectionEpoch', true);

    const event: RecordedEvent = {
      schemaVersion: 1,
      runId: this.runId,
      sequence: this.sequence + 1,
      connectionEpoch,
      receivedAt: new Date().toISOString(),
      monotonicOffsetNs: (process.hrtime.bigint() - this.startedAtNs).toString(),
      kind,
      payload: payload === undefined ? null : payload,
    };
    let buffer: Buffer;
    try { buffer = Buffer.from(`${JSON.stringify(event)}\n`, 'utf8'); }
    catch (error) { this.failed = error; throw error; }
    if (this.byteCount + buffer.length > this.maxBytes) {
      const error = new Error(`Recording exceeds maxBytes=${this.maxBytes}`);
      this.failed = error;
      throw error;
    }
    if (buffer.length > MAX_RECORDING_LINE_BYTES) {
      const error = new Error(`Recording event exceeds the ${MAX_RECORDING_LINE_BYTES}-byte line limit`);
      this.failed = error;
      throw error;
    }
    if (this.segmentMaxBytes !== null && buffer.length > this.segmentMaxBytes) {
      const error = new Error(`Recording event exceeds segmentMaxBytes=${this.segmentMaxBytes}`);
      this.failed = error;
      throw error;
    }

    try {
      if (this.segmentMaxBytes !== null && this.segmentByteCount > 0
        && this.segmentByteCount + buffer.length > this.segmentMaxBytes) this.rotate();
      if (this.descriptor === null || this.segmentHash === null || this.globalHash === null) {
        throw new Error('Recording segment is not open');
      }
      let offset = 0;
      while (offset < buffer.length) {
        const written = writeSync(this.descriptor, buffer, offset, buffer.length - offset, null);
        if (written <= 0) throw new Error('Recording write made no progress');
        const writtenBytes = buffer.subarray(offset, offset + written);
        this.globalHash.update(writtenBytes);
        this.segmentHash.update(writtenBytes);
        this.byteCount += written;
        this.segmentByteCount += written;
        offset += written;
      }
      this.sequence += 1;
      this.segmentEventCount += 1;
      this.stopped = kind === 'stop';
      const now = Date.now();
      if (this.fsyncEveryMs === 0 || now - this.lastFsyncAt >= this.fsyncEveryMs) {
        fsyncSync(this.descriptor);
        this.lastFsyncAt = now;
      }
    } catch (error) {
      this.failed = error;
      throw error;
    }
  }

  close(): RecordingSummary {
    if (this.summary) return this.summary;
    if (this.closed) throw new Error('Recording writer closed without a summary');
    this.closed = true;
    const finishError = this.finishSegment();
    const sha256 = this.globalHash!.digest('hex');
    this.globalHash = null;
    this.summary = {
      events: this.sequence,
      bytes: this.byteCount,
      sha256,
      ...(this.segmentMaxBytes === null ? {} : { segments: [...this.segmentSummaries] }),
    };
    if (finishError) throw finishError;
    return this.summary;
  }
}

function validateEvent(value: unknown, expectedSequence: number, runId: string | null): RecordedEvent {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Recorded line is not an object');
  const event = value as Partial<RecordedEvent>;
  if (event.schemaVersion !== 1) throw new Error('Unsupported recording schemaVersion');
  if (typeof event.runId !== 'string' || event.runId.length === 0 || (runId !== null && event.runId !== runId)) {
    throw new Error('Recording runId is missing or changed');
  }
  if (event.sequence !== expectedSequence) throw new Error(`Recording sequence must equal ${expectedSequence}`);
  if (!Number.isSafeInteger(event.connectionEpoch) || event.connectionEpoch! < 0) throw new Error('Recording connectionEpoch is invalid');
  if (typeof event.receivedAt !== 'string' || !event.receivedAt.endsWith('Z') || !Number.isFinite(Date.parse(event.receivedAt))) {
    throw new Error('Recording receivedAt is invalid');
  }
  if (typeof event.monotonicOffsetNs !== 'string' || !decimalInteger.test(event.monotonicOffsetNs)) {
    throw new Error('Recording monotonicOffsetNs is invalid');
  }
  if (typeof event.kind !== 'string' || !kindSet.has(event.kind)) throw new Error('Recording event kind is invalid');
  if (!Object.hasOwn(event, 'payload')) throw new Error('Recording payload is missing');
  return event as RecordedEvent;
}

interface ScanState {
  globalHash: Hash;
  bytes: number;
  events: number;
  runId: string | null;
  hasStop: boolean;
  previousOffset: bigint;
}

async function scanOneFile(
  filePath: string,
  state: ScanState,
  onEvent?: (event: RecordedEvent) => void | Promise<void>,
): Promise<RecordingSegmentSummary & { truncatedTail: boolean }> {
  const fileHash = createHash('sha256');
  let pending = Buffer.alloc(0);
  let bytes = 0;
  let events = 0;
  for await (const chunk of createReadStream(filePath)) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    state.globalHash.update(buffer);
    fileHash.update(buffer);
    state.bytes += buffer.length;
    bytes += buffer.length;
    pending = pending.length === 0 ? buffer : Buffer.concat([pending, buffer]);
    let newline = pending.indexOf(0x0a);
    while (newline >= 0) {
      if (newline > MAX_RECORDING_LINE_BYTES) throw new Error(`Recording line ${state.events + 1} exceeds the ${MAX_RECORDING_LINE_BYTES}-byte limit`);
      const line = pending.subarray(0, newline);
      pending = pending.subarray(newline + 1);
      if (line.length === 0) throw new Error(`Recording line ${state.events + 1} is empty`);
      if (state.hasStop) throw new Error(`Recording line ${state.events + 1} appears after the stop event`);
      let parsed: unknown;
      try { parsed = JSON.parse(line.toString('utf8')); }
      catch { throw new Error(`Recording line ${state.events + 1} contains invalid JSON`); }
      const event = validateEvent(parsed, state.events + 1, state.runId);
      const offset = BigInt(event.monotonicOffsetNs);
      if (offset < state.previousOffset) throw new Error(`Recording line ${state.events + 1} moves backwards in monotonic time`);
      state.previousOffset = offset;
      state.runId ??= event.runId;
      state.events += 1;
      events += 1;
      state.hasStop ||= event.kind === 'stop';
      await onEvent?.(event);
      newline = pending.indexOf(0x0a);
    }
    if (pending.length > MAX_RECORDING_LINE_BYTES) throw new Error(`Recording line ${state.events + 1} exceeds the ${MAX_RECORDING_LINE_BYTES}-byte limit`);
  }
  return {
    file: path.basename(filePath),
    events,
    bytes,
    sha256: fileHash.digest('hex'),
    truncatedTail: pending.length > 0,
  };
}

function validateManifest(recordingPath: string, segments: readonly RecordingSegmentSummary[]): string[] {
  if (segments.length === 0) throw new Error('Recording segment manifest is empty');
  const absolutePath = path.resolve(recordingPath);
  const canonicalDirectory = realpathSync(path.dirname(absolutePath));
  const seen = new Set<string>();
  return segments.map((segment, index) => {
    const expectedFile = path.basename(segmentPath(absolutePath, index + 1));
    if (!segment.file || path.basename(segment.file) !== segment.file || segment.file !== expectedFile
      || segment.file === '.' || segment.file === '..' || seen.has(segment.file)) {
      throw new Error(`Recording segment ${index + 1} has an unsafe or out-of-order filename`);
    }
    seen.add(segment.file);
    positiveInteger(segment.events, `segments[${index}].events`, true);
    positiveInteger(segment.bytes, `segments[${index}].bytes`, true);
    if (!sha256Pattern.test(segment.sha256)) throw new Error(`Recording segment ${index + 1} sha256 is invalid`);
    const candidate = path.join(canonicalDirectory, segment.file);
    if (!existsSync(candidate)) throw new Error(`Recording segment does not exist: ${segment.file}`);
    const canonicalFile = realpathSync(candidate);
    if (path.dirname(canonicalFile) !== canonicalDirectory) throw new Error(`Recording segment ${segment.file} escapes its recording directory`);
    return canonicalFile;
  });
}

export async function scanRecording(
  recordingPath: string,
  onEvent?: (event: RecordedEvent) => void | Promise<void>,
  segments?: readonly RecordingSegmentSummary[],
): Promise<RecordingScanSummary> {
  if (!existsSync(recordingPath)) throw new Error(`Recording does not exist: ${recordingPath}`);
  const paths = segments === undefined ? [path.resolve(recordingPath)] : validateManifest(recordingPath, segments);
  const result = await scanPaths(paths, onEvent, segments);
  if (segments !== undefined) {
    const nextPath = segmentPath(path.resolve(recordingPath), segments.length + 1);
    if (existsSync(nextPath)) throw new Error(`Recording segment manifest omits ${path.basename(nextPath)}`);
  }
  return result;
}

async function scanPaths(
  paths: readonly string[],
  onEvent?: (event: RecordedEvent) => void | Promise<void>,
  expectedSegments?: readonly RecordingSegmentSummary[],
  includeSegments = false,
): Promise<RecordingScanSummary> {
  const state: ScanState = {
    globalHash: createHash('sha256'), bytes: 0, events: 0, runId: null, hasStop: false, previousOffset: -1n,
  };
  const scannedSegments: RecordingSegmentSummary[] = [];
  let truncatedTail = false;
  for (const [index, filePath] of paths.entries()) {
    const scanned = await scanOneFile(filePath, state, onEvent);
    if (scanned.truncatedTail && index !== paths.length - 1) {
      throw new Error(`Recording segment ${index + 1} has a partial tail before the final segment`);
    }
    truncatedTail = scanned.truncatedTail;
    const { truncatedTail: _tail, ...summary } = scanned;
    scannedSegments.push(summary);
    if (expectedSegments !== undefined) {
      const expected = expectedSegments[index];
      if (summary.events !== expected.events || summary.bytes !== expected.bytes || summary.sha256 !== expected.sha256) {
        throw new Error(`Recording segment ${index + 1} does not match its manifest`);
      }
    }
  }
  return {
    events: state.events,
    bytes: state.bytes,
    sha256: state.globalHash.digest('hex'),
    ...(expectedSegments === undefined && !includeSegments && paths.length === 1 ? {} : { segments: scannedSegments }),
    truncatedTail,
    hasStop: state.hasStop,
    runId: state.runId,
  };
}

interface SegmentInventory {
  paths: string[];
  fingerprint: string;
}

function escapedPattern(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function discoverSegmentInventory(recordingPath: string): SegmentInventory {
  const absolutePath = path.resolve(recordingPath);
  const canonicalDirectory = realpathSync(path.dirname(absolutePath));
  const baseName = path.basename(absolutePath);
  const extension = path.extname(baseName);
  const stem = extension ? baseName.slice(0, -extension.length) : baseName;
  const laterPattern = new RegExp(`^${escapedPattern(stem)}-(\\d{6})${escapedPattern(extension)}$`);
  const indexed = new Map<number, string>();
  for (const file of readdirSync(canonicalDirectory)) {
    if (file === baseName) {
      indexed.set(1, file);
      continue;
    }
    const match = laterPattern.exec(file);
    if (!match) continue;
    const index = Number(match[1]);
    if (index < 2 || indexed.has(index)) throw new Error(`Recording has an invalid segment filename: ${file}`);
    indexed.set(index, file);
  }
  if (!indexed.has(1)) throw new Error(`Recording does not exist: ${recordingPath}`);
  const ordered = [...indexed.entries()].sort(([left], [right]) => left - right);
  const paths: string[] = [];
  const fingerprint: Array<Record<string, string>> = [];
  for (const [position, [index, file]] of ordered.entries()) {
    if (index !== position + 1) throw new Error(`Recording segment sequence has a gap before ${file}`);
    const candidate = path.join(canonicalDirectory, file);
    const canonicalFile = realpathSync(candidate);
    if (path.dirname(canonicalFile) !== canonicalDirectory) {
      throw new Error(`Recording segment ${file} escapes its recording directory`);
    }
    const metadata = statSync(canonicalFile, { bigint: true });
    if (!metadata.isFile()) throw new Error(`Recording segment ${file} is not a regular file`);
    paths.push(canonicalFile);
    fingerprint.push({
      file,
      dev: metadata.dev.toString(),
      ino: metadata.ino.toString(),
      size: metadata.size.toString(),
      mtimeNs: metadata.mtimeNs.toString(),
      ctimeNs: metadata.ctimeNs.toString(),
    });
  }
  return { paths, fingerprint: JSON.stringify(fingerprint) };
}

/**
 * Recovers a crash-left segmented recording without claiming it matches a finalized manifest.
 * The file inventory must remain unchanged for the complete streaming scan.
 */
export async function scanUnfinalizedRecording(
  recordingPath: string,
  onEvent?: (event: RecordedEvent) => void | Promise<void>,
): Promise<RecordingScanSummary> {
  const before = discoverSegmentInventory(recordingPath);
  const result = await scanPaths(before.paths, onEvent, undefined, true);
  const after = discoverSegmentInventory(recordingPath);
  if (before.fingerprint !== after.fingerprint) {
    throw new Error('Recording segment inventory changed during unfinalized scan');
  }
  return result;
}
