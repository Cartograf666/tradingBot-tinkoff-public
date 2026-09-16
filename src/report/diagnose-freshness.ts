import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import type { ObservationManifest } from '../research/market-observation.js';
import { ObservationAccumulator, observationAnalysisProvenance } from '../research/market-recording-report.js';
import { scanRecording } from '../research/market-recording.js';

/** Read-only diagnostics with the current analyzer. Never overwrites capture summaries,
 * edits archived provenance, or claims a faithful rebuild by the original analyzer. */
export async function diagnoseRecordingFreshness(directory: string) {
  const manifestBytes = readFileSync(path.join(directory, 'manifest.json'));
  let manifest: ObservationManifest;
  try { manifest = JSON.parse(manifestBytes.toString('utf8')) as ObservationManifest; }
  catch { throw new Error('Invalid diagnostic manifest JSON'); }
  if (manifest.schemaVersion !== 1 || manifest.status !== 'COMPLETE' || !manifest.recording
    || !/sandbox/i.test(manifest.endpoint) || !['exchange', 'dealer', 'all'].includes(manifest.source)
    || !manifest.instruments?.length || new Set(manifest.instruments.map(i => i.uid)).size !== manifest.instruments.length
    || manifest.instruments.some(i => !i.uid || !i.exchange || !Number.isSafeInteger(i.lot) || i.lot <= 0)
    || !Array.isArray(manifest.intervals) || !manifest.codeHashes
    || !Number.isFinite(manifest.settings?.maxBookAgeMs) || manifest.settings.maxBookAgeMs <= 0
    || !Number.isFinite(manifest.settings.maxFutureSkewMs) || manifest.settings.maxFutureSkewMs < 0
    || !Number.isFinite(manifest.settings.sampleIntervalMs) || manifest.settings.sampleIntervalMs < 100
    || !Number.isFinite(manifest.settings.budgetRub) || manifest.settings.budgetRub <= 0
    || !Number.isFinite(manifest.settings.commissionRate) || manifest.settings.commissionRate < 0 || manifest.settings.commissionRate >= .5) {
    throw new Error('Diagnostics require a complete valid sandbox recording');
  }
  if (manifest.settings.segmentMaxBytes !== undefined && !manifest.recording.segments) throw new Error('Diagnostic recording is missing its segment manifest');
  const accumulator = new ObservationAccumulator(manifest);
  const integrity = await scanRecording(path.join(directory, 'events.ndjson'), event => accumulator.consume(event), manifest.recording.segments);
  if (!integrity.hasStop || integrity.truncatedTail || integrity.sha256 !== manifest.recording.sha256
    || integrity.events !== manifest.recording.events || integrity.bytes !== manifest.recording.bytes) {
    throw new Error('Diagnostic recording integrity or completion mismatch');
  }
  const provenance = observationAnalysisProvenance();
  const extension = path.extname(fileURLToPath(import.meta.url));
  const runtimeFile = fileURLToPath(import.meta.url);
  let sourceFile = runtimeFile;
  if (extension !== '.ts') {
    // observationAnalysisProvenance already locates the package and sources.
    sourceFile = path.resolve(path.dirname(runtimeFile), '../../../src/report/diagnose-freshness.ts');
  }
  const hash = (file: string) => createHash('sha256').update(readFileSync(file)).digest('hex');
  const observed = accumulator.result();
  return {
    schemaVersion: 1,
    analysisKind: 'CURRENT_ANALYZER_READ_ONLY_FRESHNESS_DIAGNOSTICS',
    originalAnalysisSourcesMatch: Object.entries(provenance.sourceHashes).every(([file, sha]) => manifest.codeHashes[file] === sha),
    manifestSha256: createHash('sha256').update(manifestBytes).digest('hex'),
    recordingSha256: integrity.sha256, recordingEvents: integrity.events, integrityVerified: true,
    analysisProvenance: { ...provenance, diagnosticSourceSha256: hash(sourceFile), diagnosticRuntimeSha256: hash(runtimeFile) },
    // Indices bind to the unchanged private manifest. No raw prices, identifiers,
    // payloads, user metadata, or endpoint strings are copied into this output.
    freshnessDiagnostics: observed.freshnessDiagnostics,
    clockJumps: observed.clockJumps, disconnects: observed.disconnects, gapMarkers: observed.gapMarkers,
    sourceReorders: observed.sourceReorders, samplingCoverage: observed.samplingCoverage,
    groups: observed.groups.filter(g => g.bookEvents).map(g => ({
      instrumentIndex: g.instrumentIndex, source: g.source,
      freshnessDiagnostics: g.freshnessDiagnostics,
    })),
    limitations: 'These are timestamp observations, not a provider/network/local-processing diagnosis. Original recorder provenance is preserved. No original quality threshold or acceptance gate is changed; current analysis is not a faithful rebuild when source hashes differ.',
  };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    if (process.argv.length !== 3) throw new Error('Usage: diagnose-freshness <recording-directory>');
    console.log(JSON.stringify(await diagnoseRecordingFreshness(path.resolve(process.argv[2])), null, 2));
  } catch (error) {
    console.error(error instanceof Error ? error.message : 'Freshness diagnostics failed');
    process.exitCode = 1;
  }
}
