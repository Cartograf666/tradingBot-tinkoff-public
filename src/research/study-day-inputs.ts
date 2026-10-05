import { hashStudyValue, planStudyBlock, type StudyBlockPlan, type StudyChunkPlan } from './study-protocol.js';
import type { StudyChunkReceipt, StudyLedger } from './study-state.js';

type StudyDayBounds = Pick<StudyBlockPlan, 'sessionDate' | 'mainStart' | 'mainEnd'>;

export function expectedStudyDayChunks(plan: StudyDayBounds): Array<{ key: string; chunk: StudyChunkPlan }> {
  const early = planStudyBlock(plan.sessionDate, plan.mainStart, plan.mainEnd, 'early', Date.parse(`${plan.sessionDate}T05:50:00.000Z`));
  const late = planStudyBlock(plan.sessionDate, plan.mainStart, plan.mainEnd, 'late', Date.parse(`${plan.sessionDate}T10:50:00.000Z`));
  if (!early || !late) throw new Error('Cannot reconstruct full-day study bounds');
  return [early, late].flatMap(block => block.chunks.map(chunk => ({
    key: `${plan.sessionDate}:${block.block}:${chunk.index}`, chunk,
  })));
}

/** The first confirmed receipt is a deterministic fallback only when no PASS
 * canonical receipt exists. Window quality is never relabelled as PASS. */
export function selectStudyDayInputs(ledger: StudyLedger, plan: StudyDayBounds): {
  receipts: StudyChunkReceipt[]; expectedCount: number; phase: 'DEVELOPMENT' | 'HOLDOUT' | null;
} {
  const expected = expectedStudyDayChunks(plan), byKey = new Map(expected.map(item => [item.key, item.chunk]));
  const finalized = ledger.days.filter(day => day.sessionDate === plan.sessionDate);
  if (finalized.length > 1) throw new Error('Study date has conflicting finalized reports');
  const pinned = finalized[0]?.chunkAssetIds;
  const pinnedIds = pinned ? new Set(pinned) : null;
  const candidates = new Map<string, StudyChunkReceipt[]>(), assets = new Set<number>(), phases = new Set<string>();
  for (const receipt of ledger.chunks.filter(item => item.sessionDate === plan.sessionDate)) {
    // Once a day is sealed, later confirmed archives remain auditable in the
    // ledger but cannot change its scientific inputs or diagnostic replay.
    if (pinnedIds && !pinnedIds.has(receipt.assetId)) continue;
    if (assets.has(receipt.assetId)) throw new Error(`Asset ${receipt.assetId} appears more than once in daily chunk receipts`);
    assets.add(receipt.assetId);
    const key = `${receipt.sessionDate}:${receipt.block}:${receipt.chunkIndex}`;
    const planned = byKey.get(key);
    if (!planned || receipt.chunkId !== key || receipt.plannedStart !== planned.plannedStart
      || receipt.plannedEnd !== planned.plannedEnd) throw new Error(`Chunk ${receipt.chunkId} does not match full-day planned bounds`);
    phases.add(receipt.phase);
    const group = candidates.get(key) ?? [];
    group.push(receipt); candidates.set(key, group);
  }
  if (phases.size > 1) throw new Error('Study day inputs span multiple phases');
  const receipts: StudyChunkReceipt[] = [];
  for (const [index, { key }] of expected.entries()) {
    const group = candidates.get(key) ?? [];
    if (pinned) {
      const selected = group.filter(item => item.assetId === pinned[index]);
      if (selected.length !== 1) throw new Error(`Finalized study day lost its pinned chunk ${key}`);
      receipts.push(selected[0]!);
      continue;
    }
    const canonicalAssetId = ledger.canonicalChunks[key];
    if (canonicalAssetId !== undefined) {
      const canonical = group.filter(item => item.assetId === canonicalAssetId);
      if (canonical.length !== 1 || canonical[0]!.quality !== 'PASS') {
        throw new Error(`Canonical chunk ${key} has no unique quality-passing matching receipt`);
      }
      receipts.push(canonical[0]!);
    } else if (group.some(item => item.quality === 'PASS')) {
      throw new Error(`Quality-passing chunk ${key} lacks its canonical mapping`);
    } else if (group.length) {
      receipts.push([...group].sort((left, right) => left.uploadedAt.localeCompare(right.uploadedAt)
        || left.assetId - right.assetId)[0]!);
    }
  }
  return { receipts, expectedCount: expected.length,
    phase: (phases.values().next().value as 'DEVELOPMENT' | 'HOLDOUT' | undefined) ?? null };
}

export function studyDayInputHash(receipts: StudyChunkReceipt[]): string {
  return hashStudyValue(receipts.map(receipt => ({ chunkId: receipt.chunkId, assetId: receipt.assetId,
    archiveSha256: receipt.archiveSha256, recordingHash: receipt.recordingHash })));
}
