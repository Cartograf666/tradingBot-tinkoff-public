import type { StudyChunkReceipt } from '../research/study-state.js';

type ReplayIdentity = Pick<StudyChunkReceipt, 'replayConfigHash' | 'simulatorHashes'>;
export type ReplayCompatibilityReason = 'NO_SELECTED_RECEIPTS' | 'REPLAY_CONFIG_MISMATCH' | 'SIMULATOR_SOURCE_MISMATCH';

/** Every archived receipt must have been recorded under precisely the runtime
 * being used for a new replay. Missing and additional source keys both fail. */
export function checkRecordedReplayCompatibility(receipts: readonly ReplayIdentity[], current: ReplayIdentity):
  { status: 'COMPATIBLE'; reasons: [] } | { status: 'RECORDED_RUNTIME_REQUIRED'; reasons: ReplayCompatibilityReason[] } {
  const reasons: ReplayCompatibilityReason[] = [];
  const digest = /^[0-9a-f]{64}$/;
  if (!receipts.length) reasons.push('NO_SELECTED_RECEIPTS');
  if (!digest.test(current.replayConfigHash) || receipts.some(receipt => receipt.replayConfigHash !== current.replayConfigHash)) {
    reasons.push('REPLAY_CONFIG_MISMATCH');
  }
  const currentEntries = Object.entries(current.simulatorHashes);
  if (!currentEntries.length || currentEntries.some(([file, hash]) => !file || !digest.test(hash)) || receipts.some(receipt => {
    const archived = receipt.simulatorHashes;
    return Object.keys(archived).length !== currentEntries.length
      || currentEntries.some(([file, hash]) => archived[file] !== hash);
  })) reasons.push('SIMULATOR_SOURCE_MISMATCH');
  return reasons.length ? { status: 'RECORDED_RUNTIME_REQUIRED', reasons } : { status: 'COMPATIBLE', reasons: [] };
}
