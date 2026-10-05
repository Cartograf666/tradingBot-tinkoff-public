import type { LiquidationSnapshot } from './order-book-simulator.js';

/** Samples only actual received frames. Unknown intervals remain unknown. */
export class ObservedLiquidationTracker {
  private peak: number;
  private maximumDrawdownRub = 0;
  private minimumKnownEquityRub: number | null = null;
  private maximumKnownEquityRub: number | null = null;
  private latestKnownEquityRub: number | null = null;
  private latestKnownAtMs: number | null = null;
  private samples = 0;
  private knownSamples = 0;
  private positionSamples = 0;
  private knownPositionSamples = 0;
  private adjacentKnownEndpointMs = 0;
  private otherIntervalMs = 0;
  private readonly unavailableReasons: Record<string, number> = {};
  private last: { atMs: number; known: boolean } | null = null;

  constructor(initialCashRub: number) { this.peak = initialCashRub; }

  observe(snapshot: LiquidationSnapshot): void {
    if (snapshot.atMs === null) return;
    this.samples++;
    const known = snapshot.equityRub !== null;
    if (known) {
      this.knownSamples++;
      const equity = snapshot.equityRub!;
      this.minimumKnownEquityRub = Math.min(this.minimumKnownEquityRub ?? equity, equity);
      this.maximumKnownEquityRub = Math.max(this.maximumKnownEquityRub ?? equity, equity);
      this.latestKnownEquityRub = equity;
      this.latestKnownAtMs = snapshot.atMs;
      this.peak = Math.max(this.peak, equity);
      this.maximumDrawdownRub = Math.max(this.maximumDrawdownRub, this.peak - equity);
    }
    if (snapshot.coverage.positionCount) {
      this.positionSamples++;
      if (known) this.knownPositionSamples++;
    }
    for (const reason of snapshot.unavailableReasons) {
      const key = reason.split(':')[0];
      this.unavailableReasons[key] = (this.unavailableReasons[key] ?? 0) + 1;
    }
    if (this.last) {
      const dt = Math.max(0, snapshot.atMs - this.last.atMs);
      if (dt <= 2_000 && this.last.known && known) this.adjacentKnownEndpointMs += dt;
      else this.otherIntervalMs += dt;
    }
    this.last = { atMs: snapshot.atMs, known };
  }

  result() {
    return { maximumDrawdownRub: this.positionSamples && !this.knownPositionSamples ? null : this.maximumDrawdownRub,
      minimumKnownEquityRub: this.minimumKnownEquityRub,
      maximumKnownEquityRub: this.maximumKnownEquityRub,
      latestKnownEquityRub: this.latestKnownEquityRub,
      latestKnownAtMs: this.latestKnownAtMs,
      samples: this.samples, knownSamples: this.knownSamples,
      positionSamples: this.positionSamples, knownPositionSamples: this.knownPositionSamples,
      knownPositionSampleFraction: this.positionSamples ? this.knownPositionSamples / this.positionSamples : null,
      adjacentKnownEndpointMs: this.adjacentKnownEndpointMs, otherIntervalMs: this.otherIntervalMs,
      unavailableReasons: { ...this.unavailableReasons },
      limitation: 'Observed-frame liquidation drawdown only. Intervals with known endpoints are not guaranteed known throughout; no interpolation or bound on unobserved loss.' };
  }
}
