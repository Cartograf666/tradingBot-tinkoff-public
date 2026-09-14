import { readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import type { reportMarketRecording } from './market-recording-report.js';
import type { ObservationManifest } from './market-observation.js';

type Summary = Awaited<ReturnType<typeof reportMarketRecording>>;
export function assessPilotQuality(summary: Summary, manifest: ObservationManifest) {
  const rows = manifest.instruments.map(instrument => {
    const group = summary.groups.find(g => g.source === 'EXCHANGE' && g.ticker === instrument.ticker);
    const phase = group?.phases.find(p => p.phase === 'regular_trading_session_main');
    const coverage = phase?.usableShareOfObservedScheduledTicks ?? null;
    return { ticker: instrument.ticker, usableTicks: phase?.eligibleSamples ?? 0,
      observedTicks: phase?.observedScheduledTicks ?? 0, coverage,
      passes: coverage !== null && coverage >= .8 && (group?.bookEvents ?? 0) > 0,
      exclusions: phase?.sampleExclusions ?? {},
      breakEvenMedianBps: phase?.breakEvenRiseBps.median ?? null,
      breakEvenP95Bps: phase?.breakEvenRiseBps.p95 ?? null };
  });
  const checks = {
    exchangeSource: manifest.source === 'exchange',
    finishedWholeRecording: summary.complete && manifest.capture?.reason === 'duration' && summary.elapsedSeconds >= 1795,
    allSubscriptions: summary.allSubscriptionsAcknowledged && summary.expectedSubscriptions.length === 18,
    receivedExchangeData: summary.exchangeSamples > 0,
    recordedTimeCoverage: (summary.samplingCoverage.recordedShare ?? 0) >= .99,
    eachInstrumentCoverage: rows.length === 6 && rows.every(r => r.passes),
  };
  return { status: Object.values(checks).every(Boolean) ? 'PASS' as const : 'INSUFFICIENT_DATA' as const,
    checks, rows, meaning: 'Engineering data-quality targets. No trading profitability has been measured.' };
}

export function writePilotQualityReport(directory: string) {
  const manifest = JSON.parse(readFileSync(path.join(directory, 'manifest.json'), 'utf8')) as ObservationManifest;
  const summary = JSON.parse(readFileSync(path.join(directory, 'summary.json'), 'utf8')) as Summary;
  const result = assessPilotQuality(summary, manifest);
  const reportPath = path.join(directory, 'QUALITY.md');
  const names: Record<keyof typeof result.checks, string> = {
    exchangeSource: 'Биржевой источник', finishedWholeRecording: 'Полная получасовая запись',
    allSubscriptions: 'Все 18 подписок', receivedExchangeData: 'Пригодные биржевые данные получены',
    recordedTimeCoverage: 'Записано ≥99% ожидаемых тиков', eachInstrumentCoverage: 'По каждой акции пригодны ≥80% наблюдавшихся срезов',
  };
  const pct = (n: number | null) => n === null ? '—' : `${(100 * n).toFixed(1)}%`;
  const bps = (n: number | null) => n === null ? '—' : `${(n / 100).toFixed(3)}%`;
  const text = `# Качество биржевого пилота\n\n${result.status === 'PASS' ? 'Рабочие ориентиры качества данных выполнены.' : 'Данных недостаточно по одному или нескольким рабочим ориентирам.'} Это проверка записи; прибыльность стратегии не измерялась.\n\n${Object.entries(result.checks).map(([key, passed]) => `- ${passed ? 'Пройдено' : 'Не пройдено'}: ${names[key as keyof typeof names]}`).join('\n')}\n\n| Акция | Пригодных / наблюдавшихся срезов | Покрытие | Безубыточность, медиана | Безубыточность, p95 |\n| --- | ---: | ---: | ---: | ---: |\n${result.rows.map(r => `| ${r.ticker} | ${r.usableTicks}/${r.observedTicks} | ${pct(r.coverage)} | ${bps(r.breakEvenMedianBps)} | ${bps(r.breakEvenP95Bps)} |`).join('\n')}\n\nПропущенное время проверяется отдельно от пригодности полученных срезов. Порог безубыточности — расчёт по текущему стакану с расходами, без прогноза будущей прибыли.\n\n[Расходы и подробности записи](${path.join(directory, 'REPORT.md')}) · [Причины исключения и проверки](${path.join(directory, 'quality.json')})\n`;
  writeFileSync(path.join(directory, 'quality.json'), `${JSON.stringify(result, null, 2)}\n`, { mode: 0o600 });
  writeFileSync(reportPath, text, { mode: 0o600 });
  return { ...result, reportPath };
}
