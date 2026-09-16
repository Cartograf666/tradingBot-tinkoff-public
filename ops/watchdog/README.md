# Market-study watchdog

Stateless Cloudflare Worker, which every five minutes on business days observes
only GitHub Actions run metadata for the exact public repository
`Cartograf666/tradingBot-tinkoff-public`, branch `main`, and workflow
`market-study.yml`. It has no broker, storage, or repository write credentials.

The default is deliberately inert: `WATCHDOG_ENABLED=false`. The sole secret is
`GITHUB_DISPATCH_TOKEN`; use a fine-grained GitHub token limited to that repository
and **Actions: write**. Create the Cloudflare API token for only the selected
account and the Worker deployment permissions it needs. Keep both out of
`wrangler.jsonc`, source control, logs, and HTTP responses. No KV namespace is
used or required.

## Scheduling policy

All decisions use `Europe/Moscow` and exclude Saturday/Sunday.

| MSK window | Dispatch |
| --- | --- |
| 08:20–08:49 / 13:20–13:49 | `arm` for `early` / `late` |
| 08:50–09:15 / 13:50–14:15 | late fallback `campaign` |
| 08:10–08:15 and 19:20–19:35 | `report` (requires the report mode added to the workflow) |

For a market block the Worker allows at most three launches whose GitHub metadata
was created on that Moscow date; each report session (morning and evening) allows
two. Report metadata before noon MSK belongs to the morning session, and later
metadata belongs to the evening session, so a successful morning report cannot
suppress the evening report. It never dispatches when a matching run is queued,
pending, waiting, requested, action-required, or in progress, including a stale
active run from an earlier day. It also stops after one metadata-observed success
for the target market block or report session. GitHub workflow concurrency remains
the final overlap guard.

Recent history starts at the prior Moscow midnight, so old completed runs cannot
make the bounded query grow forever. Every active GitHub status is additionally
queried without that date filter: an old active block is still a stop condition.
Each query is at most three pages of 100. If GitHub reports more results than were
read, or any GitHub read/auth/timeout response is invalid, the Worker fails closed
and dispatches nothing. A dispatch failure is reported as a failed cron invocation
and can use the next five-minute slot inside its window.

`success_observed` means only that GitHub run metadata says `success`; it does
not establish a captured stream, data completeness, or report correctness.
The Worker stores no raw data. Its only HTTP response is `GET /health`-style
readiness JSON (`ok`, `enabled`, `configured`); there is no public dispatch route.

## Test and deployment

Run locally with a supported Node version:

```sh
node --test ops/watchdog/test/*.test.js
```

After reviewing the workflow's `report` input, authenticate Wrangler and set the
secret without printing it:

```sh
cd ops/watchdog
npx wrangler secret put GITHUB_DISPATCH_TOKEN
npx wrangler deploy
```

Set `WATCHDOG_ENABLED` to `true` through the Worker environment-variable UI (or a
reviewed deployment configuration), then open the deployed workers.dev `/health`
URL and verify it reports both enabled and configured. Every other public path is a
404 and cannot dispatch. Deploying the Worker alone cannot start a campaign while
the variable remains `false`. The parent workflow must accept `mode=report`; until
then report dispatches will be rejected safely by GitHub.
