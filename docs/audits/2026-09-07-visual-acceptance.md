# Issue 16 visual UI acceptance

Current status at 2026-09-07 19:21 UTC: live release `a9088be` (source-audit v9) is deployed through GitOps PR144. Flux is ready on `000b82d`; both pods are ready with zero restarts and verified image digests. All 15 runtime checks passed, including absent output caps, 90/120-second audit deadlines, unchanged models and all 453 original review rows. All local visual flows remain passed; their frontend tree is unchanged. The original insurance question remains unaccepted: its last v8 run exposed source starvation now fixed in v9. A fresh native exact-pack diagnostic awaits the specific approval requested by automatic review. Ordinary corpus migration, vector reconciliation and schedule restoration remain pending. Earlier observations below are historical checkpoints.


Operator: Codex, September 7, 2026. User explicitly requested computer/browser-control visual end-to-end testing. This record supplements automated browser tests; it does not certify production corpus or model accuracy.

## Environment and evidence

- Actual Chrome browser control, tab 419352174, session `KG acceptance`, localhost 3107.
- Built production Next.js standalone frontend; disposable localhost fixture on 8485. Desktop 1900×823 and phones 390×844 / 320×740, dark and light themes; temporary viewport restored.
- Manual screenshots and DOM/accessibility observations are retained in this conversation's browser-control tool results, captured per flow. They are separate from automated screenshots in ignored `frontend/.browser-artifacts/`.
- Initial implementation 498f6f8, followed by catalog/accessibility fixes described below. No production mutation was performed through this fixture.

## Observed flows

| Surface | Actions and visible outcome | State |
| --- | --- | --- |
| Navigation and themes | All seven routes opened; desktop icon controls gained accessible names. Dark/light toggle, phone bottom navigation and return to default desktop size worked. | Passed: all eight controls fit at 320px; Home visible/accessibility names agree; theme toggles |
| Chat answer | Strict synthetic question streamed activity then final answer, trust/claim ledger, exact source link and source sheet. Correct excerpt, title and Paperless destination visible. | Passed |
| Conversation history | Renamed conversation persisted across route changes; new chat cleared old answer; switching restored saved answer; mobile history opened and closed; desktop hide/show worked. | Passed: deletion persisted and final sheet descriptions emitted no new warning |
| Chat controls | Fast/Deep/Strict switching, model menu, copy and automatic insurance-hub question shortcut worked. Failed stream never displayed unsupported draft, showed connection-loss guidance, and left composer usable. | Passed: timeline date/event also visibly rendered |
| Document detail/review | Citation opened correct raw OCR/chunk. Synthetic report opened, reindex refreshed processing time and preserved open report, resolution required note and recorded reviewed receipt. | Passed |
| Catalog | Last page displayed 301–303 of 303; search recovered to one correct document; phone cards fit. | Passed: final error state has no stale counts/facets, sort/type/selection and two-document synthetic batch reindex also passed; genuine empty result distinguished from error |
| Graph | Canvas rendered in 2D and 3D; fit, entity selection, two repeated expansions kept 4 nodes/3 relationships; Person filter showed 1/0; source inspector preserved literal markup and marked inference. | Passed |
| Graph search | 120 matches paged in groups of 50; page-two selection opened entity 050. Failed subsequent search preserved the previous search label/results and displayed a clear error. | Passed |
| Entity review | Three separate synthetic candidates exercised merge, keep split and ignore; each disappeared and displayed its result. Refresh retained decisions. Steward task completed with summary; empty candidate state rendered. | Passed for fixture/API UI contract |
| Hubs | Last page 301–303 of 303, domain change reset to page one, insurance question shortcut submitted the intended text in chat. | Passed for navigation/paging; fixture is not a domain relevance oracle |
| Debug | Connected indicator, INFO/ERROR filtering, pause/resume, auto-scroll toggle and clear all worked. | Passed |
| Dashboard | Cards/coverage rendered; full-reindex confirmation text and Cancel worked; ordinary synthetic sync admitted and completion appeared. | Passed: 1/1, one processed, zero skipped/errors, elapsed time and dismissal rendered |
| Production | Deployed read-only navigation/query verification must use intended immutable release. | In progress on deployed bbbf82d: dashboard counts, stale-fingerprint notice and active-canary controls visually confirmed |

## Reproduced defects and fixture limitations

1. Failed catalog search reused the prior count and pagination while claiming “No documents found.” Fixed: explicit unavailable state, no stale facets/pagination or false empty result. Regression `failed catalog searches do not claim an empty result or reuse old totals` ran red and passes after rebuilding.
2. Icon-only navigation/chat/document controls lacked accessible names. Added names and current-route state; browser control now identifies navigation by function.
3. Radix warned that source/history sheets lacked descriptions. Added concise accessible descriptions; final rebuilt visual check passed without new missing-description warnings.
4. Initial fixture omitted required `docs_with_embeddings` and task progress fields, producing NaN coverage and blank task counts. These were fixture defects; supplied the real response fields rather than claiming a production failure.
5. The browser emitted a Three.js duplicate-import warning while both 2D and 3D worked. No uncaught application error was observed. Synthetic 503/stream failures were deliberately induced and tested.

6. At 320px the bottom navigation clipped the theme button. Fixed equal-width flexible tabs and shortened the visible Dashboard caption to Home with a matching accessible name. Added a red-to-green viewport-boundary and theme-toggle browser regression.

## Remaining acceptance

All local interactions and final-revision checks passed (328 real-datastore backend tests, 12 frontend regressions, 26 automated browser scenarios). Complete deployed read-only/live-query flows; live Chrome tab 419352177 uses the immutable bbbf82d release. No local screenshot or mocked answer proves real source support, successful migration or writer drain.

## Deployed observations in progress

- Immutable bbbf82d release, Chrome tab 419352177, desktop 1900×767. Dashboard displays actual 891-document coverage, expected fingerprint migration warning, and active canary counts; competing sync actions disabled.
- Catalog type filter plus search displays matching insurance records and pagination. Graph rendered a 674-node/984-relationship real sample in 2D; Organization filter rendered 144/209 in 3D. Search and source selection opened the document inspector with relationship direction, provenance explanation and document links. Two neighbor expansions stabilized at 683 nodes/1,029 relationships without duplication. These are visible/rendered counts, not a dense-graph performance benchmark or semantic-accuracy claim.

- Follow-up 5d7f41b: live hubs exposed unnamed source icons and nested navigation controls. Source/question controls now use one Button-asChild anchor each; source links name their document; dashboard search submit has an accessible name. All 26 browser scenarios, 12 frontend regressions, lint/typecheck/build and both independent reviews pass. Browser-control tab 419352174 on immutable local build port 3108 confirmed single named links, unchanged visual layout, and the common-question shortcut actually submitted the expected text and rendered a final answer. This follow-up remains undeployed while the large canary runs.

- Live review suggestions rendered both names, descriptions, typed scores and review actions; no production review decision was submitted. Live hub pagination reached 85–92 of 92 (page 8 of 8) with Next/Last disabled. Debug connected, ERROR filter showed zero lines, and Pause changed to Resume. Rebuilt dashboard search submitted its text to chat and received a fixture final answer.

- Live Paperless source link opened the correct document destination in a new tab and reached Paperless sign-in with that document preserved in the return URL. This browser is not authenticated to Paperless; its separate authenticated document viewer was not exercised. KG raw OCR, indexed chunks and source navigation were visually verified. Live browser diagnostics contained only the known Three.js duplicate-import warning, with no application errors.

## Additional live defects and closure work

- The large source canary completed all 50 windows with 192 exact OCR chunks; source coverage, typed vector bindings and 14 identity/history controls passed. Its extraction did not propose a positive alias pair, so this did not close alias acceptance.
- A stable targeted strict query retrieved its requested source first but omitted it from bounded synthesis/audit selection. PR20 preserves explicit IDs and quoted titles, including duplicate-title and multi-document cases. Live retest remains pending rollout.
- Targeted insurance reprocessing corrected the provider identity and preserved 454 immutable history rows and 3267 other-document evidence records. Its role edge still lacked the provider metadata quote. The reviewed follow-up attaches only exact, accepted provider-field source spans; final live confirmation is pending.
- Real short-source alias controls distinguished proposal loss from persistence: the unchanged prompt combined a full name/acronym and retained no proof; the clarified final prompts retained one independently affirmed, current-direct proof. Full-pipeline persisted/repeat acceptance is still required.
- All follow-up deployments continue through GitOps. Production remains on the initial accepted release with sync/steward schedules paused while final representative gates are completed.

## Deployed strict-query diagnostic on b1eb57e

At 1900×767 in Chrome through computer control, submitted the original insurance question in Strict mode on the deployed application. Streaming status and keepalives remained visible; the composer became usable after completion in 169.1 seconds. The answer was withheld with an unsupported label, 35/35 units audited and 23 supported/12 unsupported. The visible ledger exposed standalone horizontal rules, split numbered headings and a number detached at `No.`; these drove the PR23 segmentation and prompt follow-up. The saved conversation contains both messages, matching finalization/ledger metadata and strict mode.

Opened the grouped insurance source button, inspected the settled Source detail sheet screenshot, its retrieved excerpt, title/type/date and Paperless link, then closed it successfully. Private source text and screenshots stay in the operator conversation/private evidence, not this repository. This diagnostic does not close the original-query acceptance gate; repeat on the new release and inspect a useful source-grounded result and its persistence.

On ac65ee0, a new live Strict conversation with the original insurance question completed in44.3seconds, with15/15claims audited,13supported, no token/time-limit error, and a usable composer. Two evidence failures withheld the answer: a full-year assertion supported only by a two-digit-year quote, and an invalid reference. The saved conversation retains the complete ledger privately. These observations drove PR24's precise repair feedback and dated source-observation prompts. A private native replay subsequently returned two fully supported observations with a current-status qualification; final deployed chat/source-sheet/history verification is still pending.

## Latest live query checkpoints

- On `1d59cf0` (v7), the original insurance question completed in 109.6 seconds with one four-unit audit batch unchecked. Runtime logs identified the 45-second source-auditor timeout. The UI finished cleanly, exposed the incomplete ledger and restored the composer. A separate targeted source query was qualified with exact source support; its original supported-only acceptance script was corrected in a reviewed supplemental assessment without modifying the captured response.
- GitOps PR143 applied `5885269` (v8), including failed-cache retry handling, bounded answer context and finite 90/120-second call/wave settings. Both image digests and all runtime/history checks passed. The new targeted query completed in 51.8 seconds: three supported historical assertions, seven exact OCR references solely to the requested source, correct citation/candidate/answer digests, and a stable current source.
- In a new live Chrome conversation at 1900×767, the exact original insurance question in Strict mode completed in 85.7 seconds. All ten units were audited; six were supported and four lacked valid references. No token-limit or timeout error occurred. The final failure and full ledger rendered, conversation persisted, and composer became usable. This is a reproduced failure, not original-query acceptance.
- Reconstructed all 90 original evidence-item hashes privately. The mixed audit batch omitted every window containing one policy assertion's identifier; single-assertion ranking found the correct window. PR26's per-assertion source reservation restores that window locally without changing source bytes or the 28,000-character budget. Native exact-pack replay was stopped before execution by automatic approval review and awaits the specifically requested payload/destination authorization.

The insurance acceptance verifier also needed a correction: reference offsets and content digests belong to individual OCR chunks. The revised private checker binds each original evidence-item hash, validates exact chunk-relative references, and separately checks each quotation against fresh full Paperless OCR. Its independent review does not replace the still-pending successful original UI query.
