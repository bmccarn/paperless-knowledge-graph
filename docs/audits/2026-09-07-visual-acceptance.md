# Issue 16 visual UI acceptance

Operator: Codex, September 7, 2026. User explicitly requested computer/browser-control visual end-to-end testing. This record supplements automated browser tests; it does not certify production corpus or model accuracy.

## Environment and evidence

- Actual Chrome browser control, tab 419352174, session `KG acceptance`, localhost 3107.
- Built production Next.js standalone frontend; disposable localhost fixture on 8485. Desktop 1900×823 and phone 390×844, dark and light themes; temporary viewport restored.
- Manual screenshots and DOM/accessibility observations are retained in this conversation's browser-control tool results, captured per flow. They are separate from automated screenshots in ignored `frontend/.browser-artifacts/`.
- Initial implementation 498f6f8, followed by catalog/accessibility fixes described below. No production mutation was performed through this fixture.

## Observed flows

| Surface | Actions and visible outcome | State |
| --- | --- | --- |
| Navigation and themes | All seven routes opened; desktop icon controls gained accessible names. Dark/light toggle, phone bottom navigation and return to default desktop size worked. | 320px clipped the theme button; fixed and regression passes, final visual check pending |
| Chat answer | Strict synthetic question streamed activity then final answer, trust/claim ledger, exact source link and source sheet. Correct excerpt, title and Paperless destination visible. | Passed |
| Conversation history | Renamed conversation persisted across route changes; new chat cleared old answer; switching restored saved answer; mobile history opened and closed; desktop hide/show worked. | Passed; delete and final description-label check pending |
| Chat controls | Fast/Deep/Strict switching, model menu, copy and automatic insurance-hub question shortcut worked. Failed stream never displayed unsupported draft, showed connection-loss guidance, and left composer usable. | Passed; timeline event display pending |
| Document detail/review | Citation opened correct raw OCR/chunk. Synthetic report opened, reindex refreshed processing time and preserved open report, resolution required note and recorded reviewed receipt. | Passed |
| Catalog | Last page displayed 301–303 of 303; search recovered to one correct document; phone cards fit. | Passed: final error state has no stale counts/facets, sort/type/selection and two-document synthetic batch reindex also passed; genuine empty result distinguished from error |
| Graph | Canvas rendered in 2D and 3D; fit, entity selection, two repeated expansions kept 4 nodes/3 relationships; Person filter showed 1/0; source inspector preserved literal markup and marked inference. | Passed |
| Graph search | 120 matches paged in groups of 50; page-two selection opened entity 050. Failed subsequent search preserved the previous search label/results and displayed a clear error. | Passed |
| Entity review | Three separate synthetic candidates exercised merge, keep split and ignore; each disappeared and displayed its result. Refresh retained decisions. Steward task completed with summary; empty candidate state rendered. | Passed for fixture/API UI contract |
| Hubs | Last page 301–303 of 303, domain change reset to page one, insurance question shortcut submitted the intended text in chat. | Passed for navigation/paging; fixture is not a domain relevance oracle |
| Debug | Connected indicator, INFO/ERROR filtering, pause/resume, auto-scroll toggle and clear all worked. | Passed |
| Dashboard | Cards/coverage rendered; full-reindex confirmation text and Cancel worked; ordinary synthetic sync admitted and completion appeared. | Pending final task-payload check |
| Production | Deployed read-only navigation/query verification must use intended immutable release. | Pending release |

## Reproduced defects and fixture limitations

1. Failed catalog search reused the prior count and pagination while claiming “No documents found.” Fixed: explicit unavailable state, no stale facets/pagination or false empty result. Regression `failed catalog searches do not claim an empty result or reuse old totals` ran red and passes after rebuilding.
2. Icon-only navigation/chat/document controls lacked accessible names. Added names and current-route state; browser control now identifies navigation by function.
3. Radix warned that source/history sheets lacked descriptions. Added concise accessible descriptions; final rebuilt visual check pending.
4. Initial fixture omitted required `docs_with_embeddings` and task progress fields, producing NaN coverage and blank task counts. These were fixture defects; supplied the real response fields rather than claiming a production failure.
5. The browser emitted a Three.js duplicate-import warning while both 2D and 3D worked. No uncaught application error was observed. Synthetic 503/stream failures were deliberately induced and tested.

6. At 320px the bottom navigation clipped the theme button. Fixed equal-width flexible tabs and shortened the visible Dashboard caption to Home while retaining its accessible name. Added a red-to-green viewport-boundary and theme-toggle browser regression.

## Remaining acceptance

Complete the pending interactions above, rerun checks on the final revision, and visually verify deployed read-only/live-query flows. No local screenshot or mocked answer proves real source support, successful migration or writer drain.
