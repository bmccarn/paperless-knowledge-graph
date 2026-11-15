# App-level components

These components are built for the redesign on top of the shared primitives. The redesign issue describes where each one is used.

| Component | Content | Variants and states |
|---|---|---|
| `StatusLabel` | icon, word and token for a state | seven answer states, four claim results; sm, md |
| `EvidenceLevel` | word plus three-segment meter | high, medium, low |
| `VerdictLine` | state, reason, level, elapsed time | seven states |
| `StreamStatus` | step label, elapsed seconds | running, done, failed, reduced motion |
| `ClaimProse` | prose with underlines and claim numbers | active claim, no claims |
| `MarginNote` | number, result, quote, title, date | margin, inline, phone; active |
| `ClaimTable` | claim, result, source | stacked on phone; selected; left out |
| `SourceList` | numbered sources with the claims each supports | selected |
| `QuestionCoverage` | existing component, restyled | complete, partial, unavailable |
| `KindShape`, `TypeDot` | shape, color, icon for kinds and types | fill, outlined, faded |
| `GraphLegend` | kinds and link styles in view | open, collapsed, sheet |
| `EmptyState` | one sentence, one action | page, panel, table |
| `AppShell`, `SidebarNav`, `TabBar`, `MoreSheet` | navigation | current page, health |
