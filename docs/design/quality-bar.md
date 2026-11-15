# Quality bar

Every new or changed screen meets each rule below before review. The reviewer checks the pull request against this list. The values come from `frontend/src/app/globals.css`, the token source, and the redesign issue.

## Evidence and trust

- Every answer shows one of the seven states (Verified, Documented, Partial, Withheld, Not checked, Retry needed, Failed) with its icon and word.
- The answer state comes from the backend disposition, never from the answer text.
- No screen shows a confidence percentage. The evidence level is a word: high, medium or low.
- A Withheld answer shows no answer text.
- Each verified claim shows its quote and the document title, plus the date when the source has one.
- A link in the graph says "Supported", "Inferred" or "Stated in N, inferred in M". A link without a score says "No score".

## Color and contrast

- Every color comes from a token. No raw palette class or color literal appears outside `globals.css`.
- Text reaches 4.5:1 on its background. Control borders, the focus ring, status rules and node colors reach 3:1.
- Color is never the only signal. A state has an icon and a word. A node kind has a shape and a label. An inferred link has a dash.
- Document types appear as a colored dot with a text label, never as colored text.
- Both themes pass every check. Dark is the default.

## Type

- The question heading and page titles are Geist Serif at weight 400. Answer prose and document text (quotes, document titles, OCR text) are Source Serif 4. All interface text is Geist Sans. Geist Mono is only for log lines and identifiers.
- Text is 12 px or larger. Phone text inputs are 16 px.
- Each page has one `<h1>` in the page-title style.
- Sentence case everywhere. No all-caps labels.
- Tables, counts, dates, IDs and timers use tabular figures.

## Layout and spacing

- Spacing uses the 4 px grid: 2, 4, 6, 8, 12, 16, 20, 24, 32, 40, 48, 64.
- Radius is 4 px for chips, 6 px for controls, 10 px for panels and 12 px for dialogs.
- In-page surfaces have no shadow. Only overlays use the overlay shadow.
- The page works at 390 px wide with no horizontal scroll. Phone touch targets are at least 44 px.
- Sticky bars never cover the focused control.

## Interaction and accessibility

- Every interactive element is reachable with the keyboard and shows the 2 px focus ring.
- Every icon-only button has an accessible name.
- Toggles expose `aria-pressed`; expanders expose `aria-expanded`; the sorted column exposes `aria-sort`.
- Destructive actions ask for confirmation, and dialogs return focus to the control that opened them.
- Status changes (streaming, toasts, filter counts) are announced through a live region.
- Motion runs only in answer to a user action. With reduced motion on, nothing animates.
- axe reports no violations for the screen in any state that the screenshot checks capture.

## States

- Each screen has a designed loading state, empty state and error state. An empty state is one sentence and one action.
- An error says what failed and offers Retry. The previous content stays visible where possible.

## Review evidence

- The CI comparison index is the pull request's before and after screenshots, at 1440 × 900 and 390 × 844 in both themes.
- The screenshot and accessibility checks pass in CI. Any new baseline is named in the pull request and approved by its author.
- The pull request lists the browser checks it updates because it renames or removes visible text.
