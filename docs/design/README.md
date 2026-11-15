# Design reference

This folder holds the reference material for the interface redesign in direction C, "Margin". The [redesign issue #109](https://github.com/bmccarn/paperless-knowledge-graph/issues/109) is the specification. When a file here disagrees with the issue, the issue wins. All names, documents, amounts and counts in these files are invented.

## Files

| File | What it is |
|---|---|
| `tokens.css` | The frozen input of the mockups: the design tokens for both themes as the mockups use them. The token source for the app is `frontend/src/app/globals.css`; change tokens there. |
| `answer-mockup.html` | A static Ask page in direction C. It shows a Verified answer with claim underlines, margin notes and a source list, then short Documented, Partial and Withheld examples. Open it in a browser and resize it to see the three layouts (margin column at 1180 px and wider, inline notes, phone). Change `data-theme` on the `<html>` element to switch themes. It embeds Geist, Geist Mono, Geist Serif and Source Serif 4, which are licensed under the SIL Open Font License 1.1. |
| `quality-bar.md` | The checklist every new screen must pass before review. |
| `categories.md` | Document-type labels and colors, and the relationship families with their keyword prefixes. |
| `components.md` | The app-level components with their content, variants and states. |
| `FONT-LICENSE.txt` | Copyright notices and SIL Open Font License 1.1 for the embedded fonts. |
| `README.md` | This file index, scope, and reference limitations. |
| `graph/01-landing.html` | Desktop graph landing wireframe. |
| `graph/02-neighborhood.html` | One-hop neighborhood and inspector wireframe. |
| `graph/03-hub-caps.html` | Hub caps and aggregate groups wireframe. |
| `graph/04-path.html` | Path view wireframe. |
| `graph/05-filters.html` | Graph filters wireframe. |
| `graph/06-evidence.html` | Link evidence wireframe. |
| `graph/07-phone-entity.html` | Phone list-first entity wireframe. |
| `graph/08-phone-graph.html` | Phone graph and bottom sheet wireframe. |
| `graph/09-table.html` | Accessible table view wireframe. |
| `graph/wf.css` | Shared wireframe styles. Desktop frames are 1440 × 900; phone frames are 390 × 844. Yellow notes explain each screen. |

## Reference limits

The answer mockup opens directly from disk without network access. Its fonts and styles are embedded. It is a static visual reference: controls, claim selection, and source links do not implement the application behavior. On phones, claim 2 is the fixed active example. The graph wireframes use the local `graph/wf.css` file and system fonts.

The mockup establishes the visual direction, not completion of the quality bar. Later tickets implement all seven answer states, interaction, accessibility, and the final layouts. The specification controls differences such as sidebar width, phone targets, and source metadata. Page counts in the mockup are invented decoration; the answer API does not supply page numbers.

Run `node frontend/tests/design-reference-ui.mjs` after installing the frontend dependencies and Playwright Chromium to check the file index, local links, embedded fonts, both themes, and offline rendering.

## How to read the graph wireframes

The wireframes show structure and behavior. They do not show the final look.

- **Colors and shapes.** The wireframes use placeholder hex colors and an earlier shape set. The issue's palette tables set the real node colors, shapes and icons. The issue proposes a pentagon for agreements and a star for time; the graph mockup settles them. A high-fidelity mockup in direction C replaces these wireframes before graph build work starts.
- **Status colors.** Inferred links appear in amber in some wireframes. In the build, inferred links use a dash and a word, never a color.

These wireframe parts are not part of this redesign:

| Wireframe | Not built |
|---|---|
| 01 landing | The archive map, "Life areas", the "Check the graph" queue, "Recently added", "Pinned" and "Saved views". The landing has search, Recent and Most connected only. |
| 02 neighborhood | Hops 2 and 3, the "Documents as nodes" switch, the "Co-mentions" switch, "Save view" and "Pin". The view is one hop with documents as evidence. |
| 03 hub caps | Hops 2 and 3 and "Save view". The rank selector is not built; ranking is fixed. |
| 04 path | "Save view", maximum path length 5, "Allow inferred links", and "Allow same document hops". Paths have at most 4 links, allow inferred links, and never pass through documents. "Include the owner" replaces "Avoid the owner". |
| 05 filters | The "Entity types" group, "mentioned together" links and "show hidden as ghosts". Filtered nodes are removed. |
| 06 evidence | "Show in text". |
| 07 phone entity | "Pin" (the star). |
| 09 table | "Export CSV" and "Space selects for compare". |

All screens also show a "Map" view and a breadcrumb with a "Map" root. Neither is built: the views are Neighborhood, Path and Table, and Back and Forward replace the breadcrumb.
