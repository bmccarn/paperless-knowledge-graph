# Graph views

An addendum to the graph sections of the [redesign specification, issue #109](https://github.com/bmccarn/paperless-knowledge-graph/issues/109), written for [issue #193](https://github.com/bmccarn/paperless-knowledge-graph/issues/193). It is a proposal for the owner's approval. Once approved, it amends #109 where the section "Changes to the specification" says so, and #109 controls everything else. The mockup is `views-mockup.html` in this folder; screens are named below as `views-mockup.html#<screen>`.

The graph today draws one entity and its direct neighbors on a ring, with no links between the neighbors. It reads as a wheel, not a network. This addendum puts six views of the same data behind one switcher. Each view answers one question that the others cannot.

| View | The question it answers | Data | Renderer |
|---|---|---|---|
| Focus | What is this entity linked to, and how do those things link to each other? | `GET /graph/node/{id}?summary=1`, new `GET /graph/subgraph` | force-graph canvas, fixed positions (today's) |
| Explore | What lies two or three steps away, and where do the links loop back? | the summary route per expanded entity, `GET /graph/subgraph` | force-graph canvas, fixed positions |
| Timeline | When was each link documented, and what arrived when? | the summary route only | SVG and HTML |
| Table | Every link of this entity, sorted and filtered (#140) | the summary route | HTML table |
| Paths | How are these two entities connected? (#143) | `GET /graph/paths` | SVG, layered (#143) |
| Overview | What is in the archive, and which parts hang together? | new `GET /graph/overview` | force-graph canvas, fixed positions; a prototype first |

Names, documents and counts in the mockup are invented. The numbers in this text come from the real archive: 7,358 nodes and 14,883 links, of which 79 percent are document mentions (so about 3,100 links join two entities). The largest entity is one Person with 660 links, whose summary response is 373 KB. Path searches finish in under 0.7 s.

## Rules that hold in every view

- Documents are evidence, never nodes. No view draws a Document node or a `MENTIONS` link. Documents appear in the evidence shelf, the inspector, the Timeline's document band and marks, and the link evidence.
- The encoding is the one in #109 and `mockup.html#encoding`: shape, color and icon by kind; radius from evidence documents; link width from supporting documents; inferred links dashed 6 on, 4 off; arrows and link labels only on the selection and on paths. One exception, in Overview at map zoom, is stated in that section.
- Positions are computed once per change by a pure function and never move afterwards. No view runs a continuous simulation, and no node can be dragged. The same URL draws the same picture.
- Every view has the same header, filters sheet, inspector (380 px), legend (only what is in view) and evidence shelf where the view has a focus entity.

## The switcher

**Place.** The switcher replaces the "View" segmented control at the right of the graph header, before Filters (`views-mockup.html#focus`). It holds six buttons in this order: Focus, Explore, Timeline, Table, Paths, then a 1 px divider and Overview. Overview is set apart because it shows the whole archive, not the focus entity. Labels are words only; each button's tooltip names its key ("Timeline, key 3").

**States.** The current view is the pressed button (`--accent-solid` fill). With no focus entity (Overview opened from the landing page), Focus, Explore, Timeline and Table have `aria-disabled="true"` in `--fg-subtle`. Pressing one announces "Pick an entity on the map or search first." Paths stays enabled because it has its own pickers. `views-mockup.html#switcher` shows every state.

**Keyboard and screen reader.**
- The switcher is `role="toolbar"` with `aria-label="View"`. It is one Tab stop with a roving tabindex: Left and Right move, Home and End jump, Enter and Space switch. Each button has `aria-pressed`.
- Keys 1 to 6 switch views from anywhere on the graph page, in switcher order. The #109 keys stay as aliases: `n` is Focus, `t` is Table, `p` is Paths. No key acts while a text field has focus. Timeline has no letter key, because `t` is taken.
- After a switch, keyboard focus stays on the switcher, and the live region says "<View> of <focus name>", for example "Timeline of Policy HM-4471 (auto)".

**Phone.** Below 768 px the header has no room for six buttons. The Graph tab of the phone entity page (#144) shows a 44 px "View: Focus" button above the canvas. It opens a bottom sheet with six 56 px rows, each with the view name and its question in one line (`views-mockup.html#phone-views`). On phone, Table opens the Links tab, Paths is the stepper list from #109, Timeline is a vertical list, Overview is a list of groups, and Focus and Explore are canvases.

## Shared state

**URL.** The #109 scheme stays. These values change or are added:

```
/graph?focus=<id>&view=explore|timeline|table|path|overview&sel=<id, link key or grp:<id>>
      &x=<id>,<id>,…        Explore: expanded entities, in the order they were expanded
      &cross=0              Focus: links between neighbors hidden (shown by default)
      &from=2024-01&to=2026-11 …   the #109 filters, shared by every view
```

- Focus is the default view, so it has no `view` value. `view=neighborhood` from older links opens Focus.
- `x` holds at most 20 entity ids. With 36-character ids that is under 800 characters.
- `sel=grp:<id>` selects an Overview group by the id of its lead entity (see Overview).

**History.**
- Pushed (Back undoes them): a focus change, a view change, an Explore expansion or collapse.
- Replaced: a selection, a filter, the Timeline's date range (it is the document date filter), the cross-link switch.
- A focus change clears `x` and `sel`. A view change keeps `sel` when the new view contains that entity or link, and clears it otherwise.

**Shared parts.**
- The filters sheet applies to every view. The Owner group's default depends on the view: shown in Focus, Timeline and Table; hidden in Explore and Paths, because the owner links to almost everything and would pull every expansion toward one node. Overview has no per-document records and always leaves out the owner, so its sheet disables the Owner, Document date, Document type and Confidence groups and says "The map does not use document dates, types or confidence, and leaves out the owner." Their values stay in the URL for the other views.
- The inspector shows the selected entity, link or (in Overview) group in every view. A link between two neighbors opens the same link evidence as any other link (#139).
- The legend lists only what the current view draws, as #109 says. Timeline and Overview add their own rows, named in their sections.
- The evidence shelf stays under Focus, Explore (for the root entity) and Timeline. Table, Paths and Overview do not show it.

## Focus

The capped one-hop neighborhood from #109, unchanged in layout, caps, pills, sectors and labels, plus the links among the neighbors that are drawn (`views-mockup.html#focus`).

**Question.** What is this entity linked to, and how do those things link to each other? A ring alone answers only the first half. The cross-links show which neighbors share an employer, a policy or an address, and that is the network the owner asked for.

**Data.** After the summary arrives and the ring is laid out, the page requests `GET /graph/subgraph?ids=<drawn neighbor ids>` (see "New backend routes"). Pills and their hidden members are not sent. When "+N more" adds neighbors, the page requests the route again with the new set.

**What is drawn.**
- A cross-link is any link whose two ends are drawn neighbors and that the summary did not already draw. Evidence-family links and Document ends are excluded, as everywhere.
- Shape: a quadratic curve between the two neighbors. Its control point sits on the bisector of the shorter arc between them, halfway between the chord's midpoint and the ring. Each curve bows toward the ring and away from the focus, so the center stays clear and the curves never reach the labels outside the ring. Two neighbors that face each other across the ring get a curve that passes the focus at a quarter of the ring radius.
- Order of drawing: sector arcs, cross-links, focus links, nodes, labels. Width, dash and color follow the encoding. Two links between the same pair curve at ±0.15 more, as #109 says for parallel links.
- Selecting a neighbor keeps its own focus link and its cross-links at full strength and fades the rest, as for any selection. Its cross-links get arrows and labels ("EMPLOYED_BY · 6 docs").
- Cap: at most 60 cross-links, strongest first (supporting documents, then key). Over the cap, the caption adds "Showing the 60 strongest of 214 links between neighbors."
- Switch: "Links between neighbors" is a switch button (`aria-pressed`) in the canvas toolbar above the zoom buttons. Off sets `cross=0`.

**Accessible alternative.** The canvas name gains the count: "Neighborhood of Policy HM-4471 (auto): 16 links in 8 families, 3 inferred; 11 links between neighbors." The inspector of a selected neighbor gains a group "Linked to other neighbors" with one row per cross-link, which is the keyboard route to them.

**States.**
- Loading: the ring draws first. If cross-links take more than 1 s, the status line says "Loading links between neighbors".
- None: the caption adds "No links between these neighbors."
- Error: the ring stays; the caption says "Links between neighbors did not load." with a Retry button.

**Phone.** The same canvas in the Graph tab, with at most 30 cross-links (`views-mockup.html#phone-focus`). At 390 px the ring has no room for 16 labels, so labels go in evidence order and a label that would overlap an earlier one is skipped, as `mockup.html#phone-graph` already shows. Hover, selection and the focus are still always labeled.

## Explore

A network that grows in place from one entity (`views-mockup.html#explore`). It is the one view where hops 2 and 3 exist.

**Question.** What lies two or three steps away from this entity, and where do the links loop back? Focus cannot show a second hop, and Paths needs both ends chosen in advance.

**Start.** The focus entity is the root, at the center. Its 8 strongest links (ranked as in Focus: supporting documents, then latest document date, then name, then id) sit on a ring at 160 graph units. They are spaced evenly from 12 o'clock in family order, then rank, so that families still sit together. There are no sector arcs and no pills.

**Expand and collapse.**
- A click or tap selects a node, as in every view. A hovered or selected node shows a "+N" handle on its outer side, or beside it when that spot is taken: N is the number of its links to entities not in view. Activating the handle expands the node. The same action is the inspector button "Show 6 more links from here", and the `+` key while the node is selected. `-` collapses it.
- An expansion fetches the node's summary (cached for the session), then adds up to 6 of its links that lead to entities not in view, strongest first. Links from the node to entities already in view are drawn too. Those loops are what makes Explore a network.
- Depth: the root is hop 0. A node at hop 3 cannot expand. Its handle reads "Open in Focus" and opens it as the new focus.
- Collapse removes the nodes that this expansion added, together with the nodes added by expanding them. A node added by an earlier expansion stays.
- The owner entity is skipped by expansions while the Owner filter hides it (the default in Explore).
- Caps: at most 150 entities in view, the #109 limit. An expansion that would pass it adds what fits and the caption says "This view holds at most 150 entities. Collapse a branch or open one entity in Focus." At most 20 expansions, the `x` limit.

**Layout rule.** A pure function of the summary data and the ordered `x` list. Nodes already placed never move.
1. A node at angle θ from its parent fans its new children on an arc around itself at radius 120. The arc is centered on θ and spans the smaller of 150° and 25° per child, in rank order. The root's children use the full ring.
2. Each child takes the first free candidate: radius 120, 150, 180, then 210, each tried at the arc angle and at ±12° and ±24°. Free means its shape stays at least 32 px from every placed node's shape, which leaves room for labels.
3. If no candidate is free, it takes radius 210 at +24°; a unit test reports overlaps in the fixture so they stay rare.
4. The camera fits once on the first draw. After an expansion it pans, without zooming, only when new nodes are off screen. With reduced motion the pan is instant.

**Data.** The summary route per expanded node; nothing new for nodes. After each change, `GET /graph/subgraph?ids=<all ids in view>` returns the links among them, including links between two branches that no single summary holds. Cross-links in Explore use the same cap of 60, strongest first.

**Trail panel.** A panel at the left of the canvas, "Explored from <root>", lists the root and each expansion in order: kind swatch, name, the number of entities it added ("+4"), and a Collapse button. It is the URL's `x` list made visible, the way to collapse a branch, and, with the inspector's link list, the accessible route through the view.

**Accessible alternative.** The canvas name says "Explore from <root>: N entities, L links, H hops." The trail panel and the inspector's "Linked in this view" group reach every node and link.

**States.**
- Loading: the expanding node shows its keyboard-focus ring until the new nodes draw. After 1 s the status line says "Loading links of <name>".
- Nothing to add: the handle reads "All links shown".
- Error: nothing moves; the trail row shows "Links of <name> did not load." with Retry.
- A root with no entity links shows the Focus message ("No direct links extracted…").

**Phone.** The canvas in the Graph tab with 6 starting links and 4 per expansion, at most 60 entities. The trail is a bottom sheet with a 56 px peek ("3 expansions"). A selected node's sheet has 44 px "Show more links" and "Collapse" buttons. `views-mockup.html#phone-explore` shows the trail peek.

## Timeline

One entity's links and evidence documents on a time axis, grouped by family (`views-mockup.html#timeline`).

**Question.** When was each link documented, and what arrived when? No other view shows time. A link has no date of its own; its dates are the dates of the documents that support it.

**Data.** The summary route only: each link's `records` give document ids, and the `documents` map gives each date and type. No new route.

**Layout.**
- An axis across the top runs from the month of the earliest dated document to the month of the latest, with one month of space at each end. A span of more than 18 months shows years with month ticks; a shorter span labels months.
- A document band under the axis has one 8 px dot per evidence document, in its document type color, stacked upward per month. A stack taller than six dots shows six and "+N".
- Below, one lane per family in the fixed family order, then one row per neighbor, ranked and capped as in Focus: 6 rows per family, 2 for a hub, then a "+N more" row that adds 25. A neighbor appears once, in the family that Focus gives it.
- The family name sits in a 176 px column beside its first row. A row has a 220 px name column (swatch and name), then a track for each link that Focus draws to that neighbor, at its documents' dates. Two or more links give one track each, 12 px apart, in key order. A track runs from the link's first to its last document, with the link's width and dash from the encoding. One mark sits at each document, in the color of the neighbor's kind: filled for a stated record, a hollow ring for an inferred one. Two documents in one month sit side by side.
- Documents without a date go to a "No date" column at the right end of the axis, after a gap.
- A link with more than 60 records draws one mark per month with the count beside it. This protects the 660-link entity.

**Interaction.**
- Hovering a mark shows the document title, type, date, support word and the link type of its track. A mark selects the link of its track and highlights its document in the evidence shelf. A name selects the neighbor.
- Dragging across the axis selects a date range. The range is the shared document date filter (`from`, `to`), so it also applies to Focus, Explore and Table. The From and To month controls in the toolbar set the same range by keyboard. Marks outside the range use `--graph-faded`.
- Each row is a button that names its content for screen readers: "Harbor Mutual Insurance, issued by, 9 documents from March 2024 to September 2026, all stated." A row with two or more tracks names each link in turn.

**Legend rows.** A strip under the rows: "Stated document" (filled mark), "Inferred document" (ring), the two line styles, "Marks take the color of the linked entity's kind.", and the document type dots in view.

**States.**
- Loading: the axis and name column draw from the summary at once; there is no second request.
- No dated documents: "None of the 12 documents has a date." The rows still show their marks in the No date column.
- No links: the document band and the Focus message.
- Error: as Focus, an inline alert with Retry over the previous view.

**Phone.** Time runs down the screen. Sections by year, then month, newest first; each document is a row with dot, title and date, and under it the links it supports ("Issued by Harbor Mutual Insurance, Covers 2019 Subaru Outback"). Rows are 48 px (`views-mockup.html#phone-timeline`).

## Table and Paths in the switcher

**Table (#140).** Unchanged, except that it is reached through the switcher (key 4, or `t`). In Explore it lists the root's links. Its "On canvas" column refers to the Focus canvas.

**Paths (#143).** Unchanged, except for the switcher button (key 5, or `p`) and its plural label: the view lists up to five paths. The URL value stays `view=path`. A switch to Paths sets A to the focus entity when A is empty.

## Overview

A zoomable map of the whole archive, in groups of entities that link to each other (`views-mockup.html#overview`). #109 deferred this view to a prototype. This section defines that prototype and its limits.

**Question.** What is in the archive, and which parts of it hang together? Every other view starts from one entity.

**Data.** The new `GET /graph/overview` (see "New backend routes"): every entity with at least one link to another entity, and those links merged per pair. Entities linked only to documents are counted, not drawn.

**Groups.** The browser forms groups in a Web Worker.
1. Label propagation over the entity links, owner excluded. Nodes are visited in id order; a node takes the label most common among its neighbors, and ties go to the smaller label. It stops after 20 rounds or when nothing changes. Deterministic.
2. A group is a label shared by at least 4 entities. Smaller sets go to "Small groups".
3. A group is named after its lead entity: the one with the most evidence documents (ties: more links, then the smaller id). The map shows the name and the entity count ("Brightline Logistics 214"). The lead entity's id is the group's key in the URL.

**Layout.**
1. Each group is laid out on its own by the d3-force-3d simulation that force-graph already ships. It starts from the library's phyllotaxis positions in id order, runs `min(300, 60 + size)` ticks with the timer stopped, and is then frozen.
2. Groups are placed from largest to smallest along a spiral from the center, each at the first spot where its circle touches no other.
3. Small groups pack in a band above the map, ordered by size, then name, under the heading "Small groups" with their count.
4. The result is kept in memory for the session, keyed by the response's `version`.

**Renderer.** The force-graph canvas that the app already uses (`react-force-graph-2d` 1.29.2 over `force-graph` 1.52.0), with fixed positions and `cooldownTicks` 0, so no simulation runs. Reasons:
- It draws today's encoding already; a WebGL library such as Sigma would need its own shape programs for ten node shapes.
- The force-graph README lists a "Large size graph (~75k elements)" example; the archive is under 10,000.
- In a headless Chromium test on the build machine (software rendering), one full redraw of 6,250 shaped nodes and 3,125 links took 23 to 40 ms.

d3-force-3d 3.0.6 becomes a direct dependency at the version the lockfile already holds, so no new code ships.

**Two zoom levels.**
- Map zoom (scale below 1): nodes are dots 3 to 7 px across (by evidence documents) in kind color, links are 1 px hairlines in `--border`, links between groups are dotted, and each group's name sits above it in Geist Serif 15 px with its size. Node shapes and labels are not drawn: at 6,000 nodes a 6 px minimum radius would cover most of the screen. This is the one exception to the shared encoding; it needs the owner's approval (see "Changes to the specification"), and the legend says so ("Dots show kind by color. Zoom in for shapes, sizes and names."). The legend names each color's kind family, so color is not the only signal.
- Detail zoom (scale 1 or more): the shared encoding, unchanged, with labels in evidence order and overlapping labels skipped.

**Interaction.**
- A group name or a click on empty space inside a group selects the group (`sel=grp:<id>`). The inspector shows the group: name, size, its 10 entities with the most evidence documents, its kind mix, and "Open <lead entity> in Focus" and "Explore from <lead entity>".
- A node at detail zoom selects the entity. The inspector's actions open it in Focus or Explore.
- Opening an entity sets the focus and leaves Overview (a pushed history step), so Back returns to the map.

**Caps.**
- The map draws at most 8,000 entities. Above that, the route returns the 8,000 with the most links and `truncated: true`, and the caption says "The map shows the 8,000 most linked of N entities." The archive today has at most 6,250 linked entities, the upper bound from 3,100 links.

**Accessible alternative.** A "Show as list" switch in the toolbar replaces the canvas with a table of groups (name, entities, links, kind mix as text), sortable by size. Each row opens the group in the inspector. The canvas name says "Map of the archive: N entities in G groups and S small groups."

**States.**
- Loading: "Loading the archive map", then "Arranging N entities" while the worker runs.
- Empty: "No links between entities yet. Run a sync, then come back." with a Search entities button.
- Error: an alert that names the failed step (loading or arranging) with Retry.

**Phone.** No canvas. The list of groups, 56 px rows with name, size, a kind mix bar and its three main kinds in words (`views-mockup.html#phone-overview`). A row opens a sheet with its top entities; a row there opens Focus.

**Prototype limits.** The prototype ticket builds Overview behind its URL only: `view=overview` works, and the switcher does not list it until the owner accepts it on the real archive. Pass thresholds, measured on the owner's desktop browser:

| Measure | Pass |
|---|---|
| `/graph/overview` server time | 1.0 s or less |
| Response size | 1.5 MB or less |
| Groups and layout in the worker | 1.5 s or less |
| Pan and zoom at map zoom, whole archive | 30 frames per second or more |
| Group quality | At least half of the linked entities sit in groups of 4 or more, and the owner recognizes the names of the 10 largest groups |

If a threshold fails, the prototype stays out of the switcher, and the ticket records the numbers. The owner then picks a next step: lay out on the server, draw with WebGL, or drop the view.

## Views not proposed

Three other views were considered. None answers a question the six do not.
- An adjacency matrix answers "which neighbors link to each other", which Focus now draws.
- A Sankey diagram of money between organizations needs amounts on links, which the graph does not store.
- A map of places needs coordinates, which the graph does not store.

## New backend routes

Both are read-only `GET` routes behind the existing frontend proxy, with a stable order. Existing routes do not change.

### `GET /graph/subgraph`

Links among a set of entities, for Focus and Explore.

- Parameter: `ids`, comma-separated entity ids, 2 to 150. Document ids and unknown ids are ignored. More than 150 ids returns 400.
- Returns each link whose two ends are in the set, outside the `evidence` family, with no Document end. Links are sorted by distinct supporting documents (descending), then key. At most 600 links; `total` is the count before the cap, and `truncated` says whether more existed. The browser applies its filters to the links it received. When `truncated` is true and a filter is on, the caption adds "Filters apply to the 600 strongest links between neighbors."
- Each link has the same fields as a summary link, with `source` and `target` instead of `direction` and `neighbor`. `documents` holds the documents that its records name, so date and type filters work.

```json
{ "links": [{"key": "a~EMPLOYED_BY~b", "type": "EMPLOYED_BY", "family": "parties",
             "source": "a", "target": "b", "support_state": "supported",
             "stated_count": 6, "inferred_count": 0,
             "records": [{"doc_id": 4120, "inferred": false, "confidence": 0.9}]}],
  "documents": {"4120": {"title": "…", "date": "2026-04-01", "doc_type": "work"}},
  "total": 1, "truncated": false }
```

- Size limit: under 250 KB for a synthetic set of 150 entities with 600 links, asserted by a test.
- Request size: 150 ids of 36 characters with encoded commas make a query of about 5.9 KB. That fits the 16 KiB request-head limit of h11 0.16.0 (`h11/_connection.py:69`, `DEFAULT_MAX_INCOMPLETE_EVENT_SIZE = 16 * 1024`) and Node's default 16 KiB header limit in front of it.

### `GET /graph/overview`

The whole-archive data for Overview, in a compact form.

- Nodes: every entity with at least one link outside the `evidence` family to another entity, Documents excluded, sorted by id. Fields are given once in `columns`; names are cut at 60 characters.
- Links: one row per pair of entities, all their relationships merged. `weight` is the count of distinct supporting documents. `support` is 0 (supported), 1 (mixed) or 2 (inferred), by the #109 rule over all records. `families` is a bit mask of the family indices of all the pair's links. The family filter hides a pair only when none of its families is shown.
- `unlinked` counts entities that have only document links. `owner` is the owner's node index or null. `version` is a hash of the node ids and link keys, for the browser's layout cache.

```json
{ "version": "9c1e…", "columns": ["id", "label", "name", "evidence_doc_count", "link_count"],
  "nodes": [["…", "Organization", "Brightline Logistics", 58, 41]],
  "links": [[0, 17, 6, 0, 5]], "link_columns": ["source", "target", "weight", "support", "families"],
  "owner": 112, "unlinked": 1840, "truncated": false }
```

- Size limit: under 1.5 MB for a synthetic archive of 6,250 entities and 3,125 links, asserted by a test. The proxy passes the body through uncompressed.
- At most 8,000 nodes, the ones with the most links, then `truncated: true`.

## Performance budgets

Budgets for an archive of 7,400 nodes and 14,900 links with one entity of 660 links. A ticket's tests assert the size limits; the times are checked by hand in the pull request.

| Step | Budget | Basis |
|---|---|---|
| Focus: summary of an entity with fewer than 100 links | 0.5 s | today's route |
| Focus: summary of the 660-link entity | 1.5 s, 373 KB | measured size |
| `/graph/subgraph` for 150 ids | 0.3 s server, 250 KB | new, asserted size |
| Explore: one expansion, request to drawn | 1 s, or 2 s for a hub | one summary plus one subgraph request |
| Explore and Focus layout function | 16 ms for 150 entities | pure function, no request |
| Timeline: first draw for the 660-link entity | 100 ms after the summary | capped rows, merged marks |
| Paths | 0.7 s typical, 5 s server limit | measured; #109 limit |
| `/graph/overview` | 1.0 s server, 1.5 MB | prototype threshold |
| Overview groups and layout | 1.5 s in a worker | measured 0.24 to 0.44 s in Node, synthetic |
| Overview pan and zoom | 33 ms per frame | measured 23 to 33 ms as dots, headless, software rendering |

The #109 limit stays: revisit server paging of the summary route when an entity passes about 2,000 links. The real 660-link summary (373 KB) is already above the 300 KB that #109 asserts for a synthetic 660-link entity. That test (`tests/test_graph_summary.py`) gives most links one record and names two documents; the real links name more documents, so the real response is larger. The 1.5 s budget allows for the real size.

## Measurements behind the budgets

These ran in this pull request's work, not on the real archive, with Node 24 on the build machine and synthetic preferential-attachment graphs (a few entities with many links, most with one or two). The entity counts are guesses, because the number of linked entities in the real archive is not known: 3,500 entities, and the upper bound of 6,250.

| Graph | One force layout of the whole graph, 300 ticks | Label propagation | Groups of 4 or more | Layout per group |
|---|---|---|---|---|
| 3,500 entities, 3,125 links | 6.6 to 6.8 s | 8 to 26 ms | 177 groups, 2,244 entities (64 %) | 0.41 to 0.44 s |
| 6,250 entities, 3,125 links | 11.7 to 11.8 s | 7 ms | 318 groups, 2,429 entities (39 %) | 0.24 s |

The whole-graph layout is too slow for a page, so Overview lays out each group alone. The share of entities in groups depends on the real archive, and the group quality threshold checks it. Redraw cost: 6,250 nodes and 3,125 links on a 1,256 × 836 canvas in headless Chromium (software rendering) took 23 to 33 ms per frame as dots and 36 to 40 ms with shapes.

## Changes to the specification

When the owner approves this addendum, these parts of #109 change.
- **Out of scope.** "Hops 2 and 3" is allowed in Explore only. "A whole-archive view" and "the archive map and its overview endpoint" become the Overview prototype. "Performance budgets" are the table above. "A layout engine" as a new dependency: d3-force-3d becomes direct at its locked version. "A different graph renderer" stays out of scope.
- **Graph backend API.** Two more read-only routes, `/graph/subgraph` and `/graph/overview`, besides `/graph/link`, paths and relationship types.
- **Neighborhood view.** It is called Focus and gains links between neighbors. Its layout rules do not change.
- **URL state.** The view values, `x`, `cross` and `sel=grp:<id>` above; history rules as above.
- **Keyboard model.** Keys 1 to 6; `n`, `t` and `p` stay as aliases.
- **Filters.** The Owner default per view, and the groups that Overview disables.
- **Labels.** On phone, the rule "with 40 or fewer nodes, every node is labeled" gives way to skipping overlapping labels, as in Focus.
- **Encoding.** Overview's map zoom draws dots instead of shapes (see "Two zoom levels"). This is the one change to the shared encoding, and the owner's approval of this addendum covers it. Without that approval, Overview opens at detail zoom only.
- `README.md` in this folder says the views are Neighborhood, Path and Table. It now points here.

## Ticket breakdown

Each ticket is one pull request under the working rules in the first comment of #109. Every ticket that draws UI is blocked by the owner's approval of this addendum. The labels V1 to V8 stand in for issue numbers until the tickets are filed.

**V1. Add the view switcher and the view URL state**
- **Blocked by:** none (#132 is merged).
- **Parallel with:** V2, V6.
- **What it delivers:** the switcher toolbar in the graph header with the six views, Overview hidden until V8; the view values with `neighborhood` as an alias; keys 1 to 6 and the `n`, `t`, `p` aliases; the disabled state without a focus; the live region; the phone "View" button and its sheet; history and selection rules for a view change. Views that do not exist yet are not listed.
- **Acceptance criteria:**
  - [ ] Keys 1 and 2 switch views, push one history entry each, and do nothing while a text field has focus.
  - [ ] `/graph?focus=<id>&view=neighborhood` opens Focus.
  - [ ] The switcher is one Tab stop; Left and Right move between buttons; each has `aria-pressed`.
  - [ ] At 390 px, the View button opens a sheet with 56 px rows.

**V2. Add the links-among route `GET /graph/subgraph`**
- **Blocked by:** none.
- **Parallel with:** V1, V6.
- **What it delivers:** the route, its 2-to-150 id rule, the 600-link cap with `total` and `truncated`, and the documents map.
- **Acceptance criteria:**
  - [ ] For three fixture entities, the route returns exactly the links among them, without evidence links or Document ends.
  - [ ] 151 ids return 400.
  - [ ] A synthetic set of 150 entities with 600 links returns under 250 KB.

**V3. Draw the links between neighbors in Focus**
- **Blocked by:** V1, V2.
- **Parallel with:** V5.
- **What it delivers:** the curved cross-links, the 60-link cap and its caption, the "Links between neighbors" switch and `cross=0`, the selection rule, the inspector group "Linked to other neighbors", the canvas name, and the loading, empty and error captions. The gated Focus screenshots change.
- **Acceptance criteria:**
  - [ ] The fixture focus draws each fixture link between two drawn neighbors once, and no link to a pill member.
  - [ ] With 61 or more cross-links, 60 are drawn and the caption names the total.
  - [ ] Reloading twice yields identical positions.
  - [ ] The canvas name ends with the cross-link count.

**V4. Add Explore**
- **Blocked by:** V3.
- **Parallel with:** V5, V7.
- **What it delivers:** `view=explore`, the start ring, the "+N" handle, expansion and collapse with `+` and `-`, the layout function with its unit tests, `x` in the URL, the hop and entity caps, the trail panel, the phone form, and the states.
- **Acceptance criteria:**
  - [ ] Unit test: the same summaries and `x` list yield identical positions, and an expansion never moves a placed node.
  - [ ] Expanding a node adds at most 6 new entities and draws its links to entities already in view.
  - [ ] Back after an expansion restores the previous view and URL.
  - [ ] A hop-3 node offers "Open in Focus" instead of expanding.
  - [ ] An expansion past 150 entities adds what fits and shows the limit message.

**V5. Add Timeline**
- **Blocked by:** V1, #141 (the date range is its date filter).
- **Parallel with:** V3, V4.
- **What it delivers:** `view=timeline` with the axis, document band, family lanes and rows, stated and inferred marks, the No date column, merged marks above 60 records, the range brush and month controls, row names for screen readers, the legend rows, the phone list, and the states.
- **Acceptance criteria:**
  - [ ] Each fixture link draws one mark per supporting document at that document's month; an undated document's mark sits in the No date column.
  - [ ] Dragging a range sets `from` and `to`, replaces the history entry, and Focus then shows the filtered counts.
  - [ ] The fixture hub draws at most 2 rows per family plus a "+N more" row.

**V6. Add the overview route `GET /graph/overview`**
- **Blocked by:** none.
- **Parallel with:** V1, V2.
- **What it delivers:** the route with merged pair links and their `families` mask, `columns`, `version`, `owner`, `unlinked`, and the 8,000-node cap.
- **Acceptance criteria:**
  - [ ] Two fixture entities joined by two relationship types return one link row whose weight counts distinct documents.
  - [ ] No Document node and no evidence link appears.
  - [ ] A synthetic archive of 6,250 entities and 3,125 links returns under 1.5 MB.

**V7. Build the Overview prototype**
- **Blocked by:** V1, V6.
- **Parallel with:** V4, V5.
- **What it delivers:** `view=overview` by URL only; label propagation and per-group layout in a Web Worker; d3-force-3d as a direct dependency at its locked version; the two zoom levels; group selection and its inspector; "Show as list"; the phone group list; the states. The pull request reports the threshold measurements from the synthetic fixture and asks the owner to run them on the real archive.
- **Acceptance criteria:**
  - [ ] Unit test: the same response yields the same groups and positions.
  - [ ] Selecting a group sets `sel=grp:<id>` and lists its 10 entities with the most evidence documents.
  - [ ] "Show as list" shows a table of groups that is sortable by size.

**V8. Accept or remove the Overview prototype**
- **Blocked by:** V7 and the owner's trial on the real archive.
- **Parallel with:** none.
- **What it delivers:** if every threshold passes, Overview in the switcher with key 6 and a link from the landing page ("See the whole archive"). Otherwise the owner's chosen next step, recorded in the issue.
- **Acceptance criteria:**
  - [ ] The pull request quotes the owner's measured thresholds.

**Existing tickets that change.**
- #133 (phone and accessible canvas): no change. V3 extends the canvas name that #133 adds.
- #139 (link evidence): a link between two neighbors, where neither end is the focus, opens the same evidence. Add that as a criterion.
- #140 (table view): its view button lives in the V1 switcher, so it is blocked by V1. In Explore it lists the root's links.
- #141 (filters): the filters apply to cross-links, Explore and Timeline. Add the per-view Owner defaults and the groups that Overview disables.
- #143 (path view): its button lives in the V1 switcher, labeled "Paths", so it is blocked by V1. A switch sets A to the focus when A is empty.
- #144 (phone entity page): the Graph tab holds the phone View button, and on phone the Table view opens the Links tab.

**Recommended order.** V1, V2 and V6 first, in parallel. Then V3, because links between neighbors answer the owner's first question with the least new code. Then V5 once #141 lands, and V4. V7 and V8 come last, because Overview is the only view with an open risk.

## What is still unverified

- The number of linked entities in the real archive, and so the real group sizes and layout time.
- Frame rates on a real GPU and on the owner's phone; the redraw figures come from headless software rendering.
- Whether the real archive's groups carry recognizable names. The owner judges that in V8.
- The response times in the budget table for the new routes; they are targets until V2 and V6 measure them.
