# Graph close-up zoom and label readability

Status: complete. Independently reviewed PR31 merged as b24b0b0 and is included in deployed cbdc610 through GitOps PR149. Final live visual acceptance passed September 8, 2026.

## Report and scope

The graph becomes crowded and stops zooming before individual nodes can be inspected comfortably. The reported baseline 2D renderer explicitly set `maxZoom={4}`. Labels use screen-sized fonts and nodes, but some background padding and offsets remain in graph coordinates. Reproduce both behaviors in the actual browser renderer before editing them.

The intended change removes the application-specific close-up restriction, retaining the graph library's supported interaction range. Keep label backgrounds and offsets in screen pixels so close-up labels remain attached to their nodes without growing boxes. Preserve wheel/trackpad interaction, pan, node selection, source inspection, the existing Fit view control, and 3D navigation. Do not change graph retrieval, identities, ingestion fingerprints, or evidence contracts.

## Acceptance

- An actual wheel gesture moves the 2D renderer beyond the former 4x ceiling, with nodes separating on screen. A close-up substantially beyond that boundary remains readable.
- Label font, background padding and node-to-label gap remain stable in screen pixels across zoom levels; selection and hit areas remain usable.
- Fit view returns from a close-up to an overview. Switching 2D/3D and back remains functional. Browser errors remain empty.
- Exercise the production frontend with the localhost synthetic fixture; capture the failing baseline and passing result. Run frontend regressions, lint, type checking, build and browser contracts. Visually verify the real controls using computer/browser control.

## Delivery sequence

Prepare locally while the admitted targeted recovery continues. Do not deploy during ingestion. Finish recovery and verify actual writer drain before the graph release. Obtain independent Standards and Spec review and exact-head CI, publish immutable images, and deploy through GitOps while preserving paused schedules and the separately requested paperless-brain pause. Include the graph close-up check in final live acceptance before restoring normal KG schedules. Retain the existing corpus, vector, history and original-query acceptance gates.

## Reproduction and local result

The real browser wheel regression failed against the unchanged production frontend: zoom remained exactly 4, label horizontal padding was 24 screen pixels instead of 6, label height was 27 instead of 15, and the person-node label offset was 22 instead of 10. Removing only `maxZoom={4}` allowed the same gestures to reach the library's default 1000 limit but made the uncorrected label geometry grow correspondingly. Dividing background padding and text offsets by the supplied zoom scale fixed those independent failures. Automatic Fit view did not cause the zoom ceiling.

`frontend/tests/graph-zoom-ui.mjs` observes actual canvas paint geometry and the renderer's zoom transform while driving wheel gestures, canvas node selection, background dragging and Fit view. It does not set the zoom transform or camera. The test waits for the initial camera to settle and guards the localhost synthetic fixture before interaction. It now passes at 1000x with 11-pixel text, 6-pixel total horizontal padding, 15-pixel background height and a 10-pixel person-node label offset.

Node 24 lint, all 12 unit regressions, type checking, production build and the full production browser suite passed. Actual browser screenshots also confirm 3D orbit and wheel zoom, Fit view, switching back to 2D, and expanded source evidence. These are controlled local checks; final live verification is recorded below. No ingestion, evidence, graph data or identity rules changed.

## Final live result

On the deployed 624-node/980-relationship graph, actual wheel gestures reached 265.7x with stable screen-size label geometry. Visually inspected the close-up, populated source inspector, Fit return, 3D orbit/zoom and 2D return; no uncaught page errors occurred. Normal schedules were restored only after this and the other [closure gates](issue16-acceptance-closure.md) passed. This is functional verification, not a dense-graph performance benchmark.

## Follow-up: keep selected nodes visible when the viewport changes

At a deep 2D zoom, opening the source inspector narrows the canvas and can leave the selected node outside its visible bounds. Preserve the selected node's visibility across selection and viewport-size changes without resetting the user's zoom. This applies to inspector and responsive-layout resizing, not a particular node or graph dataset.

After the renderer applies valid dimensions, center the selected visible node using its finite renderer-owned coordinates. Do not alter zoom, graph data, pinned coordinates or evidence. An absent/filtered node, missing renderer, invalid coordinates or zero-sized viewport must not move the camera. Leave ordinary background panning and explicit Fit view effective; do not continuously recenter during force ticks or 3D orbit. Preserve existing 3D behavior.

Extend the real-browser regression: pan a deeply zoomed node near the canvas edge, open its inspector, verify the same node remains visible and zoom is unchanged, resize the viewport and repeat, then prove background panning and Fit view still work. Retain a failing baseline, review plan and code independently, run frontend checks and exact-head CI, and verify the deployed interaction visually. Deploy immutable frontend artifacts through GitOps after the live query finishes; preserve the query acceptance results and all data/runtime gates.

The follow-up plan passed both independent review axes. The localhost browser reproduces the selected node leaving the inspector-resized viewport while zoom remains1000x. Selection also recreated the filtered graph when neighborhood focus was disabled, unnecessarily reheating the force layout; preserve that data identity when membership is unchanged, then center the selected visible node after the resize without altering zoom.

Visibility means unobscured by the inspector, not merely inside canvas coordinates. At widths where the interface presents both graph and details, dock the inspector beside the canvas and measure the remaining graph area. Use a compact inspector width on intermediate screens. Below the small-screen breakpoint, retain the existing full-width detail overlay; closing it restores the graph view using the existing selection-dismissal behavior. Browser acceptance must use hit-testing at the selected node across desktop/tablet widths and verify the phone overlay can be closed without losing the inspected node location or zoom.
