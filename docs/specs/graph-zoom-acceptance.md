# Graph close-up zoom and label readability

Status: implemented and validated locally; review and GitOps delivery pending. Requested September 8, 2026, as a follow-up after corpus recovery.

## Report and scope

The graph becomes crowded and stops zooming before individual nodes can be inspected comfortably. The current 2D renderer explicitly sets `maxZoom={4}`. Labels use screen-sized fonts and nodes, but some background padding and offsets remain in graph coordinates. Reproduce both behaviors in the actual browser renderer before editing them.

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

Node 24 lint, all 12 unit regressions, type checking, production build and the full production browser suite passed. Actual browser screenshots also confirm 3D orbit and wheel zoom, Fit view, switching back to 2D, and expanded source evidence. These are controlled local checks; final live verification remains a release gate. No ingestion, evidence, graph data or identity rules changed.
