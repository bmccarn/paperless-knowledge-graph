# Graph close-up zoom and label readability

Status: investigating. Requested September 8, 2026, as a follow-up after corpus recovery.

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
