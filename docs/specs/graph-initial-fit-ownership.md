# Initial graph fit must yield to user navigation

Status: implemented and independently reviewed; released in application PR #51 via
GitOps PR #167. Production-build browser regressions and live visual checks pass.

The graph's `onEngineStop` performs its first `zoomToFit` whenever `fitted.current`
is false. A user can already wheel, pan, drag or select before those cooldown ticks
finish. The initial fit then overrides their view. In a live 630-node/980-edge
sample, early wheel zoom reached 4.29x and was reset to 0.52x after roughly one second;
a later wheel gesture remained at 4.82x. The existing synthetic zoom test explicitly
waits for automatic fit before interacting, so it cannot catch this startup race.

Track ownership of the initial viewport: automatic fitting is allowed only until
the first deliberate interaction with the renderer. Cancel eligibility on a real
wheel or pointer-down within the canvas before initial fitting. Make only the initial automatic fit immediate (duration zero), so a queued fit
animation cannot continue overriding a gesture after ownership changes. Keep initial fit
for an untouched graph, and preserve the explicit Fit view button. Do not infer
user intent from a generic renderer zoom callback, because programmatic initial
resize/fit may emit it. Reset initial eligibility only when a new renderer is
created (the existing 2D/3D lifecycle); do not add camera resets for selection,
inspector changes or viewport resize. No graph/data/query mutations are involved.

Add a browser regression that wheels as soon as the canvas appears, before engine
cooldown completes and at its completion boundary; record actual canvas zoom, wait through the startup settling
period, and require the user zoom to remain. Keep the existing after-fit close-up,
label geometry, node inspection, resize, pan and explicit Fit view checks. Include
an untouched-load control so preserving interaction cannot silently disable all
initial fitting. Use actual DOM gestures, not application camera APIs. Visually
inspect the early-zoom and fitted screenshots after the tested production build.

Review the small implementation diff, run frontend checks and browser contracts,
and include this in application delivery only via reviewed CI and GitOps. The live
reproduction is not a claim that every graph feature has passed acceptance.
