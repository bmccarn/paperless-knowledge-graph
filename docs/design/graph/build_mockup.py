#!/usr/bin/env python3
"""Build mockup.html, the high-fidelity graph mockup in direction C.

Usage: python3 docs/design/graph/build_mockup.py docs/design

The argument is the design folder. The script reads its tokens.css and the
font faces embedded in its answer-mockup.html, and writes graph/mockup.html
there. Every name, document, amount and count is invented. The SVG is drawn
here, at build time, so the mockup is a static file with no script-drawn
canvas. Icon paths come from lucide 1.73.0 (ISC licence).
"""
import html
import json
import math
import pathlib
import re
import sys

ICONS = json.loads(r"""{"user-round":[["circle",{"cx":"12","cy":"8","r":"5"}],["path",{"d":"M20 21a8 8 0 0 0-16 0"}]],"building-2":[["path",{"d":"M10 12h4"}],["path",{"d":"M10 8h4"}],["path",{"d":"M14 21v-3a2 2 0 0 0-4 0v3"}],["path",{"d":"M6 10H4a2 2 0 0 0-2 2v7a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2h-2"}],["path",{"d":"M6 21V5a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v16"}]],"map-pin":[["path",{"d":"M20 10c0 4.993-5.539 10.193-7.399 11.799a1 1 0 0 1-1.202 0C9.539 20.193 4 14.993 4 10a8 8 0 0 1 16 0"}],["circle",{"cx":"12","cy":"10","r":"3"}]],"house":[["path",{"d":"M15 21v-8a1 1 0 0 0-1-1h-4a1 1 0 0 0-1 1v8"}],["path",{"d":"M3 10a2 2 0 0 1 .709-1.528l7-6a2 2 0 0 1 2.582 0l7 6A2 2 0 0 1 21 10v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"}]],"receipt":[["path",{"d":"M12 17V7"}],["path",{"d":"M16 8h-6a2 2 0 0 0 0 4h4a2 2 0 0 1 0 4H8"}],["path",{"d":"M4 3a1 1 0 0 1 1-1 1.3 1.3 0 0 1 .7.2l.933.6a1.3 1.3 0 0 0 1.4 0l.934-.6a1.3 1.3 0 0 1 1.4 0l.933.6a1.3 1.3 0 0 0 1.4 0l.933-.6a1.3 1.3 0 0 1 1.4 0l.934.6a1.3 1.3 0 0 0 1.4 0l.933-.6A1.3 1.3 0 0 1 19 2a1 1 0 0 1 1 1v18a1 1 0 0 1-1 1 1.3 1.3 0 0 1-.7-.2l-.933-.6a1.3 1.3 0 0 0-1.4 0l-.934.6a1.3 1.3 0 0 1-1.4 0l-.933-.6a1.3 1.3 0 0 0-1.4 0l-.933.6a1.3 1.3 0 0 1-1.4 0l-.934-.6a1.3 1.3 0 0 0-1.4 0l-.933.6a1.3 1.3 0 0 1-.7.2 1 1 0 0 1-1-1z"}]],"file-signature":[["path",{"d":"M14.364 13.634a2 2 0 0 0-.506.854l-.837 2.87a.5.5 0 0 0 .62.62l2.87-.837a2 2 0 0 0 .854-.506l4.013-4.009a1 1 0 0 0-3.004-3.004z"}],["path",{"d":"M14.487 7.858A1 1 0 0 1 14 7V2"}],["path",{"d":"M20 19.645V20a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l2.516 2.516"}],["path",{"d":"M8 18h1"}]],"shield-check":[["path",{"d":"M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z"}],["path",{"d":"m9 12 2 2 4-4"}]],"package":[["path",{"d":"M11 21.73a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73z"}],["path",{"d":"M12 22V12"}],["polyline",{"points":"3.29 7 12 12 20.71 7"}],["path",{"d":"m7.5 4.27 9 5.15"}]],"cpu":[["path",{"d":"M12 20v2"}],["path",{"d":"M12 2v2"}],["path",{"d":"M17 20v2"}],["path",{"d":"M17 2v2"}],["path",{"d":"M2 12h2"}],["path",{"d":"M2 17h2"}],["path",{"d":"M2 7h2"}],["path",{"d":"M20 12h2"}],["path",{"d":"M20 17h2"}],["path",{"d":"M20 7h2"}],["path",{"d":"M7 20v2"}],["path",{"d":"M7 2v2"}],["rect",{"x":"4","y":"4","width":"16","height":"16","rx":"2"}],["rect",{"x":"8","y":"8","width":"8","height":"8","rx":"1"}]],"stethoscope":[["path",{"d":"M11 2v2"}],["path",{"d":"M5 2v2"}],["path",{"d":"M5 3H4a2 2 0 0 0-2 2v4a6 6 0 0 0 12 0V5a2 2 0 0 0-2-2h-1"}],["path",{"d":"M8 15a6 6 0 0 0 12 0v-3"}],["circle",{"cx":"20","cy":"10","r":"2"}]],"flask-conical":[["path",{"d":"M14 2v6a2 2 0 0 0 .245.96l5.51 10.08A2 2 0 0 1 18 22H6a2 2 0 0 1-1.755-2.96l5.51-10.08A2 2 0 0 0 10 8V2"}],["path",{"d":"M6.453 15h11.094"}],["path",{"d":"M8.5 2h7"}]],"calendar":[["path",{"d":"M8 2v3"}],["path",{"d":"M16 2v3"}],["rect",{"x":"3","y":"3","width":"18","height":"18","rx":"2"}],["path",{"d":"M3 9h18"}]],"calendar-clock":[["path",{"d":"M16 14v2.2l1.6 1"}],["path",{"d":"M16 2v3"}],["path",{"d":"M21 7.338V5a2 2 0 00-2-2H5a2 2 0 00-2 2v14a2 2 0 002 2h2.338"}],["path",{"d":"M3 9h5.859"}],["path",{"d":"M8 2v3"}],["circle",{"cx":"16","cy":"16","r":"6"}]],"file-text":[["path",{"d":"M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z"}],["path",{"d":"M14 2v5a1 1 0 0 0 1 1h5"}],["path",{"d":"M10 9H8"}],["path",{"d":"M16 13H8"}],["path",{"d":"M16 17H8"}]],"file-question":[["path",{"d":"M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z"}],["path",{"d":"M9 9.5a3 2.5 0 016 0c0 2-2 2-3 3.5"}],["path",{"d":"M12 17h.01"}]],"circle-help":[["circle",{"cx":"12","cy":"12","r":"10"}],["path",{"d":"M9 9.5a3 2.5 0 016 0c0 2-2 2-3 3.5"}],["path",{"d":"M12 17h.01"}]],"search":[["path",{"d":"m21 21-4.34-4.34"}],["circle",{"cx":"11","cy":"11","r":"8"}]],"arrow-left":[["path",{"d":"m12 19-7-7 7-7"}],["path",{"d":"M19 12H5"}]],"x":[["path",{"d":"M18 6 6 18"}],["path",{"d":"m6 6 12 12"}]],"zoom-in":[["circle",{"cx":"11","cy":"11","r":"8"}],["line",{"x1":"21","x2":"16.65","y1":"21","y2":"16.65"}],["line",{"x1":"11","x2":"11","y1":"8","y2":"14"}],["line",{"x1":"8","x2":"14","y1":"11","y2":"11"}]],"zoom-out":[["circle",{"cx":"11","cy":"11","r":"8"}],["line",{"x1":"21","x2":"16.65","y1":"21","y2":"16.65"}],["line",{"x1":"8","x2":"14","y1":"11","y2":"11"}]],"maximize":[["path",{"d":"M8 3H5a2 2 0 0 0-2 2v3"}],["path",{"d":"M21 8V5a2 2 0 0 0-2-2h-3"}],["path",{"d":"M3 16v3a2 2 0 0 0 2 2h3"}],["path",{"d":"M16 21h3a2 2 0 0 0 2-2v-3"}]],"sliders-horizontal":[["path",{"d":"M10 5H3"}],["path",{"d":"M12 19H3"}],["path",{"d":"M14 3v4"}],["path",{"d":"M16 17v4"}],["path",{"d":"M21 12h-9"}],["path",{"d":"M21 19h-5"}],["path",{"d":"M21 5h-7"}],["path",{"d":"M8 10v4"}],["path",{"d":"M8 12H3"}]],"chevron-right":[["path",{"d":"m9 18 6-6-6-6"}]],"chevron-down":[["path",{"d":"m6 9 6 6 6-6"}]],"chevron-up":[["path",{"d":"m18 15-6-6-6 6"}]],"check":[["path",{"d":"M20 6 9 17l-5-5"}]],"arrow-down":[["path",{"d":"M12 5v14"}],["path",{"d":"m19 12-7 7-7-7"}]],"arrow-right":[["path",{"d":"M5 12h14"}],["path",{"d":"m12 5 7 7-7 7"}]],"external-link":[["path",{"d":"M15 3h6v6"}],["path",{"d":"M10 14 21 3"}],["path",{"d":"M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"}]],"flag":[["path",{"d":"M4 22V4a1 1 0 0 1 .4-.8A6 6 0 0 1 8 2c3 0 5 2 7.333 2q2 0 3.067-.8A1 1 0 0 1 20 4v10a1 1 0 0 1-.4.8A6 6 0 0 1 16 16c-3 0-5-2-8-2a6 6 0 0 0-4 1.528"}]],"info":[["circle",{"cx":"12","cy":"12","r":"10"}],["path",{"d":"M12 16v-4"}],["path",{"d":"M12 8h.01"}]],"message-square-text":[["path",{"d":"M22 17a2 2 0 0 1-2 2H6.828a2 2 0 0 0-1.414.586l-2.202 2.202A.71.71 0 0 1 2 21.286V5a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2z"}],["path",{"d":"M7 11h10"}],["path",{"d":"M7 15h6"}],["path",{"d":"M7 7h8"}]],"network":[["rect",{"x":"16","y":"16","width":"6","height":"6","rx":"1"}],["rect",{"x":"2","y":"16","width":"6","height":"6","rx":"1"}],["rect",{"x":"9","y":"2","width":"6","height":"6","rx":"1"}],["path",{"d":"M5 16v-3a1 1 0 0 1 1-1h12a1 1 0 0 1 1 1v3"}],["path",{"d":"M12 12V8"}]],"users-round":[["path",{"d":"M18 21a8 8 0 0 0-16 0"}],["circle",{"cx":"10","cy":"8","r":"5"}],["path",{"d":"M22 20c0-3.37-2-6.5-4-8a5 5 0 0 0-.45-8.3"}]],"activity":[["path",{"d":"M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2"}]],"layout-grid":[["rect",{"width":"7","height":"7","x":"3","y":"3","rx":"1"}],["rect",{"width":"7","height":"7","x":"14","y":"3","rx":"1"}],["rect",{"width":"7","height":"7","x":"14","y":"14","rx":"1"}],["rect",{"width":"7","height":"7","x":"3","y":"14","rx":"1"}]],"scroll-text":[["path",{"d":"M15 12h-5"}],["path",{"d":"M15 8h-5"}],["path",{"d":"M19 17V5a2 2 0 0 0-2-2H4"}],["path",{"d":"M8 21h12a2 2 0 0 0 2-2v-1a1 1 0 0 0-1-1H11a1 1 0 0 0-1 1v1a2 2 0 1 1-4 0V5a2 2 0 1 0-4 0v2a1 1 0 0 0 1 1h3"}]],"ellipsis":[["circle",{"cx":"12","cy":"12","r":"1"}],["circle",{"cx":"19","cy":"12","r":"1"}],["circle",{"cx":"5","cy":"12","r":"1"}]],"arrow-left-right":[["path",{"d":"M8 3 4 7l4 4"}],["path",{"d":"M4 7h16"}],["path",{"d":"m16 21 4-4-4-4"}],["path",{"d":"M20 17H4"}]],"plus":[["path",{"d":"M5 12h14"}],["path",{"d":"M12 5v14"}]],"minus":[["path",{"d":"M5 12h14"}]]}""")


def esc(s):
    return html.escape(str(s), quote=True)


def icon(name, size=16, cls="i", sw=1.75):
    parts = []
    for tag, attrs in ICONS[name]:
        a = " ".join(f'{k}="{esc(v)}"' for k, v in attrs.items())
        parts.append(f"<{tag} {a}/>")
    return (f'<svg class="{cls}" width="{size}" height="{size}" viewBox="0 0 24 24" '
            f'stroke-width="{sw}" aria-hidden="true">{"".join(parts)}</svg>')


# ---------------------------------------------------------------- vocabulary

FAMILIES = [  # fixed order, spec "Families"
    ("parties", "Parties and roles"), ("money", "Money and accounts"),
    ("coverage", "Coverage and obligations"), ("health", "Health"),
    ("service", "Service and government"), ("places", "Places"), ("time", "Time"),
    ("doclinks", "Document links"), ("loose", "Loose association"), ("other", "Other"),
]
FAM_NAME = dict(FAMILIES)
FAM_ORDER = [k for k, _ in FAMILIES]

# label -> (palette key, outlined, shape, icon, word)
KINDS = {
    "Person": ("people", False, "circle", "user-round", "Person"),
    "Organization": ("org", False, "square", "building-2", "Organization"),
    "Location": ("place", False, "diamond", "map-pin", "Location"),
    "Address": ("place", True, "diamond", "house", "Address"),
    "FinancialItem": ("money", False, "hexagon", "receipt", "Financial item"),
    "Contract": ("agree", False, "pentagon", "file-signature", "Contract"),
    "InsurancePolicy": ("agree", True, "pentagon", "shield-check", "Insurance policy"),
    "Product": ("thing", False, "triangle", "package", "Product"),
    "System": ("thing", True, "triangle", "cpu", "System"),
    "Condition": ("health", False, "plus", "stethoscope", "Condition"),
    "MedicalResult": ("health", True, "plus", "flask-conical", "Medical result"),
    "Event": ("time", False, "star", "calendar", "Event"),
    "DateEvent": ("time", True, "star", "calendar-clock", "Date"),
    "Document": ("docs", False, "page", "file-text", "Document"),
    "DocumentRef": ("docs", True, "page", "file-question", "Document reference"),
    "Other": ("unknown", False, "dashed", "circle-help", "Other"),
}
PALETTE_ROWS = [  # the issue's palette table, in its order
    ("people", "people", ["Person"]), ("org", "org", ["Organization"]),
    ("place", "place", ["Location", "Address"]), ("money", "money", ["FinancialItem"]),
    ("agree", "agree", ["Contract", "InsurancePolicy"]), ("thing", "thing", ["Product", "System"]),
    ("health", "health", ["Condition", "MedicalResult"]), ("time", "time", ["Event", "DateEvent"]),
    ("docs", "docs", ["Document", "DocumentRef"]), ("unknown", "unknown", ["Other"]),
]
SHAPE_WORD = {"circle": "Circle", "square": "Rounded square", "diamond": "Diamond",
              "hexagon": "Hexagon", "pentagon": "Pentagon", "triangle": "Triangle",
              "plus": "Plus", "star": "Five-point star", "page": "Page with folded corner",
              "dashed": "Dashed circle"}
DOC_TYPES = [("financial_invoice", "Invoice"), ("government_tax", "Tax"), ("property_home", "Home"),
             ("insurance", "Insurance"), ("military", "Military"), ("legal_contract", "Legal"),
             ("personal", "Personal"), ("work", "Work"), ("medical_lab", "Medical")]
DOC_WORD = dict(DOC_TYPES)


def radius(d):
    return max(6.0, min(18.0, 6 + 2.5 * math.log2(1 + d)))


def link_width(w):
    return max(1.0, min(4.0, 1 + 0.75 * math.log2(max(w, 1))))


# ---------------------------------------------------------------- shapes

def shape_d(shape, cx, cy, r):
    """Path data for a node shape whose visual weight matches a circle of r."""
    def poly(n, rr, rot=-90.0, inner=None):
        pts = []
        steps = n * 2 if inner else n
        for i in range(steps):
            a = math.radians(rot + i * 360.0 / steps)
            q = rr if (inner is None or i % 2 == 0) else inner
            pts.append((cx + q * math.cos(a), cy + q * math.sin(a)))
        return "M" + "L".join(f"{x:.2f},{y:.2f}" for x, y in pts) + "Z"
    if shape in ("circle", "dashed"):
        return (f"M{cx - r:.2f},{cy:.2f}a{r:.2f},{r:.2f} 0 1,0 {2 * r:.2f},0"
                f"a{r:.2f},{r:.2f} 0 1,0 {-2 * r:.2f},0Z")
    if shape == "square":
        h = r * 0.9
        k = 2 * h * 0.22
        return (f"M{cx - h + k:.2f},{cy - h:.2f}H{cx + h - k:.2f}Q{cx + h:.2f},{cy - h:.2f} {cx + h:.2f},{cy - h + k:.2f}"
                f"V{cy + h - k:.2f}Q{cx + h:.2f},{cy + h:.2f} {cx + h - k:.2f},{cy + h:.2f}"
                f"H{cx - h + k:.2f}Q{cx - h:.2f},{cy + h:.2f} {cx - h:.2f},{cy + h - k:.2f}"
                f"V{cy - h + k:.2f}Q{cx - h:.2f},{cy - h:.2f} {cx - h + k:.2f},{cy - h:.2f}Z")
    if shape == "diamond":
        return poly(4, r * 1.22)
    if shape == "hexagon":
        return poly(6, r * 1.08, rot=-90)
    if shape == "pentagon":
        return poly(5, r * 1.14)
    if shape == "triangle":
        rr = r * 1.32
        return _tri(cx, cy + rr * 0.12, rr)  # centroid sits a little low, like the visual centre
    if shape == "star":
        return poly(5, r * 1.32, inner=r * 1.32 * 0.5)
    if shape == "plus":
        R = r * 1.12
        a = R * 2 / 3 / 2  # half of the arm width, arm width 2/3 of the radius
        return (f"M{cx - a:.2f},{cy - R:.2f}H{cx + a:.2f}V{cy - a:.2f}H{cx + R:.2f}V{cy + a:.2f}"
                f"H{cx + a:.2f}V{cy + R:.2f}H{cx - a:.2f}V{cy + a:.2f}H{cx - R:.2f}V{cy - a:.2f}H{cx - a:.2f}Z")
    if shape == "page":
        w, h = r * 0.82, r * 1.06
        f = w * 0.55
        return (f"M{cx - w:.2f},{cy - h:.2f}H{cx + w - f:.2f}L{cx + w:.2f},{cy - h + f:.2f}V{cy + h:.2f}"
                f"H{cx - w:.2f}Z")
    raise ValueError(shape)


def _tri(cx, cy, rr):
    pts = [(cx + rr * math.cos(math.radians(a)), cy + rr * math.sin(math.radians(a))) for a in (-90, 30, 150)]
    return "M" + "L".join(f"{x:.2f},{y:.2f}" for x, y in pts) + "Z"


def kind_svg(label, cx, cy, r, state="", icon_ok=True, hub=False):
    """One node: optional halo and rings, the shape, its icon."""
    pal, outlined, shape, ic, _ = KINDS[label]
    d = shape_d(shape, cx, cy, r)
    out = []
    if "sel" in state:
        out.append(f'<path d="{shape_d(shape, cx, cy, r + 3)}" class="halo-mark"/>')
    if hub:
        out.append(f'<circle cx="{cx:.2f}" cy="{cy:.2f}" r="{extent(label, r) + 3:.2f}" class="hub-ring"/>')
    faded = "faded" in state
    if shape == "dashed":
        cls = "nd-unknown" + (" is-faded" if faded else "")
        out.append(f'<path d="{d}" class="ring-bg"/><path d="{d}" class="{cls}"/>')
    elif outlined:
        out.append(f'<path d="{d}" class="ring-bg"/>'
                   f'<path d="{d}" class="nd-out" style="stroke:var({"--graph-faded" if faded else "--cat-" + pal})"/>')
    else:
        out.append(f'<path d="{d}" class="nd-fill" style="fill:var({"--graph-faded" if faded else "--cat-" + pal})"/>')
    if "sel" in state:
        out.append(f'<path d="{shape_d(shape, cx, cy, r + 1.5)}" class="ring-sel"/>')
    if "hover" in state:
        out.append(f'<path d="{shape_d(shape, cx, cy, r + 3)}" class="ring-hover"/>')
    if "kbd" in state:
        out.append(f'<path d="{shape_d(shape, cx, cy, r + 4)}" class="ring-kbd"/>')
    if icon_ok and r >= 10:
        s = 16 if r >= 14 else 12
        oy = cy + (r * 0.16 if shape == "triangle" else 0)
        if outlined or shape == "dashed":
            color = "var(--graph-faded)" if faded else ("var(--fg-subtle)" if shape == "dashed" else f"var(--cat-{pal})")
        else:
            color = "var(--bg)" if faded else f"var(--on-cat-{pal})"
        out.append(f'<g transform="translate({cx - s / 2:.2f},{oy - s / 2:.2f})" style="color:{color}">'
                   f'{icon(ic, s, "gi", 2 if s < 16 else 1.9)}</g>')
    return "".join(out)


def swatch(label, size=16, state=""):
    """A small inline KindShape for lists and the legend."""
    r = size / 2 - 2.5
    c = size / 2
    return (f'<svg class="ks" width="{size}" height="{size}" viewBox="0 0 {size} {size}" aria-hidden="true">'
            f'{kind_svg(label, c, c, r, state, icon_ok=False)}</svg>')


# ---------------------------------------------------------------- data

OWNER = "Alex Rivera"

# Neighborhood of an ordinary entity. Tuple: name, label, family, type,
# weight (link docs), evidence_doc_count, support, extra
POLICY = dict(name="Policy HM-4471 (auto)", label="InsurancePolicy", docs=12, aliases="HM4471-A, auto policy")
POLICY_N = [
    ("Harbor Mutual Insurance", "Organization", "parties", "ISSUED_BY", 9, 33, "supported"),
    (OWNER, "Person", "parties", "INSURED_PARTY", 7, 212, "supported"),
    ("Dana Whitlock", "Person", "parties", "INSURED_PARTY", 3, 39, "supported"),
    ("Premium, Apr 2026, $612", "FinancialItem", "money", "PREMIUM_FOR", 4, 4, "supported"),
    ("Premium, Oct 2025, $588", "FinancialItem", "money", "PREMIUM_FOR", 2, 2, "supported"),
    ("2019 Subaru Outback", "Product", "coverage", "COVERS", 7, 14, "supported"),
    ("2014 Honda Fit", "Product", "coverage", "COVERS", 2, 6, "mixed"),
    ("Northgate Credit Union", "Organization", "coverage", "INSURES_LIENHOLDER", 2, 21, "supported"),
    ("14 Larkspur Ct", "Address", "places", "GARAGED_AT", 1, 18, "inferred"),
    ("Port Ellery", "Location", "places", "LOCATED_IN", 2, 40, "supported"),
    ("1 Apr 2026", "DateEvent", "time", "EFFECTIVE_FROM", 2, 3, "supported"),
    ("1 Oct 2026", "DateEvent", "time", "EXPIRES_ON", 2, 3, "supported"),
    ("Policy HM-4402 (2025)", "InsurancePolicy", "doclinks", "SUPERSEDES", 3, 5, "supported"),
    ("Bayline Auto Body", "Organization", "loose", "RELATED_TO", 1, 2, "inferred"),
    ("Telematics plug TX-2", "System", "other", "MONITORED_BY", 1, 1, "inferred"),
    ("Safe driver discount", "Other", "other", "QUALIFIES_FOR", 2, 2, "supported"),
]
POLICY_DOCS = [  # title, type, date
    ("Renewal notice, autumn 2026", "insurance", "2 Sep 2026"),
    ("Premium bill, April 2026", "financial_invoice", "4 Apr 2026"),
    ("Declarations page 2026", "insurance", "1 Apr 2026"),
    ("ID cards, two vehicles", "insurance", "1 Apr 2026"),
    ("Lienholder notice", "legal_contract", "18 Mar 2026"),
    ("Telematics enrolment letter", "insurance", "2 Feb 2026"),
    ("Premium bill, October 2025", "financial_invoice", "6 Oct 2025"),
    ("Declarations page 2025", "insurance", "1 Oct 2025"),
    ("Body shop estimate", "personal", "14 Jul 2025"),
    ("Discount eligibility letter", "insurance", "3 Jun 2025"),
    ("Policy booklet PA-11", "insurance", "12 Mar 2024"),
    ("Application summary", "insurance", "3 Mar 2024"),
]

# The owner as a hub: family totals and the top two of each family.
HUB_TOTALS = {"parties": 88, "money": 102, "coverage": 37, "health": 44, "service": 29,
              "places": 50, "time": 18, "doclinks": 6, "loose": 9, "other": 7}
HUB_N = [
    ("Brightline Logistics", "Organization", "parties", "EMPLOYED_BY", 41, 58, "supported"),
    ("Dana Whitlock", "Person", "parties", "SIGNED_WITH", 12, 39, "supported"),
    ("Coastal Power Co.", "Organization", "money", "BILLED_BY", 61, 61, "supported"),
    ("Northgate Credit Union", "Organization", "money", "ACCOUNT_AT", 44, 21, "supported"),
    ("Policy HM-4471 (auto)", "InsurancePolicy", "coverage", "INSURED_UNDER", 7, 12, "supported"),
    ("Policy HO-2210 (home)", "InsurancePolicy", "coverage", "INSURED_UNDER", 6, 9, "supported"),
    ("Hypertension", "Condition", "health", "DIAGNOSED_WITH", 8, 11, "supported"),
    ("Lipid panel, Mar 2026", "MedicalResult", "health", "PATIENT_IN", 3, 3, "supported"),
    ("US Navy", "Organization", "service", "SERVED_IN", 19, 26, "supported"),
    ("County Tax Office", "Organization", "service", "FILED_WITH", 14, 44, "supported"),
    ("14 Larkspur Ct", "Address", "places", "LIVES_AT", 52, 18, "supported"),
    ("Port Ellery", "Location", "places", "RESIDES_IN", 20, 40, "supported"),
    ("Lease renewal, Jun 2026", "Event", "time", "RENEWED_ON", 4, 4, "supported"),
    ("Passport expiry, 2031", "DateEvent", "time", "EXPIRES_ON", 2, 2, "supported"),
    ("Mortgage note 88-2031", "Contract", "doclinks", "REFERENCED_IN", 5, 7, "supported"),
    ("Will and testament", "Contract", "doclinks", "AMENDED_BY", 1, 2, "inferred"),
    ("Riverside 10K", "Event", "loose", "RELATED_TO", 3, 3, "inferred"),
    ("Bayline Auto Body", "Organization", "loose", "ASSOCIATED_WITH", 2, 2, "supported"),
    ("Benefits portal", "System", "other", "USER_OF", 3, 5, "supported"),
    ("Library card", "Other", "other", "HOLDS", 2, 2, "supported"),
]
HUB_PARTIES_MORE = [
    ("Sam Okafor", "Person", "WORKS_WITH", 11), ("Harbor Mutual Insurance", "Organization", "MEMBER_OF", 9),
    ("Jordan Rivera", "Person", "SIGNED_WITH", 9), ("Port Ellery Clinic", "Organization", "PATIENT_OF", 8),
    ("Morgan Ellis", "Person", "PREPARED_BY", 7), ("Coastal Credit Partners", "Organization", "BORROWER_FROM", 6),
    ("Riley Chen", "Person", "AUTHOR_OF", 5), ("Lakeside HOA", "Organization", "MEMBER_OF", 5),
    ("Ellis and Park LLP", "Organization", "PARTY_TO", 4), ("Avery Brooks", "Person", "OFFICER_OF", 3),
]
assert len(HUB_N) == 20
for fam, total in HUB_TOTALS.items():
    shown = sum(1 for n in HUB_N if n[2] == fam)
    assert shown == 2, fam
assert sum(HUB_TOTALS.values()) == 390


# ---------------------------------------------------------------- canvas

def polar(cx, cy, R, deg):
    a = math.radians(deg)
    return cx + R * math.sin(a), cy - R * math.cos(a)


def radial(neigh, cx, cy, R, pills=None):
    """Fixed sectors: parties at 12 o'clock, then clockwise. Returns slots."""
    fams = [f for f in FAM_ORDER if any(n[2] == f for n in neigh) or (pills and f in pills)]
    sw = 360.0 / len(fams)
    slots, sectors = [], []
    for i, f in enumerate(fams):
        members = [n for n in neigh if n[2] == f]
        start = i * sw - sw / 2
        sectors.append((f, start, start + sw, len(members), pills.get(f, 0) if pills else 0))
        capped = bool(pills and pills.get(f))
        for j, it in enumerate(members):
            frac = (0.22, 0.78)[j] if capped and len(members) == 2 else (j + 0.5) / len(members)
            a = start + frac * sw
            x, y = polar(cx, cy, R, a)
            slots.append((it, a, x, y))
        if capped:  # the group sits just outside the ring, centred on its sector
            x, y = polar(cx, cy, R + 60, i * sw)
            slots.append((("__pill__", f), i * sw, x, y))
    return slots, sectors


def arc_path(cx, cy, R, a0, a1):
    x0, y0 = polar(cx, cy, R, a0)
    x1, y1 = polar(cx, cy, R, a1)
    large = 1 if (a1 - a0) > 180 else 0
    return f"M{x0:.2f},{y0:.2f}A{R:.2f},{R:.2f} 0 {large},1 {x1:.2f},{y1:.2f}"


def trim(x0, y0, x1, y1, r0, r1):
    dx, dy = x1 - x0, y1 - y0
    L = math.hypot(dx, dy) or 1
    ux, uy = dx / L, dy / L
    return x0 + ux * r0, y0 + uy * r0, x1 - ux * r1, y1 - uy * r1


def cut(s, n=28):
    return s if len(s) <= n else s[: n - 1] + "…"


def text_w(text, size=12, weight=400):
    return len(text) * (6.25 if size == 12 else 7.1) * (1.04 if weight >= 500 else 1)


class Placer:
    """Greedy label placement: first free candidate wins, else the label is skipped."""

    def __init__(self, W, H, blocks=()):
        self.W, self.H = W, H
        self.boxes = list(blocks)

    def add(self, b):
        self.boxes.append(b)

    def free(self, b):
        x0, y0, x1, y1 = b
        if x0 < 6 or y0 < 6 or x1 > self.W - 6 or y1 > self.H - 6:
            return False
        return all(x1 <= o[0] or x0 >= o[2] or y1 <= o[1] or y0 >= o[3] for o in self.boxes)

    def place(self, cands, w, force=False):
        for lx, ly, anchor in cands:
            x0 = lx - (w if anchor == "end" else w / 2 if anchor == "middle" else 0)
            b = (x0 - 2, ly - 11, x0 + w + 2, ly + 3)
            if self.free(b):
                self.add(b)
                return lx, ly, anchor
        if force:  # the first candidate inside the canvas, even if it overlaps
            for lx, ly, anchor in cands:
                x0 = lx - (w if anchor == "end" else w / 2 if anchor == "middle" else 0)
                if 6 <= x0 and x0 + w <= self.W - 6:
                    self.add((x0 - 2, ly - 11, x0 + w + 2, ly + 3))
                    return lx, ly, anchor
            return cands[0]
        return None


def label_cands(x, y, r, a):
    """Beside the node on the outer side, then below, then above."""
    s = math.sin(math.radians(a))
    c = -math.cos(math.radians(a))  # +1 at 6 o'clock
    side = (x + (r + 7) * (1 if s > 0 else -1), y + 4, "start" if s > 0 else "end")
    below = (x, y + r + 16, "middle")
    above = (x, y - r - 7, "middle")
    nudge = [(side[0], side[1] + d, side[2]) for d in ((-9, 9) if c < 0 else (9, -9))]
    if abs(s) < 0.17:
        return [below, side, *nudge, above] if c > 0 else [above, side, *nudge, below]
    return [side, *nudge, below, above] if c > 0 else [side, *nudge, above, below]


def link_svg(x0, y0, x1, y1, w, support, state=""):
    dash = ' stroke-dasharray="6 4"' if support == "inferred" else ""
    out = []
    if "sel" in state:
        out.append(f'<line x1="{x0:.1f}" y1="{y0:.1f}" x2="{x1:.1f}" y2="{y1:.1f}" class="lk-halo" style="stroke-width:{w + 12:.1f}"/>')
    cls = "lk" + (" lk-dim" if "dim" in state else "")
    out.append(f'<line x1="{x0:.1f}" y1="{y0:.1f}" x2="{x1:.1f}" y2="{y1:.1f}" class="{cls}" style="stroke-width:{w:.2f}"{dash}/>')
    if "arrow" in state:
        ax, ay = x0 + (x1 - x0) * 0.9, y0 + (y1 - y0) * 0.9
        ang = math.degrees(math.atan2(y1 - y0, x1 - x0))
        out.append(f'<path d="M0,0L-6,-3.5L-6,3.5Z" class="lk-arrow" transform="translate({ax:.1f},{ay:.1f}) rotate({ang:.1f})"/>')
    return "".join(out)


def extent(label, r):
    """Outer radius of a node's shape, for rings drawn around it."""
    return r * {"circle": 1, "dashed": 1, "square": 1.15, "diamond": 1.22, "hexagon": 1.08, "pentagon": 1.14,
                "triangle": 1.2, "star": 1.32, "plus": 1.2, "page": 1.1}[KINDS[label][2]]


def neighborhood_svg(W, H, cx, cy, R, focus, neigh, pills=None, sel_node=None, sel_link=None,
                     focus_r=None, show_sectors=True, hidden=(), blocks=()):
    """Return (svg, html_overlay) for a capped one-hop neighborhood."""
    slots, sectors = radial(neigh, cx, cy, R, pills)
    fr = focus_r or radius(focus["docs"])
    any_sel = isinstance(sel_node, str)
    pl = Placer(W, H, blocks)
    links, nodes, labels, overlay, sec = [], [], [], [], []
    if show_sectors:
        for f, a0, a1, n, extra in sectors:
            n -= sum(1 for m in neigh if m[2] == f and m[0] in hidden)  # filtered links leave the count
            if n + extra == 0:
                continue
            gap = 2.5
            sec.append(f'<path d="{arc_path(cx, cy, R - 34, a0 + gap, a1 - gap)}" class="sector"/>')
            mid = (a0 + a1) / 2
            sn = math.sin(math.radians(mid))
            sx, sy = polar(cx, cy, R - (46 if abs(sn) > 0.3 else 64), mid)
            anchor = "end" if sn > 0.3 else ("start" if sn < -0.3 else "middle")
            name = FAM_NAME[f]
            count = f'<tspan class="sl-n">{n + extra}</tspan>'
            if anchor != "middle" and len(name) > 14:  # two short lines keep side labels out of the centre
                k = name.rfind(" ", 0, len(name) // 2 + 4)
                body = (f'<tspan x="{sx:.1f}" dy="-4">{esc(name[:k])}</tspan>'
                        f'<tspan x="{sx:.1f}" dy="16">{esc(name[k + 1:])} {count}</tspan>')
            else:
                body = f"{esc(name)} {count}"
            sec.append(f'<text x="{sx:.1f}" y="{sy + 4:.1f}" text-anchor="{anchor}" class="sl">{body}</text>')
            lines = [name] if body.startswith(esc(name)) else [name[:k], name[k + 1:] + " 00"]
            sw_ = max(text_w(t) for t in lines) + 16
            sx0 = sx - (sw_ if anchor == "end" else sw_ / 2 if anchor == "middle" else 0)
            pl.add((sx0, sy - 12 - (4 if len(lines) > 1 else 0), sx0 + sw_, sy + 6 + (12 if len(lines) > 1 else 0)))
    # obstacles first: every node and pill, the focus label
    placed = []
    for it, a, x, y in slots:
        if it[0] == "__pill__":
            f = it[1]
            n = pills[f]
            text = f"+{n} more"
            w = text_w(text) + 26
            pl.add((x - w / 2 - 3, y - 16, x + w / 2 + 3, y + 16))
            x0, y0, x1, y1 = trim(cx, cy, x, y, fr + 4, 15)
            links.append(f'<line x1="{x0:.1f}" y1="{y0:.1f}" x2="{x1:.1f}" y2="{y1:.1f}" class="lk-agg"/>')
            cls = "pill" + (" is-sel" if sel_node == ("pill", f) else "")
            overlay.append(f'<button class="{cls}" style="left:{x:.1f}px;top:{y:.1f}px"'
                           f' aria-label="{n} more in {esc(FAM_NAME[f])}">{esc(text)}</button>')
            continue
        name, label, fam, typ, w, d, sup = it[:7]
        if name in hidden:
            continue
        r = radius(d)
        e = extent(label, r) + (4 if name == OWNER else 0)
        pl.add((x - e - 2, y - e - 2, x + e + 2, y + e + 2))
        placed.append((it, a, x, y, r, e))
    fe = extent(focus["label"], fr) + (4 if focus.get("hub") else 0)
    pl.add((cx - fe - 2, cy - fe - 2, cx + fe + 2, cy + fe + 2))
    fy = cy + fe + 18
    fname = cut(focus["name"])
    pl.place([(cx, fy, "middle")], text_w(fname, 13, 500), force=True)
    labels.append(f'<text x="{cx:.1f}" y="{fy:.1f}" text-anchor="middle" class="nl nl-focus">{esc(fname)}</text>')
    # links, with the selected link's label reserved first
    for it, a, x, y, r, e in placed:
        name, label, fam, typ, w, d, sup = it[:7]
        is_sel = sel_node == name
        lstate = ("" if is_sel else "dim") if any_sel else ""
        if sel_link == (name, typ):
            lstate = "sel arrow"
        if is_sel:
            lstate = "arrow"
        x0, y0, x1, y1 = trim(cx, cy, x, y, fe + 3, e + 3)
        links.append(link_svg(x0, y0, x1, y1, link_width(w), sup, lstate))
        if "arrow" in lstate:
            text = f"{typ} · {w} docs"
            spots = [(x0 + (x1 - x0) * t, y0 + (y1 - y0) * t + dy, "middle") for t in (0.55, 0.4, 0.7) for dy in (-9, 18)]
            spot = pl.place(spots, text_w(text, 12, 500), force=True)
            labels.append(f'<text x="{spot[0]:.1f}" y="{spot[1]:.1f}" text-anchor="middle" class="nl ll">{esc(text)}</text>')
        state = "sel" if is_sel else ("faded" if any_sel else "")
        nodes.append(kind_svg(label, x, y, r, state, hub=(name == OWNER)))
    # node labels: selection first, then evidence order
    order = sorted(placed, key=lambda p: (p[0][0] != sel_node, (sel_link or ("",))[0] != p[0][0], -p[0][4], p[0][0]))
    for it, a, x, y, r, e in order:
        name = it[0]
        text = cut(name)
        force = name == sel_node or (sel_link and sel_link[0] == name)
        spot = pl.place(label_cands(x, y, e, a), text_w(text), force=bool(force))
        if spot:
            lcls = "nl" + (" nl-dim" if any_sel and name != sel_node else "")
            labels.append(f'<text x="{spot[0]:.1f}" y="{spot[1]:.1f}" text-anchor="{spot[2]}" class="{lcls}">{esc(text)}</text>')
    nodes.append(kind_svg(focus["label"], cx, cy, fr, hub=focus.get("hub", False)))
    svg = (f'<svg class="canvas" width="{W}" height="{H}" viewBox="0 0 {W} {H}" role="img" '
           f'aria-label="{esc(focus.get("aria", ""))}">{"".join(sec)}{"".join(links)}{"".join(nodes)}{"".join(labels)}</svg>')
    return svg, "".join(overlay)


# ---------------------------------------------------------------- shell parts

NAV = [("Ask", "message-square-text"), ("Documents", "file-text"), ("Graph", "network"), ("Entities", "users-round")]
NAV2 = [("Status", "activity"), ("Hubs", "layout-grid"), ("Logs", "scroll-text")]


def sidebar():
    items = "".join(f'<a href="#"{" aria-current=page" if n == "Graph" else ""}>{icon(i)}{n}</a>' for n, i in NAV)
    more = "".join(f'<a href="#">{icon(i)}{n}</a>' for n, i in NAV2)
    return (f'<nav class="nav" aria-label="Main"><p class="brand">Knowledge Graph</p>{items}'
            f'<p class="more">More</p>{more}<p class="health">{icon("activity", 14)}Backend up</p></nav>')


def tabbar():
    items = [("Ask", "message-square-text"), ("Documents", "file-text"), ("Graph", "network"),
             ("Entities", "users-round"), ("More", "ellipsis")]
    return ('<nav class="tabbar" aria-label="Main">' + "".join(
        f'<a href="#"{" aria-current=page" if n == "Graph" else ""}>{icon(i, 20)}<span>{n}</span></a>'
        for n, i in items) + "</nav>")


def views(cur):
    return ('<div class="seg" role="group" aria-label="View">' + "".join(
        f'<button aria-pressed="{str(v == cur).lower()}">{v}</button>' for v in ("Neighborhood", "Table", "Path"))
        + "</div>")


def header(view, filters=0, query=""):
    val = f' value="{esc(query)}"' if query else ""
    fl = f'<span class="cnt num">{filters}</span>' if filters else ""
    return (f'<header class="gh"><h1>Graph</h1>'
            f'<label class="search">{icon("search")}<span class="sr">Search entities</span>'
            f'<input placeholder="Search entities"{val}><kbd>/</kbd></label>'
            f'<div class="gh-r">{views(view)}<button class="btn">{icon("sliders-horizontal")}Filters{fl}</button></div></header>')


def zoom():
    return ('<div class="zoom" role="group" aria-label="Zoom">'
            f'<button class="ib" aria-label="Zoom in">{icon("plus")}</button>'
            f'<button class="ib" aria-label="Zoom out">{icon("minus")}</button>'
            f'<button class="ib" aria-label="Fit view">{icon("maximize")}</button></div>')


def dt(t):
    return f'<span class="dt" style="--dot:var(--doc-{t.replace("_", "-")})">{esc(DOC_WORD[t])}</span>'


def shelf(docs, marked=(), more=0):
    chips = "".join(
        f'<button class="ev{" is-mark" if t in marked else ""}"><span class="dot" style="--dot:var(--doc-{ty.replace("_", "-")})"></span>'
        f'<span class="ev-t">{esc(t)}</span><span class="ev-d num">{esc(d)}</span></button>'
        for t, ty, d in docs)
    if more:
        chips += f'<button class="ev ev-more">{more} more</button>'
    return f'<div class="shelf"><p class="shelf-h">Evidence · <span class="num">{len(docs) + more}</span> documents</p><div class="shelf-c">{chips}</div></div>'


def legend(kinds, links=("supported", "inferred"), agg=False, open_=True, cls="legend"):
    if not open_:
        return f'<button class="{cls} legend-c" aria-expanded="false">{icon("chevron-up", 14)}Legend</button>'
    rows = "".join(f'<li>{swatch(k, 16)}<span>{esc(KINDS[k][4])}</span></li>' for k in kinds)
    ls = []
    if "supported" in links:
        ls.append('<li><svg width="24" height="10" aria-hidden="true"><line x1="1" y1="5" x2="23" y2="5" class="lk" style="stroke-width:2"/></svg><span>Supported</span></li>')
    if "inferred" in links:
        ls.append('<li><svg width="24" height="10" aria-hidden="true"><line x1="1" y1="5" x2="23" y2="5" class="lk" style="stroke-width:2" stroke-dasharray="6 4"/></svg><span>Inferred</span></li>')
    if agg:
        ls.append('<li><svg width="24" height="10" aria-hidden="true"><line x1="1" y1="5" x2="23" y2="5" class="lk-agg"/></svg><span>Hidden group</span></li>')
    size = ('<p class="lg-size"><svg width="46" height="20" aria-hidden="true">'
            '<circle cx="7" cy="12" r="4" class="lg-c"/><circle cx="20" cy="12" r="6" class="lg-c"/>'
            '<circle cx="36" cy="10" r="9" class="lg-c"/></svg>Size: evidence documents</p>')
    return (f'<section class="{cls}" aria-label="Legend"><div class="lg-h"><h2>Legend</h2>'
            f'<button class="ib ib-s" aria-label="Collapse legend" aria-expanded="true">{icon("chevron-down", 14)}</button></div>'
            f'<ul class="lg-k">{rows}</ul><ul class="lg-l">{"".join(ls)}</ul>{size}</section>')


def kinds_in(neigh, focus_label):
    seen = []
    order = list(KINDS)
    for lab in [focus_label] + [n[1] for n in neigh]:
        if lab not in seen:
            seen.append(lab)
    return sorted(seen, key=order.index)


SUPPORT_WORD = {"supported": "Supported", "inferred": "Inferred", "mixed": "Stated in 1, inferred in 1"}


def words(typ):
    return typ.replace("_", " ").capitalize()


# ---------------------------------------------------------------- inspectors

def insp_entity_policy():
    fams = {}
    for n in POLICY_N:
        fams.setdefault(n[2], []).append(n)
    groups = []
    for i, f in enumerate([f for f in FAM_ORDER if f in fams]):
        rows = ""
        if i == 0:
            rows = '<ul class="lrows">' + "".join(
                f'<li>{swatch(lab)}<span class="lr-n">{esc(name)}</span><span class="lr-t">{words(typ)}</span>'
                f'<span class="lr-d num">{w} docs</span></li>' for name, lab, _, typ, w, _, _ in fams[f]) + "</ul>"
        inf = sum(1 for n in fams[f] if n[6] != "supported")
        note = f'<span class="fx-i">{inf} inferred</span>' if inf else ""
        groups.append(f'<div class="fx{" is-open" if i == 0 else ""}"><button class="fx-h" aria-expanded="{str(i == 0).lower()}">'
                      f'{icon("chevron-down" if i == 0 else "chevron-right", 14)}<span>{FAM_NAME[f]}</span>{note}'
                      f'<span class="num fx-c">{len(fams[f])}</span></button>{rows}</div>')
    docs = "".join(f'<li><span class="dl-t">{esc(t)}</span><span class="dl-m num">{dt(ty)}<span>{d}</span></span></li>'
                   for t, ty, d in POLICY_DOCS[:4])
    return f'''<aside class="insp" aria-label="Inspector">
<p class="in-k">{swatch("InsurancePolicy", 16)}Insurance policy</p>
<h2 class="in-name">Policy HM-4471 (auto)</h2>
<p class="in-al">Also HM4471-A, auto policy</p>
<div class="in-acts"><button class="btn pri">{icon("message-square-text")}Ask about this</button><button class="btn">{icon("file-text")}<span><span class="num">12</span> documents</span></button></div>
<div class="in-acts2"><button class="lnk">Path from here</button><button class="lnk">Open in table</button></div>
<h3>Key facts</h3>
<dl class="facts">
<div><dt>Issued by</dt><dd>Harbor Mutual Insurance</dd><dd class="sw">Supported</dd></div>
<div><dt>Insured</dt><dd>Alex Rivera</dd><dd class="sw">Supported</dd></div>
<div><dt>Premium</dt><dd>$612, April 2026</dd><dd class="sw">Supported</dd></div>
<div><dt>Term</dt><dd class="num">1 Apr 2026 to 1 Oct 2026</dd><dd class="sw">Supported</dd></div>
</dl>
<p class="in-sum">Seen in <span class="num">12</span> documents from 3 Mar 2024 to 2 Sep 2026. <span class="num">3</span> links are inferred.</p>
<h3>Links by family</h3>
{"".join(groups)}
<h3>Mentioned in</h3>
<ul class="dlist">{docs}</ul>
<button class="lnk">Show all 12 documents</button>
</aside>'''


def insp_link():
    return f'''<aside class="insp" aria-label="Inspector">
<div class="in-top"><p class="in-k">Link in Coverage and obligations</p><button class="ib" aria-label="Clear selection">{icon("x")}</button></div>
<h2 class="in-name in-link">Policy HM-4471 (auto)<span class="in-rel">covers</span>2014 Honda Fit</h2>
<p class="in-meta"><span class="sup-mixed">Stated in 1, inferred in 1</span><span class="num">2 documents</span><span class="num">Lowest confidence 0.62</span></p>
<article class="src is-stated">
<header><h3 class="src-t">Declarations page 2026</h3><span class="src-s">Stated</span></header>
<p class="src-m num">{dt("insurance")}<span>1 Apr 2026</span><span>Confidence 0.94</span></p>
<blockquote>“Vehicles covered under this policy: 1. 2019 Subaru Outback … 2. <mark>2014 Honda Fit</mark>, VIN ending 3381.”</blockquote>
<div class="src-a"><button class="btn btn-s">Open document</button><button class="btn btn-s">{icon("external-link", 14)}Open in Paperless</button><button class="lnk">{icon("flag", 14)}Wrong link?</button></div>
</article>
<article class="src is-inferred">
<header><h3 class="src-t">Renewal notice, autumn 2026</h3><span class="src-s">Inferred</span></header>
<p class="src-m num">{dt("insurance")}<span>2 Sep 2026</span><span>Confidence 0.62</span></p>
<blockquote>“Your renewal premium for <mark>HM-4471</mark> reflects changes to listed vehicles and drivers.”</blockquote>
<p class="src-r"><span>Rationale</span> The notice names the policy and its listed vehicles. The 2014 Honda Fit is listed on the same account in the declarations page.</p>
<div class="src-a"><button class="btn btn-s">Open document</button><button class="btn btn-s">{icon("external-link", 14)}Open in Paperless</button><button class="lnk">{icon("flag", 14)}Wrong link?</button></div>
</article>
<div class="in-acts"><button class="btn pri">{icon("message-square-text")}Ask about this link</button></div>
</aside>'''


def insp_pill():
    rows = "".join(f'<li>{swatch(lab)}<span class="lr-n">{esc(n)}</span><span class="lr-t">{words(t)}</span>'
                   f'<span class="lr-d num">{w} docs</span></li>' for n, lab, t, w in HUB_PARTIES_MORE)
    return f'''<aside class="insp" aria-label="Inspector">
<p class="in-k">{swatch("Person", 16)}Person, the archive owner</p>
<h2 class="in-name">Alex Rivera</h2>
<p class="in-al">Hub: <span class="num">390</span> links in <span class="num">10</span> families. The canvas shows the top 2 of each family.</p>
<div class="grp">
<div class="in-top"><h3 class="grp-h">Parties and roles</h3><button class="ib" aria-label="Clear selection">{icon("x")}</button></div>
<p class="in-sum"><span class="num">88</span> links. <span class="num">2</span> are on the canvas and <span class="num">86</span> are in this group, ranked by evidence documents.</p>
<div class="in-acts"><button class="btn pri">{icon("plus")}Add 25 to the canvas</button><button class="btn">Open in table</button></div>
<ul class="lrows">{rows}</ul>
<button class="lnk">Show all 86</button>
</div>
</aside>'''


# ---------------------------------------------------------------- screens

CV_W, CV_H = 876, 836  # canvas area of a desktop graph screen
SHELF_H = 64
LEG_H = 196  # open legend height


def blocks_for(W, H, legend_open=True, caption_w=560):
    leg = (16, H - 16 - LEG_H, 16 + 248, H - 16) if legend_open else (16, H - 48, 112, H - 16)
    return [(0, 0, caption_w, 40), leg, (W - 52, H - 118, W - 14, H - 14)]


def graph_screen(view_canvas, inspector, view="Neighborhood", caption="", filters=0, extra="", chips=""):
    return f'''<div class="app">{sidebar()}<div class="ws">{header(view, filters)}
<div class="gb{"" if inspector else " no-insp"}"><div class="cv">{chips}{view_canvas}{extra}
<p class="cap" role="status">{caption}</p>{zoom()}</div>{inspector}</div></div></div>'''


def screen_neighborhood():
    focus = dict(POLICY, aria="Neighborhood of Policy HM-4471 (auto): 16 links in 8 families, 3 inferred")
    H = CV_H - SHELF_H
    svg, ov = neighborhood_svg(CV_W, H, 438, 372, 210, focus, POLICY_N, blocks=blocks_for(CV_W, H))
    lg = legend(kinds_in(POLICY_N, "InsurancePolicy"))
    canvas = f'<div class="stage" style="height:{H}px">{svg}{ov}{lg}</div>{shelf(POLICY_DOCS[:5], more=7)}'
    return graph_screen(canvas, insp_entity_policy(),
                        caption="Policy HM-4471 (auto): <span class=num>16</span> links in <span class=num>8</span> families, <span class=num>3</span> inferred")


def screen_link():
    focus = dict(POLICY, aria="Neighborhood of Policy HM-4471 (auto), link COVERS 2014 Honda Fit selected")
    H = CV_H - SHELF_H
    svg, ov = neighborhood_svg(CV_W, H, 438, 372, 210, focus, POLICY_N, sel_link=("2014 Honda Fit", "COVERS"),
                               blocks=blocks_for(CV_W, H, legend_open=False))
    canvas = (f'<div class="stage" style="height:{H}px">{svg}{ov}{legend([], open_=False)}</div>'
              f'{shelf(POLICY_DOCS[:5], marked=("Renewal notice, autumn 2026",), more=7)}')
    return graph_screen(canvas, insp_link(),
                        caption="Policy HM-4471 (auto): <span class=num>16</span> links in <span class=num>8</span> families, <span class=num>3</span> inferred")


def screen_hub():
    focus = dict(name=OWNER, label="Person", docs=212, hub=True,
                 aria="Neighborhood of Alex Rivera: 390 links in 10 families, 31 inferred")
    pills = {f: HUB_TOTALS[f] - 2 for f in HUB_TOTALS}
    H = CV_H - SHELF_H
    svg, ov = neighborhood_svg(CV_W, H, 438, 392, 250, focus, HUB_N, pills=pills, sel_node=("pill", "parties"),
                               blocks=blocks_for(CV_W, H, legend_open=False, caption_w=640))
    lg = legend(kinds_in(HUB_N, "Person"), agg=True, open_=False)
    docs = [("W-2, Brightline Logistics 2025", "work", "28 Jan 2026"), ("Power bill, October 2026", "financial_invoice", "6 Nov 2026"),
            ("Lab report, lipid panel", "medical_lab", "12 Mar 2026"), ("Property tax statement 2026", "government_tax", "2 Mar 2026"),
            ("Lease renewal 2026", "legal_contract", "1 Jun 2026")]
    canvas = f'<div class="stage" style="height:{H}px">{svg}{ov}{lg}</div>{shelf(docs, more=207)}'
    return graph_screen(canvas, insp_pill(),
                        caption="Alex Rivera: <span class=num>390</span> links in <span class=num>10</span> families. Large hub: the top <span class=num>2</span> of each family are shown.")


def filter_groups(touch=False):
    """The seven filter groups with live counts for the current focus."""
    ck = "ck ck-t" if touch else "ck"
    fam_counts = [("parties", 3), ("money", 2), ("coverage", 3), ("places", 2), ("time", 2), ("doclinks", 1),
                  ("loose", 1), ("other", 2)]
    fams = "".join(f'<label class="{ck}"><input type="checkbox" checked><span>{FAM_NAME[f]}</span><span class="num ck-n">{n}</span></label>'
                   for f, n in fam_counts)
    fams += f'<label class="{ck}"><input type="checkbox"><span>Evidence</span><span class="num ck-n">31</span></label>'
    sup = "".join(f'<label class="{ck}"><input type="radio" name="sup"{" checked" if w == "Supported only" else ""}><span>{w}</span><span class="num ck-n">{n}</span></label>'
                  for w, n in (("All", 16), ("Supported only", 13), ("Inferred only", 3)))
    types = "".join(f'<button class="tchip" aria-pressed="{str(k in ("insurance", "financial_invoice")).lower()}"><span class="dot" style="--dot:var(--doc-{k.replace("_", "-")})"></span>{w}</button>'
                    for k, w in DOC_TYPES)
    docs = "".join(f'<button aria-pressed="{str(n == 1).lower()}">{n}</button>' for n in (1, 2, 3, 5, 10))
    return f'''<fieldset><legend>Relationship families</legend>{fams}</fieldset>
<fieldset><legend>Support</legend>{sup}</fieldset>
<fieldset><legend>Confidence</legend><div class="range"><span>Minimum</span><span class="num range-v">0.70</span></div>
<div class="slider" role="presentation"><span class="sl-fill" style="width:70%"></span><span class="sl-th" style="left:70%"></span></div>
<label class="{ck}"><input type="checkbox" checked><span>Include links with no score</span><span class="num ck-n">4</span></label></fieldset>
<fieldset><legend>Evidence documents per link</legend><div class="seg{" seg-t" if touch else ""}">{docs}</div></fieldset>
<fieldset><legend>Document date</legend><div class="months"><label><span>From</span><button class="sel">Jan 2026{icon("chevron-down", 14)}</button></label><label><span>To</span><button class="sel">Nov 2026{icon("chevron-down", 14)}</button></label></div></fieldset>
<fieldset><legend>Document type</legend><div class="tchips">{types}</div></fieldset>
<fieldset><legend>Owner</legend><label class="{ck}"><input type="checkbox"><span>Hide the owner entity</span></label></fieldset>'''


def screen_filters():
    focus = dict(POLICY, aria="Neighborhood of Policy HM-4471 (auto), filtered: 10 of 16 links")
    H = CV_H - SHELF_H - 52
    W = 1256
    # inferred links, the Honda link (lowest confidence 0.62), a bill dated before January 2026,
    # and a link whose only document is a Legal one
    hidden = ("14 Larkspur Ct", "Bayline Auto Body", "Telematics plug TX-2", "2014 Honda Fit",
              "Premium, Oct 2025, $588", "Northgate Credit Union")
    svg, ov = neighborhood_svg(W, H, 808, 352, 210, focus, POLICY_N, hidden=hidden,
                               blocks=[(0, 0, 372, H), (W - 52, H - 118, W - 14, H - 14)])
    chips = ('<div class="chips" aria-label="Active filters">'
             + "".join(f'<button class="fchip">{t}{icon("x", 14)}</button>' for t in
                       ("Supported only", "Confidence 0.70 or more", "From Jan 2026", "Insurance, Invoice"))
             + '<button class="lnk">Reset filters</button></div>')
    sheet = f'''<section class="fsheet" aria-label="Filters">
<div class="fs-h"><h2>Filters</h2><button class="ib" aria-label="Close filters">{icon("x")}</button></div>
<div class="fs-b">{filter_groups()}</div>
<div class="fs-f"><button class="btn">Reset filters</button><button class="btn pri"><span>Show <span class="num">10</span> links</span></button></div>
</section>'''
    canvas = f'{chips}<div class="stage" style="height:{H}px">{svg}{ov}</div>{shelf(POLICY_DOCS[:4], more=4)}'
    return graph_screen(canvas, "", filters=4, extra=sheet,
                        caption="<span class=num>10</span> of <span class=num>16</span> links match. <span class=num>6</span> are hidden by filters.")


PATHS = [  # nodes, links (type, weight, support), weakest
    ([("Dana Whitlock", "Person"), ("Loan 88-2031", "FinancialItem"), ("Northgate Credit Union", "Organization")],
     [("CO_BORROWER_ON", 3, "supported"), ("LOAN_FROM", 5, "supported")]),
    ([("Dana Whitlock", "Person"), ("Brightline Logistics", "Organization"), ("Northgate Credit Union", "Organization")],
     [("EMPLOYED_BY", 6, "supported"), ("PAYROLL_ACCOUNT_AT", 2, "supported")]),
    ([("Dana Whitlock", "Person"), ("14 Larkspur Ct", "Address"), ("Northgate Credit Union", "Organization")],
     [("RESIDES_AT", 6, "supported"), ("MAILING_ADDRESS_OF", 1, "inferred")]),
    ([("Dana Whitlock", "Person"), ("Lease 2024", "Contract"), ("Harbor Property Management", "Organization"), ("Northgate Credit Union", "Organization")],
     [("SIGNED", 2, "supported"), ("PARTY_TO", 2, "supported"), ("ACCOUNT_AT", 1, "supported")]),
]


def path_rank_ok():
    keys = [(-min(w for _, w, _ in p[1]), len(p[1]), sum(s == "inferred" for _, _, s in p[1])) for p in PATHS]
    assert keys == sorted(keys), keys


path_rank_ok()


def path_svg(W, H, sel=0):
    cols = 4
    x0, x1 = 90, W - 120
    xs = [x0 + (x1 - x0) * i / (cols - 1) for i in range(cols)]
    cy = H / 2 - 30
    a = (xs[0], cy)
    b = (xs[-1], cy)
    out_l, out_n, out_t = [], [], []
    mids = []
    for pi, (nodes, links) in enumerate(PATHS):
        pts = [a]
        inner = nodes[1:-1]
        if len(inner) == 1:
            pts.append(((xs[1] + xs[2]) / 2, cy + (-150, 0, 150)[pi]))
        else:
            pts += [(xs[1], cy + 260), (xs[2], cy + 260)]
        pts.append(b)
        mids.append((pi, nodes, links, pts))
    order = [m for m in mids if m[0] != sel] + [m for m in mids if m[0] == sel]
    for pi, nodes, links, pts in order:
        on = pi == sel
        for k, (t, w, s) in enumerate(links):
            (ax, ay), (bx, by) = pts[k], pts[k + 1]
            mx = (ax + bx) / 2
            d = f"M{ax:.1f},{ay:.1f}C{mx:.1f},{ay:.1f} {mx:.1f},{by:.1f} {bx:.1f},{by:.1f}"
            dash = ' stroke-dasharray="6 4"' if s == "inferred" else ""
            cls = "pth-on" if on else "pth-off"
            out_l.append(f'<path d="{d}" class="{cls}"{dash}/>')
            if on:  # 6 px arrowhead at 90 % along the curve
                t_ = 0.9
                px = (1 - t_) ** 3 * ax + 3 * (1 - t_) ** 2 * t_ * mx + 3 * (1 - t_) * t_ ** 2 * mx + t_ ** 3 * bx
                py = (1 - t_) ** 3 * ay + 3 * (1 - t_) ** 2 * t_ * ay + 3 * (1 - t_) * t_ ** 2 * by + t_ ** 3 * by
                dx = 3 * (1 - t_) ** 2 * (mx - ax) + 6 * (1 - t_) * t_ * 0 + 3 * t_ ** 2 * (bx - mx)
                dy = 6 * (1 - t_) * t_ * (by - ay)
                out_t.append(f'<path d="M0,0L-6,-3.5L-6,3.5Z" class="lk-arrow" transform="translate({px:.1f},{py:.1f}) rotate({math.degrees(math.atan2(dy, dx)):.1f})"/>')
            if on:  # beside the curve's midpoint, on its outer side
                ty = (ay + by) / 2
                anchor, tx = ("middle", mx) if abs(by - ay) < 1 else (("end", mx - 14) if by < ay else ("start", mx + 14))
                out_t.append(f'<text x="{tx:.1f}" y="{ty - (10 if anchor == "middle" else 4):.1f}" text-anchor="{anchor}" class="nl ll">{t} · {w} docs</text>')
        for k, (n, lab) in enumerate(nodes[1:-1], start=1):
            x, y = pts[k]
            out_n.append(kind_svg(lab, x, y, radius(6), "" if on else "faded"))
            out_t.append(f'<text x="{x:.1f}" y="{y + radius(6) + 18:.1f}" text-anchor="middle" class="nl{"" if on else " nl-dim"}">{esc(n)}</text>')
    out_n.append(kind_svg("Person", a[0], a[1], 18))
    out_n.append(kind_svg("Organization", b[0], b[1], 18))
    out_t.append(f'<text x="{a[0]:.1f}" y="{a[1] + 40:.1f}" text-anchor="middle" class="nl nl-focus">Dana Whitlock</text>')
    out_t.append(f'<text x="{b[0]:.1f}" y="{b[1] + 40:.1f}" text-anchor="middle" class="nl nl-focus">Northgate Credit Union</text>')
    return (f'<svg class="canvas" width="{W}" height="{H}" viewBox="0 0 {W} {H}" role="img" '
            f'aria-label="4 paths from Dana Whitlock to Northgate Credit Union; path 1 selected">'
            f'{"".join(out_l)}{"".join(out_n)}{"".join(out_t)}</svg>')


def screen_path():
    W, H = CV_W, CV_H - 56
    svg = path_svg(W, H)
    bar = f'''<div class="pbar">
<label class="pick"><span>From</span><span class="pick-v">{swatch("Person", 14)}Dana Whitlock</span></label>
<button class="ib" aria-label="Swap the two entities">{icon("arrow-left-right")}</button>
<label class="pick"><span>To</span><span class="pick-v">{swatch("Organization", 14)}Northgate Credit Union</span></label>
<div class="pb-g"><span class="pb-l">Most links</span><div class="seg"><button aria-pressed="false">2</button><button aria-pressed="false">3</button><button aria-pressed="true">4</button></div></div>
<label class="tog"><span class="sw-t" aria-hidden="true"></span><span>Include the owner</span></label>
</div>'''
    plist = []
    for i, (nodes, links) in enumerate(PATHS):
        weak = min(w for _, w, _ in links)
        inf = any(s == "inferred" for _, _, s in links)
        route = f'<span class="pl-r">{" → ".join(esc(n) for n, _ in nodes[1:-1])}</span>'
        meta = f'<span class="num">{len(links)} links</span><span>weakest link <span class="num">{weak}</span> {"doc" if weak == 1 else "docs"}</span>' + ('<span>has an inferred link</span>' if inf else "")
        plist.append(f'<li><button class="pl{" is-sel" if i == 0 else ""}" aria-pressed="{str(i == 0).lower()}"><span class="pl-n num">{i + 1}</span><span class="pl-b">{route}<span class="pl-m">{meta}</span></span></button></li>')
    steps = []
    nodes, links = PATHS[0]
    quotes = ["“Borrowers: Alex Rivera and <mark>Dana Whitlock</mark>, jointly and severally.”",
              "“<mark>Northgate Credit Union</mark> (the Lender) agrees to lend the principal sum of $18,400.”"]
    titles = ["Loan agreement 88-2031, 12 Feb 2024", "Loan statement, March 2026"]
    for k, (t, w, s) in enumerate(links):
        steps.append(f'''<li class="pstep"><p class="ps-e">{swatch(nodes[k][1], 14)}{esc(nodes[k][0])}</p>
<div class="ps-l"><p class="ps-t">{words(t)} <span class="sw">Supported, <span class="num">{w}</span> docs</span></p>
<blockquote>{quotes[k]}</blockquote><p class="ps-s">{esc(titles[k])}</p></div></li>''')
    steps.append(f'<li class="pstep"><p class="ps-e">{swatch(nodes[-1][1], 14)}{esc(nodes[-1][0])}</p></li>')
    insp = f'''<aside class="insp" aria-label="Inspector">
<h2 class="in-name">How they are related</h2>
<p class="in-al"><span class="num">4</span> paths of at most <span class="num">4</span> links, strongest first. The owner is left out.</p>
<ol class="plist">{"".join(plist)}</ol>
<h3>Path 1</h3>
<ol class="steps">{"".join(steps)}</ol>
<div class="in-acts"><button class="btn pri">{icon("message-square-text")}Ask how they are related</button></div>
</aside>'''
    canvas = f'{bar}<div class="stage" style="height:{H}px">{svg}</div>'
    return f'''<div class="app">{sidebar()}<div class="ws">{header("Path")}
<div class="gb"><div class="cv">{canvas}{zoom()}</div>{insp}</div></div></div>'''


TABLE_ROWS = [  # on canvas, family, type, dir, other, label, support, docs, conf, latest
    (True, "money", "BILLED_BY", "in", "Coastal Power Co.", "Organization", "supported", 61, "0.93", "6 Nov 2026"),
    (True, "places", "LIVES_AT", "out", "14 Larkspur Ct", "Address", "supported", 52, "0.97", "2 Nov 2026"),
    (True, "money", "ACCOUNT_AT", "out", "Northgate Credit Union", "Organization", "supported", 44, "0.91", "31 Oct 2026"),
    (True, "parties", "EMPLOYED_BY", "out", "Brightline Logistics", "Organization", "supported", 41, "0.95", "28 Oct 2026"),
    (False, "money", "PAID_TO", "out", "Port Ellery Water", "Organization", "supported", 33, "0.92", "20 Oct 2026"),
    (False, "money", "TAXED_BY", "in", "County Tax Office", "Organization", "supported", 22, "0.88", "2 Mar 2026"),
    (True, "places", "RESIDES_IN", "out", "Port Ellery", "Location", "supported", 20, "No score", "2 Nov 2026"),
    (True, "service", "SERVED_IN", "out", "US Navy", "Organization", "supported", 19, "0.90", "11 Nov 2025"),
    (False, "money", "PAYS", "out", "Lakeside HOA", "Organization", "mixed", 15, "0.71", "1 Oct 2026"),  # 14 stated, 1 inferred
    (True, "service", "FILED_WITH", "out", "County Tax Office", "Organization", "supported", 14, "0.89", "14 Apr 2026"),
    (True, "parties", "SIGNED_WITH", "out", "Dana Whitlock", "Person", "supported", 12, "0.86", "1 Jun 2026"),
    (False, "parties", "WORKS_WITH", "out", "Sam Okafor", "Person", "inferred", 11, "0.58", "9 Sep 2026"),
    (False, "health", "PATIENT_OF", "out", "Port Ellery Clinic", "Organization", "supported", 9, "0.94", "12 Mar 2026"),
    (False, "parties", "MEMBER_OF", "out", "Harbor Mutual Insurance", "Organization", "supported", 9, "No score", "2 Sep 2026"),
    (True, "health", "DIAGNOSED_WITH", "out", "Hypertension", "Condition", "supported", 8, "0.83", "12 Mar 2026"),
    (True, "coverage", "INSURED_UNDER", "out", "Policy HM-4471 (auto)", "InsurancePolicy", "supported", 7, "0.90", "2 Sep 2026"),
    (False, "coverage", "BENEFICIARY_OF", "out", "Life policy LT-3301", "InsurancePolicy", "inferred", 6, "0.61", "5 May 2026"),
    (True, "coverage", "INSURED_UNDER", "out", "Policy HO-2210 (home)", "InsurancePolicy", "supported", 6, "0.88", "14 Jun 2026"),
    (False, "health", "TREATED_BY", "out", "Dr. Priya Nand", "Person", "supported", 6, "0.92", "12 Mar 2026"),
    (True, "doclinks", "REFERENCED_IN", "out", "Mortgage note 88-2031", "Contract", "supported", 5, "0.84", "12 Feb 2024"),
]


FILLER = [  # rows 21 to 50 of the first page: name, label, family, type, docs
    (n, lab, fam, typ, d) for (n, lab, fam, typ), d in zip(
        [(n, lab, "parties", t) for n, lab, t, _ in HUB_PARTIES_MORE[4:]]
        + [("Port Ellery Water", "Organization", "money", "BILLED_BY"), ("Coastal Credit Partners", "Organization", "money", "LOAN_FROM"),
           ("Car loan CL-5512", "FinancialItem", "money", "PAYS"), ("Property tax 2026", "FinancialItem", "money", "TAXED_BY"),
           ("Dental plan DP-90", "InsurancePolicy", "coverage", "COVERED_BY"), ("Umbrella policy UP-12", "InsurancePolicy", "coverage", "INSURED_UNDER"),
           ("Blood pressure log", "MedicalResult", "health", "PATIENT_IN"), ("Allergy, penicillin", "Condition", "health", "DIAGNOSED_WITH"),
           ("Navy Reserve", "Organization", "service", "SERVED_IN"), ("DMV, Port Ellery office", "Organization", "service", "FILED_WITH"),
           ("Harbor View Apartments", "Location", "places", "RESIDED_AT"), ("2 Harbor Way", "Address", "places", "ADDRESS_OF"),
           ("Passport renewal, 2021", "Event", "time", "RENEWED_ON"), ("Lease start, Jun 2024", "DateEvent", "time", "STARTS_ON"),
           ("Lease 2024", "Contract", "doclinks", "REFERENCED_IN"), ("Tax return 2025", "Contract", "doclinks", "AMENDED_BY"),
           ("Fitness club", "Organization", "loose", "RELATED_TO"), ("Bike, trail model", "Product", "loose", "ASSOCIATED_WITH"),
           ("Home router", "System", "other", "USER_OF"), ("Parking permit", "Other", "other", "HOLDS"),
           ("Sam Okafor", "Person", "loose", "RELATED_TO"), ("Library card", "Other", "other", "HOLDS"),
           ("Laptop, work issued", "Product", "other", "ASSIGNED"), ("Riverside 10K", "Event", "loose", "RELATED_TO")],
        [5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1])
]


def screen_table():
    sup = {"supported": "Supported", "inferred": "Inferred", "mixed": "Stated in 14, inferred in 1"}
    body = "".join(
        f'<tr{" aria-selected=true class=is-sel" if i == 3 else ""}><td class="c-on">{icon("check", 14) if on else "<span class=sr>Not on canvas</span>"}</td>'
        f'<td>{FAM_NAME[f]}</td><td>{words(t)}</td><td class="c-dir">{icon("arrow-right" if d == "out" else "arrow-left", 14)}{"Out" if d == "out" else "In"}</td>'
        f'<td class="c-ent">{swatch(lab, 14)}<span>{esc(o)}</span></td><td class="c-mut">{KINDS[lab][4]}</td>'
        f'<td class="{"c-inf" if s != "supported" else "c-mut"}">{sup[s]}</td><td class="num c-r">{n}</td>'
        f'<td class="num c-r{" c-mut" if c == "No score" else ""}">{c}</td><td class="num c-mut">{dd}</td></tr>'
        for i, (on, f, t, d, o, lab, s, n, c, dd) in enumerate(
            TABLE_ROWS + [(False, fam, typ, "out", n, lab, "supported", d, "No score" if k % 7 == 3 else f"0.{88 - k}", f"{1 + k % 27} Sep 2026")
                          for k, (n, lab, fam, typ, d) in enumerate(FILLER)]))
    assert len(TABLE_ROWS) + len(FILLER) == 50
    head = ("<tr><th>On canvas</th><th><button>Family</button></th><th><button>Relationship</button></th>"
            "<th>Direction</th><th><button>Other entity</button></th><th>Type</th><th><button>Support</button></th>"
            f'<th aria-sort="descending" class="c-r"><button>Documents{icon("arrow-down", 14)}</button></th>'
            "<th class=c-r><button>Lowest confidence</button></th><th><button>Latest document</button></th></tr>")
    return f'''<div class="app">{sidebar()}<div class="ws">{header("Table")}
<div class="tbl-wrap"><div class="tbl-top"><h2 class="tbl-h">Links of Alex Rivera</h2>
<p class="tbl-live" role="status"><span class="num">390</span> links, <span class="num">31</span> inferred</p>
<div class="chips"><button class="fchip">All families except Evidence{icon("x", 14)}</button></div></div>
<div class="tbl-scroll"><table class="tbl"><thead>{head}</thead><tbody>{body}</tbody></table></div>
<div class="pager"><span class="num">1–50 of 390</span><button class="btn btn-s" disabled>{icon("arrow-left", 14)}Previous</button><button class="btn btn-s">Next{icon("arrow-right", 14)}</button></div>
</div></div></div>'''


RECENT = [("Policy HM-4471 (auto)", "InsurancePolicy", 12), ("Harbor Mutual Insurance", "Organization", 33),
          ("Dana Whitlock", "Person", 39), ("Lipid panel, Mar 2026", "MedicalResult", 3),
          ("14 Larkspur Ct", "Address", 18), ("Northgate Credit Union", "Organization", 21),
          ("Loan 88-2031", "FinancialItem", 9), ("Hypertension", "Condition", 11),
          ("2019 Subaru Outback", "Product", 14), ("Lease 2024", "Contract", 6)]
CONNECTED = [("Coastal Power Co.", "Organization", 61), ("Brightline Logistics", "Organization", 58),
             ("County Tax Office", "Organization", 44), ("Port Ellery", "Location", 40),
             ("Dana Whitlock", "Person", 39), ("Harbor Mutual Insurance", "Organization", 33),
             ("US Navy", "Organization", 26), ("Northgate Credit Union", "Organization", 21),
             ("14 Larkspur Ct", "Address", 18), ("Port Ellery Clinic", "Organization", 17)]


def erow(n, lab, d, cls="er", bar=None):
    b = f'<span class="er-b" aria-hidden="true"><span style="width:{100 * d / bar:.0f}%"></span></span>' if bar else ""
    return (f'<li><a href="#" class="{cls}">{swatch(lab, 16)}<span class="er-n">{esc(n)}</span>'
            f'<span class="er-t">{KINDS[lab][4]}</span>{b}<span class="er-d num">{d} documents</span></a></li>')


def screen_landing():
    rec = "".join(erow(*r) for r in RECENT)
    con = "".join(erow(*r, cls="er er-bar", bar=CONNECTED[0][2]) for r in CONNECTED)
    return f'''<div class="app">{sidebar()}<main class="land">
<h1>Graph</h1>
<p class="land-s">Find a person, organization, place or policy, then follow its links to the documents behind them.</p>
<label class="bigsearch is-focus">{icon("search", 20)}<span class="sr">Search entities</span><input placeholder="Search entities by name or alias"><kbd>/</kbd></label>
<div class="land-c"><section><h2>Recent</h2><ul class="elist">{rec}</ul></section>
<section><h2>Most connected</h2><ul class="elist">{con}</ul><p class="land-n">Ranked by evidence documents. Your own entity is left out.</p></section></div>
</main></div>'''


SEARCH = [
    ("People", [("Lena Harborne", "Person", 4, None)]),
    ("Organizations", [("Harbor Mutual Insurance", "Organization", 33, None),
                       ("Harbor Property Management", "Organization", 8, None),
                       ("Northgate Credit Union", "Organization", 21, "Harbor Northgate CU")]),
    ("Money and agreements", [("Policy HM-4471 (auto)", "InsurancePolicy", 12, "Harbor auto policy"),
                              ("Policy HO-2210 (home)", "InsurancePolicy", 9, "Harbor home policy")]),
    ("Places", [("Harbor View Apartments", "Location", 3, None), ("2 Harbor Way", "Address", 2, None)]),
]


def hl(s, q="Harbor"):
    i = s.lower().find(q.lower())
    if i < 0:
        return esc(s)
    return esc(s[:i]) + "<mark>" + esc(s[i:i + len(q)]) + "</mark>" + esc(s[i + len(q):])


def screen_search():
    groups = []
    first = True
    for g, rows in SEARCH:
        lis = []
        for n, lab, d, al in rows:
            alias = f'<span class="er-a">Also {hl(al)}</span>' if al else ""
            lis.append(f'<li><a href="#" class="er{" is-act" if first else ""}"{" aria-selected=true" if first else ""}>{swatch(lab, 16)}'
                       f'<span class="er-n">{hl(n)}{alias}</span><span class="er-t">{KINDS[lab][4]}</span><span class="er-d num">{d} documents</span></a></li>')
            first = False
        groups.append(f'<li class="rg"><p class="rg-h">{g}</p><ul>{"".join(lis)}</ul></li>')
    rec = "".join(erow(*r) for r in RECENT[:6])
    con = "".join(erow(*r) for r in CONNECTED[:6])
    return f'''<div class="app">{sidebar()}<main class="land">
<h1>Graph</h1>
<p class="land-s">Find a person, organization, place or policy, then follow its links to the documents behind them.</p>
<div class="combo"><label class="bigsearch is-focus">{icon("search", 20)}<span class="sr">Search entities</span><input value="harbor" role="combobox" aria-expanded="true"><kbd>/</kbd></label>
<ul class="results" role="listbox" aria-label="8 results">{"".join(groups)}</ul></div>
<div class="land-c is-under"><section><h2>Recent</h2><ul class="elist">{rec}</ul></section>
<section><h2>Most connected</h2><ul class="elist">{con}</ul></section></div>
</main></div>'''


# ---------------------------------------------------------------- encoding reference

def screen_encoding():
    rows = []
    for pal, _, labels in PALETTE_ROWS:
        fam_word = {"people": "People", "org": "Organizations", "place": "Places", "money": "Money", "agree": "Agreements",
                    "thing": "Things", "health": "Health", "time": "Time", "docs": "Documents", "unknown": "Other kinds"}[pal]
        cells = []
        for lab in labels:
            cells.append(f'<span class="enc-n"><svg width="44" height="44" viewBox="0 0 44 44" aria-hidden="true">{kind_svg(lab, 22, 22, 15)}</svg>'
                         f'<span><b>{esc(KINDS[lab][4])}</b><span class="enc-s">{"Outlined" if KINDS[lab][1] else ("Dashed outline" if pal == "unknown" else "Filled")}</span></span></span>')
        if len(labels) == 1:
            cells.append('<span class="enc-n enc-none">No second type</span>')
        faded = f'<svg width="44" height="44" viewBox="0 0 44 44" aria-hidden="true">{kind_svg(labels[0], 22, 22, 15, "faded")}</svg>'
        shape = KINDS[labels[0]][2]
        rows.append(f'<tr><th scope="row">{fam_word}</th><td>{SHAPE_WORD[shape]}</td><td>{"".join(cells)}</td><td>{faded}</td></tr>')
    sizes = "".join(
        f'<span class="sz"><svg width="44" height="44" viewBox="0 0 44 44" aria-hidden="true">{kind_svg("Person", 22, 22, radius(d))}</svg>'
        f'<span class="num">{d}</span></span>' for d in (0, 1, 3, 7, 15, 40, 120))
    st = [("Default", ""), ("Hover", "hover"), ("Selected", "sel"), ("Keyboard focus", "kbd"), ("Faded", "faded")]
    states = "".join(f'<span class="sz"><svg width="60" height="60" viewBox="0 0 60 60" aria-hidden="true">{kind_svg("Organization", 30, 30, 15, s)}</svg><span>{w}</span></span>' for w, s in st)
    states += f'<span class="sz"><svg width="60" height="60" viewBox="0 0 60 60" aria-hidden="true">{kind_svg("Person", 30, 30, 15, hub=True)}</svg><span>Hub or owner</span></span>'

    def lrow(svg, word):
        return f'<li><svg width="132" height="28" aria-hidden="true">{svg}</svg><span>{word}</span></li>'
    lk = [
        lrow(link_svg(4, 14, 128, 14, link_width(1), "supported"), "Supported, 1 document"),
        lrow(link_svg(4, 14, 128, 14, link_width(4), "supported"), "Supported, 4 documents"),
        lrow(link_svg(4, 14, 128, 14, link_width(16), "supported"), "Supported, 16 or more"),
        lrow(link_svg(4, 14, 128, 14, link_width(2), "mixed"), "Stated in 1, inferred in 1 (solid)"),
        lrow(link_svg(4, 14, 128, 14, link_width(2), "inferred"), "Inferred (dashed 6 on, 4 off)"),
        lrow('<line x1="4" y1="14" x2="128" y2="14" class="lk-agg"/>', "Hidden group (dashed 3 on, 3 off)"),
        lrow(link_svg(4, 14, 124, 14, link_width(4), "supported", "sel arrow"), "Selected link"),
        lrow('<path d="M4,14H128" class="pth-on"/>', "Selected path"),
    ]
    return f'''<div class="app">{sidebar()}<main class="enc">
<h1>Graph encoding</h1>
<p class="land-s">Every node kind has its own shape. Color repeats the shape, and the second type of a family is drawn outlined. Shapes in this sheet are proposals for the owner to confirm: pentagon for agreements, star for time and dashed circle for other kinds.</p>
<div class="enc-g">
<section class="enc-k"><h2>Node kinds</h2>
<table class="enc-t"><thead><tr><th>Family</th><th>Shape</th><th>Types</th><th>Faded</th></tr></thead><tbody>{"".join(rows)}</tbody></table></section>
<div class="enc-r">
<section><h2>Links</h2><ul class="enc-l">{"".join(lk)}</ul></section>
<section><h2>Size: evidence documents</h2><div class="szs">{sizes}</div><p class="enc-p">Radius 6 to 18 px, growing with the log of the document count. Icons appear from 10 px.</p></section>
<section><h2>States</h2><div class="szs">{states}</div></section>
<section><h2>Group of hidden neighbors</h2><p class="enc-pill"><button class="pill pill-static">+86 more</button><button class="pill pill-static is-sel">+42 more</button></p><p class="enc-p">The family name sits in the sector label beside the group. Selected, as on the right, the group lists its members in the inspector.</p></section>
</div></div></main></div>'''


# ---------------------------------------------------------------- phone

def phone_head(title, back=True, search=True):
    b = f'<button class="ib ib-t" aria-label="Back">{icon("arrow-left", 20)}</button>' if back else ""
    s = f'<button class="ib ib-t" aria-label="Search">{icon("search", 20)}</button>' if search else ""
    return f'<header class="ph">{b}<p class="ph-t">{esc(title)}</p>{s}</header>'


def phone_tabs(cur):
    t = [("Links", 16), ("Evidence", 12), ("Graph", None)]
    return ('<div class="ptabs" role="tablist">' + "".join(
        f'<button role="tab" aria-selected="{str(n == cur).lower()}">{n}{f" <span class=num>{c}</span>" if c else ""}</button>'
        for n, c in t) + "</div>")


def phone_entity():
    fams = {}
    for n in POLICY_N:
        fams.setdefault(n[2], []).append(n)
    groups = []
    for i, f in enumerate([f for f in FAM_ORDER if f in fams]):
        rows = "".join(
            f'<li><a href="#" class="prow">{swatch(lab, 18)}<span class="pr-b"><span class="pr-n">{esc(name)}</span>'
            f'<span class="pr-m">{words(typ)}, {SUPPORT_WORD[sup].lower() if sup != "mixed" else "stated in 1, inferred in 1"}</span></span>'
            f'<span class="pr-d num">{w}</span>{icon("chevron-right", 16)}</a></li>'
            for name, lab, _, typ, w, _, sup in fams[f])
        groups.append(f'<section class="pfam"><h3>{FAM_NAME[f]} <span class="num">{len(fams[f])}</span></h3><ul>{rows}</ul></section>')
    return f'''<div class="pwrap">{phone_head("Graph")}<main class="pm">
<div class="pe-h"><p class="in-k">{swatch("InsurancePolicy", 16)}Insurance policy</p>
<h1 class="pe-name">Policy HM-4471 (auto)</h1>
<dl class="facts pfacts"><div><dt>Issued by</dt><dd>Harbor Mutual Insurance</dd></div><div><dt>Term</dt><dd class="num">1 Apr 2026 to 1 Oct 2026</dd></div></dl>
<div class="pe-a"><button class="btn pri btn-t">{icon("message-square-text")}Ask about this</button><button class="btn btn-t"><span><span class="num">12</span> documents</span></button></div></div>
{phone_tabs("Links")}
<div class="pfams">{"".join(groups)}</div>
</main>{tabbar()}</div>'''


def phone_graph(sheet="node"):
    focus = dict(POLICY, aria="Neighborhood of Policy HM-4471 (auto): 16 links in 8 families, 3 inferred")
    W, H = 390, 844 - 56 - 48 - 56
    sel = "Harbor Mutual Insurance" if sheet == "node" else None
    neigh = POLICY_N
    blocks = [(W - 56, 0, W, 168)]
    if sheet:
        blocks.append((0, H - (300 if sheet == "node" else 392), W, H))
    svg, ov = neighborhood_svg(W, H, 184, 204 if sheet == "node" else 184, 148, focus, neigh, sel_node=sel,
                               focus_r=14, show_sectors=False, blocks=blocks)
    if sheet == "node":
        bottom = f'''<section class="bsheet" style="height:300px" aria-label="Harbor Mutual Insurance">
<span class="grab" aria-hidden="true"></span>
<div class="in-top"><p class="in-k">{swatch("Organization", 16)}Organization</p><button class="ib ib-t" aria-label="Close">{icon("x", 20)}</button></div>
<h2 class="in-name">Harbor Mutual Insurance</h2>
<p class="in-sum">Issued this policy. <span class="sw">Supported, <span class="num">9</span> docs</span></p>
<blockquote class="pq">“This policy is issued by <mark>Harbor Mutual Insurance</mark> Company, a mutual insurer.”</blockquote>
<div class="pe-a"><button class="btn pri btn-t">Focus Harbor Mutual</button><button class="btn btn-t">Evidence</button></div>
</section>'''
    else:
        kinds = kinds_in(neigh, "InsurancePolicy")
        bottom = f'''<section class="bsheet" style="height:392px" aria-label="Legend">
<span class="grab" aria-hidden="true"></span>
<div class="in-top"><h2 class="grp-h">Legend</h2><button class="ib ib-t" aria-label="Close legend">{icon("x", 20)}</button></div>
<ul class="lg-k lg-k2">{"".join(f'<li>{swatch(k, 18)}<span>{esc(KINDS[k][4])}</span></li>' for k in kinds)}</ul>
<ul class="lg-l"><li><svg width="28" height="10" aria-hidden="true"><line x1="1" y1="5" x2="27" y2="5" class="lk" style="stroke-width:2"/></svg><span>Supported</span></li><li><svg width="28" height="10" aria-hidden="true"><line x1="1" y1="5" x2="27" y2="5" class="lk" style="stroke-width:2" stroke-dasharray="6 4"/></svg><span>Inferred</span></li></ul>
<p class="lg-size"><svg width="46" height="20" aria-hidden="true"><circle cx="7" cy="12" r="4" class="lg-c"/><circle cx="20" cy="12" r="6" class="lg-c"/><circle cx="36" cy="10" r="9" class="lg-c"/></svg>Size: evidence documents</p>
</section>'''
    tools = (f'<div class="ptools"><button class="ib ib-t" aria-label="Legend">{icon("info", 20)}</button>'
             f'<button class="ib ib-t" aria-label="Filters">{icon("sliders-horizontal", 20)}</button>'
             f'<button class="ib ib-t" aria-label="Fit view">{icon("maximize", 20)}</button></div>')
    return f'''<div class="pwrap">{phone_head("Policy HM-4471 (auto)")}{phone_tabs("Graph")}
<div class="pstage" style="height:{H}px">{svg}{ov}{tools}<div class="scrim" aria-hidden="true"></div>{bottom}</div>{tabbar()}</div>'''


def phone_landing():
    rec = "".join(erow(*r, cls="er per") for r in RECENT[:6])
    con = "".join(erow(*r, cls="er per") for r in CONNECTED[:4])
    return f'''<div class="pwrap"><main class="pm pland">
<h1>Graph</h1>
<label class="bigsearch psearch">{icon("search", 20)}<span class="sr">Search entities</span><input placeholder="Search entities"></label>
<section><h2>Recent</h2><ul class="elist">{rec}</ul></section>
<section><h2>Most connected</h2><ul class="elist">{con}</ul></section>
</main>{tabbar()}</div>'''


def phone_path():
    nodes, links = PATHS[0]
    quotes = ["“Borrowers: Alex Rivera and <mark>Dana Whitlock</mark>, jointly and severally.”",
              "“<mark>Northgate Credit Union</mark> (the Lender) agrees to lend the principal sum.”"]
    steps = []
    for k, (t, w, s) in enumerate(links):
        steps.append(f'''<li class="pstep"><p class="ps-e">{swatch(nodes[k][1], 16)}{esc(nodes[k][0])}</p>
<div class="ps-l"><p class="ps-t">{words(t)} <span class="sw">Supported, <span class="num">{w}</span> docs</span></p><blockquote>{quotes[k]}</blockquote></div></li>''')
    steps.append(f'<li class="pstep"><p class="ps-e">{swatch(nodes[-1][1], 16)}{esc(nodes[-1][0])}</p></li>')
    others = "".join(
        f'<li><button class="pl"><span class="pl-n num">{i + 2}</span><span class="pl-b"><span class="pl-r">{" → ".join(esc(n) for n, _ in p[0][1:-1])}</span>'
        f'<span class="pl-m"><span class="num">{len(p[1])} links</span><span>weakest <span class="num">{min(w for _, w, _ in p[1])}</span></span>{"<span>inferred</span>" if any(s == "inferred" for _, _, s in p[1]) else ""}</span></span></button></li>'
        for i, p in enumerate(PATHS[1:]))
    return f'''<div class="pwrap">{phone_head("Path", search=False)}<main class="pm ppath">
<div class="pp-pick"><label class="pick"><span>From</span><span class="pick-v">{swatch("Person", 14)}Dana Whitlock</span></label>
<label class="pick"><span>To</span><span class="pick-v">{swatch("Organization", 14)}Northgate Credit Union</span></label></div>
<div class="pp-o"><div class="pb-g"><span class="pb-l">Most links</span><div class="seg"><button aria-pressed="false">2</button><button aria-pressed="false">3</button><button aria-pressed="true">4</button></div></div>
<label class="tog"><span class="sw-t" aria-hidden="true"></span><span>Include the owner</span></label></div>
<h2 class="pp-h">Path 1 of 4 <span>· <span class="num">2</span> links, weakest <span class="num">3</span> docs</span></h2>
<ol class="steps">{"".join(steps)}</ol>
<h2 class="pp-h">Other paths</h2><ol class="plist">{others}</ol>
</main>{tabbar()}</div>'''


def phone_filters():
    return f'''<div class="pwrap pfull"><header class="ph"><p class="ph-t ph-tl">Filters</p><button class="ib ib-t" aria-label="Close filters">{icon("x", 20)}</button></header>
<main class="pm pfs">{filter_groups(touch=True)}</main>
<div class="fs-f pfs-f"><button class="btn btn-t">Reset filters</button><button class="btn pri btn-t"><span>Show <span class="num">10</span> links</span></button></div></div>'''


SCREENS = [
    ("landing", "desk", "Landing", screen_landing),
    ("search", "desk", "Landing with search results", screen_search),
    ("neighborhood", "desk", "Neighborhood of an entity, with legend, evidence shelf and inspector", screen_neighborhood),
    ("hub", "desk", "Hub with caps, a group selected", screen_hub),
    ("link", "desk", "Selected link with its evidence", screen_link),
    ("encoding", "desk", "Encoding reference: every node kind, link style, size and state", screen_encoding),
    ("path", "desk", "Path view", screen_path),
    ("table", "desk", "Table view", screen_table),
    ("filters", "desk", "Filters", screen_filters),
    ("phone-landing", "phone", "Phone landing", phone_landing),
    ("phone-entity", "phone", "Phone entity page, Links tab", phone_entity),
    ("phone-graph", "phone", "Phone graph tab with a selected node", lambda: phone_graph("node")),
    ("phone-legend", "phone", "Phone graph tab with the legend sheet", lambda: phone_graph("legend")),
    ("phone-path", "phone", "Phone path list", phone_path),
    ("phone-filters", "phone", "Phone filters", phone_filters),
]


def build(design_dir):
    tokens = (design_dir / "tokens.css").read_text()
    src = (design_dir / "answer-mockup.html").read_text()
    faces = [f for f in re.findall(r"@font-face\{[^}]*\}", src) if "Geist Mono" not in f]
    assert len(faces) == 4, len(faces)
    css = (pathlib.Path(__file__).with_name("mockup.css")).read_text()
    boards = "".join(f'<section id="{i}" class="board {k}" aria-label="{esc(t)}">{fn()}</section>' for i, k, t, fn in SCREENS)
    index = "".join(f'<li><a href="#{i}">{esc(t)}</a><span>{"1440 × 900" if k == "desk" else "390 × 844"}</span></li>' for i, k, t, _ in SCREENS)
    return f'''<!doctype html><html lang="en" data-theme="dark"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Graph mockup</title>
<script>(function(){{var t=new URLSearchParams(location.search).get("theme");if(t==="light"||t==="dark")document.documentElement.dataset.theme=t;addEventListener("keydown",function(e){{if(e.key==="t"&&!/INPUT|TEXTAREA/.test(e.target.tagName)){{var d=document.documentElement;d.dataset.theme=d.dataset.theme==="dark"?"light":"dark"}}}})}})();</script>
<style>
{chr(10).join(faces)}
{tokens}
{css}
</style></head><body>
<main class="index"><h1>Graph mockup</h1>
<p>Static screens of the redesigned graph in direction C. Open a screen, then press <kbd>t</kbd> to switch the theme, or add <code>?theme=light</code> to the address. All names, documents, amounts and counts are invented.</p>
<ol>{index}</ol></main>
{boards}
</body></html>'''


if __name__ == "__main__":
    d = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "docs/design")
    out = pathlib.Path(sys.argv[2]) if len(sys.argv) > 2 else d / "graph" / "mockup.html"
    out.write_text(build(d))
    print(f"wrote {out} ({out.stat().st_size // 1024} KB)")
