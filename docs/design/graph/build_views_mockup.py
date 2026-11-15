#!/usr/bin/env python3
"""Build views-mockup.html, the mockup of the graph views addendum (views.md).

Usage: python3 docs/design/graph/build_views_mockup.py docs/design

The argument is the design folder. The script imports the encoding, shapes, label placer and shell
from build_mockup.py, so both mockups draw the same marks. It inlines tokens.css, mockup.css,
views-mockup.css and the font faces of answer-mockup.html, and writes graph/views-mockup.html.
Every name, document, amount and count is invented. Only the Python standard library is used.
"""
import math
import pathlib
import re
import sys

sys.dont_write_bytecode = True  # no __pycache__ folder in docs/design
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import build_mockup as bm  # noqa: E402
from build_mockup import (DOC_TYPES, dt, FAM_NAME, FAM_ORDER, KINDS, OWNER, POLICY, POLICY_DOCS, POLICY_N,  # noqa: E402
                          CV_H, CV_W, SHELF_H, Placer, esc, extent, icon, kind_svg, label_cands, link_width,
                          radius, shelf, sidebar, swatch, tabbar, text_w, trim, zoom)

VIEWS = [("focus", "Focus", "What is this entity linked to, and how do those things link to each other?", "1", "n"),
         ("explore", "Explore", "What lies two or three steps away, and where do the links loop back?", "2", ""),
         ("timeline", "Timeline", "When was each link documented, and what arrived when?", "3", ""),
         ("table", "Table", "Every link of this entity, sorted and filtered.", "4", "t"),
         ("path", "Paths", "How are these two entities connected?", "5", "p"),
         ("overview", "Overview", "What is in the archive, and which parts hang together?", "6", "")]


def switcher(cur, no_focus=False, kbd=None):
    out = []
    for key, word, _, num, _ in VIEWS:
        if key == "overview":
            out.append('<span class="vsw-d" aria-hidden="true"></span>')
        dis = no_focus and key in ("focus", "explore", "timeline", "table")
        attrs = f' aria-pressed="{str(key == cur).lower()}"' + (' aria-disabled="true"' if dis else "")
        attrs += f' title="{word}, key {num}"' + ("" if key == (kbd or cur) else ' tabindex="-1"')
        out.append(f'<button{attrs}{" class=is-kbd" if key == kbd else ""}>{word}</button>')
    return f'<div class="vsw" role="toolbar" aria-label="View">{"".join(out)}</div>'


def vheader(cur, filters=0, no_focus=False, kbd=None):
    fl = f'<span class="cnt num">{filters}</span>' if filters else ""
    return (f'<header class="gh"><h1>Graph</h1>'
            f'<label class="search">{icon("search")}<span class="sr">Search entities</span>'
            f'<input placeholder="Search entities"><kbd>/</kbd></label>'
            f'<div class="gh-r">{switcher(cur, no_focus, kbd)}<button class="btn">{icon("sliders-horizontal")}Filters{fl}</button></div></header>')


def app(cur, body, filters=0, no_focus=False):
    return f'<div class="app">{sidebar()}<div class="ws">{vheader(cur, filters, no_focus)}{body}</div></div>'


def toggle(word, on=True):
    return (f'<button class="tsw" aria-pressed="{str(on).lower()}"><span class="sw-t{" on" if on else ""}" aria-hidden="true"></span>'
            f'{word}</button>')


def num(s):
    return f'<span class="num">{s}</span>'


# ---------------------------------------------------------------- focus with links between neighbors

CROSS = [  # a, b, type, weight, support: links between two neighbors of Policy HM-4471
    (OWNER, "Dana Whitlock", "SIGNED_WITH", 12, "supported"),
    (OWNER, "14 Larkspur Ct", "LIVES_AT", 52, "supported"),
    ("Dana Whitlock", "14 Larkspur Ct", "RESIDES_AT", 6, "supported"),
    (OWNER, "Northgate Credit Union", "ACCOUNT_AT", 44, "supported"),
    (OWNER, "Harbor Mutual Insurance", "MEMBER_OF", 9, "supported"),
    ("2019 Subaru Outback", "Northgate Credit Union", "LIEN_HELD_BY", 3, "supported"),
    ("Bayline Auto Body", "2019 Subaru Outback", "REPAIRED", 1, "inferred"),
    ("Policy HM-4402 (2025)", "Harbor Mutual Insurance", "ISSUED_BY", 3, "supported"),
    ("14 Larkspur Ct", "Port Ellery", "LOCATED_IN", 4, "supported"),
    ("Premium, Apr 2026, $612", "Northgate Credit Union", "PAID_FROM", 2, "supported"),
    ("Telematics plug TX-2", "2019 Subaru Outback", "INSTALLED_IN", 1, "inferred"),
]


def bezier(p0, c, p2, n=24):
    return [((1 - t) ** 2 * p0[0] + 2 * (1 - t) * t * c[0] + t ** 2 * p2[0],
             (1 - t) ** 2 * p0[1] + 2 * (1 - t) * t * c[1] + t ** 2 * p2[1]) for t in (i / n for i in range(n + 1))]


def cross_svg(neigh, cx, cy, R, links, sel=None):
    """Curves between two neighbors on the ring. The control point sits on the bisector of the shorter arc
    between them, halfway between the chord and the ring, so each curve bows toward the ring and away from the focus."""
    slots, _ = bm.radial(neigh, cx, cy, R, None, keep_slot=True)
    at = {it[0]: (x, y, extent(it[1], radius(it[5])) + (4 if it[0] == OWNER else 0), a) for it, a, x, y in slots}
    out = []
    for a, b, typ, w, sup in links:
        (x0, y0, e0, a0), (x2, y2, e2, a2) = at[a], at[b]
        d = (a2 - a0 + 540) % 360 - 180
        chord = math.cos(math.radians(abs(d) / 2))  # distance of the chord's midpoint from the centre, in R
        c = bm.polar(cx, cy, (chord + 0.5 * (1 - chord)) * R, a0 + d / 2)
        # trim each end along its tangent, so the curve stops 3 px short of the node shape
        s0 = trim(x0, y0, *c, e0 + 3, 0)[:2]
        s2 = trim(x2, y2, *c, e2 + 3, 0)[:2]
        dash = ' stroke-dasharray="6 4"' if sup == "inferred" else ""
        dim = sel is not None and sel not in (a, b)
        out.append(f'<path d="M{s0[0]:.1f},{s0[1]:.1f}Q{c[0]:.1f},{c[1]:.1f} {s2[0]:.1f},{s2[1]:.1f}" '
                   f'class="xl{" xl-dim" if dim else ""}" style="stroke-width:{link_width(w):.2f}"{dash}/>')
    return "".join(out)


def screen_focus():
    focus = dict(POLICY, aria="Neighborhood of Policy HM-4471 (auto): 16 links in 8 families, 3 inferred; 11 links between neighbors")
    H = CV_H - SHELF_H
    blocks = bm.blocks_for(CV_W, H) + [(CV_W - 236, 0, CV_W, 52)]
    svg, ov = bm.neighborhood_svg(CV_W, H, 438, 380, 210, focus, POLICY_N, blocks=blocks, where="focus")
    i = svg.index("<line ")  # links between neighbors draw above the sector arcs and below the focus links
    svg = svg[:i] + cross_svg(POLICY_N, 438, 380, 210, CROSS) + svg[i:]
    lg = bm.legend(bm.kinds_in(POLICY_N, "InsurancePolicy"))
    canvas = (f'<div class="stage" style="height:{H}px">{svg}{ov}{lg}'
              f'<div class="ctool">{toggle("Links between neighbors")}</div></div>{shelf(POLICY_DOCS[:5], more=7)}')
    cap = (f"Policy HM-4471 (auto): {num(16)} links in {num(8)} families, {num(3)} inferred. "
           f"{num(11)} links between neighbors.")
    body = (f'<div class="gb"><div class="cv">{canvas}<p class="cap" role="status">{cap}</p>{zoom()}</div>'
            f'{bm.insp_entity_policy()}</div>')
    return app("focus", body)


# ---------------------------------------------------------------- explore

ROOT = POLICY["name"]
# name -> (label, evidence docs); entities that Explore adds beyond the Focus data
EXTRA_NODES = {
    "Policy HO-2210 (home)": ("InsurancePolicy", 9), "Life policy LT-3301": ("InsurancePolicy", 4),
    "Morgan Ellis": ("Person", 7), "2 Harbor Way": ("Address", 3),
    "Car loan CL-5512": ("FinancialItem", 9), "DMV, Port Ellery office": ("Organization", 6),
    "Recall notice, 2025": ("Event", 2), "Coastal Credit Partners": ("Organization", 6),
    "Loan payment, Oct 2026": ("FinancialItem", 1), "Loan payoff, 2029": ("DateEvent", 2),
    "Lakeside HOA": ("Organization", 5),
}
START = ["Harbor Mutual Insurance", "Dana Whitlock", "Premium, Apr 2026, $612", "2019 Subaru Outback",
         "2014 Honda Fit", "Northgate Credit Union", "Port Ellery", "Policy HM-4402 (2025)"]  # top 8, family order
EXPANSIONS = [  # node, [(child, type, weight, support)], [(already in view, type, weight, support)]
    ("Harbor Mutual Insurance",
     [("Policy HO-2210 (home)", "ISSUED", 6, "supported"), ("Morgan Ellis", "AGENT_OF", 5, "supported"),
      ("Life policy LT-3301", "ISSUED", 3, "supported"), ("2 Harbor Way", "LOCATED_AT", 2, "supported")],
     [("Policy HM-4402 (2025)", "ISSUED", 3, "supported")]),
    ("2019 Subaru Outback",
     [("Car loan CL-5512", "FINANCED_BY", 6, "supported"), ("Bayline Auto Body", "REPAIRED_BY", 2, "supported"),
      ("DMV, Port Ellery office", "REGISTERED_WITH", 2, "supported"), ("Recall notice, 2025", "SUBJECT_OF", 1, "inferred")],
     [("Northgate Credit Union", "LIEN_HELD_BY", 3, "supported")]),
    ("Car loan CL-5512",
     [("Coastal Credit Partners", "SERVICED_BY", 4, "supported"), ("Loan payment, Oct 2026", "PAYMENT_ON", 1, "supported"),
      ("Loan payoff, 2029", "MATURES_ON", 2, "supported")],
     [("Northgate Credit Union", "REFINANCED_FROM", 1, "inferred")]),
    ("Policy HO-2210 (home)",
     [("14 Larkspur Ct", "COVERS", 5, "supported"), ("Lakeside HOA", "NAMED_IN", 1, "inferred")],
     [("Dana Whitlock", "INSURED_PARTY", 3, "supported")]),
]
EXPLORE_CROSS = [("Dana Whitlock", "14 Larkspur Ct", "RESIDES_AT", 6, "supported"),
                 ("14 Larkspur Ct", "Port Ellery", "LOCATED_IN", 4, "supported"),
                 ("DMV, Port Ellery office", "Port Ellery", "LOCATED_IN", 3, "supported"),
                 ("Bayline Auto Body", "Port Ellery", "LOCATED_IN", 1, "inferred")]
SEL_EXPLORE = "Bayline Auto Body"


def node_info(name):
    if name == ROOT:
        return POLICY["label"], POLICY["docs"]
    for n in POLICY_N:
        if n[0] == name:
            return n[1], n[5]
    return EXTRA_NODES[name]


def explore_layout(cx, cy, s, start, expansions):
    """The addendum's rule: a start ring at 160, then fans of radius 120 around each expanded node, first free spot."""
    pos, ang, hop = {ROOT: (cx, cy)}, {ROOT: 0.0}, {ROOT: 0}
    for i, name in enumerate(start):
        a = i * 360 / len(start)
        pos[name], ang[name], hop[name] = bm.polar(cx, cy, 160 * s, a), a, 1

    def ext(name):
        lab, d = node_info(name)
        return extent(lab, radius(d))
    for node, kids, _ in expansions:
        px, py = pos[node]
        th = ang[node]
        span = min(150.0, 25.0 * len(kids))
        for j, (kid, *_r) in enumerate(kids):
            a = th - span / 2 + (j + 0.5) * span / len(kids)
            cands = [(r, a + d) for r in (120, 150, 180, 210) for d in (0, 12, -12, 24, -24)]
            best = None
            for r, aa in cands:
                x, y = bm.polar(px, py, r * s, aa)
                if all(math.hypot(x - ox, y - oy) >= ext(kid) + ext(o) + 32 for o, (ox, oy) in pos.items()):  # 32 px between shapes
                    best = (x, y, aa)
                    break
            if best is None:
                x, y = bm.polar(px, py, 210 * s, a + 24)
                best = (x, y, a + 24)
            pos[kid], ang[kid], hop[kid] = best[:2], best[2], hop[node] + 1
    return pos, ang, hop


def explore_svg(W, H, cx, cy, s, blocks, sel=None, hover=None, start=START, expansions=EXPANSIONS,
                cross=EXPLORE_CROSS, where="explore"):
    pos, ang, hop = explore_layout(cx, cy, s, start, expansions)
    links = [(ROOT, n, *[(x[3], x[4], x[6]) for x in POLICY_N if x[0] == n][0]) for n in start]
    for node, kids, loops in expansions:
        links += [(node, k, t, w, sup) for k, t, w, sup in kids] + [(node, o, t, w, sup) for o, t, w, sup in loops]
    links += [l for l in cross if l[0] in pos and l[1] in pos]
    pl = Placer(W, H, blocks)
    for name, (x, y) in pos.items():
        lab, d = node_info(name)
        e = extent(lab, radius(d)) + (4 if name == OWNER else 0)
        pl.add((x - e - 2, y - e - 2, x + e + 2, y + e + 2), "node", name)
    for name, (x, y) in pos.items():  # every node must stay on the canvas, clear of the panels
        lab, d = node_info(name)
        e = extent(lab, radius(d))
        assert 6 < x - e and x + e < W - 6 and 6 < y - e and y + e < H - 6, f"{where}: {name} off the canvas"
        assert not any(bm.hits((x - e, y - e, x + e, y + e), b) for b in blocks), f"{where}: {name} under a panel"
    out_l, out_n, out_t, ov = [], [], [], []
    for a, b, typ, w, sup in links:
        (x0, y0), (x1, y1) = pos[a], pos[b]
        ea = extent(node_info(a)[0], radius(node_info(a)[1])) + 3
        eb = extent(node_info(b)[0], radius(node_info(b)[1])) + 3
        tx0, ty0, tx1, ty1 = trim(x0, y0, x1, y1, ea, eb)
        state = "" if sel is None or sel in (a, b) else "dim"
        if sel in (a, b):
            state = "arrow"
        out_l.append(bm.link_svg(tx0, ty0, tx1, ty1, link_width(w), sup, state))
        pl.line([(tx0, ty0), (tx1, ty1)], link_width(w))
    for name, (x, y) in pos.items():
        lab, d = node_info(name)
        st = "sel" if name == sel else "hover" if name == hover else ("faded" if sel else "")
        out_n.append(kind_svg(lab, x, y, radius(d), st, hub=(name == OWNER)))
    # the "+N" handle of the hovered or selected node, on its outer side
    if sel or hover:
        x, y = pos[sel or hover]
        lab, d = node_info(sel or hover)
        e = extent(lab, radius(d))
        tw = text_w("+5", 12, 600) + 20
        for dist, turn in [(q, t) for q in (34, 24) for t in (0, 45, -45, 90, -90, 135, -135, 180)]:  # outward first
            hx, hy = bm.polar(x, y, e + dist, ang[sel or hover] + turn)
            hb = (hx - tw / 2 - 4, hy - 16, hx + tw / 2 + 4, hy + 16)
            if pl.free(hb, lines=False):
                break
        else:
            raise AssertionError(f"{where}: no room for the handle")
        pl.add(hb, "block", "handle")
        ov.append(f'<button class="hnd" style="left:{hx:.1f}px;top:{hy:.1f}px" aria-label="Show 5 more links of {esc(sel or hover)}">+5</button>')
    # labels: root, selection, then evidence order
    order = sorted(pos, key=lambda n: (n != ROOT, n not in (sel, hover), -node_info(n)[1], n))
    for name in order:
        x, y = pos[name]
        lab, d = node_info(name)
        e = extent(lab, radius(d))
        text = bm.cut(name)
        big = name == ROOT
        tw = text_w(text, 13 if big else 12, 500 if big else 400)
        if big:
            cands = [(x, y + e + 18, "middle"), (x + e + 8, y + 5, "start"), (x - e - 8, y + 5, "end"), (x, y - e - 8, "middle")]
            lx, ly, anc = pl.place(cands, tw, force=True, chip=True, name=text, at=(x, y))
            x0 = lx - (tw if anc == "end" else tw / 2 if anc == "middle" else 0)
            out_t.append(f'<rect x="{x0 - 3:.1f}" y="{ly - 13:.1f}" width="{tw + 6:.1f}" height="18" rx="4" class="chip"/>'
                         f'<text x="{lx:.1f}" y="{ly:.1f}" text-anchor="{anc}" class="nl nl-focus">{esc(text)}</text>')
            continue
        cands = label_cands(x, y, e, ang[name])
        spot, chip = pl.place(cands, tw, name=text, at=(x, y)), False
        if not spot:
            more = [(x + dx, y + dy, "middle") for dx in range(-48, 49, 8) for dy in (-e - 7, e + 16, -e - 14, e + 23)]
            spot = pl.place(more, tw, name=text, at=(x, y))
        if not spot:
            spot, chip = pl.place(cands + [(x, y + e + 16, "middle"), (x, y - e - 7, "middle")], tw, chip=True, name=text, at=(x, y)), True
        assert spot, f"{where}: no room for the label of {name}"
        cls = "nl" + (" nl-dim" if sel and name != sel else "")
        if chip:
            x0 = spot[0] - (tw if spot[2] == "end" else tw / 2 if spot[2] == "middle" else 0)
            out_t.append(f'<rect x="{x0 - 3:.1f}" y="{spot[1] - 13:.1f}" width="{tw + 6:.1f}" height="18" rx="4" class="chip"/>')
        out_t.append(f'<text x="{spot[0]:.1f}" y="{spot[1]:.1f}" text-anchor="{spot[2]}" class="{cls}">{esc(text)}</text>')
    pl.check(where)
    n_links = len(links)
    hops = max(hop.values())
    svg = (f'<svg class="canvas" width="{W}" height="{H}" viewBox="0 0 {W} {H}" role="img" '
           f'aria-label="Explore from {esc(ROOT)}: {len(pos)} entities, {n_links} links, {hops} hops">'
           f'{"".join(out_l)}{"".join(out_n)}{"".join(out_t)}</svg>')
    return svg, "".join(ov), len(pos), n_links, hops


def trail(expansions=EXPANSIONS, root_count=8):
    rows = [f'<li class="is-root">{swatch("InsurancePolicy")}<span class="tr-n">{esc(ROOT)}</span><span class="tr-c num">{root_count} links</span></li>']
    for node, kids, loops in expansions:
        lab = node_info(node)[0]
        rows.append(f'<li>{swatch(lab)}<span class="tr-n">{esc(node)}</span><span class="tr-c num">+{len(kids)}</span>'
                    f'<button class="ib ib-s" aria-label="Collapse {esc(node)}">{icon("x", 14)}</button></li>')
    return (f'<section class="trail" aria-label="Explored from {esc(ROOT)}"><h2>Explored from {esc(ROOT)}</h2>'
            f'<p class="tr-s">{num(len(expansions))} expansions. Select a node, then press +.</p><ol>{"".join(rows)}</ol></section>')


def screen_explore():
    W, H = 1256, CV_H - SHELF_H
    blocks = [(0, 0, 304, 268), (W - 52, H - 118, W - 14, H - 14), (320, 0, 900, 40), (16, H - 16 - 128, 16 + 268, H - 16)]
    svg, ov, n_nodes, n_links, hops = explore_svg(W, H, 720, 440, 0.82, blocks, hover=SEL_EXPLORE)
    kinds = sorted({node_info(n)[0] for n in [ROOT] + START + [k for _, ks, _ in EXPANSIONS for k, *_ in ks]}, key=list(KINDS).index)
    cap = (f"Explore from Policy HM-4471 (auto): {num(n_nodes)} entities, {num(n_links)} links, {num(hops)} hops. "
           f"The owner is hidden by the Owner filter.")
    body = (f'<div class="gb no-insp"><div class="cv"><div class="stage" style="height:{H}px">{svg}{ov}{trail()}'
            f'<section class="legend" aria-label="Legend" style="width:268px"><div class="lg-h"><h2>Legend</h2>'
            f'<button class="ib ib-s" aria-label="Collapse legend" aria-expanded="true">{icon("chevron-down", 14)}</button></div>'
            f'<ul class="lg-k">{"".join(f"<li>{swatch(k, 16)}<span>{esc(KINDS[k][4])}</span></li>" for k in kinds[:8])}</ul>'
            f'<p class="lg-note">and {num(len(kinds) - 8)} more kinds. Size: evidence documents. Dashed: inferred.</p></section>'
            f'</div>{shelf(POLICY_DOCS[:5], more=7)}'
            f'<p class="cap" role="status" style="left:320px">{cap}</p>{zoom()}</div></div>')
    return app("explore", body)


# ---------------------------------------------------------------- timeline

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def ym(d):
    """'4 Apr 2026' -> (2026, 4); None stays None."""
    if d is None:
        return None
    _, m, y = d.split()
    return int(y), MONTHS.index(m) + 1


TL_DOCS = [(t, ty, None if t == "Policy booklet PA-11" else d) for t, ty, d in POLICY_DOCS]  # one undated document
TL_LINKS = {  # neighbor -> [(doc index, inferred)]
    "Harbor Mutual Insurance": [(i, False) for i in (0, 1, 2, 3, 6, 7, 9, 10, 11)],
    OWNER: [(i, False) for i in (0, 2, 3, 7, 9, 10, 11)],
    "Dana Whitlock": [(i, False) for i in (2, 3, 7)],
    "Premium, Apr 2026, $612": [(i, False) for i in (0, 1, 2, 3)],
    "Premium, Oct 2025, $588": [(i, False) for i in (6, 7)],
    "2019 Subaru Outback": [(i, False) for i in (0, 2, 3, 4, 5, 7, 11)],
    "2014 Honda Fit": [(2, False), (0, True)],
    "Northgate Credit Union": [(i, False) for i in (2, 4)],
    "14 Larkspur Ct": [(11, True)],
    "Port Ellery": [(i, False) for i in (7, 11)],
    "1 Apr 2026": [(i, False) for i in (2, 3)],
    "1 Oct 2026": [(i, False) for i in (0, 2)],
    "Policy HM-4402 (2025)": [(i, False) for i in (0, 2, 7)],
    "Bayline Auto Body": [(8, True)],
    "Telematics plug TX-2": [(5, True)],
    "Safe driver discount": [(i, False) for i in (7, 9)],
}
for _n in POLICY_N:  # the timeline agrees with the Focus data: one mark per supporting document
    assert len(TL_LINKS[_n[0]]) == _n[4], _n[0]
    assert (_n[6] == "inferred") == all(inf for _, inf in TL_LINKS[_n[0]]), _n[0]
    assert (_n[6] == "mixed") == (len({inf for _, inf in TL_LINKS[_n[0]]}) == 2), _n[0]


def timeline_svg(W, hover=("2014 Honda Fit", 0), rng=((2025, 10), (2026, 9))):
    """Axis, document band, then family lanes: a heading line, then one row per neighbor."""
    x_fam, x_sw, x_name, x0 = 24, 200, 222, 440
    nd_w = 64
    x1 = W - 24 - nd_w - 28
    first, last = (2024, 2), (2026, 10)  # a month of space either side of Mar 2024 and Sep 2026
    span = (last[0] - first[0]) * 12 + last[1] - first[1]
    step = (x1 - x0) / (span + 1)

    def mx(y, m):
        return x0 + ((y - first[0]) * 12 + m - first[1] + 0.5) * step

    def inr(k):
        return k is not None and rng[0] <= k <= rng[1]
    ndx = W - 24 - nd_w
    fams = [(f, sorted([n for n in POLICY_N if n[2] == f], key=lambda n: (-n[4], n[0]))) for f in FAM_ORDER]
    fams = [(f, m) for f, m in fams if m]
    ax_y, band_base = 22, 104
    RH, HH, GAP = 26, 0, 8
    rows_top = band_base + 14
    h = rows_top + sum(HH + len(m) * RH + GAP for _, m in fams) + 4
    out = []
    bx0, bx1 = mx(*rng[0]) - step / 2, mx(*rng[1]) + step / 2
    out.append(f'<rect x="{bx0:.1f}" y="{ax_y + 8}" width="{bx1 - bx0:.1f}" height="{h - ax_y - 8}" class="brush"/>'
               f'<line x1="{bx0:.1f}" y1="{ax_y + 8}" x2="{bx0:.1f}" y2="{h}" class="brush-e"/>'
               f'<line x1="{bx1:.1f}" y1="{ax_y + 8}" x2="{bx1:.1f}" y2="{h}" class="brush-e"/>')
    for k in range(span + 1):  # more than 18 months: years, with month ticks
        y, m = first[0] + (first[1] - 1 + k) // 12, (first[1] - 1 + k) % 12 + 1
        x = x0 + k * step
        major = m == 1
        out.append(f'<line x1="{x:.1f}" y1="{ax_y + (4 if major else 10)}" x2="{x:.1f}" y2="{ax_y + 14}" class="{"ax-tm" if major else "ax-t"}"/>')
        if major:
            out.append(f'<text x="{x + 4:.1f}" y="{ax_y}" class="ax ax-y">{y}</text>')
        elif m in (4, 7, 10):
            out.append(f'<text x="{x + 3:.1f}" y="{ax_y}" class="ax">{MONTHS[m - 1]}</text>')
    out.append(f'<text x="{ndx + nd_w / 2:.1f}" y="{ax_y}" text-anchor="middle" class="ax">No date</text>'
               f'<rect x="{ndx:.1f}" y="{ax_y + 8}" width="{nd_w}" height="{h - ax_y - 8}" rx="4" class="nodate"/>')
    out.append(f'<text x="{x_fam}" y="{band_base - 10}" class="fam-h">Documents</text>'
               f'<text x="{x_fam}" y="{band_base + 6}" class="fam-n">{len(TL_DOCS)} evidence documents</text>')
    stacks = {}
    for t, ty, d in TL_DOCS:  # one dot per document, stacked upward per month
        k = ym(d)
        n = stacks.get(k, 0)
        stacks[k] = n + 1
        x = mx(*k) if k else ndx + nd_w / 2
        col = "--graph-faded" if k and not inr(k) else f"--doc-{ty.replace('_', '-')}"
        out.append(f'<circle cx="{x:.1f}" cy="{band_base - 4 - n * 11:.1f}" r="4" style="fill:var({col})"/>')
    out.append(f'<line x1="{x_fam}" y1="{band_base + 12}" x2="{W - 24}" y2="{band_base + 12}" class="ax-tm"/>')
    tip = None
    y = rows_top
    for f, members in fams:
        out.append(f'<text x="{x_fam}" y="{y + RH / 2 + 4}" class="fam-h">{esc(FAM_NAME[f])} <tspan class="fam-n">{len(members)}</tspan></text>')
        y += HH
        for i, n in enumerate(members):
            name, lab, _, typ, w, _, sup = n[:7]
            cy = y + i * RH + RH / 2
            recs = TL_LINKS[name]
            dated = sorted(ym(TL_DOCS[j][2]) for j, _ in recs if TL_DOCS[j][2])
            faded = not any(inr(k) for k in dated) and all(TL_DOCS[j][2] for j, _ in recs)
            if len(dated) > 1 and dated[-1] > dated[0]:
                dash = ' stroke-dasharray="6 4"' if sup == "inferred" else ""
                out.append(f'<line x1="{mx(*dated[0]):.1f}" y1="{cy:.1f}" x2="{mx(*dated[-1]):.1f}" y2="{cy:.1f}" '
                           f'class="lk{" lk-dim" if faded else ""}" style="stroke-width:{link_width(w):.2f}"{dash}/>')
            pal = KINDS[lab][0]
            seen = {}
            for j, inf in recs:
                k = ym(TL_DOCS[j][2])
                x = mx(*k) if k else ndx + nd_w / 2
                off = seen.get(x, 0)
                seen[x] = off + 1
                x += off * 6  # two documents in one month sit side by side
                col = "--graph-faded" if (k and not inr(k)) else ("--fg-subtle" if pal == "unknown" else f"--cat-{pal}")
                if inf:
                    out.append(f'<circle cx="{x:.1f}" cy="{cy:.1f}" r="4" class="mk-i" style="stroke:var({col})"/>')
                else:
                    out.append(f'<circle cx="{x:.1f}" cy="{cy:.1f}" r="5" class="mk" style="fill:var({col})"/>')
                if hover == (name, j):
                    tip = (x, cy, TL_DOCS[j], inf)
                    out.append(f'<circle cx="{x:.1f}" cy="{cy:.1f}" r="8" class="ring-hover"/>')
            out.append(f'<g transform="translate({x_sw},{cy - 8:.1f})">{swatch(lab, 16)}</g>'
                       f'<text x="{x_name}" y="{cy + 4:.1f}" class="row-n{" is-faded" if faded else ""}">{esc(bm.cut(name, 30))}</text>')
        y += len(members) * RH + GAP
        out.append(f'<line x1="{x_fam}" y1="{y - GAP / 2:.1f}" x2="{W - 24}" y2="{y - GAP / 2:.1f}" class="lane"/>')
    svg = (f'<svg width="{W}" height="{h}" viewBox="0 0 {W} {h}" role="img" aria-label="Timeline of {esc(ROOT)}: '
           f'16 links and 12 documents from March 2024 to September 2026, 1 undated">{"".join(out)}</svg>')
    return svg, h, tip


def screen_timeline():
    W = 1256
    svg, h, tip = timeline_svg(W)
    bar = (f'<div class="tl-bar"><p class="cap" role="status">Timeline of Policy HM-4471 (auto): {num(16)} links, '
           f'{num(12)} documents. {num(sum(1 for _, _, d in TL_DOCS if d and (2025, 10) <= ym(d) <= (2026, 9)))} documents fall in the date range.</p>'
           f'<div class="tl-rng"><span>From</span><button class="sel num">Oct 2025{icon("chevron-down", 14)}</button>'
           f'<span>to</span><button class="sel num">Sep 2026{icon("chevron-down", 14)}</button></div></div>')
    (tx, ty, (title, ty_, date), inf) = tip
    tooltip = (f'<div class="tip" style="right:{W - tx + 14:.0f}px;top:{ty - 30:.0f}px" role="tooltip">'
               f'<span class="tip-t">{esc(title)}</span><span class="tip-m num">{dt(ty_)}<span>{esc(date)}</span>'
               f'<span>{"Inferred" if inf else "Stated"}</span></span><span class="tip-m">Covers 2014 Honda Fit</span></div>')
    dots = "".join(f'<span><span class="dot" style="--dot:var(--doc-{k.replace("_", "-")})"></span>{w}</span>'
                   for k, w in DOC_TYPES if any(ty == k for _, ty, _ in TL_DOCS))
    leg = (f'<div class="tl-leg"><span><svg width="12" height="12" aria-hidden="true"><circle cx="6" cy="6" r="5" style="fill:var(--fg-muted)"/></svg>Stated document</span>'
           f'<span><svg width="12" height="12" aria-hidden="true"><circle cx="6" cy="6" r="4" class="mk-i" style="stroke:var(--fg-muted)"/></svg>Inferred document</span>'
           f'<span><svg width="24" height="10" aria-hidden="true"><line x1="1" y1="5" x2="23" y2="5" class="lk" style="stroke-width:2"/></svg>Supported link</span>'
           f'<span><svg width="24" height="10" aria-hidden="true"><line x1="1" y1="5" x2="23" y2="5" class="lk" style="stroke-width:2" stroke-dasharray="6 4"/></svg>Inferred link</span>'
           f'<span>Marks take the color of the linked entity\'s kind.</span>{dots}</div>')
    tl = f'<div class="tl">{bar}<div style="position:relative;flex:1;min-height:0;overflow:hidden">{svg}{tooltip}</div>{leg}</div>'
    body = (f'<div class="chips" aria-label="Active filters"><button class="fchip">Oct 2025 to Sep 2026{icon("x", 14)}</button>'
            f'<button class="lnk">Reset filters</button></div>'
            f'<div class="gb no-insp" style="min-height:0"><div class="cv">{tl}{shelf(POLICY_DOCS[:5], more=7)}</div></div>')
    assert h <= CV_H - SHELF_H - 52 - 52 - 40, h
    return app("timeline", body, filters=1)


# ---------------------------------------------------------------- overview

def rng(seed):
    state = [seed]

    def r():
        state[0] = (state[0] * 1664525 + 1013904223) % 4294967296
        return state[0] / 4294967296
    return r


KIND_PAL = {"Person": "people", "Organization": "org", "Location": "place", "Address": "place", "FinancialItem": "money",
            "Contract": "agree", "InsurancePolicy": "agree", "Product": "thing", "System": "thing", "Condition": "health",
            "MedicalResult": "health", "Event": "time", "DateEvent": "time", "Other": "unknown"}
GROUPS = [  # lead entity, size, links, kind mix as (kind, share)
    ("Brightline Logistics", 214, 341, [("Person", 40), ("Organization", 30), ("FinancialItem", 15), ("Event", 15)]),
    ("Coastal Power Co.", 168, 233, [("FinancialItem", 60), ("Organization", 15), ("Address", 15), ("DateEvent", 10)]),
    ("Harbor Mutual Insurance", 152, 241, [("InsurancePolicy", 25), ("Organization", 20), ("Person", 19), ("FinancialItem", 16), ("Product", 10), ("Other", 10)]),
    ("Port Ellery Clinic", 131, 188, [("Condition", 25), ("MedicalResult", 30), ("Person", 25), ("Organization", 20)]),
    ("County Tax Office", 118, 160, [("FinancialItem", 45), ("Organization", 25), ("Contract", 15), ("DateEvent", 15)]),
    ("US Navy", 96, 131, [("Person", 30), ("Organization", 25), ("Event", 30), ("Location", 15)]),
    ("Northgate Credit Union", 88, 120, [("FinancialItem", 55), ("Organization", 20), ("Contract", 25)]),
    ("Lakeside HOA", 61, 77, [("Address", 35), ("Organization", 25), ("FinancialItem", 25), ("Person", 15)]),
    ("Ellis and Park LLP", 54, 70, [("Contract", 40), ("Person", 35), ("Organization", 25)]),
    ("Riverside 10K", 37, 41, [("Event", 50), ("Person", 30), ("Organization", 20)]),
    ("Benefits portal", 29, 33, [("System", 45), ("Organization", 30), ("Person", 25)]),
    ("Fitness club", 22, 24, [("Organization", 30), ("Event", 40), ("Person", 30)]),
]
MINOR = 40  # unnamed groups of 4 to 12 entities
SMALL = 380  # groups of 2 or 3 entities, in the band
OV_SEL = "Harbor Mutual Insurance"


def overview_svg(W, H, sel=OV_SEL, blocks=()):
    """Small groups in a band at the top; groups spiral out from the centre of the map below it, largest first.
    A named group's label box counts as part of it, so no label touches another group or label."""
    r = rng(11)
    groups = list(GROUPS)
    for i in range(MINOR):
        size = 4 + int(r() * 9)
        mix = [(k, 1) for k in (["Person", "Organization", "FinancialItem", "Event", "Address", "Product", "Contract"][i % 7],
                                ["Organization", "Person", "DateEvent"][i % 3])]
        groups.append((None, size, size + 1, mix))
    band_top, band_h = 56, 64
    top = band_top + band_h + 8
    cx, cy = W / 2 - 20, (top + H) / 2 + 10
    boxes = [b for b in blocks]  # rectangles nothing may touch
    circles = []  # (x, y, rad)
    placed = []

    def clear(x, y, rad, lbox, gap):
        if x - rad < 16 or x + rad > W - 16 or y - rad < top or y + rad > H - 12:
            return False
        if any(math.hypot(x - px, y - py) < rad + pr + gap for px, py, pr in circles):
            return False
        cb = (x - rad, y - rad, x + rad, y + rad)
        for b in boxes:
            if bm.hits(cb, b) or (lbox and bm.hits(lbox, b)):
                return False
        if lbox and any(bm.hits(lbox, (px - pr, py - pr, px + pr, py + pr)) for px, py, pr in circles):
            return False
        return True
    for name, size, links, mix in groups:
        rad = 5.0 * math.sqrt(size) + 4
        lw = text_w(name, 15) * 0.95 + text_w(str(size)) + 8 if name else 0
        t = 0.0
        while True:
            x, y = cx + 0.95 * t * math.cos(t * 0.35), cy + 0.62 * t * math.sin(t * 0.35)
            lbox = (x - lw / 2 - 2, y - rad - 26, x + lw / 2 + 2, y - rad - 4) if name else None
            if clear(x, y, rad, lbox, 22 if name else 8):
                break
            t += 0.2
            assert t < 4000, f"overview: no room for a group of {size}"
        circles.append((x, y, rad))
        if lbox:
            boxes.append(lbox)
        placed.append((x, y, rad, name))
    out_l, out_n, out_t = [], [], []
    total = 0
    sel_circle = None
    for (x, y, rad, name), (_, size, links, mix) in zip(placed, groups):
        tw = sum(s for _, s in mix)
        kinds = []
        for k, s in mix:
            kinds += [k] * max(1, round(size * s / tw))
        pts = []
        for i in range(size):  # phyllotaxis, the most documented entities in the middle
            rr = 5.0 * math.sqrt(i + 0.5)
            a = i * 2.39996 + r() * 0.5
            pts.append((x + rr * math.cos(a), y + rr * math.sin(a)))
        for i in range(1, size):  # hairlines to the nearest earlier entity, plus a few loops to the lead
            j = min(range(i), key=lambda q: math.hypot(pts[q][0] - pts[i][0], pts[q][1] - pts[i][1]))
            out_l.append(f'<line x1="{pts[i][0]:.1f}" y1="{pts[i][1]:.1f}" x2="{pts[j][0]:.1f}" y2="{pts[j][1]:.1f}" class="ov-l"/>')
            if i % 6 == 0:
                out_l.append(f'<line x1="{pts[i][0]:.1f}" y1="{pts[i][1]:.1f}" x2="{pts[0][0]:.1f}" y2="{pts[0][1]:.1f}" class="ov-l"/>')
        for i, (px, py) in enumerate(pts):  # dots 3 to 7 px across, by evidence documents
            rr = 3.5 if i == 0 else 2.5 if i < 6 else 1.5
            pal = KIND_PAL[kinds[(i * 7) % len(kinds)]]
            col = "--fg-subtle" if pal == "unknown" else f"--cat-{pal}"
            out_n.append(f'<circle cx="{px:.1f}" cy="{py:.1f}" r="{rr}" style="fill:var({col})"/>')
        total += size
        if name:
            out_t.append(f'<text x="{x:.1f}" y="{y - rad - 9:.1f}" text-anchor="middle"><tspan class="ov-g">{esc(name)}</tspan>'
                         f'<tspan class="ov-n" dx="6">{size}</tspan></text>')
        if name == sel:
            sel_circle = (x, y, rad + 4)
    named = [p for p in placed if p[3]]
    for a, b in ((0, 2), (2, 6), (1, 7), (4, 6), (3, 5), (0, 5), (8, 2)):  # bridges between groups
        (ax, ay, ar, _), (bx, by, br, _) = named[a], named[b]
        x0, y0, x1, y1 = trim(ax, ay, bx, by, ar * 0.4, br * 0.4)
        out_l.append(f'<line x1="{x0:.1f}" y1="{y0:.1f}" x2="{x1:.1f}" y2="{y1:.1f}" class="ov-b"/>')
    band = [f'<text x="24" y="{band_top + 14}"><tspan class="ov-g" style="font-size:13px">Small groups</tspan>'
            f'<tspan class="ov-n" dx="8">{SMALL} groups of 2 or 3 entities</tspan></text>'
            f'<line x1="24" y1="{band_top + band_h + 2}" x2="{W - 24}" y2="{band_top + band_h + 2}" class="ov-band"/>']
    per_row = 76
    for i in range(SMALL):
        col, row = i % per_row, i // per_row
        x = 26 + col * ((W - 60) / per_row)
        y = band_top + 28 + row * 7
        n = 2 + (i % 3 == 0)
        k = ["Person", "Organization", "FinancialItem", "Address", "Event", "Product"][int(r() * 6)]
        for q in range(n):
            out_n.append(f'<circle cx="{x + q * 3.2:.1f}" cy="{y:.1f}" r="1.3" style="fill:var(--cat-{KIND_PAL[k]})"/>')
        total += n
    sel_svg = ""
    if sel_circle:
        x, y, rr = sel_circle
        sel_svg = f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{rr:.1f}" class="ov-selh"/><circle cx="{x:.1f}" cy="{y:.1f}" r="{rr:.1f}" class="ov-sel"/>'
    groups_n = len(GROUPS) + MINOR
    svg = (f'<svg class="canvas" width="{W}" height="{H}" viewBox="0 0 {W} {H}" role="img" '
           f'aria-label="Map of the archive: {total} entities in {groups_n} groups and {SMALL} small groups">'
           f'{sel_svg}{"".join(out_l)}{"".join(out_n)}{"".join(band)}{"".join(out_t)}</svg>')
    return svg, total, groups_n


def mix_bar(mix):
    tw = sum(s for _, s in mix)
    return ('<div class="mix" aria-hidden="true">' + "".join(
        f'<span style="width:{100 * s / tw:.1f}%;background:var({"--fg-subtle" if KIND_PAL[k] == "unknown" else "--cat-" + KIND_PAL[k]})"></span>'
        for k, s in mix) + "</div>")


def insp_group():
    name, size, links, mix = GROUPS[2]
    top = [("Harbor Mutual Insurance", "Organization", 33), ("Policy HM-4471 (auto)", "InsurancePolicy", 12),
           ("Policy HO-2210 (home)", "InsurancePolicy", 9), ("Morgan Ellis", "Person", 7),
           ("2019 Subaru Outback", "Product", 14), ("Harbor View Apartments", "Location", 3), ("Premium, Apr 2026, $612", "FinancialItem", 4),
           ("Life policy LT-3301", "InsurancePolicy", 4), ("2 Harbor Way", "Address", 3), ("Bayline Auto Body", "Organization", 2)]
    top.sort(key=lambda t: -t[2])
    assert top[0][0] == name, "a group is named after its most documented entity"
    rows = "".join(f'<li>{swatch(lab)}<span class="lr-n">{esc(n)}</span><span class="lr-t">{KINDS[lab][4]}</span>'
                   f'<span class="lr-d num">{d} docs</span></li>' for n, lab, d in top)
    tw = sum(s for _, s in mix)
    counts = [(k, round(size * s / tw)) for k, s in mix]
    counts[-1] = (counts[-1][0], size - sum(c for _, c in counts[:-1]))
    legend_rows = "".join(
        f'<li><span class="kdot" style="background:var({"--fg-subtle" if KIND_PAL[k] == "unknown" else "--cat-" + KIND_PAL[k]})"></span>'
        f'{KINDS[k][4]}<b class="num">{c}</b></li>' for k, c in counts)
    return f'''<aside class="insp" aria-label="Inspector">
<div class="in-top"><p class="in-k">{icon("layout-grid", 16)}Group</p><button class="ib" aria-label="Clear selection">{icon("x")}</button></div>
<h2 class="in-name">{esc(name)}</h2>
<p class="in-al">{num(size)} entities and {num(links)} links, named after the entity with the most evidence documents.</p>
<div class="in-acts"><button class="btn pri">Open Harbor Mutual Insurance in Focus</button><button class="btn">Explore from here</button></div>
<h3>Kinds</h3>
{mix_bar(mix)}
<ul class="mixl">{legend_rows}</ul>
<h3>Most documented</h3>
<ul class="lrows" style="padding-left:0">{rows}</ul>
</aside>'''


def screen_overview():
    W, H = CV_W, CV_H
    svg, total, groups_n = overview_svg(W, H, blocks=[(0, H - 16 - 200, 16 + 276, H), (W - 60, H - 130, W, H)])
    kinds = ["Person", "Organization", "Location", "FinancialItem", "Contract", "Product", "Condition", "Event"]
    leg = (f'<section class="legend ov-leg" aria-label="Legend"><div class="lg-h"><h2>Legend</h2>'
           f'<button class="ib ib-s" aria-label="Collapse legend" aria-expanded="true">{icon("chevron-down", 14)}</button></div>'
           f'<ul class="lg-k">' + "".join(
               f'<li><span class="kdot" style="background:var(--cat-{KIND_PAL[k]})"></span><span>{["People", "Organizations", "Places", "Money", "Agreements", "Things", "Health", "Time"][i]}</span></li>'
               for i, k in enumerate(kinds)) +
           '</ul><p class="lg-note">Dots show kind by color. Zoom in for shapes, sizes and names. Dotted lines join groups.</p></section>')
    cap = (f"Map of the archive: {num(f'{total:,}')} entities in {num(groups_n)} groups and {num(SMALL)} small groups. "
           f"{num('1,840')} entities link only to documents. The owner is hidden.")
    stage = (f'<div class="stage" style="height:{H}px">{svg}{leg}'
             f'<div class="ctool">{toggle("Show as list", on=False)}</div></div>')
    body = (f'<div class="gb"><div class="cv">{stage}<p class="cap" role="status" style="max-width:560px">{cap}</p>{zoom()}</div>'
            f'{insp_group()}</div>')
    return app("overview", body)


# ---------------------------------------------------------------- the switcher reference sheet

def screen_switcher():
    keys = "".join(
        f'<tr><td>{word}</td><td><kbd>{n}</kbd>{f"<kbd>{alias}</kbd>" if alias else ""}</td><td>{esc(q)}</td></tr>'
        for _, word, q, n, alias in VIEWS)
    return f'''<div class="app">{sidebar()}<main class="ref">
<h1>View switcher</h1>
<p class="land-s">Six views of the same data. The five on the left follow the focus entity; Overview shows the whole archive. Focus, filters, selection, the inspector and the legend carry over from view to view.</p>
<div class="ref-g">
<section class="ref-s"><h2>States</h2>
<div class="ref-row"><span>Current view: Focus</span>{switcher("focus")}</div>
<div class="ref-row"><span>Keyboard focus on Timeline</span>{switcher("focus", kbd="timeline")}</div>
<div class="ref-row"><span>No focus entity, from the map</span>{switcher("overview", no_focus=True)}</div>
<div class="ref-row"><span>Announced after a switch</span><span class="ref-live">Timeline of Policy HM-4471 (auto)</span></div>
<p class="ref-note">One Tab stop. Left and Right move between views, Home and End jump, Enter or Space switches. Without a focus entity, Focus, Explore, Timeline and Table are unavailable and say "Pick an entity on the map or search first." Paths keeps its own pickers.</p>
</section>
<section class="ref-s"><h2>Keys and questions</h2>
<table class="keys"><thead><tr><th>View</th><th>Key</th><th>The question it answers</th></tr></thead><tbody>{keys}</tbody></table>
<p class="ref-note">Number keys follow the switcher order. <kbd>n</kbd>, <kbd>t</kbd> and <kbd>p</kbd> stay from the first graph release. No key acts while a text field has focus.</p>
</section>
<section class="ref-ctx" aria-label="In the graph header">{vheader("timeline", filters=1)}</section>
</div></main></div>'''


# ---------------------------------------------------------------- phone

def phone_view_bar(word):
    return (f'<div class="pview"><button class="btn btn-t" aria-haspopup="dialog">View: <b>{word}</b>{icon("chevron-down", 16)}</button>'
            f'<button class="ib ib-t" aria-label="Filters">{icon("sliders-horizontal", 20)}</button></div>')


def phone_focus_svg(W, H, where):
    focus = dict(POLICY, aria="Neighborhood of Policy HM-4471 (auto): 16 links in 8 families, 3 inferred; 11 links between neighbors")
    svg, ov = bm.neighborhood_svg(W, H, 184, 170, 140, focus, POLICY_N, focus_r=14, show_sectors=False, where=where)
    i = svg.index("<line ")
    return svg[:i] + cross_svg(POLICY_N, 184, 170, 140, CROSS) + svg[i:], ov


def phone_focus():
    W, H = 390, 844 - 56 - 48 - 56 - 56
    svg, ov = phone_focus_svg(W, H, "phone-focus")
    return (f'<div class="pwrap">{bm.phone_head("Policy HM-4471 (auto)")}{bm.phone_tabs("Graph")}{phone_view_bar("Focus")}'
            f'<div class="pstage" style="height:{H}px">{svg}{ov}</div>{tabbar()}</div>')


def phone_views():
    W, H = 390, 844 - 56 - 48 - 56 - 56
    svg, ov = phone_focus_svg(W, H, "phone-views")  # the sheet and its scrim cover the lower canvas
    rows = "".join(
        f'<li><button class="vrow" aria-pressed="{str(k == "focus").lower()}"><span><b>{w}</b><span>{esc(q)}</span></span>'
        f'{icon("check", 20) if k == "focus" else ""}</button></li>' for k, w, q, _, _ in VIEWS)
    sheet = (f'<section class="bsheet" style="height:470px" role="dialog" aria-label="View"><span class="grab" aria-hidden="true"></span>'
             f'<div class="in-top"><h2 class="grp-h">View</h2><button class="ib ib-t" aria-label="Close">{icon("x", 20)}</button></div>'
             f'<ul class="vrows">{rows}</ul></section>')
    return (f'<div class="pwrap">{bm.phone_head("Policy HM-4471 (auto)")}{bm.phone_tabs("Graph")}{phone_view_bar("Focus")}'
            f'<div class="pstage" style="height:{H}px">{svg}{ov}<div class="pscrim" aria-hidden="true"></div>{sheet}</div>{tabbar()}</div>')


def phone_explore():
    W, H = 390, 844 - 56 - 48 - 56 - 56
    start = START[:6]
    exps = [(EXPANSIONS[1][0], EXPANSIONS[1][1], [])]
    blocks = [(0, H - 56 - 64, W, H)]
    svg, ov, n_nodes, n_links, hops = explore_svg(W, H, 196, 214, 0.66, blocks, start=start, expansions=exps, cross=EXPLORE_CROSS, where="phone-explore")
    peek = (f'<div class="peek"><p>Explored from Policy HM-4471 <span>· 1 expansion</span></p>'
            f'<button class="ib ib-t" aria-label="Open the trail" aria-expanded="false">{icon("chevron-up", 20)}</button></div>')
    return (f'<div class="pwrap">{bm.phone_head("Policy HM-4471 (auto)")}{bm.phone_tabs("Graph")}{phone_view_bar("Explore")}'
            f'<div class="pstage" style="height:{H}px">{svg}{ov}</div>{peek}{tabbar()}</div>')


def phone_timeline():
    by = {}
    for i, (t, ty, d) in enumerate(TL_DOCS):
        by.setdefault(ym(d), []).append((i, t, ty, d))
    keys = sorted((k for k in by if k), reverse=True)
    secs, year = [], None
    words = {}
    for n in POLICY_N:
        for i, inf in TL_LINKS[n[0]]:
            words.setdefault(i, []).append(f'{bm.words(n[3])} {n[0]}{" (inferred)" if inf else ""}')
    for k in keys[:5]:
        if k[0] != year:
            year = k[0]
            secs.append(f'<h3 class="num">{year}</h3>')
        secs.append(f'<h4>{["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"][k[1] - 1]}</h4>')
        for i, t, ty, d in by[k]:
            sup = words.get(i, [])
            more = f", and {len(sup) - 2} more" if len(sup) > 2 else ""
            secs.append(f'<div class="pdoc"><span class="dot" style="--dot:var(--doc-{ty.replace("_", "-")})"></span>'
                        f'<span class="pd-t">{esc(t)}</span><span class="pd-d num">{esc(d)}</span>'
                        f'<span class="pd-l">Supports: {esc(", ".join(sup[:2]))}{more}</span></div>')
    return (f'<div class="pwrap">{bm.phone_head("Policy HM-4471 (auto)")}{bm.phone_tabs("Graph")}{phone_view_bar("Timeline")}'
            f'<main class="pm ptl"><p class="pcap">{num(12)} documents from March 2024 to September 2026, {num(1)} without a date. Newest first.</p>'
            f'{"".join(secs)}</main>{tabbar()}</div>')


def phone_overview():
    def kinds(mix):  # the bar repeats as words, so color is not the only signal
        return ", ".join(KINDS[k][4] for k, _ in sorted(mix, key=lambda m: -m[1])[:3])
    rows = "".join(f'<a href="#" class="pgrp"><span class="pg-t">{esc(n)}<span class="num">{s} entities</span></span>{mix_bar(mix)}'
                   f'<span class="pg-k">Mostly {esc(kinds(mix).lower())}</span></a>'
                   for n, s, _, mix in GROUPS[:8])
    head = bm.phone_head("Graph", back=False).replace('class="ph-t"', 'class="ph-t ph-tl"')
    return (f'<div class="pwrap">{head}{phone_view_bar("Overview")}'
            f'<main class="pm"><p class="pcap">The archive in {num(52)} groups and {num(SMALL)} small groups, largest first. '
            f'The bar shows the kinds in each group.</p>{rows}</main>{tabbar()}</div>')


SCREENS = [
    ("switcher", "desk", "View switcher: states, keys and placement", screen_switcher),
    ("focus", "desk", "Focus with links between neighbors", screen_focus),
    ("explore", "desk", "Explore after four expansions, the pointer on a node", screen_explore),
    ("timeline", "desk", "Timeline with a date range and a selected link", screen_timeline),
    ("overview", "desk", "Overview at map zoom, a group selected", screen_overview),
    ("phone-focus", "phone", "Phone Focus with links between neighbors", phone_focus),
    ("phone-views", "phone", "Phone view sheet", phone_views),
    ("phone-explore", "phone", "Phone Explore with the trail peek", phone_explore),
    ("phone-timeline", "phone", "Phone Timeline list", phone_timeline),
    ("phone-overview", "phone", "Phone Overview group list", phone_overview),
]
MIN_LABELS = {"focus": 16, "explore": 21, "phone-explore": 10}  # every node except the root


def build(design_dir):
    tokens = (design_dir / "tokens.css").read_text()
    src = (design_dir / "answer-mockup.html").read_text()
    faces = [f for f in re.findall(r"@font-face\{[^}]*\}", src) if "Geist Mono" not in f]
    assert len(faces) == 4, len(faces)
    here = pathlib.Path(__file__).parent
    css = (here / "mockup.css").read_text() + "\n" + (here / "views-mockup.css").read_text()
    html = {i: fn() for i, _, _, fn in SCREENS}
    for i, n in MIN_LABELS.items():  # the overlap checks must not pass by dropping node labels
        got = len(re.findall(r'class="nl(?: nl-dim)?"', html[i]))
        assert got >= n, f"{i}: {got} node labels, expected at least {n}"
    boards = "".join(f'<section id="{i}" class="board {k}" aria-label="{bm.esc(t)}">{html[i]}</section>' for i, k, t, _ in SCREENS)
    index = "".join(f'<li><a href="#{i}">{bm.esc(t)}</a><span>{"1440 × 900" if k == "desk" else "390 × 844"}</span></li>' for i, k, t, _ in SCREENS)
    return f'''<!doctype html><html lang="en" data-theme="dark"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Graph views mockup</title>
<script>(function(){{var t=new URLSearchParams(location.search).get("theme");if(t==="light"||t==="dark")document.documentElement.dataset.theme=t;addEventListener("keydown",function(e){{if(e.key==="t"&&!/INPUT|TEXTAREA/.test(e.target.tagName)){{var d=document.documentElement;d.dataset.theme=d.dataset.theme==="dark"?"light":"dark"}}}})}})();</script>
<style>
{chr(10).join(faces)}
{tokens}
{css}
</style></head><body>
<main class="index"><h1>Graph views mockup</h1>
<p>Static screens of the view switcher and the new graph views proposed in <code>views.md</code>. Open a screen, then press <kbd>t</kbd> to switch the theme, or add <code>?theme=light</code> to the address. All names, documents, amounts and counts are invented.</p>
<ol>{index}</ol></main>
{boards}
</body></html>'''


if __name__ == "__main__":
    d = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "docs/design")
    out = pathlib.Path(sys.argv[2]) if len(sys.argv) > 2 else d / "graph" / "views-mockup.html"
    out.write_text(build(d))
    print(f"wrote {out} ({out.stat().st_size // 1024} KB)")
