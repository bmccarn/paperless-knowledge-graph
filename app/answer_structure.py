"""Keep partial answers inside their original Markdown subject scopes."""
from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field

from markdown_it import MarkdownIt

_MARKDOWN = MarkdownIt('commonmark')


def _starts(text):
    starts = [0, *(match.end() for match in re.finditer(r'\r\n?|\n', text))]
    return starts if starts[-1] == len(text) else [*starts, len(text)]


def is_colon_label(line):
    return bool(re.fullmatch(r"(?:[-+*]\s+)?(?:\*\*[^*\n]+:\*\*|[^\n]+:)", line.strip()))


def audit_context(answer):
    # Empty-line width and trailing spaces do not establish subject scope.
    # Preserve hard breaks (two spaces), indentation and paragraph boundaries.
    lines = []
    for line in answer.splitlines(keepends=True):
        body = line.rstrip('\r\n')
        ending = line[len(body):]
        trimmed = body.rstrip(' \t')
        trailing = body[len(trimmed):]
        lines.append(trimmed + ('  ' if len(trailing) >= 2 and trimmed else trailing if trimmed else '') + ending)
    return re.sub(r'((?:\r\n?|\n))(?:\r\n?|\n){2,}', r'\1\1', ''.join(lines))


def _field(text):
    text = re.sub(r'^\s*(?:[-+*]|\d+[.)])\s+', '', text)
    # Ignore only clocks explicitly introduced as times. Bare numeric keys,
    # time-keyed fields, quantities and later delimiters remain conservative.
    clock = (r'\b(?:at|by|before|after|until|since|from|effective|around)\s+'
             r'(?:[01]?\d|2[0-3]):[0-5]\d(?::[0-5]\d)?(?!\d|[.,]\s*\d)'
             r'(?:\s*[AP]M\b|(?=\s*(?:$|[.,;!?)]|\b(?:on|and|to|through)\b)))')
    text = re.sub(clock, lambda match: match[0].replace(':', ' '), text, flags=re.I)
    return bool(re.match(r'^[^\r\n:]+:', text))


def _strong_label(token):
    if token.type != 'inline' or not token.map or token.map[1] != token.map[0] + 1:
        return False
    children = [child for child in token.children or [] if child.type != 'text' or child.content.strip()]
    if (len(children) < 3 or children[0].type != 'strong_open' or children[-1].type != 'strong_close'
            or any(child.level < 1 for child in children[1:-1])):
        return False
    text = ''.join(child.content for child in children).rstrip()
    return bool(text and not re.search(r"[.!?]['\"’”)}\]]*$", text))


def strong_label_offsets(answer):
    starts = _starts(answer)
    return {starts[token.map[0]] for token in _MARKDOWN.parse(answer) if _strong_label(token)}


@dataclass
class _Item:
    start: int
    end: int
    parent: int | None
    list_id: int
    markup: str
    units: set[str] = field(default_factory=set)
    field_item: bool = False


@dataclass
class _Block:
    start: int
    end: int
    item: int | None
    kind: str
    units: set[str]
    heading_level: int = 0
    label: bool = False


def _structure(answer, units):
    starts = _starts(answer)
    tokens = _MARKDOWN.parse(answer)
    items, blocks, item_stack, list_stack = [], [], [], []
    serial = 0
    heading_level = 0
    for token in tokens:
        if token.type in {'bullet_list_open', 'ordered_list_open'}:
            serial += 1
            list_stack.append(serial)
        elif token.type in {'bullet_list_close', 'ordered_list_close'}:
            list_stack.pop()
        elif token.type == 'list_item_open':
            items.append(_Item(starts[token.map[0]], starts[token.map[1]],
                               item_stack[-1] if item_stack else None, list_stack[-1], token.markup))
            item_stack.append(len(items) - 1)
        elif token.type == 'list_item_close':
            item_stack.pop()
        elif token.type == 'heading_open':
            heading_level = int(token.tag[1:])
        elif token.type == 'heading_close':
            heading_level = 0
        elif token.type == 'inline' and token.map:
            start, end = starts[token.map[0]], starts[token.map[1]]
            ids = {u['id'] for u in units if u['start'] < end and u['end'] > start}
            owner = item_stack[-1] if item_stack else None
            blocks.append(_Block(start, end, owner, 'heading' if heading_level else 'paragraph', ids,
                                 heading_level, _strong_label(token)))
            if owner is not None:
                item = items[owner]
                if not item.units:
                    item.field_item = _field(token.content) and not heading_level
                item.units.update(ids)

    # Colon labels can share a CommonMark paragraph with the following line.
    # Locate the exact label line inside the parsed paragraph, so only the
    # joined label/first-claim unit governs the subsequent fields.
    for block in list(blocks):
        if block.kind != 'paragraph' or block.label:
            continue
        for match in re.finditer(r'[^\r\n]+', answer[block.start:block.end]):
            if is_colon_label(match.group()):
                start, end = block.start + match.start(), block.start + match.end()
                ids = {u['id'] for u in units if u['start'] < end and u['end'] > start}
                blocks.append(_Block(start, end, block.item, 'paragraph', ids, label=True))
    blocks.sort(key=lambda block: block.start)
    dependencies = {u['id']: set() for u in units}
    signatures = {u['id']: Counter() for u in units}
    own_context = set()
    # Literal blocks and quotations need their own scope model before selective
    # removal can be safe. Withhold their units, rather than reparent them.
    opaque_ranges = [(starts[t.map[0]], starts[t.map[1]]) for t in tokens
                     if t.map and t.type in {'blockquote_open', 'fence', 'code_block', 'html_block'}]
    unknown = {u['id'] for u in units for start, end in opaque_ranges
               if u['start'] < end and u['end'] > start}

    def govern(ids, governors, signature):
        for uid in ids:
            dependencies[uid].update(governors - {uid})
            signatures[uid][signature] += 1

    # A heading is scoped by its depth and containing list item. Standalone
    # bold labels remain cumulative until an explicit heading/container ends;
    # another ambiguous label cannot silently reset the subject.
    for block in blocks:
        if not (block.heading_level or block.label):
            continue
        limit = items[block.item].end if block.item is not None else len(answer)
        for later in blocks:
            if (later.start > block.start and later.item == block.item and later.heading_level
                    and (not block.heading_level or later.heading_level <= block.heading_level)):
                limit = min(limit, later.start)
                break
        governed = {u['id'] for u in units if u['end'] > block.start and u['start'] < limit}
        signature = ('section', block.heading_level, answer[block.start:block.end].strip(), tuple(sorted(block.units)))
        govern(governed, block.units, signature)
        own_context.update(block.units)

    # Every direct assertion of each containing parent item governs children.
    # A unit may span a label and its first child, so a block can share its unit
    # with an ancestor; self-dependencies are excluded.
    for block in blocks:
        owner = block.item
        if owner is None:
            continue
        signatures_for_owner = ('item', items[owner].markup)
        for uid in block.units:
            signatures[uid][signatures_for_owner] += 1
        parent = items[owner].parent
        while parent is not None:
            item = items[parent]
            govern(block.units, item.units, ('parent', item.markup, tuple(sorted(item.units))))
            own_context.update(block.units & item.units)
            if not item.units:
                unknown.update(block.units)
            parent = item.parent

    # Flat sibling fields are not independent record scopes. Carry the prior
    # run's context transitively, including an intervening rejected field.
    previous = {}
    for item in items:
        prior = previous.get(item.list_id, set())
        if item.field_item:
            govern(item.units, prior, ('field_run', tuple(sorted(prior))))
            for uid in item.units:
                if not (dependencies[uid] or uid in own_context):
                    unknown.add(uid)
            previous[item.list_id] = prior | item.units
        else:
            previous[item.list_id] = set(item.units)
    # Plain field paragraphs have the same subject dependency as list fields.
    # A leading standalone field can be resolved by the question/auditor; once
    # preceding answer context exists it cannot be silently discarded.
    root_paragraphs = {uid: set().union(*(b.units for b in blocks if b.item is None and uid in b.units))
                       for block in blocks if block.item is None for uid in block.units}
    root_headings = {uid for block in blocks if block.item is None and block.heading_level for uid in block.units}
    root_items = {uid: item.units for item in items if item.parent is None for uid in item.units}
    prior = set()
    for unit in units:
        uid = unit['id']
        if uid in root_paragraphs and uid not in root_headings and _field(unit['text']):
            govern({uid}, prior, ('paragraph_field_run', tuple(sorted(prior))))
            prior = prior | {uid}
        elif uid in root_paragraphs or uid in root_items:
            prior = root_paragraphs.get(uid, root_items.get(uid, {uid}))
    return dependencies, unknown, signatures


def supported_revision(answer, claims, unitize):
    """Select whole supported units without changing their governing context.

    Returns a candidate only if raw-range removal and a fresh parse preserve
    every survivor and its ancestry. No model support is reused for delivery.
    """
    units = [{'id': c['id'], 'start': c['start'], 'end': c['end'], 'text': c['claim']} for c in claims]
    ids = {u['id'] for u in units}
    if (len(ids) != len(units)
            or any(not isinstance(u['start'], int) or not isinstance(u['end'], int)
                   or not 0 <= u['start'] < u['end'] <= len(answer)
                   or answer[u['start']:u['end']] != u['text'] for u in units)
            or any(a['end'] > b['start'] for a, b in zip(units, units[1:]))):
        return {'candidate': None, 'reason': 'dependency_unknown', 'retained_ids': [], 'omitted_units': []}
    dependencies, unknown, original_scopes = _structure(answer, units)
    keep = {c['id'] for c in claims if c['status'] == 'supported'}
    while True:
        reduced = {uid for uid in keep if uid not in unknown and dependencies[uid] <= keep}
        if reduced == keep:
            break
        keep = reduced
    omitted = [{
        'id': c['id'], 'claim': c['claim'], 'status': c['status'],
        'rejection_reasons': c.get('rejection_reasons', []), 'value_mismatches': c.get('value_mismatches', {}),
        'omission_reason': ('source_rejection' if c['status'] != 'supported' else
                            'dependency_unknown' if c['id'] in unknown else 'dependency_omitted'),
        'dependency_ids': sorted(dependencies[c['id']] - keep),
    } for c in claims if c['id'] not in keep]
    result = {'candidate': None, 'reason': None, 'retained_ids': [u['id'] for u in units if u['id'] in keep],
              'omitted_units': omitted}
    if not keep:
        result['reason'] = 'no_supported_context'
        return result
    candidate = answer
    for unit in reversed(units):
        if unit['id'] not in keep:
            candidate = candidate[:unit['start']] + candidate[unit['end']:]
    candidate = candidate.strip('\r\n')
    survivors = [u for u in units if u['id'] in keep]
    revised = unitize(candidate)
    if [u['text'] for u in revised] != [u['text'] for u in survivors]:
        result['reason'] = 'structure_changed'
        return result
    # Original ids are used only to compare structures, never as a new audit.
    mapped = [{**u, 'id': old['id']} for u, old in zip(revised, survivors)]
    _, revised_unknown, revised_scopes = _structure(candidate, mapped)
    if revised_unknown or any(revised_scopes[uid] != original_scopes[uid] for uid in keep):
        result['reason'] = 'structure_changed'
        return result
    result['candidate'] = candidate
    return result
