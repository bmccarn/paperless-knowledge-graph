"""Exact quantity presence in original prose and structurally certified tables.

These facts supplement semantic verification; they never prove a row's role or
replace original quotes. No arithmetic, scale conversion or currency inference.
"""
from decimal import Decimal
import re
from markdown_it import MarkdownIt
from app.source_dates import VALUE_UNITS

UNIT = r'(?:' + VALUE_UNITS + r')(?:/[A-Za-zµμ°][A-Za-z0-9µμ°^+²³⁻-]*)*'
_UNIT_PATTERN = re.compile(r"(?<![A-Za-z'’/])" + UNIT + r'(?![A-Za-z/])')
NUMBER = r'[+-]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?:[eE][+-]?\d+)?'
_TABLE_MARKDOWN = MarkdownIt('commonmark').enable('table')
_CURRENCY_SYMBOL = {'USD': '$', 'CAD': '$', 'AUD': '$', 'EUR': '€', 'GBP': '£'}
_SCALE_WORDS = re.compile(r'\b(?:hundreds?|thousands?|millions?|billions?|trillions?|scaled?|scaling|factor|times|multiple|per|x)\b', re.I)
_LABEL = re.compile(r"^(?:[A-Za-z][A-Za-z '\-]*?\s+)?(?:\((" + UNIT + r")\)|(" + UNIT + r"))$")


def _physical_lines(text):
    # MarkdownIt maps CR/LF physical lines, not Python's additional Unicode separators.
    return [match.group() for match in re.finditer(r'[^\r\n]*(?:\r\n|\r|\n|$)', text) if match.group()]


def _rows(text):
    lines = [line.rstrip('\r\n') for line in _physical_lines(text)]
    # Decline escaped/inline-code structures rather than normalize them into cells.
    if len(lines) < 3 or any('\\' in line or '`' in line or '<' in line or '>' in line for line in lines):
        return None
    rows = []
    for line in lines:
        value = line.strip()
        if '|' not in value:
            return None
        rows.append([cell.strip() for cell in value.removeprefix('|').removesuffix('|').split('|')])
    if len(rows[0]) < 1 or any(len(row) != len(rows[0]) for row in rows):
        return None
    if not all(re.fullmatch(r':?-{3,}:?', cell) for cell in rows[1]):
        return None
    return rows[0], rows[2:]


def table_ranges(original):
    """Certify whole table intervals before any original text is sliced/normalized."""
    lines = _physical_lines(original)
    starts = [0]
    for line in lines:
        starts.append(starts[-1] + len(line))
    ranges = []
    for token in _TABLE_MARKDOWN.parse(original):
        if token.type != 'table_open' or token.map is None:
            continue
        first, last = token.map
        block = ''.join(lines[first:last])
        if _rows(block) is not None:
            left = starts[first] + len(block) - len(block.lstrip())
            right = starts[last] - (len(block) - len(block.rstrip()))
            ranges.append([left, right])
    return ranges


def _label_unit(label):
    if (any(char.isdigit() for char in label) or _SCALE_WORDS.search(label)
            or len(list(_UNIT_PATTERN.finditer(label))) != 1):
        return None
    match = _LABEL.fullmatch(label.strip())
    return next((unit for unit in match.groups() if unit is not None), None) if match else None


def _has_unit_annotation(label):
    return bool(_UNIT_PATTERN.search(label)
                or re.search(r'USD|EUR|GBP|CAD|AUD|JPY', label))


def table_quantities(original_quote, certified_ranges):
    pairs = set()
    for first, last in certified_ranges:
        table = _rows(original_quote[first:last])
        if table is None:
            continue
        headings, rows = table
        for row in rows:
            row_unit = _label_unit(row[0])
            for index, cell in enumerate(row):
                cell = cell.replace('−', '-')
                if not re.fullmatch(NUMBER, cell):
                    continue
                column_unit = _label_unit(headings[index])
                # A unit-bearing but unsupported label must not be bypassed by the other axis.
                labels = (headings[index], row[0])
                parsed = (column_unit, row_unit)
                if any(_has_unit_annotation(label) and unit is None for label, unit in zip(labels, parsed)):
                    continue
                if column_unit and row_unit and column_unit != row_unit:
                    continue
                unit = column_unit or row_unit
                if unit:
                    pairs.add((Decimal(cell.replace(',', '')), unit))
    return pairs


def unit_names(text):
    return {match.group() for match in _UNIT_PATTERN.finditer(text)}


def prose_quantities(text):
    pairs = {(Decimal(amount.replace(',', '')), unit) for amount, unit in re.findall(
        r'(?<![\w.,])(' + NUMBER + r')\s*(' + UNIT + r')(?![A-Za-z/])', text)}
    for unit, amount in re.findall(r'(USD|EUR|GBP|CAD|AUD|JPY|[$€£])\s*(' + NUMBER + r')(?!\w|[.,]\d)', text):
        pairs.add((Decimal(amount.replace(',', '')), unit))
    return pairs


def source_currency_units(units):
    return set(units) | {_CURRENCY_SYMBOL[unit] for unit in units if unit in _CURRENCY_SYMBOL}


def source_currency_quantities(pairs):
    return set(pairs) | {(amount, _CURRENCY_SYMBOL[unit]) for amount, unit in pairs if unit in _CURRENCY_SYMBOL}
