"""Exact quantity presence in original prose and structurally certified tables.

These facts supplement semantic verification; they never prove a row's role or
replace original quotes. No arithmetic, scale conversion or currency inference.
"""
from decimal import Decimal
import re
import unicodedata
from markdown_it import MarkdownIt
from app.source_dates import VALUE_UNIT_NAMES

NUMBER = r'[+-]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?:[eE][+-]?\d+)?'
_NUMBERS = re.compile(r'(?<![\w.,])' + NUMBER + r'(?!\d|[.,]\d)')
_CURRENCY_AMOUNT = re.compile(NUMBER + r'(?!\w|[.,]\d)')
_TABLE_MARKDOWN = MarkdownIt('commonmark').enable('table')
_CURRENCY_SYMBOL = {'USD': '$', 'CAD': '$', 'AUD': '$', 'EUR': '€', 'GBP': '£'}
_CURRENCIES = {'USD', 'EUR', 'GBP', 'CAD', 'AUD', 'JPY', '$', '€', '£'}
_BASES = sorted({unit.split('/')[0] for unit in VALUE_UNIT_NAMES}, key=lambda unit: (-len(unit), unit))
_BASE_PATTERN = re.compile('|'.join(re.escape(unit) for unit in _BASES))
_POWERS = frozenset('⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻')
_SCALE_WORDS = re.compile(r'\b(?:hundreds?|thousands?|millions?|billions?|trillions?|scaled?|scaling|factor|times|multiple|per|x|k|m|b|t|bn|mn|mm|tn|kilo|mega|giga)\b', re.I)


def _unit_continuation(char):
    category = unicodedata.category(char)
    return category[0] in {'L', 'N', 'M'} or category in {'Pc', 'Sm'} or char in '/°^+−-·⋅×'


def _unit_tokens(text):
    """Consume maximal unit-like tokens without regex suffix backtracking."""
    tokens, consumed = [], 0
    for match in _BASE_PATTERN.finditer(text):
        first, last = match.span()
        if first < consumed or (first and (text[first - 1].isalpha() or text[first - 1] in "_/'’")):
            continue
        following = text[last:last + 1]
        if following and following.isalpha():
            continue
        base = match.group()
        extend = bool(following and (following in '/^_·⋅×' or unicodedata.category(following)[0] == 'M' or following in _POWERS
                                    or (base not in _CURRENCIES and following.isdigit())))
        if extend:
            while last < len(text) and _unit_continuation(text[last]):
                last += 1
        tokens.append((first, last, text[first:last]))
        consumed = last
    return tokens


def _power_suffix(text):
    if not text or text.isdecimal() or all(char in _POWERS for char in text):
        return True
    return text.startswith('^') and text[1:].lstrip('+-').isdecimal() and not text[1:].startswith(('++', '--', '+-', '-+'))


def _label_identity(unit):
    parts = unit.split('/')
    base = _BASE_PATTERN.match(parts[0])
    if base is None or not _power_suffix(parts[0][base.end():]):
        return False
    if base.group() in _CURRENCIES and parts[0] != base.group():
        return False
    for part in parts[1:]:
        index = 0
        while index < len(part) and (part[index].isalpha() or part[index] == '°'):
            index += 1
        if not index or not _power_suffix(part[index:]):
            return False
    return True


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
    label = label.strip()
    tokens = _unit_tokens(label)
    if len(tokens) != 1:
        return None
    first, last, unit = tokens[0]
    prefix, suffix = label[:first], label[last:]
    if suffix == ')' and prefix.endswith('('):
        prefix, suffix = prefix[:-1], ''
    if suffix or (prefix and not re.fullmatch(r"[A-Za-z][A-Za-z '\-]*\s+", prefix)):
        return None
    if _SCALE_WORDS.search(prefix) or not _label_identity(unit):
        return None
    return unit


def _has_unit_annotation(label):
    return any(_label_identity(unit) or '/' in unit or '^' in unit for _, _, unit in _unit_tokens(label)) or any(code in label for code in _CURRENCIES if code.isalpha())


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
    return {unit for _, _, unit in _unit_tokens(text)}


def prose_quantities(text):
    tokens = _unit_tokens(text)
    starts = {first: unit for first, _, unit in tokens}
    pairs = set()
    for match in _NUMBERS.finditer(text):
        first, last = match.span()
        after = last
        while after < len(text) and text[after].isspace():
            after += 1
        amount = Decimal(match.group().replace(',', ''))
        if after in starts:
            pairs.add((amount, starts[after]))
    for _, last, unit in tokens:
        if unit not in _CURRENCIES:
            continue
        while last < len(text) and text[last].isspace():
            last += 1
        amount = _CURRENCY_AMOUNT.match(text, last)
        if amount is not None:
            pairs.add((Decimal(amount.group().replace(',', '')), unit))
    return pairs


def source_currency_units(units):
    return set(units) | {_CURRENCY_SYMBOL[unit] for unit in units if unit in _CURRENCY_SYMBOL}


def source_currency_quantities(pairs):
    return set(pairs) | {(amount, _CURRENCY_SYMBOL[unit]) for amount, unit in pairs if unit in _CURRENCY_SYMBOL}
