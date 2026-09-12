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
_MERIDIEM = re.compile(r'[ap]\.m\.', re.I)
_POWERS = frozenset('⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻')
_SCALE_WORDS = re.compile(r'\b(?:hundreds?|thousands?|millions?|billions?|trillions?|scaled?|scaling|factor|times|multiple|per|x|k|m|b|t|bn|mn|mm|tn|kilo|mega|giga)\b', re.I)


def _unit_continuation(char):
    category = unicodedata.category(char)
    return category[0] in {'L', 'N', 'M'} or category in {'Pc', 'Sm'} or char in '/°^+−-·⋅×'


def _unit_tokens(text):
    """Consume maximal unit-like tokens without regex suffix backtracking."""
    tokens, consumed = [], 0
    meridiem_letters = set()
    for marker in _MERIDIEM.finditer(text):
        first, last = marker.span()
        if ((first and (_unit_continuation(text[first - 1]) or text[first - 1] == '.'))
                or (last < len(text) and (_unit_continuation(text[last]) or text[last] == '.'))):
            continue
        meridiem_letters.add(first + 2)
    for match in _BASE_PATTERN.finditer(text):
        first, last = match.span()
        if first in meridiem_letters:
            continue
        # A slash may separate two explicitly prefixed currency amounts. Keep
        # consumed compound units intact and require the complete amount here.
        currency_after_separator = False
        if first and text[first - 1] == '/' and match.group() in _CURRENCIES:
            amount_start = last
            while amount_start < len(text) and text[amount_start].isspace():
                amount_start += 1
            compound_denominator = bool(tokens and tokens[-1][1] == first and tokens[-1][2].endswith('/'))
            currency_after_separator = (not compound_denominator
                and _CURRENCY_AMOUNT.match(text, amount_start) is not None)
        if first < consumed or (first and (text[first - 1].isalpha()
                or text[first - 1] in "_'’" or (text[first - 1] == '/' and not currency_after_separator))):
            continue
        following = text[last:last + 1]
        if following and following.isalpha() and unicodedata.category(following) != 'Lm':
            continue
        base = match.group()
        extend = bool(following and ((not following.isalnum() and _unit_continuation(following)) or unicodedata.category(following) == 'Lm' or following in _POWERS
                                    or (base not in _CURRENCIES and unicodedata.category(following)[0] == 'N')))
        if base in _CURRENCIES and following in {'+', '-'}:
            extend = False  # A signed currency amount is not a physical-unit suffix.
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
    def cells(line):
        # Unsupported row syntax grants no values, even beside a valid row.
        if any(char in line for char in ('\\', '`', '<', '>')) or '|' not in line:
            return None
        value = line.strip()
        return [cell.strip() for cell in value.removeprefix('|').removesuffix('|').split('|')]

    # Skipped markup can open a literal region spanning otherwise complete rows.
    if len(lines) < 3 or any(any(char in line for char in ('`', '<', '>')) for line in lines):
        return None
    headings, separator = cells(lines[0]), cells(lines[1])
    if (not headings or separator is None or len(separator) != len(headings)
            or not all(re.fullmatch(r':?-{3,}:?', cell) for cell in separator)):
        return None
    rows = []
    for line in lines[2:]:
        row = cells(line)
        if row is not None and len(row) == len(headings):
            rows.append(row)
    return headings, rows


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
