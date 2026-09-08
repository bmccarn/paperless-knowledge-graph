"""Calendar equivalence over exact source text; never rewrite quoted evidence."""
from dataclasses import dataclass
from datetime import date
import re

MONTHS = {name.casefold(): index for index, name in enumerate(
    ("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"), 1)}
MONTHS.update({name[:3]: value for name, value in list(MONTHS.items())})
MONTHS["sept"] = 9
_MONTH = r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?"
_PATTERN = re.compile(
    r"(?<![\w/+-])(?:"
    r"(?P<iso>\d{4}-\d{1,2}(?:-\d{1,2})?)"
    r"|(?P<slash>\d{1,2}/\d{1,2}/(?:\d{4}|\d{2}))"
    rf"|(?P<named>{_MONTH}\s+(?:\d{{1,2}}(?:st|nd|rd|th)?(?:,\s*|\s+))?\d{{4}})"
    rf"|(?P<day_first>\d{{1,2}}(?:st|nd|rd|th)?\s+{_MONTH}\s+\d{{4}})"
    r"|(?P<year>\d{4}))"
    r"(?!\w|[/.-]\d)", re.I)


@dataclass(frozen=True)
class SourceDate:
    start: int
    end: int
    text: str
    value: str | None
    precision: str | None
    reason: str | None = None


def source_dates(text: str, date_order: str = "mdy") -> list[SourceDate]:
    if date_order not in {"mdy", "dmy", "reject_ambiguous"}:
        raise ValueError("Unsupported numeric source date order")
    occurrences = []
    for match in _PATTERN.finditer(text):
        value, precision, reason = None, None, None
        try:
            if match["iso"]:
                parts = [int(p) for p in match["iso"].split("-")]
                year, month = parts[:2]
                day = parts[2] if len(parts) == 3 else 1
                parsed = date(year, month, day)
                precision = "day" if len(parts) == 3 else "month"
                value = parsed.isoformat() if precision == "day" else parsed.strftime("%Y-%m")
            elif match["slash"]:
                first, second, year = match["slash"].split("/")
                a, b = int(first), int(second)
                if date_order == "reject_ambiguous" and a <= 12 and b <= 12 and a != b:
                    reason = "ambiguous_date_order"
                else:
                    order = date_order
                    if order == "reject_ambiguous":
                        order = "dmy" if a > 12 else "mdy"
                    month, day = (a, b) if order == "mdy" else (b, a)
                    # A surrogate century checks calendar plausibility only; it
                    # never supplies authority to expand a short-year source.
                    parsed = date(int(year) if len(year) == 4 else 2000 + int(year), month, day)
                    if len(year) != 4:
                        reason = "unspecified_century"
                    else:
                        value, precision = parsed.isoformat(), "day"
            elif match["named"] or match["day_first"]:
                words = re.findall(r"[A-Za-z]+|\d+", match.group())
                month = next(MONTHS[word.casefold()] for word in words if word.casefold() in MONTHS)
                numbers = [int(word) for word in words if word.isdigit()]
                year = numbers[-1]
                day = numbers[0] if len(numbers) == 2 else 1
                parsed = date(year, month, day)
                precision = "day" if len(numbers) == 2 else "month"
                value = parsed.isoformat() if precision == "day" else parsed.strftime("%Y-%m")
            else:
                year = int(match["year"])
                date(year, 1, 1)
                value, precision = f"{year:04d}", "year"
        except (ValueError, StopIteration):
            reason = "invalid_calendar_date"
        occurrences.append(SourceDate(match.start(), match.end(), match.group(), value, precision, reason))
    return occurrences


def date_supported(expected: SourceDate, actual: SourceDate) -> bool:
    if expected.value is None or actual.value is None:
        # Literal short-year dates can be repeated, but never expanded to a century.
        return (expected.reason == actual.reason == "unspecified_century"
                and expected.text == actual.text)
    precision = {"year": 0, "month": 1, "day": 2}
    return (precision[actual.precision] >= precision[expected.precision]
            and (actual.value == expected.value or actual.value.startswith(expected.value + "-")))


def source_date_occurs(value: str, source: str, date_order: str = "mdy") -> bool:
    expected = source_dates(value, date_order)
    return (len(expected) == 1 and expected[0].start == 0 and expected[0].end == len(value)
            and any(date_supported(expected[0], found) for found in source_dates(source, date_order)))


def without_dates(text: str, occurrences: list[SourceDate]) -> str:
    for found in reversed(occurrences):
        text = text[:found.start] + " " * (found.end - found.start) + text[found.end:]
    return text
