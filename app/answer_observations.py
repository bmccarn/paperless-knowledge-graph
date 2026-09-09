"""Canonical rendering and atomic units for independently audited observations."""
from dataclasses import dataclass

from markdown_it import MarkdownIt


@dataclass(frozen=True)
class ObservationCandidate:
    observations: tuple[str, ...]
    strategy = 'observations_v1'

    @classmethod
    def from_response(cls, response):
        if not isinstance(response, dict) or set(response) != {'observations'}:
            raise ValueError('Invalid observation response')
        values = response['observations']
        if not isinstance(values, list) or not values:
            raise ValueError('Observations must be a nonempty list')
        parser = MarkdownIt('commonmark')
        for value in values:
            if (not isinstance(value, str) or not value.strip() or value != value.strip()
                    or len(value.splitlines()) != 1 or len(value) + 2 > 1200):
                raise ValueError('Invalid observation text')
            tokens = parser.parse(value)
            if ([token.type for token in tokens] != ['paragraph_open', 'inline', 'paragraph_close']
                    or any(token.type != 'text' for token in tokens[1].children or [])):
                raise ValueError('An observation must contain plain prose')
        return cls(tuple(values))

    @classmethod
    def from_text(cls, text):
        """Reconstruct persisted atomic units only from the exact canonical form."""
        if not isinstance(text, str):
            raise ValueError('Invalid observation candidate')
        parts = text.split('\n\n')
        if any(not part.startswith('- ') for part in parts):
            raise ValueError('Invalid observation rendering')
        candidate = cls.from_response({'observations': [part[2:] for part in parts]})
        if candidate.text != text:
            raise ValueError('Noncanonical observation rendering')
        return candidate

    @property
    def text(self):
        return '\n\n'.join('- ' + observation for observation in self.observations)

    def units(self):
        units, start = [], 0
        for observation in self.observations:
            text = '- ' + observation
            units.append({'id': f'u{len(units) + 1}', 'start': start,
                          'end': start + len(text), 'text': text})
            start += len(text) + 2
        return units

    def supported_subset(self, claims):
        units = self.units()
        if len(units) != len(claims) or any(
            any(claim.get(key) != unit[key] for key in ('id', 'start', 'end'))
            or claim.get('claim') != unit['text'] for claim, unit in zip(claims, units)
        ):
            raise ValueError('Observation ledger does not match its candidate')
        keep = [index for index, claim in enumerate(claims) if claim['status'] == 'supported']
        candidate = ObservationCandidate(tuple(self.observations[index] for index in keep)) if keep else None
        return candidate, {
            'reason': None if candidate else 'no_supported_context',
            'retained_ids': [claims[index]['id'] for index in keep],
            'omitted_units': [{
                'id': claim['id'], 'claim': claim['claim'], 'status': claim['status'],
                'rejection_reasons': claim.get('rejection_reasons', []),
                'value_mismatches': claim.get('value_mismatches', {}),
                'omission_reason': 'source_rejection', 'dependency_ids': [],
            } for claim in claims if claim['status'] != 'supported'],
        }
