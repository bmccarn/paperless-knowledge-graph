"""Canonical rendering and atomic units for independently audited observations."""
from dataclasses import dataclass
import json

from markdown_it import MarkdownIt


class ObservationValidationError(ValueError):
    """Content-free failure categories safe to retain with a terminal answer."""
    reasons = frozenset({
        'transport_unavailable', 'invalid_json', 'duplicate_key', 'invalid_object',
        'invalid_observations', 'empty_observations', 'non_string_observation',
        'empty_observation', 'padded_observation', 'multiline_observation',
        'oversized_observation', 'formatted_observation', 'invalid_attribution',
    })

    def __init__(self, reason, item_index=None):
        self.reason = reason if reason in self.reasons else 'invalid_object'
        self.item_index = item_index if type(item_index) is int and 0 <= item_index <= 1_000_000 else None
        super().__init__(self.reason)

    @property
    def diagnostic(self):
        return {'reason': self.reason, **({'item_index': self.item_index} if self.item_index is not None else {})}


@dataclass(frozen=True)
class ObservationCandidate:
    observations: tuple[str, ...]
    strategy = 'observations_v1'

    @staticmethod
    def response_format():
        return {'type': 'json_schema', 'json_schema': {
            'name': 'answer_observations', 'strict': True,
            'schema': {'type': 'object', 'additionalProperties': False,
                       'required': ['observations'], 'properties': {'observations': {
                           'type': 'array', 'minItems': 1, 'items': {
                               'type': 'string',
                               'description': 'One self-contained source observation in plain text on one line. Include its own subject and relevant date. No Markdown, headings, links or citations.',
                           }}}},
        }}

    @classmethod
    def from_json(cls, text):
        """Consume the complete editor response without salvaging inner JSON."""
        def unique_object(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ObservationValidationError('duplicate_key')
                result[key] = value
            return result
        if not isinstance(text, str) or not text.strip():
            raise ObservationValidationError('transport_unavailable')
        try:
            response = json.loads(text, object_pairs_hook=unique_object)
        except ObservationValidationError:
            raise
        except (ValueError, RecursionError):
            raise ObservationValidationError('invalid_json') from None
        return cls.from_response(response)

    @classmethod
    def from_response(cls, response):
        if not isinstance(response, dict) or set(response) != {'observations'}:
            raise ObservationValidationError('invalid_object')
        values = response['observations']
        if not isinstance(values, list):
            raise ObservationValidationError('invalid_observations')
        if not values:
            raise ObservationValidationError('empty_observations')
        parser = MarkdownIt('commonmark')
        for index, value in enumerate(values):
            if not isinstance(value, str):
                raise ObservationValidationError('non_string_observation', index)
            if not value.strip():
                raise ObservationValidationError('empty_observation', index)
            if len(value.splitlines()) != 1:
                raise ObservationValidationError('multiline_observation', index)
            if value != value.strip():
                raise ObservationValidationError('padded_observation', index)
            if len(value) + 2 > 1200:
                raise ObservationValidationError('oversized_observation', index)
            tokens = parser.parse(value)
            if ([token.type for token in tokens] != ['paragraph_open', 'inline', 'paragraph_close']
                    or any(token.type != 'text' for token in tokens[1].children or [])):
                raise ObservationValidationError('formatted_observation', index)
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
