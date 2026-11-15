# Categories

This file holds the document-type labels and colors used by the redesign issue. The relationship family definitions live in the backend module linked below. The colors repeat the `--doc-*` tokens in `tokens.css`. Once `globals.css` holds the tokens, it is the color source.

## Document types

Each type appears as a colored dot with its text label.

| Key | Label | Light | Dark |
|---|---|---|---|
| `financial_invoice` | Invoice | `oklch(0.30 0.050 82)` | `oklch(0.84 0.168 94)` |
| `government_tax` | Tax | `oklch(0.31 0.174 275)` | `oklch(0.70 0.124 280)` |
| `property_home` | Home | `oklch(0.62 0.056 124)` | `oklch(0.91 0.161 150)` |
| `insurance` | Insurance | `oklch(0.58 0.099 217)` | `oklch(0.76 0.123 190)` |
| `military` | Military | `oklch(0.52 0.050 140)` | `oklch(0.55 0.050 121)` |
| `legal_contract` | Legal | `oklch(0.54 0.190 33)` | `oklch(0.65 0.131 86)` |
| `personal` | Personal | `oklch(0.65 0.190 314)` | `oklch(0.92 0.050 306)` |
| `work` | Work | `oklch(0.43 0.093 269)` | `oklch(0.58 0.186 261)` |
| `medical_lab` | Medical | `oklch(0.42 0.050 9)` | `oklch(0.76 0.133 31)` |
| any other | the key in sentence case | `--fg-subtle` | `--fg-subtle` |

## Relationship families

The source is [`app/relationship_families.py`](../../app/relationship_families.py).
It defines the fixed family order, display names, token prefixes and explicit seed map.
Only graph routes use the classifier; extraction and answer retrieval keep their existing behavior.

An explicit map entry wins. Otherwise the type is split on `_`, and the first family
in the fixed order with a prefix that starts a token wins. Every other type is `other`.
`GET /graph/relationship-types` lists stored types with counts, families and assignment methods.
