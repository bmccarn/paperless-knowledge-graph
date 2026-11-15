# Categories

This file holds the document-type labels and colors and the relationship families used by the redesign issue. The colors repeat the `--doc-*` tokens in `tokens.css`. Once `globals.css` holds the tokens and the backend family module exists, those are the sources, and this file points at them instead of repeating them.

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

The backend maps each relationship type to a family. An explicit map entry wins. Otherwise the type is split on `_`, and the first family in this table with a prefix that starts one of the tokens wins. Every other type is `other`.

| Key | Name | Keyword prefixes |
|---|---|---|
| `parties` | Parties and roles | WORK, EMPLOY, OFFICER, OWNER, SIGNED, AUTHOR, PREPAR, PARTY, CONTRACT, ISSUE, PROVIDER, MEMBER, BORROWER |
| `money` | Money and accounts | PAID, PAYS, PAYMENT, PAYROLL, BILLED, BILLING, INVOICE, LOAN, MORTGAGE, PREMIUM, INCOME, TAXED, ACCOUNT, WITHHELD |
| `coverage` | Coverage and obligations | COVER, INSUR, GOVERN, APPLIES, BENEFICIAR |
| `health` | Health | PATIENT, TREAT, DIAGNOS, CONDITION, PRESCRIB, MEDICA, DISCHARG |
| `service` | Service and government | SERVED, SERVICE, STATION, DEPLOY, ASSIGN, BRANCH, FILED, SUBMIT, VETERAN, ENLIST |
| `places` | Places | LOCATED, LIVES, RESIDE, ADDRESS, HEADQUARTER, GARAGED |
| `time` | Time | EFFECTIVE, EXPIRE, STARTS, ENDS, DATED, RENEW |
| `doclinks` | Document links | REFERENC, SUPERSED, ATTACHMENT, SUPPLEMENT, AMEND, CORRECT |
| `evidence` | Evidence | none (explicit only) |
| `loose` | Loose association | RELATED, ASSOCIAT |
| `other` | Other | none |
