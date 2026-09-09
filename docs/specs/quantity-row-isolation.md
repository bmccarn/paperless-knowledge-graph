# Preserve complete quantity rows beside unstructured table text

Status: proposed after the frozen original-table development experiment.
Parent: structural-quantity-evidence.md. No current native manifest changes.

Reproduction: the original contains a complete three-column Markdown table, one
numeric measurement row, then a pipe-free prose line without a blank separator.
MarkdownIt includes the trailing prose as a one-cell table row. `_rows` currently
rejects the entire block; no quantity survives even though the complete original
header and numeric row are present. The native auditor supported the measurement,
but all three corrected-local repetitions were rejected by this structural guard.

Keep whole-table certification against raw original text before slicing. Require
a complete, unambiguous header and separator. For quantity derivation, accept only
individual rows with the exact header column count and supported plain cell syntax.
An incomplete, pipe-free, escaped/code/HTML-containing or otherwise unsupported data
row supplies no quantity; it must not erase independent complete rows. Never repair,
pad, join or inherit cells from the skipped row. If no complete data row remains,
return no quantities. Invalid header/separator or code/HTML context still rejects
certification of the table. Preserve all original quotes and offsets unchanged.

This refines the earlier all-or-nothing ragged-table rule, not its prohibition on
inventing missing cells. Verify native finalizer acceptance for a complete row plus
trailing prose, rejection for quantities only on malformed/skipped rows, no cross-row
unit inheritance, and unaffected literal-table, scale, sign and swapped-role guards.
Run focused and full checks and review before a newly frozen native experiment.
