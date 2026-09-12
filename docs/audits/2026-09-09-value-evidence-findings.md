# Value-evidence findings from synthetic query controls

Status: reproduced false rejections; no guard changes or release claim.
Observed with frozen reader-scope code `120938b` and existing finalizer.

The development baseline rejected supported observations for distinct reasons:

- A signed credit of `-$40 USD` supports the statement that a charge was reduced
  by `$40 USD`, but the lexical guard requires the positive scalar to occur in the
  source. Do not fix this with unconditional absolute-value matching: direction
  must remain verified, and negative controls must retain their signs.
- Currency in explicit Markdown column headings (`Charge USD`, `Balance USD`) is
  not bound to the numeric cells by the current quantity matcher. It therefore
  rejects `$100 USD`/`$500 USD` in a supported transaction observation.
- A table row labelled `Concentration mg/L` with result `12` and target `4` lacks
  an adjacent `12 mg/L` string. The guard rejects the supported measured result.
  Result and target roles must still be distinguished, and `mg` is not `mg/L`.

These are independent of adding a source reader. A reviewed value-evidence design
must retain exact original provenance, derive units only from structurally bound
original table headings/row labels, preserve currency identity, signs, scale and
compound units, and leave relationship/date/record-role proof to explicit checks.
Never infer a missing table header across unrelated excerpts. Native adversarial
controls are required before any relaxation of existing lexical rejection behavior.
The current experiment manifests remain unchanged. No gold labels were altered.
