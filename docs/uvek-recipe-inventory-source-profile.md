# Preserve the source version of a reused inventory model

A newer caller can use an explicitly reviewed older manufacturing model as a
screening proxy. Its reference quantities and omission audits still describe
the older model. Previously, the forward UVEK materializer treated every audit
as belonging to the caller's source version and rejected that otherwise explicit
reuse: “Recipe source omission belongs to a different source profile.” A synthetic
regression reproduced that failure before the runtime change.

An input pointing to an existing recipe can now declare its model version:

```json
{
  "recipe": "reviewed-older-production",
  "amount": 1,
  "inventory_source_profile": {
    "family": "ecoinvent",
    "version": "3.9",
    "system_model": "cutoff"
  }
}
```

That profile is inherited through the selected dependency graph. It must be a
declared source profile in the resource, in the caller's family and system
model. Exact rule/profile approvals remain required; there is no inferred
version fallback. Existing omission-owner, reference-sign and zero-stock guards
remain. The field is supported only for forward ecoinvent-to-UVEK recipes;
reverse and other routes reject it in resource validation.

Generated provenance and omission losses retain both the actual model profile
and the caller profile. Helpers in different contexts have distinct identities
and cache entries. Explicit changes of model version produce a technology/vintage
information-loss finding and obey the existing strict/warning policy. The field
is consumed as model metadata rather than exported as an exchange property.

Default recipes preserve their existing identifiers and behavior. A private
comparison against the preceding published materializer finds identical
caller data, all 22 helper inventories, identifiers, audits and reports for
positive and negative demands on the existing source-3.9 ferrosilicon model.
Synthetic tests cover inheritance, two contexts for one helper, source
non-mutation, strict losses, missing declarations and prohibited profile changes.
This is a general provenance fix in one Brightpath runtime module, with no
Premise transformation or notebook changes. It does not approve a new proxy by
itself; a separate exact-profile scientific review must authorize its use.
