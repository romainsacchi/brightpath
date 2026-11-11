# Legacy herbicide identity in UVEK preparation

The normal CLIC fuel preparation failed on the separately listed agricultural-soil emission “Quizalofop ethyl ester” in its ecoinvent 3.8 cohort. Installed source metadata declares CAS 076578-14-8; the installed ecoinvent 3.10 target “Quizalofop-ethyl” declares CAS 76578-14-8. The first-block zero padding is immaterial and both normalized CAS check digits agree. This is not Quizalofop-P or quizalofop-p-ethyl.

The reviewed correspondence is restricted to that source release, kg and the full agricultural-soil compartment. It changes neither signed quantity nor uncertainty, formula or chemical identity. Other source releases retain explicit unresolved decisions. Metadata witnesses and limitations are in `brightpath/data/export/uvek_quizalofop_synonym_review.json`.

A versioned canonical resource preserves the earlier file byte for byte: existing content-bound reconstruction reviews remain reproducible. The sole production Python change selects the successor resource filename; the loader, strict identity checks, migration algorithms and Premise transformations are unchanged. The authoring generator emits both versions.

Verification on 11 November 2026: 72 existing/new canonical identity tests passed, followed by six focused tests including signed mass, uncertainty and formula retention. Full authoring replay reproduced all three resources byte for byte. The real UVEK 2026 / ecoinvent 3.10 REMIND SSP2-NPi 2050 fuel preparation then prepared 484 additional records. Its standard fuel transformation subsequently stopped at an empty gasoline supplier filter in CAZ. Preparation used a diagnostic bypass of the public sector support guard; fuel update and export are not qualified by this result.
