"""Relationship families for graph browsing, in fixed display order."""

# Each row is (key, display name, token prefixes, explicit relationship types).
FAMILIES = (
    ('parties', 'Parties and roles',
     'WORK EMPLOY OFFICER OWNER SIGNED AUTHOR PREPAR PARTY CONTRACT ISSUE PROVIDER MEMBER BORROWER',
     'WORKS_AT EMPLOYED_BY OFFICER_OF OWNER_OF PREPARED_BY SIGNED_BY AUTHORIZED_BY PARTY_TO CONTRACTED_WITH ISSUED_BY AUTHORED_BY PROVIDER_FOR'),
    ('money', 'Money and accounts',
     'PAID PAYS PAYMENT PAYROLL BILLED BILLING INVOICE LOAN MORTGAGE PREMIUM INCOME TAXED ACCOUNT WITHHELD',
     'PAID_TO PAID_BY BILLED_BY INVOICED_BY WITHHELD_BY MORTGAGE_WITH LOAN_FROM PREMIUM_PAID_TO INCOME_FROM'),
    ('coverage', 'Coverage and obligations', 'COVER INSUR GOVERN APPLIES BENEFICIAR',
     'COVERS INSURES GOVERNS APPLIES_TO'),
    ('health', 'Health', 'PATIENT TREAT DIAGNOS CONDITION PRESCRIB MEDICA DISCHARG',
     'PATIENT_OF TREATED_BY DIAGNOSED_WITH HAS_CONDITION PRESCRIBED TAKES ORDERED_BY RESULTED_IN'),
    ('service', 'Service and government', 'SERVED SERVICE STATION DEPLOY ASSIGN BRANCH FILED SUBMIT VETERAN ENLIST',
     'SERVED_IN PARTICIPATED_IN STATIONED_AT DEPLOYED_TO ASSIGNED_TO BRANCH_OF_SERVICE RATED_AT FILED_WITH SUBMITTED_TO SERVICE_RECORD_OF'),
    ('places', 'Places', 'LOCATED LIVES RESIDE ADDRESS HEADQUARTER GARAGED',
     'LOCATED_IN LOCATED_AT LIVES_IN RESIDES_AT HEADQUARTERS_IN MAILING_ADDRESS'),
    ('time', 'Time', 'EFFECTIVE EXPIRE STARTS ENDS DATED RENEW', 'EFFECTIVE_FROM EXPIRES_ON'),
    ('doclinks', 'Document links', 'REFERENC SUPERSED ATTACHMENT SUPPLEMENT AMEND CORRECT',
     'REFERENCES SUPERSEDES ATTACHMENT_TO SUPPLEMENT_TO AMENDS CORRECTS'),
    ('evidence', 'Evidence', '', 'MENTIONS CONTAINS_RESULT'),
    ('loose', 'Loose association', 'RELATED ASSOCIAT', 'RELATED_TO ASSOCIATED_WITH'),
    ('other', 'Other', '', ''),
)

EXPLICIT_FAMILIES = {name: key for key, _, _, names in FAMILIES for name in names.split()}


def classify_relationship(relationship_type: str) -> tuple[str, str]:
    """Return the family key and assignment method for a stored type."""
    if relationship_type in EXPLICIT_FAMILIES:
        return EXPLICIT_FAMILIES[relationship_type], 'explicit'
    tokens = relationship_type.split('_')
    for key, _, prefixes, _ in FAMILIES:
        if prefixes and any(token.startswith(tuple(prefixes.split())) for token in tokens):
            return key, 'keyword'
    return 'other', 'unmapped'
