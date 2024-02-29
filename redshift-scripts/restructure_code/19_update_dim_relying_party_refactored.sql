UPDATE conformed_refactored.dim_relying_party_refactored AS dim
SET relying_party_name=ref.client_name
    ,display_name=ref.display_name
    ,department_name = ref.department_name
    ,agency_name = ref.agency_name
FROM conformed_refactored.ref_relying_parties_refactored AS ref
WHERE dim.client_id = ref.client_id;