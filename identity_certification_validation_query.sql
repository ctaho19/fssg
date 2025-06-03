-- Snowflake Identity Certification Validation Query
-- Validates System account certifications by comparing expected vs actual results
-- Implements COF System ID Exclusion Rules

WITH certification_config AS (
    SELECT '2025_Q2_SYSCERT' AS cert_tag
),

-- Get all system accounts and their entitlements
system_accounts AS (
    SELECT 
        i.name AS eid,
        i.cof_account_type,
        i.cof_managed_by,
        a.name AS application,
        a.cof_app_certification_freq,
        l.native_identity,
        l.display_name,
        l.created,
        CASE WHEN l.iiq_disabled = '0' THEN 'false' ELSE 'true' END AS account_disable_flag,
        ie.name AS attribute,
        ie.value,
        CASE 
            WHEN ie.value IS NULL AND ie.name IS NULL THEN 'Account'
            WHEN ie.name IN ('detectedRoles', 'assignedRoles') THEN 'Bundle'
            ELSE 'Exception'
        END AS item_type
    FROM identityiq.spt_identity i
    LEFT JOIN identityiq.spt_link l ON l.identity_id = i.id
    LEFT JOIN identityiq.spt_application a ON l.application = a.id
    LEFT JOIN identityiq.spt_identity_entitlement ie 
        ON l.application = ie.application 
        AND l.identity_id = ie.identity_id 
        AND l.native_identity = ie.native_identity
    WHERE i.cof_account_type = 'System'
        AND i.cof_managed_by IS NOT NULL
        AND l.native_identity NOT LIKE '%OU=Resource Mailboxes%'
),

-- Get managed attributes for entitlements
managed_attributes AS (
    SELECT 
        ma.value,
        ma.attribute,
        ma.display_name,
        ma.requestable,
        ma.cof_ent_related_application,
        a.name AS application_name,
        a.id AS application_id
    FROM identityiq.spt_managed_attribute ma
    JOIN identityiq.spt_application a ON ma.application = a.id
),

-- Get certification exclusion rules
exclusion_rules AS (
    SELECT DISTINCT
        type,
        application,
        attribute,
        value,
        certification_type
    FROM iiqcap1.certification_exclusion
    WHERE certification_type = 'SystemId_Q2'
),

-- Get actual certification results
actual_certifications AS (
    SELECT DISTINCT
        sptc.name AS cert_name,
        sptci.id AS item_id,
        sptc.id AS cert_id,
        sptci.type AS item_type,
        sptce.target_name AS eid,
        sptci.exception_application AS application,
        es.native_identity,
        CASE
            WHEN sptci.type = 'Bundle' AND sptci.sub_type IS NULL THEN 'detectedRoles'
            WHEN sptci.type = 'Bundle' AND sptci.sub_type IS NOT NULL THEN 'assignedRoles'
            ELSE sptci.exception_attribute_name
        END AS attribute,
        CASE
            WHEN sptci.type = 'Bundle' THEN sptci.bundle
            ELSE sptci.exception_attribute_value
        END AS value,
        'Inclusion' AS actual_result
    FROM identityiq.spt_certification sptc
    JOIN identityiq.spt_certification_entity sptce ON sptc.id = sptce.certification_id
    LEFT JOIN identityiq.spt_certification_item sptci ON sptce.id = sptci.certification_entity_id
    LEFT JOIN identityiq.spt_entitlement_snapshot es ON sptci.exception_entitlements = es.id
    JOIN identityiq.spt_certification_tags ct ON sptc.id = ct.certification_id
    JOIN identityiq.spt_tag t ON ct.elt = t.id
    WHERE t.name IN (SELECT cert_tag FROM certification_config)
    
    UNION ALL
    
    -- Archived/excluded items
    SELECT DISTINCT
        sptc.name AS cert_name,
        saci.id AS item_id,
        sptc.id AS cert_id,
        saci.type AS item_type,
        sace.target_name AS eid,
        saci.exception_application AS application,
        saci.exception_native_identity AS native_identity,
        CASE
            WHEN saci.type = 'Bundle' AND saci.sub_type IS NULL THEN 'detectedRoles'
            WHEN saci.type = 'Bundle' AND saci.sub_type IS NOT NULL THEN 'assignedRoles'
            ELSE saci.exception_attribute_name
        END AS attribute,
        CASE
            WHEN saci.type = 'Bundle' THEN saci.bundle
            ELSE saci.exception_attribute_value
        END AS value,
        'Exclusion' AS actual_result
    FROM identityiq.spt_certification sptc
    JOIN identityiq.spt_archived_cert_entity sace ON sptc.id = sace.certification_id
    LEFT JOIN identityiq.spt_archived_cert_item saci ON sace.id = saci.parent_id
    JOIN identityiq.spt_certification_tags ct ON sptc.id = ct.certification_id
    JOIN identityiq.spt_tag t ON ct.elt = t.id
    WHERE t.name IN (SELECT cert_tag FROM certification_config)
),

-- Apply exclusion rules and determine expected results
validation_results AS (
    SELECT 
        sa.*,
        ac.cert_name,
        ac.cert_id,
        ac.item_id,
        COALESCE(ac.actual_result, 'No Data in Cert') AS actual_result,
        
        -- Apply exclusion rules based on item type and business logic
        CASE
            -- ACCOUNT TYPE RULES
            WHEN sa.item_type = 'Account' THEN
                CASE
                    -- AD account with no entitlements
                    WHEN sa.application = 'Active Directory' AND sa.value IS NULL 
                        THEN 'Exclusion'
                    -- Application not marked for quarterly certification
                    WHEN sa.cof_app_certification_freq != 'Quarterly' 
                        THEN 'Exclusion'
                    -- Application in exclusion list
                    WHEN EXISTS (
                        SELECT 1 FROM exclusion_rules er 
                        WHERE er.type = 'applicationsToBeExcluded' 
                        AND er.application = sa.application
                    ) THEN 'Exclusion'
                    -- Disabled account in specific applications
                    WHEN sa.account_disable_flag = 'true' AND EXISTS (
                        SELECT 1 FROM exclusion_rules er 
                        WHERE er.type = 'disabledAccountApplicationsToBeExcluded' 
                        AND er.application = sa.application
                    ) THEN 'Exclusion'
                    ELSE 'Inclusion'
                END
                
            -- BUNDLE TYPE RULES
            WHEN sa.item_type = 'Bundle' THEN
                CASE
                    -- Base type bundles
                    WHEN EXISTS (
                        SELECT 1 FROM identityiq.spt_bundle b 
                        WHERE b.name = sa.value AND b.type = 'base'
                    ) THEN 'Exclusion'
                    -- Team site/role exclusions
                    WHEN EXISTS (
                        SELECT 1 FROM identityiq.spt_bundle b 
                        WHERE b.name = sa.value 
                        AND b.type IN ('it', 'business')
                        AND (b.name LIKE '%_TEAM_SITE%' OR b.name LIKE '%_TEAM_ROLE%')
                    ) THEN 'Exclusion'
                    -- Roles in exclusion list
                    WHEN EXISTS (
                        SELECT 1 FROM exclusion_rules er 
                        WHERE er.type = 'rolesToBeExcluded' 
                        AND er.value = sa.value
                    ) THEN 'Exclusion'
                    -- Detected role with assigned role exists
                    WHEN sa.attribute = 'detectedRoles' AND EXISTS (
                        SELECT 1 FROM identityiq.spt_identity_entitlement ie2
                        JOIN identityiq.spt_identity i2 ON i2.id = ie2.identity_id
                        WHERE i2.name = sa.eid 
                        AND ie2.name = 'assignedRoles'
                        AND ie2.value = sa.value
                    ) THEN 'Exclusion'
                    ELSE 'Inclusion'
                END
                
            -- EXCEPTION TYPE RULES
            WHEN sa.item_type = 'Exception' THEN
                CASE
                    -- Entitlement in exclusion list
                    WHEN EXISTS (
                        SELECT 1 FROM exclusion_rules er 
                        WHERE er.type = 'entitlementsToBeExcluded'
                        AND er.application = sa.application
                        AND UPPER(er.value) = UPPER(sa.value)
                        AND UPPER(er.attribute) = UPPER(sa.attribute)
                    ) THEN 'Exclusion'
                    -- AD non-requestable entitlements
                    WHEN sa.application = 'Active Directory' AND EXISTS (
                        SELECT 1 FROM managed_attributes ma
                        WHERE ma.application_name = sa.application
                        AND ma.value = sa.value
                        AND ma.cof_ent_related_application IS NULL
                        AND (ma.requestable IS NULL OR ma.requestable = 0)
                    ) THEN 'Exclusion'
                    -- AWS Connect Security Profile exclusion
                    WHEN sa.application = 'AWS Connect - Card_CDE-East' 
                        AND sa.attribute = 'SecurityProfileNames'
                        THEN 'Exclusion'
                    -- AWS IAM without native identity
                    WHEN sa.application = 'AWS IAM User/Role Request' 
                        AND sa.native_identity IS NULL
                        THEN 'Exclusion'
                    -- Disabled account entitlements
                    WHEN EXISTS (
                        SELECT 1 FROM identityiq.spt_link l2
                        JOIN identityiq.spt_application a2 ON l2.application = a2.id
                        WHERE a2.name = sa.application
                        AND l2.native_identity = sa.native_identity
                        AND l2.iiq_disabled = '1'
                        AND EXISTS (
                            SELECT 1 FROM exclusion_rules er 
                            WHERE er.type = 'disabledAccountApplicationsToBeExcluded'
                            AND er.application = sa.application
                        )
                    ) THEN 'Exclusion'
                    ELSE 'Inclusion'
                END
                
            ELSE 'Inclusion'
        END AS expected_result,
        
        -- Provide reason for exclusion/inclusion
        CASE
            WHEN sa.item_type = 'Account' THEN
                CASE
                    WHEN sa.application = 'Active Directory' AND sa.value IS NULL 
                        THEN 'Account Only effective application is "Active Directory" and it is not requestable'
                    WHEN sa.cof_app_certification_freq != 'Quarterly' 
                        THEN 'Application not marked for quarterly certification'
                    WHEN EXISTS (
                        SELECT 1 FROM exclusion_rules er 
                        WHERE er.type = 'applicationsToBeExcluded' 
                        AND er.application = sa.application
                    ) THEN 'Application in exclusion list'
                    WHEN sa.account_disable_flag = 'true' AND EXISTS (
                        SELECT 1 FROM exclusion_rules er 
                        WHERE er.type = 'disabledAccountApplicationsToBeExcluded' 
                        AND er.application = sa.application
                    ) THEN 'Disabled account in excluded application'
                    ELSE 'No exclusion found'
                END
                
            WHEN sa.item_type = 'Bundle' THEN
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM identityiq.spt_bundle b 
                        WHERE b.name = sa.value AND b.type = 'base'
                    ) THEN 'Base type bundle'
                    WHEN EXISTS (
                        SELECT 1 FROM identityiq.spt_bundle b 
                        WHERE b.name = sa.value 
                        AND b.type IN ('it', 'business')
                        AND (b.name LIKE '%_TEAM_SITE%' OR b.name LIKE '%_TEAM_ROLE%')
                    ) THEN 'Team site/role exclusion'
                    WHEN EXISTS (
                        SELECT 1 FROM exclusion_rules er 
                        WHERE er.type = 'rolesToBeExcluded' 
                        AND er.value = sa.value
                    ) THEN 'Role in exclusion list'
                    WHEN sa.attribute = 'detectedRoles' AND EXISTS (
                        SELECT 1 FROM identityiq.spt_identity_entitlement ie2
                        JOIN identityiq.spt_identity i2 ON i2.id = ie2.identity_id
                        WHERE i2.name = sa.eid 
                        AND ie2.name = 'assignedRoles'
                        AND ie2.value = sa.value
                    ) THEN 'assignedRole exists'
                    ELSE 'No exclusion found'
                END
                
            WHEN sa.item_type = 'Exception' THEN
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM exclusion_rules er 
                        WHERE er.type = 'entitlementsToBeExcluded'
                        AND er.application = sa.application
                        AND UPPER(er.value) = UPPER(sa.value)
                        AND UPPER(er.attribute) = UPPER(sa.attribute)
                    ) THEN 'Entitlement in COF Certification Exclusion Custom'
                    WHEN sa.application = 'Active Directory' AND EXISTS (
                        SELECT 1 FROM managed_attributes ma
                        WHERE ma.application_name = sa.application
                        AND ma.value = sa.value
                        AND ma.cof_ent_related_application IS NULL
                        AND (ma.requestable IS NULL OR ma.requestable = 0)
                    ) THEN 'AD Account not requestable and no related app'
                    WHEN sa.application = 'AWS Connect - Card_CDE-East' 
                        AND sa.attribute = 'SecurityProfileNames'
                        THEN 'AWS Connect Security Profile exclusion'
                    WHEN sa.application = 'AWS IAM User/Role Request' 
                        AND sa.native_identity IS NULL
                        THEN 'AWS IAM User/Role Request without awsAccountList'
                    WHEN EXISTS (
                        SELECT 1 FROM identityiq.spt_link l2
                        JOIN identityiq.spt_application a2 ON l2.application = a2.id
                        WHERE a2.name = sa.application
                        AND l2.native_identity = sa.native_identity
                        AND l2.iiq_disabled = '1'
                        AND EXISTS (
                            SELECT 1 FROM exclusion_rules er 
                            WHERE er.type = 'disabledAccountApplicationsToBeExcluded'
                            AND er.application = sa.application
                        )
                    ) THEN 'Disabled account entitlement'
                    ELSE 'No exclusion found'
                END
                
            ELSE 'No exclusion found'
        END AS reason
        
    FROM system_accounts sa
    LEFT JOIN actual_certifications ac 
        ON sa.eid = ac.eid
        AND COALESCE(sa.application, 'No Data') = COALESCE(ac.application, 'No Data')
        AND COALESCE(sa.native_identity, 'No Data') = COALESCE(ac.native_identity, 'No Data')
        AND COALESCE(sa.attribute, 'No Data') = COALESCE(ac.attribute, 'No Data')
        AND COALESCE(UPPER(sa.value), 'No Data') = COALESCE(UPPER(ac.value), 'No Data')
        AND sa.item_type = ac.item_type
)

-- Final output showing discrepancies
SELECT 
    item_type,
    eid,
    application,
    native_identity,
    attribute,
    value,
    reason,
    expected_result,
    actual_result,
    CASE 
        WHEN expected_result != actual_result 
            AND NOT (expected_result = 'Exclusion' AND actual_result = 'No Data in Cert')
        THEN 'DISCREPANCY'
        ELSE 'OK'
    END AS status
FROM validation_results
WHERE expected_result != actual_result 
    AND NOT (expected_result = 'Exclusion' AND actual_result = 'No Data in Cert')
ORDER BY eid, item_type, application, native_identity;