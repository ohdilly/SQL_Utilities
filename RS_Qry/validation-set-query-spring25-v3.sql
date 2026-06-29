WITH i AS (
    SELECT
        CASE
            WHEN n.show_set = 1 THEN
                n.base_set_name
            WHEN n.show_set = 0 THEN
                'Default configuration'
            ELSE
                '-'
        END base_set_name,
        t.valiset_id,
        t.ruleset_id,
        t.configset_base_set_num
    FROM
        validata.ivd_validation_set_config_set t
        LEFT OUTER JOIN validata.vd_evo_base_set               n ON ( n.base_set_num = t.configset_base_set_num
                                                        AND n.version_status = 'LATEST' )
)
SELECT DISTINCT
    base_set.base_set_num                                AS "Validation Set ID",
    base_set.base_set_name                               AS "Validation_SetName",
    base_set.description,
    base_set.status,
    base_set.effective_start_date,
    base_set.effective_end_date,
    base_set.base_set_type,
    (
        SELECT DISTINCT
            i.base_set_name
        FROM
            i
        WHERE
                i.ruleset_id = a.ruleset_id
            AND ROWNUM = 1
    )                                                    inputset,
    decode(vs.stamp_dependent_error_flag, 1, 'Y', 'N')   AS "Stamp dependent validations",
    decode(vs.mpm_error, 1, 'Y', 'N')                    AS "Check for missing products",
    decode(vs.service_provider_id_handling, 1, 'Y', 'N') AS "Use submitted Service Provider ID even when invalid",
    decode(vs.skip_pn_cn_validations, 1, 'Y', 'N')       AS "Skip validations for PN/CN data levels",
    vs.dup_validation_run_order                          AS "Duplicates validation run order",
    vs.cdp_validation_run_order                          AS "Debit/Credit pair validation run order",
    a.error_code,
    a.error_sub_category,
    (
        SELECT
            z.description
        FROM
            validata.ivd_severity_level z
        WHERE
            z.severity_level_code = sr.severity_level_code
    )                                                    AS "Severity Level Code",
    decode(sr.use, 1, 'Y', 'N')                          AS "Use"
FROM
    validata.ivd_error_def           a,
    validata.ivd_severity_set_record sr,
    validata.ivd_validation_set      vs,
    validata.vd_evo_base_set_slice   set_sl,
    validata.vd_evo_base_set         base_set
WHERE
        1 = 1
    AND a.error_code = sr.error_code
    AND vs.severity_set_id = sr.severity_set_id
    AND vs.valiset_id = set_sl.config_id
    AND set_sl.base_set_num = base_set.base_set_num
    AND sr.use = 1
    AND set_sl.is_latest_accessed = 1
    AND base_set.version_status = 'LATEST'
    AND base_set.base_set_type IN ( 'ValidationSetCG', 'ValidationSet' )
ORDER BY
    1;