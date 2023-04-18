SELECT TGT_V_SET.NAME   AS "Mig Name",
  SRC_V_SET.NAME        AS "Pre-Mig Name",
  CASE
    WHEN NVL(TGT_V_SET.NAME,0) = NVL(SRC_V_SET.NAME,0)
    THEN 'No'
    ELSE 'Yes'
  END                     AS "Name Diff",
  TGT_V_SET.version     AS "Mig version",
  SRC_V_SET.version     AS "Pre-Mig version",
  CASE
    WHEN NVL(TGT_V_SET.version,0) = NVL(SRC_V_SET.version,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Version Diff",
  TGT_V_SET.description AS "Mig Description",
  SRC_V_SET.description AS "Pre-Mig Description",
  CASE
    WHEN NVL(TGT_V_SET.description,0) = NVL(SRC_V_SET.description,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Description Diff",
  TGT_S_SET.name        AS "Mig Sev Set Name",
  SRC_S_SET.name        AS "Pre-Mig Sev Set Name",
  CASE
    WHEN NVL(TGT_S_SET.NAME,0) = NVL(SRC_S_SET.NAME,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Severity Set Name Diff",
  TGT_V_SET.status      AS "Mig Status",
  SRC_V_SET.status      AS "Pre-Mig Status",
  CASE
    WHEN NVL(TGT_V_SET.status,0) = NVL(SRC_V_SET.status,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Status Diff",
  TGT_R.display_name      AS "Mig Selected Ruleset",
  SRC_R.display_name      AS "Pre-Mig Selected Ruleset",
  CASE
    WHEN NVL(TGT_R.ruleset_id,0) = NVL(SRC_R.ruleset_id,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Selected Ruleset Diff"
FROM IVD_VALIDATION_SET TGT_V_SET
INNER JOIN IVD_SEVERITY_SET TGT_S_SET
ON (TGT_V_SET.severity_set_id = TGT_S_SET.severity_set_id)
INNER JOIN IVD_VALIDATION_SET_CONFIG TGT_V_CON
ON (TGT_V_SET.VALISET_ID = TGT_V_CON.VALISET_ID)
INNER JOIN IVD_RULESET TGT_R
ON (TGT_R.ruleset_id = TGT_V_CON.ruleset_id)
LEFT OUTER JOIN IVD_VALIDATION_SET_SRC SRC_V_SET
ON (SRC_V_SET.VALISET_ID = TGT_V_SET.VALISET_ID)
LEFT OUTER JOIN IVD_SEVERITY_SET_SRC SRC_S_SET
ON (SRC_S_SET.severity_set_id = SRC_V_SET.severity_set_id
AND SRC_S_SET.SEVERITY_SET_ID = TGT_V_SET.SEVERITY_SET_ID)
LEFT OUTER JOIN IVD_VALIDATION_SET_CONFIG_SRC SRC_V_CON
ON (SRC_V_SET.VALISET_ID               = SRC_V_SET.VALISET_ID
AND SRC_V_CON.VALIDATION_SET_CONFIG_ID = TGT_V_CON.VALIDATION_SET_CONFIG_ID)
LEFT OUTER JOIN IVD_RULESET_SRC SRC_R
ON (SRC_V_CON.RULESET_ID = SRC_R.RULESET_ID
AND SRC_R.RULESET_ID     = TGT_R.RULESET_ID);