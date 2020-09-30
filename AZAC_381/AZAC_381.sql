

UPDATE validata.ivd_source sc
    SET
        sc.source_id = (
            SELECT
                az.new_source_id
            FROM
                validata.az_ivd_source_conv az
            WHERE
                az.old_source_id = sc.source_id
                and az.NEW_SOURCE_ID is not null
        )
    where exists (select 1 from 
    validata.az_ivd_source_conv
    where old_source_id  = sc.source_id
    and new_source_id is not null);