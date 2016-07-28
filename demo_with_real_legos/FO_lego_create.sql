INSERT INTO lego_refresh_group 
 (refresh_group, run_in_initial_load, comments)
VALUES
 (30, 'N', 'test group - can be dropped!');

INSERT INTO lego_refresh 
 (object_name,source_name,refresh_method,refresh_schedule,
  refresh_group,refresh_dependency_order,refresh_on_or_after_time, 
  refresh_sql,refresh_object_name_1,refresh_object_name_2,synonym_name)
VALUES
  ('LEGO_PERSON_CDC_DEMO','USPROD','SQL TOGGLE','DAILY',30,1, TRUNC(SYSDATE) + 8/24,
  'x','PERSON_CDC_DEMO_IQP1','PERSON_CDC_DEMO_IQP2','PERSON_CDC_DEMO_IQP');

UPDATE lego_refresh
   SET refresh_sql = q'{SELECT bus_org_id, person_id, candidate_id, 
       user_name, last_name, first_name, middle_name, display_name,
       title, contact_info_id, udf_collection_id, candidate_udf_collection_id,
       do_not_rehire_flag,
       sysdate as load_date,
       'USPROD' as source_name,
       ora_hash(person_id) as source_key_hash,
       ora_hash(to_char(bus_org_id) || '^' ||
                to_char(candidate_id) || '^' ||
                user_name || '^' ||
                last_name || '^' ||
                first_name || '^' ||
                middle_name || '^' ||
                display_name || '^' ||
                title || '^' ||
                to_char(contact_info_id) || '^' ||
                to_char(udf_collection_id) || '^' ||
                to_char(candidate_udf_collection_id) || '^' ||
                do_not_rehire_flag) as attribute_hash
 FROM (
   SELECT /*+ PARALLEL(2,2) */  
          DISTINCT NVL (p.business_organization_fk, -1) bus_org_id,
          p.person_id,
          c.candidate_id,
          u.user_name,
          CASE 
            WHEN INSTR (p.last_name, '&#') > 0 THEN CAST (REGEXP_REPLACE (p.last_name, '([&#[:digit:]]*)+;', ('\1')) AS VARCHAR2 (100)) 
            ELSE CAST (p.last_name AS VARCHAR2 (100)) 
          END last_name,
          CASE 
            WHEN INSTR (p.first_name, '&#') > 0 THEN CAST (REGEXP_REPLACE (p.first_name, '([&#[:digit:]]*)+;', ('\1')) AS VARCHAR2 (100)) 
            ELSE CAST (p.first_name AS VARCHAR2 (100)) 
          END first_name,
          p.middle_name,
          CASE 
            WHEN INSTR (p.last_name, '&#') > 0 OR INSTR (p.first_name, '&#') > 0 
              THEN CAST (REGEXP_REPLACE (p.last_name, '([&#[:digit:]]*)+;', ('\1')) AS VARCHAR2 (100)) || ', ' || 
                   CAST (REGEXP_REPLACE (p.first_name, '([&#[:digit:]]*)+;', ('\1')) AS VARCHAR2 (100)) || 
                   CASE WHEN p.middle_name IS NOT NULL THEN ' ' || UPPER (SUBSTR (p.middle_name, 0, 1)) || '.' END 
            ELSE CAST (p.last_name AS VARCHAR2 (100)) || ', ' || CAST (p.first_name AS VARCHAR2 (100)) || 
                 CASE WHEN p.middle_name IS NOT NULL THEN ' ' || UPPER (SUBSTR (p.middle_name, 0, 1)) || '.' END 
          END AS display_name,
          p.title,
          contact_info_fk contact_info_id,
          p.udf_collection_fk udf_collection_id,
          c.udf_collection_fk candidate_udf_collection_id,
          CAST (CASE WHEN dnr.TRACK_RES_OVER_SUPPLIERS = 0 AND c.candidate_id = dnr.candidate_id 
                       THEN 'Y' 
                     WHEN dnr.TRACK_RES_OVER_SUPPLIERS = 1 AND c.candidate_id = dnr.candidate_id AND NVL (c.fed_id, 'x') = NVL (dnr.fed_id, 'x') AND NVL (c.fed_id_type_fk, -1) = NVL (dnr.fed_id_type_fk, -1) 
                       THEN 'Y' 
                     ELSE 'N' END AS CHAR) do_not_rehire_flag
     FROM person@db_link_name    AS OF SCN source_db_SCN p,
          iq_user@db_link_name   AS OF SCN source_db_SCN u,
          candidate@db_link_name AS OF SCN source_db_SCN c,
          (SELECT DISTINCT c.candidate_id,
                           bf.track_res_over_suppliers,
                           c.fed_id,
                           c.fed_id_type_fk
             FROM candidate@db_link_name                  AS OF SCN source_db_SCN c,
                  buyer_firm@db_link_name                 AS OF SCN source_db_SCN bf,
                  assignment_continuity@db_link_name      AS OF SCN source_db_SCN ac,
                  firm_role@db_link_name                  AS OF SCN source_db_SCN fr,
                  business_organization@db_link_name      AS OF SCN source_db_SCN bo,
                  cand_ineligible_for_rehire@db_link_name AS OF SCN source_db_SCN cifr
            WHERE bf.firm_id         = ac.owning_buyer_firm_fk
              AND c.candidate_id     = ac.candidate_fk
              AND fr.firm_id         = ac.owning_buyer_firm_fk
              AND fr.business_org_fk = bo.business_organization_id
              AND c.candidate_id     = cifr.candidate_fk
              AND bo.enterprise_fk   = cifr.enterprise_fk) dnr
    WHERE p.person_id    = u.person_fk(+)
      AND p.person_id    = c.person_fk(+)
      AND c.candidate_id = dnr.candidate_id(+)}'
 WHERE object_name = 'LEGO_PERSON_CDC_DEMO' 
   AND source_name = 'USPROD';
   
COMMIT;

