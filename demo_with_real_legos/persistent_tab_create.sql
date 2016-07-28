CREATE TABLE person_change_capture
AS 
SELECT 0 AS person_change_capture_sk,
       a.*,
       SYSDATE AS load_date,
       'USPROD' AS source_name,
       ora_hash('x') AS source_key_hash,
       ora_hash('x') AS source_attr_hash
  FROM person_iqp1 a  --person lego synonym
 WHERE 1=0;
         
CREATE INDEX person_change_capture_ix01
ON person_change_capture
(source_key_hash, source_attr_hash)
/

CREATE SEQUENCE person_change_capture_seq;

CREATE OR REPLACE PROCEDURE load_person_change_capture(pi_source IN VARCHAR2)
AS
BEGIN    
  INSERT INTO person_change_capture
  SELECT person_change_capture_seq.nextval AS person_change_capture_sk,
         l.*
    FROM person_cdc_demo_iqp l
   WHERE NOT EXISTS
       (SELECT NULL
          FROM person_change_capture s
         WHERE s.source_key_hash = l.source_key_hash
           AND s.source_attr_hash = l.attribute_hash);
           
  COMMIT;
END;
/


grant select on person_change_capture to public;
grant execute on load_person_change_capture to public;

INSERT INTO lego_refresh
 (object_name,source_name,refresh_method,refresh_schedule,
  refresh_group,refresh_dependency_order,refresh_on_or_after_time,
  refresh_procedure_name)
VALUES
  ('LEGO_PERSON_CDC_DEMO_PERSIST','USPROD','PROCEDURE ONLY','DAILY',30,2, TRUNC(SYSDATE) + 8/24,
  'load_person_change_capture');

COMMIT;
  
    
