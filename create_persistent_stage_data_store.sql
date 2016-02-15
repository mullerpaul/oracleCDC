---- create the permanent objects
CREATE SEQUENCE stage_seq
START WITH 1000000;

CREATE TABLE fo_table_sample_stage
AS
SELECT 0 AS fo_table_sample_sk,
       a.*
  FROM fo_table_sample_extract_vw a
 WHERE 1=0;

ALTER TABLE fo_table_sample_stage    -- These columns are NN. Could do this in the create above.
MODIFY (fo_table_sample_sk NOT NULL,
        load_date          NOT NULL, 
        source_name        NOT NULL,
        business_key_hash  NOT NULL,
        attr_hash          NOT NULL);
        
ALTER TABLE fo_table_sample_stage    -- These columns only have to be 32 bytes long to hold MD5sums.  Could do this in the create above.
MODIFY (business_key_hash VARCHAR2(32),
        attr_hash         VARCHAR2(32));

ALTER TABLE fo_table_sample_stage    -- SK (synthetic key) is unique in this table.  This allows for ID collisions from multiple sources.
ADD CONSTRAINT fo_table_sample_stage_sk
PRIMARY KEY (fo_table_sample_sk);        

-- now simulate the initial load
INSERT INTO fo_table_sample_stage
SELECT stage_seq.nextval AS fo_table_sample_sk,
       job_id,
       fk_1,
       fk_2,
       attr_1,
       attr_2,
       attr_3,
       attr_4,
       attr_5,
       load_date,
       source_name,
       business_key_hash,
       attr_hash
  FROM fo_table_sample_extract_vw;

COMMIT;

-- this index helps incremental load performance.  Build it now after the load.
CREATE INDEX fo_table_sample_stage_idx01
ON fo_table_sample_stage (business_key_hash, attr_hash);

--- now lets do some DML to the FO table.
--- add 100 new records and update a few old ones.
INSERT INTO fo_table_sample
SELECT ROWNUM + 30000                                               AS job_id,
       TRUNC(dbms_random.value(low => 20000, HIGH => 50000))        AS fk_1,
       CASE 
         WHEN MOD(ROWNUM, 3) != 0
           THEN TRUNC(dbms_random.value(low => 20000, HIGH => 40000))
         ELSE
           NULL
       END                                                          AS fk_2,     
       ROUND(dbms_random.value(low => 1000, HIGH => 20000), 2)      AS attr_1,
       object_name || dbms_random.string(opt => 'x', len => 60)     AS attr_2,
       TRUNC(SYSDATE-30) + dbms_random.value(low => 10, HIGH => 30) AS attr_3,       
       sys_guid()                                                   AS attr_4,
       dbms_random.string(opt => 'x', len => 80)                    AS attr_5
  FROM all_objects
 WHERE ROWNUM < 101;

UPDATE fo_table_sample
   SET fk_2 = NULL
 WHERE job_id = 11000;
 
UPDATE fo_table_sample
   SET attr_1 = 100
 WHERE job_id = 12000;
 
UPDATE fo_table_sample
   SET attr_3 = SYSDATE
 WHERE job_id = 13000;

UPDATE fo_table_sample
   SET attr_5 = 'final version'
 WHERE job_id = 14000;
 
COMMIT;

--- Put a spleep in here so the incremental load has a different load time than the initial load.
--- Of course, we wont need this in the real system; but in this demo things finish too quickly
--- and the two loads have the same (or very close) load_dates
BEGIN
  dbms_lock.sleep(2);
END;
/
  
 
-- now run an "incremental" load.  This should only pick up new or modified records.
-- This insert runs (far) more quickly after I added the index fo_table_sample_stage_idx01 above.
INSERT INTO fo_table_sample_stage
SELECT stage_seq.nextval AS fo_table_sample_sk,
       job_id,
       fk_1,
       fk_2,
       attr_1,
       attr_2,
       attr_3,
       attr_4,
       attr_5,
       load_date,
       source_name,
       business_key_hash,
       attr_hash
  FROM fo_table_sample_extract_vw fo
 WHERE NOT EXISTS 
   (SELECT NULL
      FROM fo_table_sample_stage s
     WHERE s.business_key_hash = fo.business_key_hash 
       AND s.attr_hash = fo.attr_hash);
 
COMMIT; 

--- we can build a view to see "current version" column
CREATE VIEW fo_table_sample_stage_curr_vw
AS
SELECT fo_table_sample_sk,
       job_id,
       fk_1,
       fk_2,
       attr_1,
       attr_2,
       attr_3,
       attr_4,
       attr_5,
       load_date,
       source_name,
       business_key_hash,
       attr_hash,
       CASE WHEN load_date_rank = 1 THEN 'Y' ELSE 'N' END AS current_version
  FROM (SELECT fo_table_sample_sk,
               job_id,
               fk_1,
               fk_2,
               attr_1,
               attr_2,
               attr_3,
               attr_4,
               attr_5,
               load_date,
               source_name,
               business_key_hash,
               attr_hash,
               RANK() OVER (PARTITION BY business_key_hash ORDER BY load_date DESC) AS load_date_rank  --could use DENSE_RANK or MAX functions to do the same thing
          FROM fo_table_sample_stage);


--- we can also build a view to see version current at any given point in time!  (p.i.t.)
--- I hardcoded a datetime literal in the view text.  In real life, we'd probably use a  
--- session context in the view to get the desired value in.
CREATE OR REPLACE VIEW fo_table_sample_stage_pit_vw
AS
SELECT fo_table_sample_sk,
       job_id,
       fk_1,
       fk_2,
       attr_1,
       attr_2,
       attr_3,
       attr_4,
       attr_5,
       load_date,
       source_name,
       business_key_hash,
       attr_hash,
       CASE WHEN load_date_rank = 1 THEN 'Y' ELSE 'N' END AS current_version_pit
  FROM (SELECT fo_table_sample_sk,
               job_id,
               fk_1,
               fk_2,
               attr_1,
               attr_2,
               attr_3,
               attr_4,
               attr_5,
               load_date,
               source_name,
               business_key_hash,
               attr_hash,
               RANK() OVER (PARTITION BY business_key_hash ORDER BY load_date DESC) AS load_date_rank  --could use DENSE_RANK or MAX functions to do the same thing
          FROM fo_table_sample_stage
         WHERE load_date <= to_date('2016-Feb-15 16:00','YYYY-Mon-DD hh24:mi'));


SELECT * FROM fo_table_sample_stage_curr_vw
 WHERE job_id = 14000;

SELECT * FROM fo_table_sample_stage_pit_vw
 WHERE job_id = 14000;
