---- cleanup first
DROP VIEW fo_table_sample_extract_vw;
DROP TABLE fo_table_sample;
DROP TABLE fo_table_sample_stage;
DROP SEQUENCE stage_seq;

---- create objects
CREATE TABLE fo_table_sample 
 (job_id NUMBER NOT NULL,
  fk_1   NUMBER,
  fk_2   NUMBER,
  attr_1 NUMBER,
  attr_2 VARCHAR2(100) NOT NULL,
  attr_3 DATE,
  attr_4 RAW(16),
  attr_5 VARCHAR2(100));

-- insert 5000 rows
INSERT INTO fo_table_sample
SELECT ROWNUM + 10000                                               AS job_id,
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
 WHERE ROWNUM < 5001;

COMMIT;

-- add PK to "FO" table.
ALTER TABLE fo_table_sample
ADD CONSTRAINT fo_table_sample_pk
PRIMARY KEY (job_id);

---- Build a view on the table.  This view will be used by the extract process.
---- It contains all the columns from the source, plus four additional columns.
CREATE OR REPLACE VIEW fo_table_sample_extract_vw
AS
SELECT a.*,
       SYSDATE   AS load_date,
       'FO'      AS source_name,
       rawtohex(dbms_crypto.hash(to_clob(job_id),
                                 2)    -- 2 is the PLS_INTEGER code for dbms_crypto.HASH_MD5.
               ) AS business_key_hash,
       rawtohex(dbms_crypto.hash(to_clob(to_char(fk_1) || '^' ||   -- need column separators
                                         to_char(fk_2) || '^' ||
                                         to_char(attr_1) || '^' ||
                                         attr_2 || '^' ||
                                         to_char(attr_3,'YYYY-Mon-DD-hh24:mi:ss') || '^' ||  -- can't have spaces in our concat string
                                         RAWTOHEX(attr_4) || '^' ||
                                         attr_5),
                                 2)
               ) AS attr_hash
  FROM fo_table_sample a;

