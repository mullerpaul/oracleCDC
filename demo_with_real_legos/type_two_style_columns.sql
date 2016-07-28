SELECT a.person_id, a.display_name, a.title, source_name, source_key_hash, source_attr_hash,
--       RANK() OVER (PARTITION BY source_key_hash ORDER BY load_date DESC) AS load_date_rank,
       load_date AS effective_date,
       LAG(load_date, 1, NULL) OVER (PARTITION BY source_key_hash ORDER BY load_date DESC) AS terminate_Date
  FROM operationalstore.person_change_capture a
 WHERE a.person_id IN (11000000, 10000000); 
