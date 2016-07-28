-- log into the FO database and change some people's names!
-- as IQPROD, run these
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000029;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000036;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000040;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000037;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000045;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000046;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000041;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000021;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000004;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000047;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000019;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000049;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000007;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000020;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000024;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000006;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000032;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000002;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000050;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000035;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000028;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000011;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000016;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000039;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000043;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000025;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000034;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000012;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000048;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000033;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000031;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000027;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000022;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000030;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000000;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000014;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000017;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000005;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000013;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000026;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000018;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000010;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000003;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000015;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000023;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000008;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000009;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000044;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000001;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000042;
UPDATE person SET last_name = 'Obama', first_name = 'Barack' where person_id = 11000038;


commit;


-- refresh both legos
DECLARE
  lv_g operationalstore.lego_group_list_type := operationalstore.lego_group_list_type(30);
BEGIN
  operationalstore.lego_refresh_mgr_pkg.refresh(pi_refresh_group => lv_g);
--  operationalstore.lego_refresh_mgr_pkg.refresh(pi_refresh_object => 'LEGO_PERSON_CDC_DEMO', pi_refresh_source => 'USPROD');
--  operationalstore.lego_refresh_mgr_pkg.refresh(pi_refresh_object => 'LEGO_PERSON_CDC_DEMO_PERSIST', pi_refresh_source => 'USPROD');
END;
/

-- check statuses
SELECT * FROM operationalstore.lego_Refresh_history 
 WHERE 1=1
   AND job_runtime > TRUNC(SYSDATE)
   AND refresh_group = 30
 ORDER BY 2,6,7;

-- check if changes were captured
-- first see both load times in the perm. table
SELECT load_date, COUNT(*) FROM operationalstore.person_change_capture GROUP BY load_Date;

--


