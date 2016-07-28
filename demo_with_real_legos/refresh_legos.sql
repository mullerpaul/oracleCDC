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

--check contents of both lego tables
SELECT * FROM operationalstore.person_cdc_demo_iqp1;
SELECT * FROM operationalstore.person_change_capture;
