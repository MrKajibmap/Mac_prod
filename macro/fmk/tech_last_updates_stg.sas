%macro tech_last_updates_stg(mpTABLE_NM=, mpDEEP_LVL=5);
	%local
		lmvTable_nm
		lmvDeep_level;
	
	%global
		LAST_UPDATES_STG;
		
	%let lmvTable_nm = &mpTABLE_NM.;
	%let lmvDeep_level = &mpDEEP_LVL.;
	
	PROC SQL NOPRINT OUTOBS=&lmvDeep_level.;
		SELECT 
			DISTINCT etl_extract_id FORMAT=8. as extr_id INTO :LAST_UPDATES_STG separated by ','
		FROM etl_stg.&lmvTable_nm.
		ORDER BY extr_id DESC;
	QUIT;
%mend tech_last_updates_stg;