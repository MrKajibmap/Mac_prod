%macro tech_004_hold_top_stg;
	%tech_log_event(mpMode=START, mpProcess_Nm=tech_hold_top_stg);				
	%local lmvStgTablesList;
	
	proc sql noprint;
		SELECT
			resource_nm into :lmvStgTablesList separated by ' '
		FROM etl_cfg.cfg_resource
		WHERE module_nm = 'etl_stg';
	quit;
	
	%let lmvTableCnt = %sysfunc(countw(&lmvStgTablesList.));
	
	%do i=1 %to &lmvTableCnt.;
		%let lmvTableNm = %scan(&lmvStgTablesList., &i., %str( ));
		
		%tech_hold_only_top_stg(mpTABLE_NM=&lmvTableNm., mpDEEP_LVL=7);
	%end;
	%tech_log_event(mpMode=END, mpProcess_Nm=tech_hold_top_stg);				
%mend tech_004_hold_top_stg;
%tech_004_hold_top_stg;