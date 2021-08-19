%macro tech_hold_only_top_stg(mpTABLE_NM=, mpDEEP_LVL=5);
	%local
		lmvTable_nm
		lmvDeep_level;

	%let lmvTable_nm = &mpTABLE_NM.;
	%let lmvDeep_level = &mpDEEP_LVL;

	%tech_last_updates_stg(mpTABLE_NM=&lmvTable_nm., mpDEEP_LVL=&lmvDeep_level.);

	PROC SQL NOPRINT;
		connect using etl_stg;
		EXECUTE BY etl_stg
			(
				DELETE FROM etl_stg.&lmvTable_nm.
				WHERE
					etl_extract_id NOT IN (&LAST_UPDATES_STG.)
			)
	QUIT;
%mend tech_hold_only_top_stg;