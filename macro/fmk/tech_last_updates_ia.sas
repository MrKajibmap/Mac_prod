%macro tech_last_updates_ia(mpTABLE_NM=, mpDEEP_LVL=5);
	%local
		lmvTable_nm
		lmvDeep_level;
		
	%global
		LAST_UPDATES_IA_FROM
		LAST_UPDATES_IA_TO;
		
	%let lmvTable_nm = &mpTABLE_NM.;
	%let lmvDeep_level = &mpDEEP_LVL.;
	
	PROC SQL NOPRINT OUTOBS=&lmvDeep_level.;
		SELECT DISTINCT
			valid_from_dttm,
			valid_to_dttm
		INTO 
			:LAST_UPDATES_IA_FROM separated by ' ',
			:LAST_UPDATES_IA_TO separated by ' '
		FROM
			ETL_IA.&lmvTable_nm.
		WHERE valid_to_dttm ^= &ETL_SCD_FUTURE_DTTM.
		ORDER BY
			valid_from_dttm DESC,
			valid_to_dttm DESC
		;
	QUIT;

%mend tech_last_updates_ia;