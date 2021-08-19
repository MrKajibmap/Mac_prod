%macro tech_hold_only_top_ia(mpTABLE_NM=, mpDEEP_LVL=5);
	%local
		lmvTable_nm
		lmvDeep_level;

	%let lmvTable_nm = %upcase(&mpTABLE_NM.);
	%let lmvDeep_level = &mpDEEP_LVL.;

	%tech_last_updates_ia(mpTABLE_NM=&lmvTable_nm., mpDEEP_LVL=&lmvDeep_level.);

	PROC SQL NOPRINT;
		DELETE FROM ETL_IA.&lmvTable_nm.
		WHERE
			NOT(
				valid_to_dttm = &ETL_SCD_FUTURE_DTTM.
				%do i=1 %to &lmvDeep_level.;
					%let lmvFromDttm = %scan(&LAST_UPDATES_IA_FROM., &i., %str( ));
					%let lmvToDttm = %scan(&LAST_UPDATES_IA_TO., &i., %str( ));
					
					OR (valid_from_dttm = &lmvFromDttm. AND valid_to_dttm = &lmvToDttm.)
				%end;
			);
	QUIT;
%mend tech_hold_only_top_ia;