%macro fmk_load_etl_ia_pg(mpResource=);

	%local lmvResource
			lmvResId
			lmvResNm
			lmvDateTime
	;
	%let lmvResource = %lowcase(&mpResource.);
	%tech_log_event(mpMODE=START, mpPROCESS_NM=fmk_load_etl_ia_&lmvResource.);
	
	/* В процедуре PLPGSQL закрываются "старые" версии в таблице cfg_resource_registry, текущая выгрузка выставляется в 'P', при успешном завершении - 'L'
		, при ошибке - статус не изменится с 'P', поэтому он обрабатывается ниже*/
		
	/* получаем имя и ID ресурса */
	proc sql noprint;
		select 	resource_id
				,resource_nm 
		into 	:lmvResId
				,:lmvResNm
		from etl_cfg.cfg_resource
		where lowcase(resource_nm) = "&lmvResource."
	;
	quit;

	/* вызов процедуры на стороне PG */
	proc sql noprint;
		connect to POSTGRES as dwh (server="&CUR_API_URL." port=5452 database="postgres" conopts="SSLmode=prefer" dbmax_text=32767 user=sas password="{SAS002}1D57933958C580064BD3DCA81A33DFB2");
		execute by dwh
		(
			CALL etl_ia.load_inc(%str(%')etl_ia.&lmvResource.%str(%'));
		);
		disconnect from dwh;
	quit;
	
	%fmk_load_etl_ia_error_check(mpResId = &lmvResId. ,mpResource = &lmvResource.);		
	
	%tech_log_event(mpMODE=END, mpPROCESS_NM=fmk_load_etl_ia_&lmvResource.);
	
%mend fmk_load_etl_ia_pg;