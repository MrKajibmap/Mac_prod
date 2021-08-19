%MACRO tech_log_event(mpmode=, mpprocess_nm=);
	%GLOBAL
		lmvProcessNm
		lmvProcessId
		lmvMode
	;
	%LET lmvMode = %upcase(&mpMODE.);
	%let lmvProcessNm = &mpPROCESS_NM.;
	%LET lmvPrevSasStatus=0;
	
	/* ETL */
	%LET lmvEventType=ETL;
	
	%LET lmvMetaUserName=&SYSUSERID.;
		
	%LET lmvProcessId=&SYSJOBID.; 
	
	%IF &lmvMode=START %THEN %DO;
		
		DATA _NULL_;
			PROCESS_NM="&lmvProcessNm.";
			PROCESS_ID="&lmvProcessId."; 
			PROCESS_STATUS="Job started";
			CALL SYMPUTX("lmvProcessNm",STRIP(PROCESS_NM));
			CALL SYMPUTX("lmvProcessId",PROCESS_ID); 
			CALL SYMPUTX("mvPROCESS_STATUS",STRIP(PROCESS_STATUS));
		RUN;
		
		PROC SQL NOPRINT;
			connect using etl_cfg;
			EXECUTE BY etl_cfg
				(
					INSERT INTO etl_cfg.cfg_log_event
					VALUES
						(
							DEFAULT,
							%STR(%')&lmvProcessNm.%STR(%'),
							&lmvProcessId,
							%STR(%')&mvPROCESS_STATUS.%STR(%'),
							'',
							NOW(),
							NULL,
							NULL,
							'',
							NULL,
							NULL,
							NULL,
							%STR(%')&lmvEventType.%STR(%'),
							%STR(%')&lmvMetaUserName.%STR(%')
						)
				);
		QUIT;
		
		%GLOBAL
			lmvProcessNm
			lmvProcessId
			mvLOG_EVENT_ID
			mvPRNT_EVENT_ID
		;
		
		%IF &SYSCC>4 %THEN %DO;
			%LET lmvPrevSasStatus=1;
			OPTIONS NOSYNTAXCHECK OBS=MAX;
		%END;
	
		PROC SQL NOPRINT;
			connect using etl_cfg;
			SELECT EVENT_ID INTO :mvLOG_EVENT_ID SEPARATED BY ''
			FROM CONNECTION TO etl_cfg
				(
					SELECT EVENT_ID
					FROM ETL_CFG.CFG_LOG_EVENT
					WHERE
						PROCESS_NM=%STR(%')&lmvProcessNm.%STR(%') AND
						PROCESS_ID=&lmvProcessId. AND
						END_DTTM ISNULL
				);
			;
		QUIT;
		
		%IF &lmvPrevSasStatus=1 %THEN %DO;
			OPTIONS SYNTAXCHECK OBS=0;
		%END;
		
		%LET mvPRNT_EVENT_ID=&mvLOG_EVENT_ID.;
	
	%END;
	%ELSE %IF &lmvMode.=END %THEN %DO;
		
		%IF &SYSCC>4 %THEN %DO;
			OPTIONS NOSYNTAXCHECK OBS=MAX;
		%END;
	
		%IF &SYSCC>4 %THEN %DO;
				%LET mvPROCESS_STATUS=Job finished with ERROR;
				%LET mvERR_TEXT=%SYSFUNC(COMPRESS(%SYSFUNC(TRANWRD(ERROR: %NRQUOTE(&SYSERRORTEXT.), %STR(,), %STR(:))), %STR(''"")));
				%IF %LENGTH(&mvERR_TEXT)>100 %THEN %DO;
					%LET mvERR_LEN=100;
				%END;
				%ELSE %DO;
					%LET mvERR_LEN=%LENGTH(&mvERR_TEXT);
				%END;
				%LET mvSTATUS_DESCRIPTION=%SUBSTR(%STR(&mvERR_TEXT), 1, &mvERR_LEN);
				%LET mvSTATUS_CD=1;
				%LET mvTGMessage = Process &lmvProcessNm. has been completed with ERRORS. Description: &mvSTATUS_DESCRIPTION.  ;
		%END;
		%ELSE %DO;
			%LET mvPROCESS_STATUS=Job finished SUCCESSFULLY;
			%LET mvSTATUS_DESCRIPTION=;
			%LET mvSTATUS_CD=0;
			%LET mvTGMessage = Process &lmvProcessNm. has been completed SUCCESSFULLY.;
		%END;
	
		PROC SQL NOPRINT;
			connect using etl_cfg;
			execute by etl_cfg(
					UPDATE etl_cfg.cfg_log_event
					SET
						process_status=%STR(%')&mvPROCESS_STATUS.%STR(%'),
						status_description=%STR(%')&mvSTATUS_DESCRIPTION.%STR(%'),
						status_cd=&mvSTATUS_CD.,
						end_dttm=now(),
						processed_dt=current_date
					WHERE
						process_nm=%STR(%')&lmvProcessNm.%STR(%') and
						process_id=&lmvProcessId. and
						end_dttm isnull AND
						event_id=&mvLOG_EVENT_ID.
				);
		QUIT;
		
		%local lmvProcDuration;
		
		PROC SQL NOPRINT;
			connect using etl_cfg;
			select exec_tm into :lmvProcDuration
			from connection to etl_cfg 
			(
				select
					cast(end_dttm as timestamp (0)) - cast(start_dttm as timestamp (0)) as exec_tm
				from etl_cfg.cfg_log_event
				where
					process_nm=%STR(%')&lmvProcessNm.%STR(%') and
					process_id=&lmvProcessId. and
					event_id=&mvLOG_EVENT_ID.
			);
		QUIT;
		
		%let lmvProcDuration = %trim(&lmvProcDuration.);
		/*Отправка сообщения в телеграмм */
		filename resp temp ;
		proc http 
			 method="POST"
			  url="https://api.telegram.org/bot&TG_BOT_TOKEN./sendMessage?chat_id=-1001360913796&text=&mvTGMessage.(duration: &lmvProcDuration.)"
			 ct="application/json"
			 out=resp; 
		run;
	
		%IF &SYSCC>4 %THEN %DO;
			
			OPTIONS SYNTAXCHECK OBS=0;
			
		%END;
			
	%END;

%MEND tech_log_event;