/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для подготовки csv файлов под DP
*	
*
*  ПАРАМЕТРЫ:
*	  mpInput       		- Наименование входной таблицы для экспорта
*	  mpTHREAD_CNT			- Количество потоков (на сколько частей будет биться таблица при экспорте для устранения проблемы с нехваткой памяти)
*     mpPath				- Наименование директории, в которую будет производиться экспорт
*
******************************************************************
*  Использует: 
*	  нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %dp_export_csv(mpInput=DM_ABT.PLAN_UPT_DAY
				, mpTHREAD_CNT=30
				, mpPath=/data/tmp/);
*
****************************************************************************
*  20-09-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro dp_export_csv(mpInput=DM_ABT.PLAN_UPT_DAY, mpTHREAD_CNT=10, mpPath=/data/tmp/);
		%let mvTHREAD_CNT = &mpTHREAD_CNT.;
		%let mvInput=&mpInput;
		%let mvTargetName = %scan(&mvInput.,2,%str(.));
		%let mvPath = &mpPath.;
		
		proc casutil;
			DROPTABLE CASDATA="gen_part" INCASLIB="casuser" QUIET;
		run;
		
		/* CREATE MAIN BUFFER TABLE WITH PARAMETERS FOR THREADS */
		data casuser.gen_part(promote=yes) / single=yes;
			set &mvInput.;
			row_id = _n_ ;
		run;

		/* CREATE BUFFERS TABLES WITH PARAMETERS FOR EACH THREAD */
		%MACRO mGENERATE_BATCHES;
			%GLOBAL mvBUFFER_ROW_CNT
					mvROWS_FOR_THREAD
					;
			PROC SQL NOPRINT;
				SELECT COUNT(*) AS CNT, ceil(COUNT(*)/&mvTHREAD_CNT.) AS ROWS_FOR_THREAD INTO :mvBUFFER_ROW_CNT, :mvROWS_FOR_THREAD
				FROM casuser.gen_part
				;
			QUIT;
		
			%PUT &=mvBUFFER_ROW_CNT;
			%PUT &=mvROWS_FOR_THREAD;
		
			/* CALC COUNT OF ROWS WITH PARAMETERS FOR SENDING TO THREADS */
			%DO I = 1 %TO &mvTHREAD_CNT.;
				%IF &I. = 1 %THEN %DO;
					%LET mvFIRST_OBS =1;
				%END;
				%ELSE %DO;
					%LET mvFIRST_OBS =%SYSEVALF(&I. * &mvROWS_FOR_THREAD. - &mvROWS_FOR_THREAD. +1);
				%END;
				/* DELETE BATCH TABLE FOR THREAD*/
				proc casutil; 
					DROPTABLE CASDATA="PART_TABLE_&I." INCASLIB="public" QUIET;
				run;
				/* добавить сюда  разбивку - таблица - номер потока + к нему первый элемент и последний. и дальше уже рассасывать эти куски внутри параллеи */
				/*CREATE BATCH_TABLE FOR THREADS */
				DATA public.PART_TABLE_&I.(promote=yes);
					firstobs = &mvFIRST_OBS.;
					obs = &mvFIRST_OBS.+&mvROWS_FOR_THREAD. -1;
					id = &I.;
				RUN;
			%END;
			
		%MEND mGENERATE_BATCHES;
		
		%mGENERATE_BATCHES;

		%put &=mvBUFFER_ROW_CNT;
		%put &=mvROWS_FOR_THREAD;	
		
		%macro signon;
			
			/* SIGNON THREAD_SESSIONS AND PUT PARAMETERS INTO */
			%DO mvTHREAD_NUM = 1 %TO &mvTHREAD_CNT.;
				SIGNON T_&mvTHREAD_NUM. sascmd="!sascmd" WAIT = YES;
				/* ПЕРЕДАЧА ПАРАМЕТРОВ В КАЖДУЮ СЕССИЮ */
				%SYSLPUT mvTHREAD_NUM = &mvTHREAD_NUM. / REMOTE = T_&mvTHREAD_NUM.;
				%SYSLPUT mpInput = &mvTargetName. / REMOTE = T_&mvTHREAD_NUM.;	
				%SYSLPUT mpPath = &mvPath. / REMOTE = T_&mvTHREAD_NUM.;
			%END;

		%mend signon;
		%signon;
		
		%macro main_run;
		
			/* CALC SCORING WITH THREADS */
			%DO mvTHREAD_NUM = 1 %TO &mvTHREAD_CNT.;
				RSUBMIT T_&mvTHREAD_NUM. WAIT=NO CMACVAR=T_&mvTHREAD_NUM.;
					options notes symbolgen mlogic mprint casdatalimit=all;
					/* PROC PRINTTO LOG="/data/logs/create_part_csv_&mvTHREAD_NUM..txt" NEW;
					RUN; */
			
					CAS T_&mvTHREAD_NUM. HOST="sasdevinf.ru-central1.internal" PORT=5570;
					caslib _all_ assign;
					%include "/opt/sas/mcd_config/config/initialize_global.sas";
	
					/* MAIN CODE FOR EACH THREAD */
					%MACRO THREAD_MAIN;
						DATA _NULL_;
							SET public.PART_TABLE_&mvTHREAD_NUM.(where=(id=&mvTHREAD_NUM.));
							CALL SYMPUTX("firstobs", firstobs);
							CALL SYMPUTX("obs", obs);
						RUN;
						
						DATA casuser.&mpInput._&mvTHREAD_NUM.(replace=yes datalimit=all);
							SET casuser.gen_part(FIRSTOBS = &firstobs. OBS = &obs. drop=row_id);
						RUN;
										
						%if %sysfunc(exist(casuser.&mpInput._&mvTHREAD_NUM.)) %then %do;
							proc export data=casuser.&mpInput._&mvTHREAD_NUM.(datalimit=all)
										outfile="&mpPath.&mpInput._&mvTHREAD_NUM..csv"
										dbms=dlm
										replace
										;
										delimiter='|'
										;
									%if &mvTHREAD_NUM. ne 1 %then %do;
										putnames=no;
									%end;
							run;
						%end;
						%else %do;
							%put "WARNING: Input table &mpResourceNm. does not exist. Please verify input parameters.";
							%return;
						%end;

					/*	PROC PRINTTO;
						RUN; */

					%MEND THREAD_MAIN;

					%THREAD_MAIN;

					ENDRSUBMIT;
					
			%END;	

		%mend main_run;
		%main_run;

		%macro end_thread/*(mpOut=&mpOut.)*/;
	
			%local lmvTabNmAbt lmvLibrefAbt lmvLibrefOut lmvTabNmOut;
			%member_names (mpTable=&mpAbt, mpLibrefNameKey=lmvLibrefAbt, mpMemberNameKey=lmvTabNmAbt);
			%member_names (mpTable=&mpOut, mpLibrefNameKey=lmvLibrefOut, mpMemberNameKey=lmvTabNmOut);
			
			/* CHECKING THREAD COMPLETION */
			%LET mvIN_PROCESS = 1;
			%DO %UNTIL (&mvIN_PROCESS=0);
				%DO CHECK_ITER = 1 %TO &mvTHREAD_CNT.;
					%IF &CHECK_ITER. = 1 %THEN %DO;
						%LET mvIN_PROCESS = &&T_&CHECK_ITER.;
					%END;
					%ELSE %DO;
						%LET mvIN_PROCESS = %SYSEVALF(&mvIN_PROCESS. + &&T_&CHECK_ITER.);
						%LET mvSLEEP=%SYSFUNC(SLEEP(1, 1));
						%PUT TRYING TO GO NEXT STEP;
					%END;
				%END;
			%END;
			
			%macro dp_append_csv(mpPath=/data/tmp,
					mpTargetTableNm=PLAN_UPT_DAY
					);
	
				%local lmvPATH lmvTargetTableNm;
				%let lmvTargetTableNm = &mpTargetTableNm.;
				%let lmvPATH=&mpPath.;
				%put &=lmvTargetTableNm;
				%put &=lmvPATH;
				
				/* Удаление таргет-файла */
				DATA _NULL_;
					CALL SYSTEM("rm &lmvTargetTableNm..csv &"); 
				RUN;
				%LET mvSLEEP=%SYSFUNC(SLEEP(5, 1));
				
				/* Соединение всех кусков в таргет-файл (в фоновом режиме, поэтому требуется SLEEP, 
				чтобы операция завершилась до момента удаления файлов*/
				
				%let part_tables_row = ;
				%do i = 1 %to 30;
					%let part_tables_row = &part_tables_row. &lmvTargetTableNm._&I..csv;
				%end;
				%put &=part_tables_row;
						
				DATA _NULL_;
					CALL SYSTEM("cd &lmvPATH.");
					CALL SYSTEM("cat &part_tables_row. > &lmvTargetTableNm..csv &");
				RUN;

				%LET mvSLEEP=%SYSFUNC(SLEEP(10, 1));
				
				/* Удаление промежуточных кусков  */
				DATA _NULL_;
					/* CALL SYSTEM("rm &lmvTargetTableNm._* &"); */
					 CALL SYSTEM("rm &part_tables_row. &");
				RUN; 

			%mend dp_append_csv;	
			%dp_append_csv(mpPath=&mvPath., mpTargetTableNm=&mvTargetName.);
			
			proc casutil; 
					DROPTABLE CASDATA="GEN_PART" INCASLIB="casuser" QUIET;
			run;
			
			/*SIGNOFF THREADS AND DROP BATCH TABLES*/
			%DO mvTHREAD_NUM = 1 %TO &mvTHREAD_CNT.;
				SIGNOFF T_&mvTHREAD_NUM. WAIT=yes;
				%PUT SIGNOFF OF THREAD T_&mvTHREAD_NUM. ;

				proc casutil; 
					DROPTABLE CASDATA="PART_TABLE_&mvTHREAD_NUM." INCASLIB="public" QUIET;
				run;
			%END;
			
		%mend end_thread;
		%end_thread/*(mpOut=&mpOut.)*/;	
%mend dp_export_csv;