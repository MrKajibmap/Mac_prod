/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки csv в DP
*	
*
*  ПАРАМЕТРЫ:
*     Нет
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
*     %rtp011_load_to_dp;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp011_load_to_dp(mpJobNm=);

	%local lmvJobNm
		   lmvModuleNm
		   lmvCondResNm
			;
	%let lmvJobNm=&mpJobNm.;
	
	/* Проверка на модуль процесса */
	proc sql noprint;
		select upcase(module_nm) into :lmvModuleNm
		from etl_cfg.cfg_resource
		where upcase(resource_nm) = upcase("&lmvJobNm.")
	;
	quit;
	
	%if &lmvModuleNm. eq DP_SEED %then %do;
	/* Получаем имя ресурса из условия запуска для процесса сидинга */
		proc sql noprint;
			select scan(RULE_COND,1,"/") as rule_cond into :lmvCondResNm
			 from etl_cfg.cfg_schedule_rule
			where upcase(rule_nm) = upcase("&lmvJobNm.")
		;
		quit;
		
		%do i=1 %to %sysfunc(countw(&lmvCondResNm., %str( )));
			%let lmvScanCondRes = %scan(&lmvCondResNm., &i., %str( ));
			
			%tech_update_resource_status(mpStatus=P, mpResource=&lmvScanCondRes.);
		%end;
	%end;
	%else %do;
		%tech_update_resource_status(mpStatus=P, mpResource=&lmvJobNm.);
	%end;
	
	%tech_log_event(mpMode=START, mpProcess_Nm=&lmvJobNm.);	
	
	%dp_jobexecution(mpJobName=&lmvJobNm.
						, mpAuth=YES
						);
	
	
	%if &lmvModuleNm. eq DP_SEED %then %do;
		%do i=1 %to %sysfunc(countw(&lmvCondResNm., %str( )));
			%let lmvScanCondRes = %scan(&lmvCondResNm., &i., %str( ));
			
			%tech_update_resource_status(mpStatus=L, mpResource=&lmvScanCondRes.);
		%end;
	%end;
	%else %do;
		%tech_update_resource_status(mpStatus=L, mpResource=&lmvJobNm.);
	%end;
	
	%tech_log_event(mpMode=END, mpProcess_Nm=&lmvJobNm.);	
	
	/* Если это процесс загрузки данных в ДП, то Открываем ресурс для последующего СИДИНГА */
	%if &lmvModuleNm. eq DP_INT %then %do;
		%tech_open_resource(mpResource=SEED_&lmvJobNm.);
	%end;
	
%mend rtp011_load_to_dp;