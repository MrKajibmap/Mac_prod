%macro tech_etl_vf_res_check;

	%local
		lmvResRuleCond
		lmvResCheckLst
		lmvResCheckCnt
		lmvResCheckStatus
		lmvResCheckLstComma
		lmvResNotReadyLst;
		
	proc sql noprint;
		SELECT STRIP(RULE_COND) INTO :lmvResRuleCond
		FROM etl_cfg.cfg_schedule_rule
		WHERE rule_nm = 'vf_load_data';
	quit;
	
	%let lmvResCheckLst = %scan(&lmvResRuleCond., 1, %str(/));
	%let lmvResCheckStatus = %scan(&lmvResRuleCond., 2, %str(/));
	%let lmvResCheckCnt = %sysfunc(countw(&lmvResCheckLst., %str( )));
	
	/* заполнение списка в виде 'ресурс1', 'ресурс2', ... */
	%do i=1 %to &lmvResCheckCnt.;
		%let lmvCurResNm = %scan(&lmvResCheckLst., &i., %str( ));
		%if &i. ne &lmvResCheckCnt. %then %do;
			%let lmvResCheckLstComma = &lmvResCheckLstComma. "&lmvCurResNm.",;
		%end;
		%else %do;
			%let lmvResCheckLstComma = &lmvResCheckLstComma."&lmvCurResNm.";
		%end;
	%end;
	
	/* Проверка неготовых ресурсов */
	proc sql noprint;
	 	create table work.temp as
		SELECT t1.resource_nm as res_nm
		FROM etl_cfg.cfg_resource as t1
			 left join etl_cfg.cfg_status_table as t2
				on t1.resource_id = t2.resource_id
		WHERE t1.resource_nm in (&lmvResCheckLstComma.)
			  AND (UPPER(t2.status_cd) NE UPPER("&lmvResCheckStatus.") OR t2.status_cd is NULL);
	quit;
	
	proc sql noprint;
		select res_nm into :lmvResNotReadyLst separated by ','
		from work.temp;
	quit;	

	/* Если обнаружены нехватающие ресурсы - варнинг в TG */
	%if %length(&lmvResNotReadyLst.) gt 0 %then %do;
		%let lmvBotMessage = VF_RES_CHECK_TEST : Процесс vf_load_data не готов к запуску, отсутствуют в статусе 'A' ресурсы: &lmvResNotReadyLst.;
	%end;
	%else %do;
		%let lmvBotMessage = VF_RES_CHECK_TEST : Процесс vf_load_data готов к запуску.;
	%end;
	
 		 
 	filename resp temp ; 
 	proc http  
 		 method="POST" 
 		 url="https://api.telegram.org/bot&TG_BOT_TOKEN./sendMessage?chat_id=-1001360913796&text=&lmvBotMessage." */
		 ct="application/json" 
 		 out=resp;  
 	run; 
	
%mend tech_etl_vf_res_check;
