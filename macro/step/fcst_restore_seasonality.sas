/************************************************************************************
 *	Восстановление сезонности											*
 ************************************************************************************/
/*			В используемом подходе прогнозирования GC PBO/ UNITS PBO прогнозируется
 *		обессезоненная величина. Для восстановления итоговой величины сезонность 
 *		накладывается назад после прогноза.
 *			Макрос восстанавливает сезонность прогноза обессезоненного спроса.
 *		PLM (временное закрытие PBO, IA_PBO_CLOSE_PERIOD) применяется  в 
 *		скрипте rtp7_out_integration
 */


%macro fcst_restore_seasonality(mpInputTbl= MN_DICT.TRAIN_ABT_TRP				/* Расширенная ABT - таблица со всеми фичами и сезонностью, создается на этапе подготовки витрины */
							 ,mpMode=PBO										/* Режим прогнозирования PBO GC или PBO UNITS */
							 ,mpOutTableNm = mn_dict.pbo_forecast_restored		/* Выходная таблица в двухуровневом формате */
							 ,mpAuth = YES										/* Технический параметр для регламентного запуска через Unix. При ручном запуске из SAS Studio должен быть равен NO */
							 );

/* ------------ Start. Технический макрос для поднятия CAS-сессии ----------------- */
	%tech_cas_session(mpMode = start																
							,mpCasSessNm = casauto													
							,mpAssignFlg= y															
							);
/* ------------ End. Технический макрос для поднятия CAS-сессии ------------------- */


/* 		Даты начала и окончания горизонта прогнозирования для фильтрации финального прогноза 
 *	на базе глобального макро-параметра ETL_CURRENT_DT. Параметр должен быть определен до 
 *	вызова данного макроса. По умолчанию задан в конфигурационном скрипте initialize_global.sas  
*/
	%let forecast_start_dt = %str(date%')%sysfunc(putn(&ETL_CURRENT_DT., yymmdd10.))%str(%');						
	%let forecast_end_dt = %str(date%')%sysfunc(putn(%sysfunc(intnx(day,&ETL_CURRENT_DT.,92)), yymmdd10.))%str(%');	


	%local	lmvMode				/* Локальный параметр "Режим запуска макроса GC или PBO" на базе значения входящего макро-параметра mpMode */				
			lmvInputTbl			/* Локальный параметр "Расширенная ABT" на базе значения входящего макро-параметра mpInputTbl */				
			lmvProjectId		/* Локальный параметр с идентификатором VF проекта, соответствующего режиму запуска макроса */
			lmvVfPmixName		/* Локальный параметр с наименованием VF проекта, соответствующего режиму запуска макроса */
			lmvLibrefOut		/* Локальный параметр "Результирующая CAS-библиотека" на базе значения входящего макро-параметра mpOutTableNm */				
			lmvTabNmOut			/* Локальный параметр "Результирующая CAS-таблица" на базе значения входящего макро-параметра mpOutTableNm */	
	;
	
	
	%let lmvInputTbl = &mpInputTbl.;
	%let lmvMode=&mpMode.;


/* 		Технический макрос для разделения имени в двухуровневом формате на имя таблицы и 
 *	имя библиотеки 
*/
	%member_names (mpTable=&mpOutTableNm, mpLibrefNameKey=lmvLibrefOut, mpMemberNameKey=lmvTabNmOut); 


/* ------------ Start. Извлекаем список доступных VF проектов --------------------- */
	%if &mpAuth. = YES %then %do;
		%tech_get_token(mpUsername=&SYS_ADM_USER., mpOutToken=tmp_token);
		
		filename resp TEMP;
		proc http
		  method="GET"
		  url="&CUR_API_URL./analyticsGateway/projects?limit=99999"
		  out=resp;
		  headers 
			"Authorization"="bearer &tmp_token."
			"Accept"="application/vnd.sas.collection+json";    
		run;
		%put Response status: &SYS_PROCHTTP_STATUS_CODE;
		
		libname respjson JSON fileref=resp;
		
		data work.vf_project_list;
		  set respjson.items;
		run;
	%end;
	%else %if &mpAuth. = NO %then %do;
		%vf_get_project_list(mpOut=work.vf_project_list);
	%end;
/* ------------ End.  Извлекаем список доступных VF проектов ---------------------- */


/* ------------ Start. Извлекаем ID для VF проекта по его имени ------------------- */
	%let lmvVfPmixName = &&VF_&lmvMode._NM.;
	%let lmvProjectId = %vf_get_project_id_by_name(mpName=&lmvVfPmixName., mpProjList=work.vf_project_list);
/* ------------ End. Извлекаем ID для VF проекта по его имени --------------------- */


/* ------------ Start. Удаляем предыдущую табличку  ------------------------------- */
	proc casutil;
		droptable casdata="&lmvTabNmOut." incaslib="&lmvLibrefOut." quiet;
	run;
/* ------------ End. Удаляем предыдущую табличку  --------------------------------- */


/* ------------ Start. Забираем прогноз обессезонненого спроса из VF -------------- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.HORIZON{options replace=true} AS 
	   SELECT 
			t1.CHANNEL_CD,
			t1.PBO_LOCATION_ID,
			t1.SALES_DT,
			t1.PREDICT as PREDICT_SM
	   FROM "Analytics_Project_&lmvProjectId.".horizon t1
	;
	QUIT;
/* ------------ End. Забираем прогноз обессезонненого спроса из VF ---------------- */


/* ------------ Start. Восстанавливаем сезонность --------------------------------- */
/*			При подготовке витрины для прогнозирования обессезоненного спроса 
 *		создается расширенная витрина со вспомогательными фичами и коэффициентами 
 *		сезонности.
 *			Полученный прогноз присоединяем к ней и умножаем обессезоненный прогноз
 *		на коэффициенты сезонности.
 */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.FORECAST_RESTORED{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.CHANNEL_CD, 
			  t1.new_RECEIPT_QTY, 
			  t1.RECEIPT_QTY, 
			  t1.SALES_DT, 
			  t1.WOY, 
			  t1.WBY, 
			  t1.DOW, 
			  t1.AVG_of_Detrend_sm_multi, 
			 /* t1.AVG_of_Detrend_sm_aggreg, */
			  t1.AVG_of_Detrend_multi, 
			 /* t1.AVG_of_Detrend_aggreg,  */
			  t1.AVG_of_Detrend_sm_multi_WBY, 
			  t1.AVG_of_Detrend_multi_WBY, 
			/*  t1.AVG_of_Detrend_sm_aggreg_WBY, */
			/*  t1.AVG_of_Detrend_aggreg_WBY, */
			  t1.Detrend_sm_multi, 
			  t1.Detrend_multi, 
			  t1.Deseason_sm_multi, 
			  t1.Deseason_multi, 
			  t1.COVID_pattern, 
			  t1.COVID_lockdown, 
			  t1.COVID_level, 
			  (t3.PREDICT_SM * t1.Detrend_multi) AS &lmvMode._FCST									/* Восстанавливаем сезонность */
		  FROM &lmvInputTbl. t1 																	/* Расширенная ABT с сезонностью. Входной параметр макроса*/
			   LEFT JOIN casuser.HORIZON t3 ON (t1.CHANNEL_CD = t3.CHANNEL_CD) AND 
			  (t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID) AND (t1.SALES_DT = t3.SALES_DT)
		  WHERE t1.SALES_DT between &forecast_start_dt. and &forecast_end_dt.
	;
	QUIT;
/* ------------ End. Восстанавливаем сезонность ----------------------------------- */


/* ------------ Start. Сохраняем результат в заданную табличку/библиотеку --------- */
	proc casutil;
		promote casdata='forecast_restored' incaslib='casuser' outcaslib="&lmvLibrefOut." casout="&lmvTabNmOut.";
		save incaslib="&lmvLibrefOut." outcaslib="&lmvLibrefOut." casdata="&lmvTabNmOut." casout="&lmvTabNmOut..sashdat" replace; 
	run;
/* ------------ End. Сохраняем результат в заданную табличку/библиотеку ----------- */

	
%mend fcst_restore_seasonality;