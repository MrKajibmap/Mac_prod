/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для обратной интеграции результатов модуля прогнозирования
*			- new & regular products consolidation
*			- long & short term forecasts consolidation
*			- plm data applying
*			- short term forecast for units reconciliation
*			- price data joining
* 			- result forecast tables for SAS DP
*			- upt & gc & sale & units  forecasts
*			- day & month granularities
*
******************************************************************
*  ПАРАМЕТРЫ:
* 	mpVfPmixProjName	- Наименование long-term VF-проекта для pmix (units)
* 	mpVfPboProjName		- Наименование long-term VF-проекта для gc
* 	mpMLPmixTabName		- Наименование входной таблицы ML short-term units forecast for regular (not new) products
* 	mpInEventsMkup		- Таблица Events по географической иерархии
* 	mpInWpGc			- 
* 	mpOutPmixLt			- Наименование выходной таблицы для прогноза pmix/units/sale в разрезе ПБО-SKU-месяц
* 	mpOutGcLt			- Наименование выходной таблицы для прогноза gc в разрезе ПБО-SKU-месяц
* 	mpOutUptLt			- Наименование выходной таблицы для прогноза upt в разрезе ПБО-SKU-месяц
* 	mpOutPmixSt			- Наименование выходной таблицы для прогноза pmix/units/sale в разрезе ПБО-SKU-день
* 	mpOutGcSt			- Наименование выходной таблицы для прогноза gc в разрезе ПБО-SKU-день
* 	mpOutUptSt			- Наименование выходной таблицы для прогноза upt в разрезе ПБО-SKU-день
* 	mpOutOutforgc		- 
* 	mpOutOutfor			- 
* 	mpOutNnetWp			- 
* 	mpPrmt				- Флаг Y/N, разрешающий удаление/загрузку в CAS/сохранение на диск целевых таблиц
* 	mpInLibref			- Наименование библиотеки, содержащей входные таблицы
* 	mpAuth 				- Технический параметр для регламентного запуска через Unix. При ручном запуске из SAS Studio должен быть равен NO 
*
******************************************************************
*  Пример использования:
*	 %rtp_7_out_integration(
		  mpVfPmixProjName	= &VF_PMIX_PROJ_NM.
		, mpVfPboProjName	= &VF_PBO_PROJ_NM.
		, mpMLPmixTabName	= mn_short.pmix_days_result
		, mpInEventsMkup	= mn_long.events_mkup
		, mpInWpGc			= mn_dict.wp_gc
		, mpOutPmixLt		= mn_short.plan_pmix_month
		, mpOutGcLt			= mn_short.plan_gc_month
		, mpOutUptLt		= mn_short.plan_upt_month
		, mpOutPmixSt		= mn_short.plan_pmix_day
		, mpOutGcSt			= mn_short.plan_gc_day
		, mpOutUptSt		= mn_short.plan_upt_day
		, mpOutOutforgc		= mn_short.TS_OUTFORGC
		, mpOutOutfor		= mn_short.TS_OUTFOR
		, mpOutNnetWp		= mn_dict.nnet_wp1
		, mpPrmt			= Y
		, mpInLibref		= mn_short
		, mpAuth 			= NO
	);
*
****************************************************************************/

%macro rtp_7_out_integration(
		  mpVfPmixProjName	= &VF_PMIX_PROJ_NM.
		, mpVfPboProjName	= &VF_PBO_PROJ_NM.
		, mpMLPmixTabName	= mn_short.pmix_days_result
		, mpInEventsMkup	= mn_long.events_mkup
		, mpInWpGc			= mn_dict.wp_gc
		, mpOutPmixLt		= mn_short.plan_pmix_month
		, mpOutGcLt			= mn_short.plan_gc_month
		, mpOutUptLt		= mn_short.plan_upt_month
		, mpOutPmixSt		= mn_short.plan_pmix_day
		, mpOutGcSt			= mn_short.plan_gc_day
		, mpOutUptSt		= mn_short.plan_upt_day
		, mpOutOutforgc		= mn_short.TS_OUTFORGC
		, mpOutOutfor		= mn_short.TS_OUTFOR
		, mpOutNnetWp		= mn_dict.nnet_wp1
		, mpPrmt			= Y
		, mpInLibref		= mn_short
		, mpAuth 			= NO
	);

	%let pbo_table  = MN_DICT.PBO_FORECAST_RESTORED;			/* Входная таблица прогноза UNITS PBO */
	%let gc_table   = MN_DICT.GC_FORECAST_RESTORED;				/* Входная таблица прогноза GC PBO */
	%let price_table= MN_DICT.PRICE_FULL_SKU_PBO_DAY;			/* Входная таблица с ценами в разрезе SKU-PBO-day */

	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	
	proc cas;
		table.tableExists result = rc / caslib="mn_dict" name="NNET_WP1";								/* Что это за таблица??? Где используется??? */
		if rc=0  then do;
			loadtable / caslib='mn_dict',
						path='NNET_WP1_ATTR.sashdat',
						casout={caslib="mn_dict" name='attr2', replace=true};
			loadtable / caslib='mn_dict',
						path='NNET_WP1.sashdat',
						casout={caslib="mn_dict" name='nnet_wp1', replace=true};
			attribute / task='ADD',
						   caslib="mn_dict",
						name='nnet_wp1',
						attrtable='attr2';
			table.promote / name="NNET_WP1" caslib="mn_dict" target="NNET_WP1" targetlib="mn_dict";
		end;
		else print("Table mn_dict.NNET_WP1 already loaded");
		

		table.tableExists result = rc / caslib="mn_dict" name="wp_gc";									
		if rc=0  then do;
			loadtable / caslib='mn_dict',
			path='wp_gc.sashdat',
			casout={caslib="mn_dict" name='wp_gc', replace=true};
			table.promote / name="wp_gc" caslib="mn_dict" target="wp_gc" targetlib="mn_dict";
		end;
		else print("Table mn_dict.wp_gc already loaded");	


		table.tableExists result = rc / caslib="mn_long" name="events_mkup";							/* Таблица Events по географической иерархии */
		if rc=0  then do;
			loadtable / caslib='mn_long',
			path='events_mkup.sashdat',
			casout={caslib="mn_long" name='events_mkup', replace=true};
			table.promote / name="events_mkup" caslib="mn_long" target="events_mkup" targetlib="mn_long";
		end;
		else print("Table mn_long.events_mkup already loaded");	
	quit;	

	%member_exists_list(mpMemberList=&mpMLPmixTabName.
								&mpInEventsMkup.
								&mpInWpGc.
								&mpOutNnetWp.
								);
								

	%local	lmvOutLibrefPmixSt 																			/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutTabNamePmixSt 																		/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutLibrefGcSt 																			/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutTabNameGcSt 																			/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutLibrefUptSt 																			/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutTabNameUptSt 																			/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutLibrefPmixLt 																			/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutTabNamePmixLt 																		/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutLibrefGcLt 																			/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutTabNameGcLt																			/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutLibrefUptLt 																			/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutTabNameUptLt  																		/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutLibrefOutforgc 																		/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutTabNameOutforgc 																		/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutLibrefOutfor 																			/* Где используется переменная??? Откуда приходит значение??? */
			lmvOutTabNameOutfor 																		/* Где используется переменная??? Откуда приходит значение??? */
			lmvVfPmixName																				/* Где используется переменная??? Откуда приходит значение??? */
			lmvVfPmixId																					/* Где используется переменная??? Откуда приходит значение??? */
			lmvVfPboName																				/* Где используется переменная??? Откуда приходит значение??? */
			lmvVfPboId																					/* Где используется переменная??? Откуда приходит значение??? */
			lmvInEventsMkup																				/* Где используется переменная??? Откуда приходит значение??? */
			lmvInLib																					/* Входная библиотека??? */
			lmvReportDt																					/* Где используется переменная??? Откуда приходит значение??? */
			lmvReportDttm																				/* Где используется переменная??? Откуда приходит значение??? */
			lmvInLibref																					/* Где используется переменная??? Откуда приходит значение??? */
			lmvAPI_URL																					/* Где используется переменная??? Откуда приходит значение??? */
			;
			
	%let lmvInLib		= ETL_IA;
	%let lmvReportDt	= &ETL_CURRENT_DT.;										/* Текущая дата */	
	%let lmvReportDttm	= &ETL_CURRENT_DTTM.;									/* Текущая дата-время */	
	%let lmvInLibref	= &mpInLibref.;											/* CAS-библиотека с прогнозами short-term*/
	%let lmvAPI_URL 	= &CUR_API_URL.;										/* Техническая API ссылка */
	%let lmvScoreEndDate= %sysfunc(intnx(day,&VF_HIST_END_DT_SAS.,91,s));  		/* Дата окончания short-term прогноза */
	
	/* Разбиваем двухуровневые имена таблиц на имя библиотек и таблиц по отдельности: */
	%member_names (mpTable=&mpOutOutfor, mpLibrefNameKey=lmvOutLibrefOutfor, mpMemberNameKey=lmvOutTabNameOutfor);				
	%member_names (mpTable=&mpOutOutforgc, mpLibrefNameKey=lmvOutLibrefOutforgc, mpMemberNameKey=lmvOutTabNameOutforgc); 		
	%member_names (mpTable=&mpOutGcSt, mpLibrefNameKey=lmvOutLibrefGcSt, mpMemberNameKey=lmvOutTabNameGcSt); 					
	%member_names (mpTable=&mpOutPmixSt, mpLibrefNameKey=lmvOutLibrefPmixSt, mpMemberNameKey=lmvOutTabNamePmixSt); 				
	%member_names (mpTable=&mpOutUptSt, mpLibrefNameKey=lmvOutLibrefUptSt, mpMemberNameKey=lmvOutTabNameUptSt); 				
	%member_names (mpTable=&mpOutGcLt, mpLibrefNameKey=lmvOutLibrefGcLt, mpMemberNameKey=lmvOutTabNameGcLt); 					
	%member_names (mpTable=&mpOutPmixLt, mpLibrefNameKey=lmvOutLibrefPmixLt, mpMemberNameKey=lmvOutTabNamePmixLt); 				
	%member_names (mpTable=&mpOutUptLt, mpLibrefNameKey=lmvOutLibrefUptLt, mpMemberNameKey=lmvOutTabNameUptLt); 				


/* ------------ Start. Проводим аутентификацию ------------------------------------ */
	%if &mpAuth. = YES %then %do;
		/* Напоминание: Надо поменять ru-nborzunov на, кажется, SYS_USER_ID, или вообще удалить этот кусок и вызывать до этого скрипта в основном потоке */
		%tech_get_token(mpUsername=ru-nborzunov, mpOutToken=tmp_token);	
				
		filename resp TEMP;
		proc http
		  method="GET"
		  url="&lmvAPI_URL./analyticsGateway/projects?limit=99999"
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
/* ------------ End. Проводим аутентификацию -------------------------------------- */


/*************************************************************************************
 *		Обработка long-term прогнозов из VF											 *
 ************************************************************************************/

/* ------------ Start. Извлечение ID для long-term VF-проекта PMIX по его имени ------------- */
	%let lmvVfPmixName = &mpVfPmixProjName.;
	%let lmvVfPmixId = %vf_get_project_id_by_name(mpName=&lmvVfPmixName., mpProjList=work.vf_project_list);
/* ------------ End. Извлечение ID для VF-проекта PMIX по его имени --------------- */


/* ------------ Start. Извлечение ID для long-term VF-проекта PBO по его имени -------------- */
	%let lmvVfPboName = &mpVfPboProjName.;
	%let lmvVfPboId = %vf_get_project_id_by_name(mpName=&lmvVfPboName., mpProjList=work.vf_project_list);
/* ------------ End. Извлечение ID для VF-проекта PBO по его имени ---------------- */

	
	%let lmvInEventsMkup=&mpInEventsMkup;


/* ------------ Start. Удаление целевых таблиц ------------------------------------ */
	%if &mpPrmt. = Y %then %do;
		proc casutil;
			droptable casdata="&lmvOutTabNameGcSt." incaslib="&lmvOutLibrefGcSt." quiet;
			droptable casdata="&lmvOutTabNamePmixSt." incaslib="&lmvOutLibrefPmixSt." quiet;
			droptable casdata="&lmvOutTabNameUptSt." incaslib="&lmvOutLibrefUptSt." quiet;
			droptable casdata="&lmvOutTabNameGcLt." incaslib="&lmvOutLibrefGcLt." quiet;
			droptable casdata="&lmvOutTabNamePmixLt." incaslib="&lmvOutLibrefPmixLt." quiet;
			droptable casdata="&lmvOutTabNameUptLt." incaslib="&lmvOutLibrefUptLt." quiet;
			droptable casdata="&lmvOutTabNameOutfor." incaslib="&lmvOutLibrefOutfor." quiet;
			droptable casdata="&lmvOutTabNameOutforgc." incaslib="&lmvOutLibrefOutforgc." quiet;
			droptable casdata="&lmvOutTabNameOutforgc." incaslib="&lmvOutLibrefOutforgc." quiet;
			*droptable casdata="pmix_sales" incaslib="&lmvInLibref." quiet;
			*droptable casdata="pmix_days_result" incaslib="&lmvInLibref." quiet;
			droptable casdata="all_ml_scoring" incaslib="&lmvInLibref." quiet;
			droptable casdata="all_ml_train" incaslib="&lmvInLibref." quiet;
		run;
	%end;
/* ------------ End. Удаление целевых таблиц -------------------------------------- */


/* ------------ Start. Вытащить данные из проектов --------------------------------- */
	proc fedsql sessref=casauto noprint;
		create table &lmvOutLibrefOutfor..&lmvOutTabNameOutfor.{options replace=true} as
			select t1.*
					,month(cast(t1.SALES_DT as date)) as MON_START
					,month(cast(intnx('day', cast(t1.SALES_DT as date),6) as date)) as MON_END
			from "Analytics_Project_&lmvVfPmixId".horizon t1
		;
	quit;
	proc fedsql sessref=casauto noprint;
		create table &lmvOutLibrefOutforGc..&lmvOutTabNameOutforGc.{options replace=true} as
			select t1.*
					,month(cast(t1.SALES_DT as date)) as MON_START
					,month(cast(intnx('day', cast(t1.SALES_DT as date),6) as date)) as MON_END
			from "Analytics_Project_&lmvVfPboId".horizon t1
		;
	quit;
	%if &mpPrmt. = Y %then %do;
		proc casutil;
			promote casdata="&lmvOutTabNameOutfor." incaslib="&lmvOutLibrefOutfor." outcaslib="&lmvOutLibrefOutfor.";
			promote casdata="&lmvOutTabNameOutforgc." incaslib="&lmvOutLibrefOutforgc." outcaslib="&lmvOutLibrefOutforgc.";
		run;
	%end;
/* ------------ End. Вытащить данные из проекта ----------------------------------- */


/* ------------ Start. Применяем к недельным long-term прогнозам недельные профили ---------- */
	%vf_apply_w_prof(&lmvOutLibrefOutfor..&lmvOutTabNameOutfor.,
					&lmvOutLibrefOutfor..&lmvOutTabNameOutforgc.,
					casuser.nnet_wp_scored1,
					casuser.daily_gc,
					&mpInEventsMkup.,
					&mpInWpGc.,
					&mpOutNnetWp.,
					&lmvInLibref.);
	
	/* Разворачиваем long-term прогнозы с недель до дней */
	data casuser.pmix_daily(drop=channel_cd_old);
		set casuser.nnet_wp_scored1(rename=(channel_cd=channel_cd_old));
		length channel_cd $48;
		channel_cd = channel_cd_old;
		array p_weekday{7};
		array PR_{7};
		keep CHANNEL_CD PBO_LOCATION_ID PRODUCT_ID period_dt mon_dt FF promo;
		format period_dt mon_dt date9.;
		period_dt = week_dt;
		fc = ff;
		if fc = . then fc = 0;
		miss_prof = nmiss(of p_weekday:);
		if miss_prof > 0 then
			do i = 1 to 7;
			p_weekday{i} = 1. / 7.;
			end;
		do while (period_dt <= week_dt + 6);
			mon_dt = intnx('month', period_dt, 0, 'b');
			promo = pr_{period_dt - week_dt + 1};
			ff = fc * p_weekday{period_dt - week_dt + 1};
			output;
			period_dt + 1;
		end;
	run;
	
	/* Оставляем только прогноз после окончания short-term */
	data casuser.pmix_daily;
		set casuser.pmix_daily;
		where period_dt > &lmvScoreEndDate.; 
	run;

	proc casutil;
		droptable casdata="nnet_wp_scored1" incaslib="mn_short" quiet;
	run;
	quit;
/* ------------ End. Применяем к недельным прогнозам недельные профили (longterm)-- */
	

/* ------------ Start. Прогнозирование новых товаров отдельной ML-моделью --------- */
	%vf_new_product(mpInCaslib=&lmvInLibref.);
/* ------------ End. Прогнозирование новых товаров отдельной ML-моделью ----------- */


/*************************************************************************************
 *		Соединяем таблицы долгосрочного (урезанного) и краткосрочного прогнозов (UNITS)	 *
 ************************************************************************************/
 
	/* 	Разворачиваем промо из интервалов в дни (так как требуется разделить прогноз на промо и регулярный) */
	data casuser.promo_w2;																					
		set casuser.promo_d; 			/* table from vf_apply_w_prof */
		format period_dt date9.;
		do period_dt = start_dt to min(end_dt, &vf_fc_agg_end_dt_sas.);
			output;
		end;
	run;

	/* 	Удаляем дубли из развернутой таблицы промо */
	proc fedsql sessref=casauto;
		create table casuser.promo_w1{options replace=true} as
		select distinct 
			  t1.channel_cd
			, t1.pbo_location_id
			, t1.product_id
			, t1.period_dt
			, cast(1 as double) as promo
		from casuser.promo_w2 t1
		;
	quit;

	/* Энкодим ID канала его наименованием из отдельного справочника */
	proc fedsql sessref=casauto;
        create table casuser.short_term{options replace=true} as
        select distinct 
			  t2.PBO_LOCATION_ID
			, t2.PRODUCT_ID
			, t2.sales_dt as period_dt
			, t3.channel_cd
            , cast(intnx('month',t2.sales_dt,0) as date) as mon_dt
            , t2.P_SUM_QTY as ff
			, . as promo
        from
                &mpMLPmixTabName. as t2 
        left join MN_DICT.ENCODING_CHANNEL_CD t3				
            on t2.channel_cd=t3.channel_cd_id
        where t2.sales_dt between &VF_FC_START_DT and &VF_FC_END_SHORT_DT 
    ;
    quit;

	/* Присоединяем к обрезанному long-term прогноз short-term */
	data casuser.pmix_daily(append=yes);
        set casuser.short_term;
    run;
   
	proc casutil;
		droptable casdata="short_term" incaslib="casuser" quiet;
	run;
	quit;


/************************************************************************************
 *		Вычисление матриц временных закрытий и допустимых дней продаж			*
 ************************************************************************************/

/* ------------ Start. Дни когда пбо будет уже закрыт (навсегда) ------------------ */
	data CASUSER.DAYS_PBO_DATE_CLOSE;
		set &lmvInLibref..pbo_dictionary;
		format period_dt date9.;
		keep PBO_LOCATION_ID CHANNEL_CD period_dt;
		CHANNEL_CD = "ALL"; 		/* Создаем поле "канал" */
		if A_CLOSE_DATE ne . and A_CLOSE_DATE <= &vf_fc_agg_end_dt_sas. then 
			do period_dt = max(A_CLOSE_DATE, &vf_fc_start_dt_sas.) to &vf_fc_agg_end_dt_sas.;
				output;
			end;
	run;
/* ------------ End. Дни когда пбо будет уже закрыт (навсегда) -------------------- */


/* ------------ Start. Дни когда пбо будет временно закрыт ------------------------ */
	data CASUSER.DAYS_PBO_CLOSE;
		set &lmvInLibref..PBO_CLOSE_PERIOD;
		format period_dt date9.;
		keep PBO_LOCATION_ID CHANNEL_CD period_dt;
		if channel_cd = "ALL" ;			/* Фильтруем только данные из канала 'ALL' */
		if (end_dt >= &vf_fc_start_dt_sas. and end_dt <= &vf_fc_agg_end_dt_sas.) 
			or (start_dt >= &vf_fc_start_dt_sas. and start_dt <= &vf_fc_agg_end_dt_sas.) 
			or (start_dt <= &vf_fc_start_dt_sas. and &vf_fc_start_dt_sas. <= end_dt)
		then
			do period_dt = max(start_dt, &vf_fc_start_dt_sas.) to min(&vf_fc_agg_end_dt_sas., end_dt);
				output;
			end;
	run;
/* ------------ End. Дни когда пбо будет временно закрыт -------------------------- */


/* ------------ Start. Дни когда закрыто ПБО - никаких продаж быть не должно ------ */
	data casuser.days_pbo_close(append=force); 
	  set casuser.days_pbo_date_close;
	run;
/* ------------ End. Дни когда закрыто ПБО - никаких продаж быть не должно -------- */

	
/* ------------ Start. Убираем дубликаты ------------------------------------------ */
	proc fedsql sessref = casauto;
	create table casuser.days_pbo_close{options replace=true} as
	select distinct * from casuser.days_pbo_close;
	quit;
/* ------------ End. Убираем дубликаты -------------------------------------------- */


/************************************************************************************
 *		Обработка замен T															*
 ************************************************************************************/
/*		Замечание: это рудимент, так как в PRODUCT_CHAIN больше нет строк с данным флагом.
			Соответственно полученная на данном шаге таблица будет пустая,
				и далее присоединяется к основной таблице с прогнозами через left join,
					не влияя на результат.
 */	

	/* Параметр vf_fc_agg_end_dt из файла initialize_global */
	proc fedsql sessref=casauto;
		create table casuser.plm_t{options replace=true} as
		select LIFECYCLE_CD, PREDECESSOR_DIM2_ID, PREDECESSOR_PRODUCT_ID,
			SUCCESSOR_DIM2_ID, SUCCESSOR_PRODUCT_ID, SCALE_FACTOR_PCT,
			coalesce(PREDECESSOR_END_DT,cast(intnx('day',SUCCESSOR_START_DT,-1) as date)) as PREDECESSOR_END_DT, 
			SUCCESSOR_START_DT
		/* from &lmvLCTab */
		from &lmvInLibref..PRODUCT_CHAIN
		where LIFECYCLE_CD='T' 
			and coalesce(PREDECESSOR_END_DT,cast(intnx('day',SUCCESSOR_START_DT,-1) as date))<=date %tslit(&vf_fc_agg_end_dt.)		
			/* and successor_start_dt>=intnx('month',&vf_fc_start_dt,-3); */
			and successor_start_dt>=intnx('month',&vf_fc_start_dt,-8);
		/*фильтр, отсекающий "старые" замены 
		Замены случившиеся больше 3 мес назад отсекаются 
		Замены позднее fc_agg_end_dt отсекаем*/
	quit;

    /*predcessor будет продаваться до predecessor_end_dt (включ), все остальные даты ПОСЛЕ удаляем*/
    proc fedsql sessref=casauto; 
		create table casuser.predessor_periods_t{options replace=true} as
		select PREDECESSOR_DIM2_ID as pbo_location_id,
			PREDECESSOR_PRODUCT_ID as product_id,
			min(PREDECESSOR_END_DT) as end_dt
		from casuser.plm_t group by 1,2
		;
	quit;


/************************************************************************************
 *		Обработка выводов D															*
 ************************************************************************************/

	/* Параметр vf_fc_agg_end_dt из файла initialize_global */
	proc fedsql sessref=casauto;
		create table casuser.plm_d{options replace=true} as
		select LIFECYCLE_CD, PREDECESSOR_DIM2_ID, PREDECESSOR_PRODUCT_ID,
			SUCCESSOR_DIM2_ID, SUCCESSOR_PRODUCT_ID, SCALE_FACTOR_PCT,
			PREDECESSOR_END_DT, SUCCESSOR_START_DT
		/* from &lmvLCTab */
		from &lmvInLibref..PRODUCT_CHAIN
		where LIFECYCLE_CD = 'D'
			and predecessor_end_dt <= date %tslit(&vf_fc_agg_end_dt.);									
		/*старые выводы не отсекаем
		  выводы позднее fc_agg_end_dt отсекаем*/
	quit;


/************************************************************************************
 *		Добавление прогноза по новым товарам по дням											*
 ************************************************************************************/
/*		insert-update новых товаров по дням по ключу в pmix_daily до 
 *		применения PLM с приоритетом новых товаров.
 *		Замечание: Необходимо вывести индикатор того, что прогноз из модели новых товаров,
 *		чтобы потом посчитать долю товаров с прогнозом модели новых товаров
 */	

	/* Если нет прогноза модели новых товаров */
	%if %sysfunc(exist(casuser.npf_prediction)) eq 0 %then %do;											
		proc fedsql sessref=casauto;
			create table casuser.pmix_daily_new{options replace=true} as
			select 
				t2.period_dt,
				t2.PRODUCT_ID,
				t2.channel_cd, 
				t2.PBO_LOCATION_ID, 
				t2.mon_dt,
				t2.ff
			from casuser.pmix_daily t2
			;
		quit;
	%end;

	/* Если есть прогноз модели новых товаров */
	%else %do;																							
		proc fedsql sessref=casauto;
			create table casuser.pmix_daily_new{options replace=true} as
			select 
				coalesce(t1.SALES_DT,t2.period_dt) as period_dt,
				coalesce(t1.product_id,t2.PRODUCT_ID) as product_id,
				coalesce(t1.channel_cd,t2.channel_cd) as channel_cd, 
				coalesce(t1.pbo_location_id,t2.PBO_LOCATION_ID) as PBO_LOCATION_ID, 
				coalesce(cast(intnx('month',t1.sales_dt,0) as date),t2.mon_dt) as mon_dt,
				coalesce(t1.P_SUM_QTY,t2.ff) as ff
			from casuser.npf_prediction t1 full outer join casuser.pmix_daily t2
				on t1.SALES_DT =t2.period_dt and t1.product_id=t2.product_id and 
				t1.channel_cd=t2.channel_cd and t1.pbo_location_id=t2.pbo_location_id
			;
		quit;
	%end;
	
	proc casutil;
			droptable casdata="pmix_daily" incaslib="mn_short" quiet;
			promote casdata="pmix_daily" incaslib="casuser" outcaslib="mn_short";
			save incaslib="mn_short" outcaslib="mn_short" casdata="pmix_daily" casout="pmix_daily.sashdat" replace;
	run;
	quit;


/* ------------ Start. Добавление в АМ информации из новинок ---------------------- */
	/* Замечание: проверить не появляется ли дублей */
	proc fedsql sessref=casauto;
		create table casuser.AM_new{options replace=true} as
		select 
			  product_id
  			, pbo_location_id
			, start_dt
			, end_dt
		from &lmvInLibref..ASSORT_MATRIX as t1
		;
	quit;

	/* Замечание: проверить не стала ли future_product_chain рудиментом */
	%if %sysfunc(exist(casuser.future_product_chain)) ne 0 %then %do;
		data casuser.AM_new(append=yes);
			set casuser.future_product_chain(rename=(period_start_dt=start_dt 
													period_end_dt=end_dt));
		run;
	%end;
/* ------------ End. Добавление в АМ информации из новинок ------------------------ */


/************************************************************************************
 *		Применение T,D и PLM к прогнозам											*
 ************************************************************************************/

/* ------------ Start. формирование таблицы товар-ПБО-день, которые должны 
							быть в прогнозе - на основании АМ --------------------- */
	proc fedsql sessref=casauto;
		create table casuser.plm_dist{options replace=true} as
		select pbo_location_id,product_id, start_dt,end_dt
		from casuser.AM_new
		/* нужны записи AM, пересекающиеся с периодом прогнозирования */
		where start_dt between &vf_fc_start_dt. and date %tslit(&vf_fc_agg_end_dt.)
			  or &vf_fc_start_dt. between start_dt and end_dt; 											
	quit;
/* ------------ End. формирование таблицы товар-ПБО-день, которые должны 
							быть в прогнозе - на основании АМ --------------------- */


/* ------------ Start. Дни когда товар должен продаваться по информации из АМ ----- */
	data casuser.days_prod_sale; 
	  set casuser.plm_dist;
	  format period_dt date9.;
	  keep PBO_LOCATION_ID PRODUCT_ID period_dt;
	  do period_dt=max(start_dt, &vf_fc_start_dt_sas.) to min(&vf_fc_agg_end_dt_sas.,end_dt);
	    output;
	  end;
	run;

	proc casutil;
			droptable casdata="plm_dist" incaslib="casuser" quiet;
	run;
	quit;
/* ------------ End. Дни когда товар должен продаваться по информации из АМ ------- */


/* ------------ Start. Удалить дубликаты ------------------------------------------ */
	data casuser.days_prod_sale1;
		set casuser.days_prod_sale;
		by PBO_LOCATION_ID PRODUCT_ID period_dt;
		if first.period_dt then output;
	run;
	
	proc casutil;
		droptable casdata="days_prod_sale" incaslib="casuser" quiet;
	run;
	quit;
/* ------------ End. Удалить дубликаты -------------------------------------------- */
	
	
/* ------------ Start. Удалить периоды D  ----------------------------------------- */
	
	/* Удаляем периоды period_dt после даты закрытия predecessor */
	proc fedsql sessref=casauto;
		create table casuser.plm_sales_mask{options replace=true} as
		select 
			    t1.PBO_LOCATION_ID
			  , t1.PRODUCT_ID
			  , t1.period_dt
		from  casuser.days_prod_sale1 as t1 
		left join casuser.plm_d as t2
			on  t1.product_id 		= t2.PREDECESSOR_PRODUCT_ID 
			and t1.pbo_location_id 	= t2.PREDECESSOR_DIM2_ID
		where t1.period_dt < coalesce(t2.PREDECESSOR_END_DT, cast(intnx('day', date %tslit(&vf_fc_agg_end_dt.), 1) as date))
		;
	quit;
	
	proc casutil;
		droptable casdata="days_prod_sale1" incaslib="casuser" quiet;
	run;
	quit;
/* ------------ End. Удалить периоды D  -------------------------------------------- */


/* ------------ Start. Удалить периоды временного и постоянного закрытия ПБО */
	proc fedsql sessref=casauto;
		create table casuser.plm_sales_mask1{options replace=true} as
		select 
			  main.PBO_LOCATION_ID
			, main.PRODUCT_ID
			, main.period_dt
		
		from casuser.plm_sales_mask as main 
		
		left join casuser.DAYS_PBO_CLOSE as clsd
			on  main.pbo_location_id = clsd.pbo_location_id 
			and main.period_dt		 = clsd.period_dt
		
		left join casuser.predessor_periods_t as prpt
			on  main.pbo_location_id = prpt.pbo_location_id 
			and main.product_id		 = prpt.product_id
		
		where   
			/* Когда ПБО закрыт по любым причинам по информации из casuser.days_pbo_close,
				то эти дни не должны попадать ключу ПБО - канал */
			clsd.pbo_location_id is null and clsd.period_dt is null
			
			/* Для predcessor из plm_sales_mask1 удаляем периоды с датой после даты вывода (period_dt > end_dt).
				Если ряд есть в predcessor - оставляем всё <= даты вывода, если нет - не смотрим на дату */
			and ((main.period_dt <= prpt.end_dt and prpt.end_dt is not null) or prpt.end_dt is null)
		;
	quit;
	
	proc casutil;
		droptable casdata="predessor_periods_t" incaslib="mn_short" quiet;
		promote casdata="predessor_periods_t" incaslib="casuser" outcaslib="mn_short";
		save incaslib="mn_short" outcaslib="mn_short" casdata="predessor_periods_t" casout="predessor_periods_t.sashdat" replace;
	run;
	quit;
	
	proc casutil;
		droptable casdata="plm_sales_mask" incaslib="mn_short" quiet;
		promote casdata="plm_sales_mask" incaslib="casuser" outcaslib="mn_short";
		save incaslib="mn_short" outcaslib="mn_short" casdata="plm_sales_mask" casout="plm_sales_mask.sashdat" replace;
	run;
	quit;
/* ------------ End. удалить отсюда периоды временного и постоянного закрытия ПБО - */

	
/* ------------ Start. Cоздаём дубликаты прогнозов, копируя predesessor под id successor -- */
	
	/* Замечание: таблица всегда формируется пустая... */
    proc fedsql sessref=casauto; 
		create table casuser.successor_fc{options replace=true} as
			select
				t1.period_DT,
				t2.SUCCESSOR_PRODUCT_ID as product_id,
				t1.CHANNEL_CD,
				t2.SUCCESSOR_DIM2_ID as pbo_location_id,
				t1.mon_dt,
				t1.FF * coalesce(t2.SCALE_FACTOR_PCT, 100.) / 100. as FF
			from casuser.pmix_daily_new t1 
			inner join casuser.plm_t t2 
				on  t1.PRODUCT_ID 		= t2.PREDECESSOR_PRODUCT_ID 
				and t1.PBO_LOCATION_ID 	= t2.PREDECESSOR_DIM2_ID
			where t1.period_dt >= t2.successor_start_dt
		;
	quit;
/* ------------ Start. Cоздаём дубликаты прогнозов, копируя predesessor под id successor -- */

/* ------------ Start. Подготовка прогноза для применения PLM  --------------------------- */
	/
    proc fedsql sessref=casauto;
		create table casuser.pmix_daily_new_{options replace=true} as
			select 
				  coalesce(t1.period_dt,t2.period_dt) as period_dt
				, coalesce(t1.product_id,t2.PRODUCT_ID) as product_id
				, coalesce(t1.channel_cd,t2.channel_cd) as channel_cd
				, coalesce(t1.pbo_location_id,t2.PBO_LOCATION_ID) as PBO_LOCATION_ID
				, coalesce(t1.mon_dt,t2.mon_dt) as mon_dt
				, coalesce(t1.ff,t2.ff) as ff
		from casuser.successor_fc t1 
		full outer join casuser.pmix_daily_new t2
			on  t1.period_dt 		= t2.period_dt 
			and t1.product_id		= t2.product_id 
			and t1.channel_cd		= t2.channel_cd 
			and t1.pbo_location_id	= t2.pbo_location_id
	;
	quit;
/* ------------ End. Подготовка прогноза для применения PLM  ----------------------------- */


/* ------------ Start. Сохранение и очистка промежуточных таблиц 	---------------------- */
	proc casutil;
			droptable casdata="fc_w_plm" incaslib="casuser" quiet;
			*droptable casdata="successor_fc" incaslib="casuser" quiet;
			droptable casdata="pmix_daily_new" incaslib="casuser" quiet;
			droptable casdata="percent" incaslib="casuser" quiet;
	run;
	
	proc casutil;
			droptable casdata="successor_fc" incaslib="mn_short" quiet;
			promote casdata="successor_fc" incaslib="casuser" outcaslib="mn_short";						 
			save incaslib="mn_short" outcaslib="mn_short" casdata="successor_fc" casout="successor_fc.sashdat" replace;
	run;
	quit;
/* ------------ End. Сохранение и очистка промежуточных таблиц  -------------------------- */


/* ------------ Start. Наложение plm на объединенный прогноз UNITS ------------------------ */
	proc fedsql sessref=casauto;
		create table CASUSER.FC_W_PLM{options replace=true} as 
			select 
				  fc.channel_cd
				, fc.pbo_location_id
				, fc.product_id
				, fc.period_dt
				, fc.FF
				, coalesce(prm.promo, 0) as promo
			from 
				CASUSER.PMIX_DAILY_NEW_ as fc 
			inner join 
				CASUSER.PLM_SALES_MASK1 as plm 	/* дни когда товар ДОЛЖЕН продаваться */
				on  fc.pbo_location_id  = plm.pbo_location_id 
				and fc.product_id		= plm.product_id 
				and fc.period_dt		= plm.period_dt
			left join 
				CASUSER.PROMO_W1 as prm 		/* флаг промо */
				on  fc.channel_cd		= prm.channel_cd 
				and fc.pbo_location_id  = prm.pbo_location_id 
				and fc.product_id		= prm.product_id 
				and fc.period_dt		= prm.period_dt
			;
	quit;
/* ------------ End. Наложение plm на объединенный прогноз UNITS --------------------------- */


/************************************************************************************
 *		Реконсилируем прогноз с PBO до PBO-SKU										*
 ************************************************************************************/
/*		Для повышения точности прогноза UNITS на уровне PBO-SKU-DAY используется 
 *		реконсиляция с уровня ресторана. Суммарные продажи UNITS на уровне ресторана
 *		прогнозируются отдельно и затем распределяются пропорционально на нижний
 *		уровень согласно ML прогнозу.
 * 		Реконсиляция применяется к объединенному прогнозу short-term + long-term + новинки,	
 *			после применения PLM, но по факту срабатывает только на горизонте short-term,
 *			так как прогноз на уровне ресторанов строится только для этого горизонта.
 */
	proc fedsql sessref=casauto;
/* ------------ Start. Считаем распределение прогноза на уровне PBO-SKU ----------- */
		create table casuser.percent{options replace=true} as
			select 
				  wplm.*
				, case 
					when wplm.FF = 0 
					then 0 
					else wplm.FF / sum.sum_ff
				end as fcst_pct
			from 
				casuser.fc_w_plm as wplm
			inner join
				(
				select 
					  channel_cd
					, pbo_location_id
					, period_dt
					, sum(FF) as sum_ff
				from 
					casuser.fc_w_plm
				group by 
					  channel_cd
					, pbo_location_id
					, period_dt
				) as sum
					on wplm.pbo_location_id = sum.pbo_location_id 
					and wplm.period_dt = sum.period_dt
					and wplm.channel_cd = sum.channel_cd
		;
/* ------------ End. Считаем распределение прогноза на уровне PBO-SKU ------------- */


/* ------------ Start. Реконсилируем прогноз с PBO до PBO-SKU --------------------- */
		create table casuser.fcst_reconciled{options replace=true} as
			select
				  pct.CHANNEL_CD
				, pct.pbo_location_id
				, pct.product_id
				, pct.period_dt
				, pct.FF as FF_before_rec
				, pct.fcst_pct
				, pct.promo
				, coalesce(vf.pbo_fcst * pct.fcst_pct, pct.FF) as FF
			from
				casuser.percent as pct
			left join 
				&pbo_table. as vf
			on      pct.pbo_location_id = vf.pbo_location_id 
				and pct.period_dt       = vf.sales_dt
				and pct.CHANNEL_CD 		= vf.CHANNEL_CD 
		;
	quit;

	proc casutil;
		droptable casdata="fcst_reconciled" incaslib="mn_short" quiet;
		save incaslib="casuser" outcaslib="mn_short" casdata="fcst_reconciled" casout="fcst_reconciled.sashdat" replace;
		droptable casdata="plm_sales_mask1" incaslib="mn_short" quiet;
		promote casdata="plm_sales_mask1" incaslib="casuser" outcaslib="mn_short";
		save incaslib="mn_short" outcaslib="mn_short" casdata="plm_sales_mask1" casout="plm_sales_mask1.sashdat" replace;
	run;

/* ------------ End. Реконсилируем прогноз с PBO до PBO-SKU ----------------------- */
	

/************************************************************************************
 *		Добавить прогнозы GC от отдела развития										 *
 ************************************************************************************/
/*  TODO: прогнозы GC от отдела развития - добавить к прогнозу GC insert-update */


/************************************************************************************
 *		Применение таблицы постоянных + временных закрытий к прогнозам GC           *
 ************************************************************************************/

/* ------------ Start. Наложение plm на прогноз. Объединяем ST, LT GC fcst ----- */
/*			Здесь идет речь об объединененном прогнозе short term + long term??? 
 */
	proc fedsql sessref=casauto;
		create table casuser.fc_wo_plm_gc{options replace=true} as
			select 
				  coalesce(t1.period_dt,t2.sales_dt) as period_dt
				, coalesce(t1.channel_cd,t2.channel_cd) as channel_cd
				, coalesce(t1.pbo_location_id,t2.pbo_location_id) as pbo_location_id
				, coalesce(t1.ff,t2.gc_fcst) as ff
		from casuser.daily_gc as t1 
		full outer join &gc_table. as t2
			on  t1.period_dt 		= t2.sales_dt 
			and t1.channel_cd		= t2.channel_cd 
			and t1.pbo_location_id	= t2.pbo_location_id
	;
	quit;
/* ------------ End. Наложение plm на прогноз. Объединяем ST, LT GC fcst ------- */


/* ------------ Start. Наложение plm на прогноз GC ----------------------------- */
	proc fedsql sessref=casauto;
		create table casuser.fc_w_plm_gc{options replace=true} as 
		select 
			  main.CHANNEL_CD
			, main.PBO_LOCATION_ID
			, main.period_dt
			, main.FF
		from casuser.fc_wo_plm_gc as main 
		left join casuser.days_pbo_close as clsd
			on  main.PBO_LOCATION_ID = clsd.PBO_LOCATION_ID 
			and main.period_dt		 = clsd.period_dt 
			and main.CHANNEL_CD		 = clsd.CHANNEL_CD
		/* Не должно быть информации о закрытии */
		where clsd.PBO_LOCATION_ID is null 
			and clsd.period_dt is null
			and clsd.CHANNEL_CD is null 																
		;
	quit;
/* ------------ End. Наложение plm на прогноз GC ------------------------------- */


/* ------------ Start. Очистка CAS, сохранение и promote таблиц GC ------------- */

	proc casutil;
		droptable casdata="days_pbo_close" incaslib="mn_short" quiet;
		promote casdata="days_pbo_close" incaslib="casuser" outcaslib="mn_short";
		save incaslib="mn_short" outcaslib="mn_short" casdata="days_pbo_close" casout="days_pbo_close.sashdat" replace;
	run;
	quit;	

	%if &mpPrmt. = Y %then %do;
		proc casutil;
		droptable casdata="&lmvOutTabNameGcSt." incaslib="&lmvOutLibrefGcSt." quiet;
		droptable casdata="&lmvOutTabNameUptSt." incaslib="&lmvOutLibrefUptSt." quiet;
		droptable casdata="&lmvOutTabNamePmixSt." incaslib="&lmvOutLibrefPmixSt." quiet;
		quit;
	%end;

	proc casutil;
		save incaslib="casuser" outcaslib="mn_short" casdata="fc_w_plm" casout="fc_w_plm.sashdat" replace;
		save incaslib="casuser" outcaslib="mn_short" casdata="fc_w_plm_gc" casout="fc_w_plm_gc.sashdat" replace;
		save incaslib="casuser" outcaslib="mn_short" casdata="fc_wo_plm_gc" casout="fc_wo_plm_gc.sashdat" replace;
	run;
	quit;

/* ------------ End. Очистка CAS, сохранение и promote таблиц GC ------------- */


/************************************************************************************
 *		Формирование выходных таблиц в разрезе ДЕНЬ для SHORT-горизонта 			*
 ************************************************************************************/	

/* ------------ Start. Units  ----------------------------------------------------- */
	proc fedsql sessref=casauto;
	create table &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt.{options replace=true} as
		select distinct
			  cast(t1.product_id as integer) as PROD				/* ИД продукта */
			, cast(t1.pbo_location_id as integer) as LOCATION		/* ИД ресторана */
			, t1.period_dt as DATA									/* Дата прогноза или факта (день), ДВОЙКА за перевод разработчикам */
			, 'RUR' as CURRENCY 									/* Валюта, значение по умолчанию RUR */
			
			/*'CORP' as ORG Организация, значение по умолчанию CORP*/
			/* Base-прогноз (заполняется, если в для товар-ПБО-день не было ни одной промо-акции, иначе 0) */
			case when t1.promo=1 then t1.FF else 0 end
			, case 
				when t1.promo = 0 then t1.FF 
				else 0 
			  end as BASE_FCST_UNITS																			
			
			/* Promo-прогноз (заполняется, если в для товар-ПБО-день была одна и более промо-акций, иначе 0 */		
			, case 
				when t1.promo = 1 then t1.FF 
				else 0 
			  end as PROMO_FCST_UNITS 
			 
			/* Total-прогноз (сумма прогноза базового и промо) */ 
			, t1.FF as FINAL_FCST_UNITS																	
			
			/* Overrided-прогноз (в текущей версии всегда равен Total-прогноз) */
			, t1.FF as OVERRIDED_FCST_UNITS
			/* Тригер оверрайда, по умолчанию значение 1 */
			, 1 as OVERRIDE_TRIGGER,																		
			
			/* Base-forecast, RUR, based on net-prices */
			, case 
				when promo = 0 then t1.ff * t2.price_net 
				else 0 
			  end as BASE_FCST_SALE	
			 
			/* Promo-forecast, RUR, based on net-prices */
			, case 
				when promo = 1 then t1.ff * t2.price_net 
				else 0 
			  end as PROMO_FCST_SALE 
			  
			/* Total-forecast, RUR, based on net-prices */ 
			, t1.ff * t2.price_net as FINAL_FCST_SALE
			, t1.ff * t2.price_net as OVERRIDED_FCST_SALE
			
			/* Цена NET из алгоритма расчета цен */
			, t2.price_net as AVG_PRICE 																
			
			from casuser.fcst_reconciled as t1 
			left join &price_table. t2 
				on  t1.product_id		= t2.product_id 
				and t1.pbo_location_id	= t2.pbo_location_id 
				and t1.period_dt		= t2.period_dt
			where t1.channel_cd = 'ALL' 
				and t1.period_dt between &VF_FC_START_DT. and &VF_FC_END_SHORT_DT.
			;
	quit;
/* ------------ End. Units  ------------------------------------------------------- */

/* ------------ Start. GC  -------------------------------------------------------- */
	proc fedsql sessref=casauto;
	create table &lmvOutLibrefGcSt..&lmvOutTabNameGcSt.{options replace=true} as
		select distinct
			  1 as PROD										/* ИД продукта на верхнем уровне (ALL Product значение = 1) */
			, cast(pbo_location_id as integer) as LOCATION	/* ИД ресторана */
			, period_dt as DATA								/* Дата прогноза или факта (день), горе разработчикам */
			, 'RUR' as CURRENCY								/* Валюта значение по умолчанию RUR */
			  /*, 'CORP' as ORG /*– Организация значение по умолчанию CORP*/
			, FF as BASE_FCST_GC							/* Base-forecast */
			, 0 as PROMO_FCST_GC							/* Promo-forecast */
			, FF as FINAL_FCST_GC							/* Total-forecast */
			, FF as OVERRIDED_FCST_GC						/* Overrided-forecast, equals Total */
			, 1 as OVERRIDE_TRIGGER 						/* Триггер оверрайда по умолчанию значение 1*/
		from CASUSER.FC_W_PLM_GC
		where channel_cd = 'ALL' 
			and period_dt between &VF_FC_START_DT. and &VF_FC_END_SHORT_DT.
		;
	quit;
/* ------------ End. GC  ----------------------------------------------------------- */

/* ------------ Start. UPT  -------------------------------------------------------- */
	/* Прогноз UPT рассчитывается из прогноза в ШТ и GC по формуле:
		Прогноз UPT(Товар ПБО день) = Прогноз в ШТ(Товар ПБО день) / Прогноз GC(ПБО день) * 1000
	*/
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptSt..&lmvOutTabNameUptSt.{options replace=true} as
		select distinct
			  cast(t1.prod as integer) as PROD			/* ИД продукта на верхнем уровне (ALL Product значение = 1) */
			, cast(t1.location as integer) as LOCATION	/* ИД ресторана */
			, t1.data as DATA							/* Дата прогноза или факта (день). DATA - ужас перфекциониста.. */
			, 'RUR' as CURRENCY							/* Валюта значение по умолчанию RUR */
			/*'CORP' as ORG /*– Организация значение по умолчанию CORP*/
		
			/* Base-forecast */
			, case 
				when t2.BASE_FCST_GC is not null 
					and abs(t2.BASE_FCST_GC) > 1e-5 
						then t1.BASE_FCST_UNITS / t2.BASE_FCST_GC * 1000 
				else 0
			  end as BASE_FCST_UPT																		
			
			/* Promo-forecast */
			, case 
				when t2.BASE_FCST_GC is not null 
					and abs(t2.BASE_FCST_GC) > 1e-5
						then t1.PROMO_FCST_UNITS / t2.BASE_FCST_GC * 1000
			   else 0
			   end as PROMO_FCST_UPT
			   
			, 1 as OVERRIDE_TRIGGER_D /*– тригер оверрайда по умолчанию значение 1*/
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. as t1 
		left join &lmvOutLibrefGcSt..&lmvOutTabNameGcSt. as t2
			on  t1.location	= t2.location 
			and t1.data		= t2.data
		;
	quit;
/* ------------ End. UPT  ---------------------------------------------------------- */

	%if &mpPrmt. = Y %then %do;
		proc casutil;
		promote casdata="&lmvOutTabNamePmixSt." incaslib="&lmvOutLibrefPmixSt." outcaslib="&lmvOutLibrefPmixSt.";
		save incaslib="&lmvOutLibrefPmixSt." outcaslib="&lmvOutLibrefPmixSt." casdata="&lmvOutTabNamePmixSt." casout="&lmvOutTabNamePmixSt..sashdat" replace;
		
		promote casdata="&lmvOutTabNameGcSt." incaslib="&lmvOutLibrefGcSt." outcaslib="&lmvOutLibrefGcSt.";
		save incaslib="&lmvOutLibrefGcSt." outcaslib="&lmvOutLibrefGcSt." casdata="&lmvOutTabNameGcSt." casout="&lmvOutTabNameGcSt..sashdat" replace;
		
		promote casdata="&lmvOutTabNameUptSt." incaslib="&lmvOutLibrefUptSt." outcaslib="&lmvOutLibrefUptSt.";
		save incaslib="&lmvOutLibrefUptSt." outcaslib="&lmvOutLibrefUptSt." casdata="&lmvOutTabNameUptSt." casout="&lmvOutTabNameUptSt..sashdat" replace;
		quit;
	%end;


/************************************************************************************
 *		Формирование выходных таблиц в разрезе МЕСЯЦ для LONG-горизонта 			*
 ************************************************************************************/	

	%if &mpPrmt. = Y %then %do;
		proc casutil;
			droptable casdata="&lmvOutTabNameGcLt." incaslib="&lmvOutLibrefGcLt." quiet;
			droptable casdata="&lmvOutTabNameUptLt." incaslib="&lmvOutLibrefUptLt." quiet;
			droptable casdata="&lmvOutTabNamePmixLt." incaslib="&lmvOutLibrefPmixLt." quiet;
			droptable casdata="&lmvOutTabNameOutfor." incaslib="&lmvOutLibrefOutfor." quiet;
			droptable casdata="&lmvOutTabNameOutforgc." incaslib="&lmvOutLibrefOutforgc." quiet;
		quit;
		
	%end;
	
	
/* ------------ Start. Pmix ------------------------------------------------------------------ */
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt.{options replace=true} as
		select distinct
			  cast(t1.product_id as integer) as PROD 					/* ИД продукта */
			, cast(t1.pbo_location_id as integer) as LOCATION 			/* ИД ресторана */
			, cast(intnx('month',t1.period_dt,0,'b') as date) as DATA 	/* Дата 1-го числа месяца прогноза или факта), название по-нашему! */
			, 'RUR' as CURRENCY 										/* Валюта, значение по умолчанию RUR*/
			/*'CORP' as ORG /*– Организация, значение по умолчанию CORP*/
			
			/* Base-forecast */
			, sum( case when promo=0 then t1.FF else 0 end ) as BASE_FCST_UNITS 
			, sum( case when promo=0 then t1.ff * t2.price_net else 0 end ) as BASE_FCST_SALE
			/* Promo-forecast */
			, sum( case when promo=1 then t1.FF else 0 end ) as PROMO_FCST_UNITS
			, sum( case when promo=1 then t1.ff * t2.price_net else 0 end ) as PROMO_FCST_SALE
			/* Total-forecast */
			, sum(FF) as FINAL_FCST_UNITS
			, sum(t1.ff * t2.price_net) as FINAL_FCST_SALE 
			/* Overrided-forecast */		
			, sum(FF) as OVERRIDED_FCST_UNITS 
			, sum(t1.ff * t2.price_net) as OVERRIDED_FCST_SALE
			/* Триггер оверрайда, по умолчанию значение 1 */
			, 1 as OVERRIDE_TRIGGER			
			/* Средняя цена. Считается в ETL как отношение прогноз в руб/прогноз в шт в разрезе СКЮ/ПБО*/
			, case 
				when abs(sum(t1.ff)) > 1e-5 
					then sum( t1.ff * t2.price_net ) / sum(t1.ff) 
				else 0 
			  end as AVG_PRICE 
		from CASUSER.FCST_RECONCILED as t1 
		left join &price_table. t2 
			on  t1.product_id		= t2.product_id 
			and t1.pbo_location_id	= t2.pbo_location_id 
			and t1.period_dt		= t2.period_dt
		where t1.channel_cd = 'ALL' 
		group by 1,2,3,4
		;
	quit;
/* ------------ End. Pmix ------------------------------------------------------------------ */

/* ------------ Start. GC ------------------------------------------------------------------ */
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcLt..&lmvOutTabNameGcLt.{options replace=true} as
		select distinct
			, 1 as PROD 												/*– ИД продукта на верхнем уровне (ALL Product, значение = 1)*/
			, cast(t1.pbo_location_id as integer) as LOCATION 			/*– ИД ресторана*/
			, cast(intnx('month',t1.period_dt,0,'b') as date) as DATA 	/*– Дата прогноза или факта (месяц), как вам название? */
			, 'RUR' as CURRENCY 				/*– Валюта, значение по умолчанию RUR*/
												/*'CORP' as ORG /*– Организация, значение по умолчанию CORP*/
			, sum(t1.ff) as BASE_FCST_GC 		/*– базовый прогноз по чекам*/
			, sum(t1.ff) as OVERRIDED_FCST_GC 	/*– базовый прогноз по чекам (плюс логика сохранения оверрайдов)*/
			, 1 as OVERRIDE_TRIGGER 			/*– тригер оверрайда, по умолчанию значение 1*/
		from CASUSER.FC_W_PLM_GC as t1
		where channel_cd = 'ALL'
		group by 1,2,3,4
		;
	quit;
/* ------------ End. GC -------------------------------------------------------------------- */
	
/* ------------ Start. UPT ----------------------------------------------------------------- */
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptLt..&lmvOutTabNameUptLt.{options replace=true} as
		select distinct
			  cast(t1.prod as integer) as PROD 				/*– ИД продукта*/
			, cast(t1.location as integer) as LOCATION 		/*– ИД ресторана*/
			, t1.data as DATA 								/*– Дата прогноза или факта (месяц), название - верх разработческой мысли! */
			, 'RUR' as CURRENCY 							/*– Валюта, значение по умолчанию RUR*/
															/*'CORP' as ORG /*– Организация, значение по умолчанию CORP*/
			, case 
				when t2.BASE_FCST_GC is not null 
					and abs(t2.BASE_FCST_GC) > 1e-5 
						then t1.BASE_FCST_UNITS / t2.BASE_FCST_GC * 1000 
				else 0
			  end as BASE_FCST_UPT /*– базовый прогноз*/
			, case 
				when t2.BASE_FCST_GC is not null 
					and abs(t2.BASE_FCST_GC) > 1e-5 
						then t1.PROMO_FCST_SALE / t2.BASE_FCST_GC * 1000 
				else 0
			  end as PROMO_FCST_UPT /*– промо прогноз*/
			, case 
				when t2.BASE_FCST_GC is not null 
					and abs(t2.BASE_FCST_GC) > 1e-5 
						then t1.FINAL_FCST_UNITS / t2.BASE_FCST_GC * 1000 
			   else 0
			  end as FINAL_FCST_UPT /*– суммарный прогноз */
			, case 
				when t2.BASE_FCST_GC is not null 
					and abs(t2.BASE_FCST_GC) > 1e-5 
						then t1.FINAL_FCST_UNITS / t2.BASE_FCST_GC * 1000 
				else 0
			  end as OVERRIDED_FCST_UPT /*– суммарный прогноз (с учетом логики сохранения оверрайдов) */
			, 1 as OVERRIDE_TRIGGER /*– тригер для сохранения оверрайда, по умолчанию равен 1*/
		from &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt. as t1 
		left join &lmvOutLibrefGcLt..&lmvOutTabNameGcLt. as t2
			on  t1.location	= t2.location 
			and t1.data		= t2.data
		;
	quit;
/* ------------ End. UPT ------------------------------------------------------------------- */

/* ------------ Start. Сохранение результатов на диск и promote таблиц в CAS ----------------- */
	%if &mpPrmt. = Y %then %do;
		proc casutil;
			promote casdata="&lmvOutTabNamePmixLt." incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt.";
			save incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt." casdata="&lmvOutTabNamePmixLt." casout="&lmvOutTabNamePmixLt..sashdat" replace;
			promote casdata="&lmvOutTabNameGcLt." incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt.";
			save incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt." casdata="&lmvOutTabNameGcLt." casout="&lmvOutTabNameGcLt..sashdat" replace;
			promote casdata="&lmvOutTabNameUptLt." incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt.";
			save incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt." casdata="&lmvOutTabNameUptLt." casout="&lmvOutTabNameUptLt..sashdat" replace;
		quit;
	%end;
/* ------------ End. Сохранение результатов на диск и promote таблиц в CAS ------------------- */

%mend rtp_7_out_integration;
