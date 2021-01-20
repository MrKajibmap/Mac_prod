%include "/opt/sas/mcd_config/config/initialize_global.sas"; 
%macro longterm_process;
	%let etls_jobName = longterm_process;
	%etl_job_start;

		/*Переопределение макропеременных*/
		%let VF_FC_HORIZ					=  52;
		%let VF_FC_START_DT 				= date'2020-09-14';
		%let VF_FC_START_DT_SAS				= %sysfunc(inputn(%scan(%bquote(date'2020-09-14'),2,%str(%')),yymmdd10.));
		%let VF_FC_START_MONTH_SAS 			= %sysfunc(intnx(month,&VF_FC_START_DT_SAS,0,b));
		%let VF_HIST_END_DT 				= %sysfunc(intnx(day,&VF_FC_START_DT_SAS,-1),yymmddd10.);	
		%let VF_HIST_END_DT_SAS				= %sysfunc(inputn(&VF_HIST_END_DT.,yymmdd10.));		
		%let VF_FC_END_DT 					= %sysfunc(intnx(day,&VF_FC_START_DT_sas,7*(&VF_FC_HORIZ-1)),yymmddd10.);		
		%let VF_FC_AGG_END_DT 				= %sysfunc(intnx(day,&VF_FC_START_DT_sas,7*&VF_FC_HORIZ-1),yymmddd10.);
		%let VF_FC_AGG_END_DT_SAS 			= %sysfunc(intnx(day,&VF_FC_START_DT_sas,7*&VF_FC_HORIZ-1));
		%let VF_HIST_START_DT 				= date'2017-01-02';
		%let VF_HIST_START_DT_SAS			= %sysfunc(inputn(%scan(%bquote(&VF_HIST_START_DT),2,%str(%')),yymmdd10.));
		%let VF_PMIX_ID						= 1ef9c222-17c4-477b-9667-a3ac07320c4e;
		%let VF_PBO_ID 						= c27c04d6-8789-4b2a-af8d-b2f751dc8cd0;
		/* 1. загрузка данных в CAS */
		/* Значения параметров mpEvents mpEventsMkup выставлены по умолчанию,
		*  если необходимо, значения можно изменить, но нужно учитывать, что таблица 
		*  параметра mpEventsMkup используется в:
		*  		vf_train_week_profile_gc(параметр mpInEventsMkup=)
		*  		vf_month_aggregation(параметр mpInEventsMkup=) */
		%vf_load_data(mpEvents=dm_abt.events,mpEventsMkup=dm_abt.events_mkup);
		/*2.  */
		%vf_restore_sales_gc;
		/*3. Загрузка таблицы pbo_sal_abt */
		/*	Значения параметров выставлены по умолчанию - если необходимо, их можно изменить, учитывая, 
		*	что они используются в vf_prepare_ts_abt_pmix(соответствующие параметры) */
		%vf_prepare_ts_abt_pbo(mpPboSalAbt=dm_abt.pbo_sal_abt,
									mpPromoW1=casuser.promo_w1,
									mpPromoD=casuser.promo_d, 
									mpPboSales=casuser.TS_pbo_sales,
									mpWeatherW=casuser.weather_w );

		/*4. Запуск VF-проекта на основе pbo_sal_abt*/
		/* Необходимо указать ИМЯ VF-проекта. Например, pbo_sales_v2*/
		%vf_run_project(mpProjectName=pbo_sales_v1);
		/* Загрузка таблицы pmix_sal_abt*/
		/* Необходимо указать ИМЯ VF-проекта в параметре mpProjectName, построенного на mpPboSalAbt=casuser.pbo_sal_abt */
		%vf_prepare_ts_abt_pmix(mpVfPboProjName=pbo_sales_v1,
									mpPmixSalAbt=dm_abt.pmix_sal_abt,
									mpPromoW1=casuser.promo_w1,
									mpPromoD=casuser.promo_d,
									mpPboSales=casuser.TS_pbo_sales,
									mpWeatherW=casuser.weather_w);
		/*5. Запуск VF-проекта на основе pmix_sal_abt*/
		/* Необходимо указать ИМЯ VF-проекта в параметре mpProjectName. Например, pmix_sales_v1*/
		%vf_run_project(mpProjectName=pmix_sales_v2);
		/*6. Создание модели недельного профиля для разбивки по дням и переагрегации недель до месяцев*/
		%vf_train_week_profile(mpOutWpGc=dm_abt.wp_gc); 
		/*7. Создание модели недельного профиля для разбивки GC по дням и переагрегации недель до месяцев*/
		/*%vf_train_week_profile_gc(mpInEventsMkup=dm_abt.events_mkup,
										 mpNnetWp=casuser.nnet_wp1,
										 mpPromo_W=casuser.promo_w 
										 ); */
		/* Применение недельного профиля - переразбивка прогноза pmix до разреза месяц-флаг промо, прогноза gc - до разреза месяц*/
		/* Параметры mpPrmt=Y/N (Будут ли указанные таблицы запромоучены) */
		/* Параметр mpInWpGc = таблица, формируемая в vf_train_week_profile(параметр mpOutWpGc); */
		/*8. Необходимо указать ИМЕНА VF-проектов в параметрах mpVfPmixProjName, mpVfPboProjName*/
		%vf_month_aggregation(mpVfPmixProjName=pmix_sales_v2,
									mpVfPboProjName=pbo_sales_v1,
									mpInEventsMkup=dm_abt.events_mkup,
									mpOutPmix=dm_abt.plan_pmix_month,
									mpOutGc=dm_abt.plan_gc_month, 
									mpOutOutforgc=casuser.TS_OUTFORGC,
									mpOutOutfor=casuser.TS_OUTFOR, 
									mpOutNnetWp=casuser.nnet_wp1,
									mpInWpGc=dm_abt.wp_gc,
									mpPrmt=Y);	
									
		/* 9. Выгрузка данных в CSV + в DP */
		*%vf_6_out_integration(mpVfPmixProjName=pmix_sales_v1,
									mpVfPboProjName=pbo_sales_v1,
									mpMLPmixTabName=DM_ABT.PLAN_PMIX_MONTH,
									mpInEventsMkup=dm_abt.events_mkup,
									mpInWpGc=dm_abt.wp_gc,
									mpOutPmixLt=casuser.plan_pmix_month,
									mpOutGcLt=casuser.plan_gc_month, 
									mpOutUptLt=casuser.plan_upt_month, 
									mpOutOutforgc=casuser.TS_OUTFORGC,
									mpOutOutfor=casuser.TS_OUTFOR, 
									mpOutNnetWp=public.nnet_wp1,
									mpPrmt=N) ;

		/*9. Экспорт в CSV */					
		*%dp_export_csv(mpInput=dm_abt.plan_pmix_month
								, mpTHREAD_CNT=30
								, mpPath=/data/dm_rep/);						
		*%dp_export_csv(mpInput=dm_abt.plan_gc_month
								, mpTHREAD_CNT=30
								, mpPath=/data/dm_rep/);	
								
		/*10. Запуск загрузки данных в DP */
		*%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoM);
		*%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoM);
				
	%etl_job_finish;

%mend longterm_process;
%longterm_process;