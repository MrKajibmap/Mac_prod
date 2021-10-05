options casdatalimit=20G;

libname nac "/data/MN_CALC"; /* Директория в которую складываем результат */

%macro assign;
	%let casauto_ok = %sysfunc(SESSFOUND ( casauto)) ;
	%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
	 cas casauto SESSOPTS=(TIMEOUT=31536000);
	 caslib _all_ assign;
	%end;
%mend;

%assign

%macro create_forecasts(
	data = casuser.gc_ml2,
	last_history_date = date '2020-11-27',
	last_forecast_day = date '2020-12-31',
	result = gc_ml_december
);
	/*
		Макрос берет витрину data, делит ее на train 
			и scoring, обучает модель, прогнозирует 
			целевую переменную и возвращает сезонность.
		Параметры:
		----------
			* data : витрина с лагами
			* last_history_date : послдений день истории
			* last_forecast_day : последний день скоринговой витрины
			* result : название таблицы куда класть результат
	
	*/


	/* Создание обучающей выбоки */
	proc fedsql sessref=casauto;
		create table casuser.train{options replace=true} as
			select *
			from &data.
			where sales_dt <= &last_history_date.
		;
	quit;
	
	
	/* Стираем результирующую таблицу с обученной моделью */
	proc casutil;
		droptable casdata="gc_ml_model" incaslib="casuser" quiet;
	run;
	
	/* Обучение модели */
	proc forest data=casuser.train
		&default_hyper_params.;
		input &mv_interval_feature_list. / level = interval;
		input &mv_nominal_feature_list. / level = nominal;
		id pbo_location_id sales_dt;
		target target / level = interval;
		savestate rstore=casuser.gc_ml_model;
		ods output variableimportance = casuser.varimp_&result.
		;
	run;

/* 	proc gradboost data=casuser.train */
/* 		seed=12345 */
/* 		lasso=5 */
/* 		learningrate=0.255 */
/* 		ntrees=40 */
/* 		numbin=60 */
/* 		ridge=5 */
/* 		samplingrate=1 */
/* 		vars_to_try=27 */
/* 		maxdepth=21 */
/* 		; */
/* 		input &mv_interval_feature_list. / level = interval; */
/* 		input &mv_nominal_feature_list. / level = nominal; */
/* 		id pbo_location_id sales_dt; */
/* 		target target / level = interval; */
/* 		savestate rstore=casuser.gc_ml_model; */
/* 		ods output variableimportance = casuser.varimp_&result. */
/* 		; */
/* 	run; */
	
	/* Создаем скоринговую витрину */
	proc fedsql sessref=casauto;
		create table casuser.scoring{options replace=true} as
			select
				*
			from
				&data.
			where
				(sales_dt > &last_history_date.) and
				(sales_dt <= &last_forecast_day)
		;
	quit;
	
	/* Скоринг */
	proc astore;
		score data=casuser.scoring
		copyvars=(_all_)
		rstore=casuser.gc_ml_model
		out=casuser.gc_ml_target_predict;
	quit;

	/* Возвращаем сезонность */
	proc fedsql sessref=casauto;
		create table casuser.&result.{options replace=true} as 
			select 
				t1.PBO_LOCATION_ID, 
				t1.CHANNEL_CD, 
				t1.SALES_DT, 
				t2.p_target,
				t2.target,
/* Attention! Hardcode! */			
/* 				(t2.p_target * t1.Detrend_multi) AS gc_predict	 */
				(t2.p_target * t1.Detrend_multi) * t1.AVG_KOEF_DOY AS gc_predict,
				(t2.target * t1.Detrend_multi) * t1.AVG_KOEF_DOY AS gc_actual
			from 
/* Attention! Hardcode! */
				casuser.TRAIN_ABT_TRP_GC_MP t1 								
			left join 
				casuser.gc_ml_target_predict as t2
			on
				(t1.CHANNEL_CD = t2.CHANNEL_CD) and 
				(t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID) and
				(t1.SALES_DT = t2.SALES_DT)
			where
				(t1.sales_dt > &last_history_date.) and
				(t1.sales_dt <= &last_forecast_day) and
				t1.channel_cd = 'ALL'
		;
	quit;
	
	/* Сохраняем результат на диск */
/* 	data nac.&result.; */
/* 		set casuser.&result.; */
/* 	run; */
	
	/* Удаляем промежуточные таблицы */
	proc casutil;
		droptable casdata="train" incaslib="casuser" quiet;
		droptable casdata="scoring" incaslib="casuser" quiet;
		droptable casdata="gc_ml_model" incaslib="casuser" quiet;
		droptable casdata="gc_ml_target_predict" incaslib="casuser" quiet;
/* 		droptable casdata="&result." incaslib="casuser" quiet; */
	run;

	
%mend create_forecasts;

%macro create_abt(data);
	%if not %sysfunc(exist(&data.)) %then %do;
/* 		%include '/opt/sas/mcd_config/macro/step/pt/gc_ml_forecast/fcst_create_abt_pbo_gc2.sas'; */

		/* Избавляемся от сезонности GC */
		%fcst_create_abt_pbo_gc_mp(
			  mpMode		  = gc
			, mpOutTableDmVf  = casuser.DM_TRAIN_TRP_GC	
			, mpOutTableDmABT = casuser.TRAIN_ABT_TRP_GC
		);
		
		/* Собираем лаги */
		%fcst_create_ml_abt_AF(
			inp_dm = casuser.DM_TRAIN_TRP_GC
			,outp_lib = casuser
			,outp_dm = dm_gc_ml
		);
	%end;
%mend create_abt;

%create_abt(casuser.DM_TRAIN_TRP_GC);

/* Гиперпараметры модели */
%let default_hyper_params = seed=12345 loh=0 binmethod=QUANTILE 
	 maxbranch=2 
     assignmissing=useinsearch 
	 minuseinsearch=5
     ntrees=20
     maxdepth=20
     inbagfraction=0.7
     minleafsize=5
     numbin=100
     printtarget
;

%let autotune_hyper_params = seed=12345 loh=0 binmethod=QUANTILE 
	 maxbranch=2 
     assignmissing=useinsearch 
	 minuseinsearch=5
     ntrees=40
     maxdepth=20
     inbagfraction=0.9
     minleafsize=5
     numbin=40
	 vars_to_try=7
     printtarget
;

/* Объявляем макропеременные со списком фичей */
%include "/opt/sas/mcd_config/macro/step/pt/gc_ml_fcst_frantsev/init_ml_features.sas";
%init_ml_features();

/* Декабрь */
%create_forecasts(
	data = casuser.dm_gc_ml,
	last_history_date = date '2020-11-27',
	last_forecast_day = date '2020-12-31',
	result = gc_ml_dec_AF_v2
)

/* Январь */
%create_forecasts(
	data = casuser.dm_gc_ml,
	last_history_date = date '2020-12-25',
	last_forecast_day = date '2021-01-31',
	result = gc_ml_jan_AF_v2
)

/* Март */
%create_forecasts(
	data = casuser.dm_gc_ml,
	last_history_date = date '2021-02-26',
	last_forecast_day = date '2021-03-31',
	result = gc_ml_mar_AF_v2
)

/* Май */
%create_forecasts(
	data = casuser.dm_gc_ml,
	last_history_date = date '2021-04-30',
	last_forecast_day = date '2021-05-31',
	result = gc_ml_may_AF_v2
)

/* TODO: объединять 4 таблицы с прогнозом в одну */

/* Удаляем промежуточные таблицы */
/* proc casutil; */
/* 	droptable casdata="gc_ml3" incaslib="casuser" quiet; */
/* run; */


