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
	
/* 	%let data = casuser.dm_gc_ml; */
/* 	%let last_history_date = date '2020-12-25'; */
/* 	%let last_forecast_day = date '2021-01-31'; */
/* 	%let result = gc_ml_dec_MD_v2; */

/* 	proc contents data=&data; */
/* 	quit; */
	%let _timer_start = %sysfunc(time());
	%put WARNING: [%sysfunc(putn(&_timer_start.,time8.))] starting with &result;

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
		droptable casdata="&result._MOD" incaslib="casuser" quiet;
		droptable casdata="&result._MOD" incaslib="max_casl" quiet;
		droptable casdata="&result._varimp" incaslib="casuser" quiet;
		droptable casdata="&result._varimp" incaslib="max_casl" quiet;
	run;
	
	%let _time = %sysfunc(time());
	%put WARNING: [%sysfunc(putn(&_time.,time8.))] train start;
	
	/* Обучение модели */
	proc forest data=casuser.train
		&default_hyper_params.;
		input &mv_interval_feature_list. / level = interval;
		input &mv_nominal_feature_list. / level = nominal;
		id pbo_location_id sales_dt;
		target target / level = interval;
		savestate rstore=casuser.&result._MOD;
		ods output variableimportance = casuser.&result._varimp
		;
	run;
	
	%let _time = %sysfunc(time());
	%put WARNING: [%sysfunc(putn(&_time.,time8.))] train complete;

	%put &=result;
	proc casutil;
		promote incaslib='casuser' 	casdata="&result._MOD" 
			outcaslib="max_casl" casout="&result._MOD";
		promote incaslib='casuser' 	casdata="&result._varimp" 
			outcaslib="max_casl" casout="&result._varimp";
/* 		save incaslib='casuser' casdata='gc_ml3' outcaslib="&outp_lib." casout="&outp_dm_nm."; */
	run;

	
	/* Создаем скоринговую витрину */
	/* 	морозим последнее значение */
	proc fedsql sessref=casauto;
		create table casuser.scoring1{options replace=true} as
			select
				pbo_location_id
				,MD_lag_7_med as MD_lag_7_med_last
				,MD_lag_7_avg as MD_lag_7_avg_last
			from
				&data.
			where
				(sales_dt = &last_history_date.) 		;
	quit;

	proc fedsql sessref=casauto;
		create table casuser.scoring {options replace=true} as
			select
				t1.*
				,MD_lag_7_med_last
				,MD_lag_7_avg_last
				,&last_history_date as d1
				,&last_forecast_day as d2
				
			from
				&data. as  t1
			left join casuser.scoring1 t2 on t1.pbo_location_id = t2.pbo_location_id
			where
/* 				(sales_dt > &last_history_date.) and */
				(sales_dt <= &last_forecast_day)
		;
	quit;
	
	data casuser.scoring;
		set casuser.scoring;
		if	(sales_dt > d1) then do; 
			MD_lag_7_med = MD_lag_7_med_last;
			MD_lag_7_avg = MD_lag_7_avg_last;
		end; 
		drop d1 d2
	;run;
	
	/* Скоринг */
	proc casutil;
		droptable casdata="gc_ml_target_predict" incaslib="casuser" quiet;
	run;
	proc astore;
		score data=casuser.scoring
		copyvars=(_all_)
		rstore=max_casl.&result._MOD
		out=casuser.gc_ml_target_predict;
	quit;

	%put &=result;
	proc casutil;
		droptable casdata="&result." incaslib="casuser" quiet;
	run;
	/* Возвращаем сезонность */
	proc fedsql sessref=casauto;
/* 		drop table casuser.&result.; */
/* 	quit; */
		create table casuser.&result.{options replace=true} as 
			select 
				t1.PBO_LOCATION_ID, 
				t1.CHANNEL_CD, 
				t1.SALES_DT, 
				t2.p_target,
				t2.target
				,t1.AVG_KOEF_DOY
				,t1.Detrend_multi
				,t2.target_p90
				,t2.target_p10
				,t1.RECEIPT_QTY as gc_fact_control
				,((t2.p_target*(t2.target_p90 - t2.target_p10)+t2.target_p10) * t1.Detrend_multi) * t1.AVG_KOEF_DOY AS gc_predict
				,((t2.target*(t2.target_p90 - t2.target_p10)+t2.target_p10) * t1.Detrend_multi) * t1.AVG_KOEF_DOY AS gc_actual

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
/* 				(t1.sales_dt > &last_history_date.) and */
				(t1.sales_dt <= &last_forecast_day) and
				t1.channel_cd = 'ALL'
		;
	quit;
	
	/* Сохраняем результат на диск */
/* 	data nac.&result.; */
/* 		set casuser.&result.; */
/* 	run; */
	proc casutil;
		droptable  incaslib='MAX_CASL' 
				casdata="&result" quiet;
	run;
		promote incaslib='casuser' 
				casdata="&result" 
				outcaslib="MAX_CASL" 
				casout="&result";
	run;	
	quit;

	/* Удаляем промежуточные таблицы */
	proc casutil;
		droptable casdata="train" incaslib="casuser" quiet;
		droptable casdata="scoring" incaslib="casuser" quiet;
		droptable casdata="gc_ml_model" incaslib="casuser" quiet;
		droptable casdata="gc_ml_target_predict" incaslib="casuser" quiet;
		droptable casdata="&result." incaslib="casuser" quiet;
	run;
	%let _timer_end = %sysfunc(time());
	%put WARNING: [%sysfunc(putn(&_timer_end.,time8.))] finished with &result.;
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

/* %create_abt(casuser.DM_TRAIN_TRP_GC); */

/* Гиперпараметры модели */
%let default_hyper_params = seed=12345 	
	loh=0 
/* 	binmethod=QUANTILE  */
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

%let autotune_hyper_params = seed=12345 loh=0 
	binmethod=QUANTILE 
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
/* %include "/opt/sas/mcd_config/macro/step/pt/gc_ml_fcst_frantsev/init_ml_features.sas"; */
%init_ml_features;

/* Декабрь */
%create_forecasts(
	data = casuser.dm_gc_ml2,
	last_history_date = date '2020-11-27',
	last_forecast_day = date '2020-12-31',
	result = gc_ml_dec_MD_v2
);

/* Январь */
%create_forecasts(
	data = casuser.dm_gc_ml2,
	last_history_date = date '2020-12-25',
	last_forecast_day = date '2021-01-31',
	result = gc_ml_jan_MD_v2
);

/* Март */
%create_forecasts(
	data = casuser.dm_gc_ml2,
	last_history_date = date '2021-02-26',
	last_forecast_day = date '2021-03-31',
	result = gc_ml_mar_MD_v2
);

/* Май */
%create_forecasts(
	data = casuser.dm_gc_ml2,
	last_history_date = date '2021-04-30',
	last_forecast_day = date '2021-05-31',
	result = gc_ml_may_MD_v2
);

/* Оля */
%create_forecasts(
	data = casuser.dm_gc_ml2,
	last_history_date = date '2021-09-24',
	last_forecast_day = date '2021-10-31',
	result = gc_ml_OCT_MD_v2
);

/* TODO: объединять 4 таблицы с прогнозом в одну */

/* Удаляем промежуточные таблицы */
/* proc casutil; */
/* 	droptable casdata="gc_ml3" incaslib="casuser" quiet; */
/* run; */


