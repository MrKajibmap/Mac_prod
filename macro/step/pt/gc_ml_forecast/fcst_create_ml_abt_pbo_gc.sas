%include '/opt/sas/mcd_config/macro/step/pt/gc_ml_forecast/fcst_create_abt_pbo_gc2.sas';
%include '/opt/sas/mcd_config/config/initialize_global.sas';

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


%macro fcst_create_ml_abt_pbo_gc(data = casuser.DM_TRAIN_TRP_GC	);

	/*
		Макрос создает витрину для моделм машинного обучения.
		Параметры:
		----------
			data : таблица с обессезоненным gc
	*/

	/************************************************************************************
	 * 1. Удаляем временные закрытия +- 3 дня							    	   		*
	 ************************************************************************************/

	/*	Дело в том, что при рассчете целевой переменной используется недельное
	 *		сглаживание на три дня влево и три дня право от рассматриваемой даты.
	 *		Поэтому при временных закрытиях ресторанов целевая переменная может 
	 *		странно себя вести..
	 */

	/* Загружаем таблицу с временными закрытиями */
	proc casutil;
		load data=etl_ia.pbo_close_period(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm and
				channel_cd = 'ALL'
			)
		) casout='pbo_close_period' outcaslib='casuser' replace;	
	run;

	/* Убираем эти интервалы из витрины	 */
	proc fedsql sessref=casauto;
		create table casuser.gc_ml1{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.sales_dt,
				t1.covid_pattern,
				t1.covid_level,
				t1.covid_lockdown,
				t1.sum_trp_log,
				t1.target
			from
				&data. as t1
			left join
				casuser.pbo_close_period as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt <= t2.end_dt + 3 and
				t1.sales_dt >= t2.start_dt - 3
			where
				t2.pbo_location_id is missing
		;	
	quit;

	/* Удаляем промежуточные таблицы */		
	proc casutil;
		droptable casdata="pbo_close_period" incaslib="casuser" quiet;
	run;

	/* 	------------ End. Удаляем временные закрытия +- 3 дня ------------- */	


	/************************************************************************************
	 * 2. Считаем лаги													    	   		*
	 ************************************************************************************/

	/*			Для прогнозирования временных рядов с помощью методов ML одной из
	 *		best practice является добавление лагов продаж, т.е. характеристик продаж
	 *		на истории, как "фичи" в модель ML. Примеры:
	 *			- продажи 35 день назад (желательно кратно 7 дням из-за сильной
	 *					недельной сезонности)
	 *			- средние продажи за квартал за 91 день до даты прогнозы
	 *			- медиана, стандартные отклонения, квантили и пр. 		
	 */

	/* ------------ Start. Считаем медиану и среднее арифметическое ------------------- */
	options nosymbolgen nomprint nomlogic;

	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='gc_ml1',
			caslib = 'casuser', 
			groupBy = {
				{name = 'pbo_location_id'},
				{name = 'channel_cd'}
			}
		},
		series = {{name='target'}},
		interval='day',
		timeId = {name='sales_dt'},
		trimId = "left", 
		code=
			%unquote(%str(%"))			
			%let names=; 																				
			%let minlag=35; 																			
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30 90 180 365;															
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
				%let window=%scan(&window_list.,&ic.); 													
				%let intnm=%rtp_namet(&window);        													
				%let intnm=%sysfunc(strip(&intnm.));
				do t = %eval(&lag+&window) to _length_; 												
					lag_&intnm._avg[t]=mean(%rtp_argt(target,t,%eval(&lag),%eval(&lag+&window-1)));
					lag_&intnm._med[t]=median(%rtp_argt(target,t,%eval(&lag),%eval(&lag+&window-1)));	
				end;
				%let names={name=%tslit(lag_&intnm._avg)}, &names;
				%let names={name=%tslit(lag_&intnm._med)}, &names;
		
			%end; 																						
			
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))  																		
		,
		arrayOut={
			table={name='lag_abt1', replace=true, caslib='casuser'},
			arrays={&names}
		}
	;
	run;
	quit;
	/* ------------ End. Считаем медиану и среднее арифметическое --------------------- */

	/* ------------ Start. Считаем стандартное отклонение ------------------- */
	options nosymbolgen nomprint nomlogic;

	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='gc_ml1',
			caslib = 'casuser', 
			groupBy = {
				{name = 'pbo_location_id'},
				{name = 'channel_cd'}
			}
		},
		series = {{name='target'}},
		interval='day',
		timeId = {name='sales_dt'},
		trimId = "left", 
		code=
			%unquote(%str(%"))			
			%let names=; 																				
			%let minlag=35; 																			
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30 90 180 365;															
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
				%let window=%scan(&window_list.,&ic.); 													
				%let intnm=%rtp_namet(&window);        													
				%let intnm=%sysfunc(strip(&intnm.));
				do t = %eval(&lag+&window) to _length_; 												
					lag_&intnm._std[t]=std(%rtp_argt(target,t,%eval(&lag),%eval(&lag+&window-1)));
				end;
				%let names={name=%tslit(lag_&intnm._std)}, &names;
			%end; 																						
			
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))  																		
		,
		arrayOut={
			table={name='lag_abt2', replace=true, caslib='casuser'},
			arrays={&names}
		}
	;
	run;
	quit;
	/* ------------ End. Считаем стандартное отклонение --------------------- */	

	/* ------------ Start. Считаем процентили ------------------- */
	options nosymbolgen nomprint nomlogic;

	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='gc_ml1',
			caslib = 'casuser', 
			groupBy = {
				{name = 'pbo_location_id'},
				{name = 'channel_cd'}
			}
		},
		series = {{name='target'}},
		interval='day',
		timeId = {name='sales_dt'},
		trimId = "left", 
		code=
			%unquote(%str(%"))			
			%let names=; 																				
			%let minlag=35; 																			
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30 90 180 365;															
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
				%let window=%scan(&window_list.,&ic.); 													
				%let intnm=%rtp_namet(&window);        													
				%let intnm=%sysfunc(strip(&intnm.));
				do t = %eval(&lag+&window) to _length_; 												
					lag_&intnm._pct10[t]=pctl(10,%rtp_argt(target,t,%eval(&lag),%eval(&lag+&window-1))) ;
					lag_&intnm._pct90[t]=pctl(90,%rtp_argt(target,t,%eval(&lag),%eval(&lag+&window-1))) ;
				end;
				%let names={name=%tslit(lag_&intnm._pct10)}, &names;
				%let names={name=%tslit(lag_&intnm._pct90)}, &names;

			%end; 																						
			
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))  																		
		,
		arrayOut={
			table={name='lag_abt3', replace=true, caslib='casuser'},
			arrays={&names}
		}
	;
	run;
	quit;
	
	options symbolgen mprint mlogic;
	/* ------------ End. Считаем процентили --------------------- */

	/* ------------ Start. Добавляем лаги в витрину ------------------- */
	proc fedsql sessref=casauto;
		create table casuser.gc_ml2{options replace=true} as
			select				
				abt.channel_cd,
				abt.pbo_location_id,
				abt.sales_dt,
				abt.covid_pattern,
				abt.covid_level,
				abt.covid_lockdown,
				abt.sum_trp_log,
				abt.target,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t2.lag_halfyear_std,
				t2.lag_month_std,
				t2.lag_qtr_std,
				t2.lag_week_std,
				t2.lag_year_std,
				t3.lag_halfyear_pct10,		 
				t3.lag_halfyear_pct90,		 
				t3.lag_month_pct10,
				t3.lag_month_pct90,
				t3.lag_qtr_pct10,	
				t3.lag_qtr_pct90,	
				t3.lag_week_pct10,	
				t3.lag_week_pct90,	
				t3.lag_year_pct10,	
				t3.lag_year_pct90				
			from
				casuser.gc_ml1 as abt
			left join
				casuser.lag_abt1 as t1
			on
				abt.channel_cd = t1.channel_cd and
				abt.pbo_location_id = t1.pbo_location_id and
				abt.sales_dt = t1.sales_dt
			left join
				casuser.lag_abt2 as t2
			on
				abt.channel_cd = t2.channel_cd and
				abt.pbo_location_id = t2.pbo_location_id and
				abt.sales_dt = t2.sales_dt
			left join
				casuser.lag_abt3 as t3
			on
				abt.channel_cd = t3.channel_cd and
				abt.pbo_location_id = t3.pbo_location_id and
				abt.sales_dt = t3.sales_dt
	;
	quit;
	/* ------------ End. Добавляем лаги в витрину --------------------- */

	/* Удаляем промежуточные таблицы */
	proc casutil;
		droptable casdata="lag_abt1" incaslib="casuser" quiet;
		droptable casdata="lag_abt2" incaslib="casuser" quiet;
		droptable casdata="lag_abt3" incaslib="casuser" quiet;
		droptable casdata="gc_ml1" incaslib="casuser" quiet;
	run;

%mend;


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
			select
				*
			from
				&data.
			where
				sales_dt <= &last_history_date.
		;
	quit;
	
	
	/* Стираем результирующую таблицу с обученной моделью */
	proc casutil;
		droptable casdata="gc_ml_model" incaslib="casuser" quiet;
	run;
	
	/* Обучение модели */
	proc forest data=casuser.train
		&default_hyper_params.;
		input
			sum_trp_log
			covid_pattern
			lag_halfyear_avg
			lag_halfyear_med
			lag_month_avg
			lag_month_med
			lag_qtr_avg
			lag_qtr_med
			lag_week_avg
			lag_week_med
			lag_year_avg
			lag_year_med
			lag_halfyear_std
			lag_month_std
			lag_qtr_std
			lag_week_std
			lag_year_std
			lag_halfyear_pct10		 
			lag_halfyear_pct90		 
			lag_month_pct10
			lag_month_pct90
			lag_qtr_pct10	
			lag_qtr_pct90	
			lag_week_pct10	
			lag_week_pct90	
			lag_year_pct10	
			lag_year_pct90
				/ level = interval;
		input
			covid_lockdown
			covid_level
			 / level = nominal;
		id pbo_location_id sales_dt;
		target target / level = interval;
		savestate rstore=casuser.gc_ml_model;
		;
	run;
	
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
				t1.new_RECEIPT_QTY, 
				t1.RECEIPT_QTY, 
				t1.SALES_DT, 
				t1.WOY, 
				t1.WBY, 
				t1.DOW, 
				t1.AVG_of_Detrend_sm_multi, 
				t1.AVG_of_Detrend_multi, 
				t1.AVG_of_Detrend_sm_multi_WBY, 
				t1.AVG_of_Detrend_multi_WBY, 
				t1.Detrend_sm_multi, 
				t1.Detrend_multi, 
				t1.Deseason_sm_multi as probably_target, 
				t1.Deseason_multi, 
				t1.COVID_pattern, 
				t1.COVID_lockdown, 
				t1.COVID_level,
				t2.p_target,
				t2.target,
				(t2.p_target * t1.Detrend_multi) AS gc_predict								
			from 
				casuser.TRAIN_ABT_TRP_GC t1 								
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
	data nac.&result.;
		set casuser.&result.;
	run;
	
	/* Удаляем промежуточные таблицы */
	proc casutil;
		droptable casdata="train" incaslib="casuser" quiet;
		droptable casdata="scoring" incaslib="casuser" quiet;
		droptable casdata="gc_ml_model" incaslib="casuser" quiet;
		droptable casdata="gc_ml_target_predict" incaslib="casuser" quiet;
		droptable casdata="&result." incaslib="casuser" quiet;
	run;

	
%mend;

/* Избавляемся от сезонности GC */
%fcst_create_abt_pbo_gc(
	  mpMode		  = gc
	, mpOutTableDmVf  = casuser.DM_TRAIN_TRP_GC	
	, mpOutTableDmABT = casuser.TRAIN_ABT_TRP_GC
);

/* Собираем лаги */
%fcst_create_ml_abt_pbo_gc(data = casuser.DM_TRAIN_TRP_GC);


/* Декабрь */
%create_forecasts(
	data = casuser.gc_ml2,
	last_history_date = date '2020-11-27',
	last_forecast_day = date '2020-12-31',
	result = gc_ml_december_new
)

/* Январь */
%create_forecasts(
	data = casuser.gc_ml2,
	last_history_date = date '2020-12-25',
	last_forecast_day = date '2021-01-31',
	result = gc_ml_january_new
)

/* Март */
%create_forecasts(
	data = casuser.gc_ml2,
	last_history_date = date '2021-02-26',
	last_forecast_day = date '2021-03-31',
	result = gc_ml_march_new
)

/* Май */
%create_forecasts(
	data = casuser.gc_ml2,
	last_history_date = date '2021-04-30',
	last_forecast_day = date '2021-05-31',
	result = gc_ml_may_new
)


/* Удаляем промежуточные таблицы */
proc casutil;
	droptable casdata="gc_ml2" incaslib="casuser" quiet;
run;

