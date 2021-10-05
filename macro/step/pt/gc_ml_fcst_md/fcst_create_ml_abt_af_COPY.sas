options casdatalimit=20G;

libname nac "/data/MN_CALC"; /* Директория в которую складываем результат */
%let r_path = /opt/sas/mcd_config/macro/step/pt/gc_ml_fcst_frantsev;


%macro assign;
	%let casauto_ok = %sysfunc(SESSFOUND ( casauto)) ;
	%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
	 cas casauto SESSOPTS=(TIMEOUT=31536000);
	 caslib _all_ assign;
	%end;
%mend;

%assign

%macro fcst_create_ml_abt_AF(inp_dm = casuser.DM_TRAIN_TRP_GC,
							outp_lib = casuser,
							outp_dm_nm = DM_TRAIN_TRP_GC_ML);
%mend fcst_create_ml_abt_AF;
/* debug */
/* %let inp_dm = casuser.DM_TRAIN_TRP_GC_MP; */
%let inp_dm = casuser.TRAIN_ABT_TRP_GC_MP;
%let outp_lib = casuser;
%let outp_dm_nm = DM_GC_ML;
/* end debug */

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
				t1.target_deseason_ML as target
			from &inp_dm. as t1
			left join casuser.pbo_close_period as t2
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
	
	proc means data = casuser.gc_ml1 noprint;
		by pbo_location_id channel_cd;
		var target;
		output out=casuser.TARGET_PCT10_90 p10= p90= / autoname;
	run;

/* 	на случай коротких рядов, чтобы не было деления на 0  */
	data casuser.TARGET_PCT10_90;
		set casuser.TARGET_PCT10_90;
		if (target_p10 = target_p90) then do;
			target_p10=0;
			target_p90=1;
		end;
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.gc_ml2{options replace=true} as
		select	
			t1.channel_cd,
			t1.pbo_location_id,
			t1.sales_dt,
			t1.covid_pattern,
			t1.covid_level,
			t1.covid_lockdown,
			t1.sum_trp_log,
			t1.target as target_init
			,(t1.target - t2.target_p10) / (t2.target_p90 - t2.target_p10) as target
			,t2.target_p10
			,t2.target_p90
		from casuser.gc_ml1 as t1
		inner join casuser.TARGET_PCT10_90 as t2
			on t1.pbo_location_id = t2.pbo_location_id
			and t1.channel_cd = t2.channel_cd

	;quit;

	data casuser.gc_ml1; set casuser.gc_ml2;

	proc casutil;
		droptable casdata="gc_ml2" incaslib="casuser" quiet;
	run;


	%macro qqq1;
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

			do t = 1 to _length_; 																				
				MD_lag_7_avg[t]=  mean(%rtp_argt(target,t,1,7));
				MD_lag_7_med[t]=median(%rtp_argt(target,t,1,7));
			end;
			%let names={name=%tslit(MD_lag_7_avg)}, &names;
			%let names={name=%tslit(MD_lag_7_med)}, &names;
			
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
	%mend qqq1;
	%qqq1;

	
	/* ------------ Start. Считаем погоду ------------------- */
	%macro qqq2;
	options nosymbolgen nomprint nomlogic;

	%let lib = mn_short;
	%if not %sysfunc(exist(&lib..weather)) %then %do;
		%let lib = casuser;
		data &lib..weather;
			set ia.ia_weather;
			report_dt = datepart(report_dt);
		run;
	%end;

	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='weather',
			caslib = "&lib.", 
			groupBy = {
				{name = 'pbo_location_id'}
			}
		},
		series = {{name='temperature'}},
		interval='day',
		timeId = {name='report_dt'},
		trimId = "left", 
		code=
			%unquote(%str(%"))			
			%let names=; 																				
			%let minlag=35; 																			
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30;															
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
				%let window=%scan(&window_list.,&ic.); 													
				%let intnm=%rtp_namet(&window);        													
				%let intnm=%sysfunc(strip(&intnm.));
				do t = %eval(&lag+&window) to _length_; 												
					temp_&intnm._std[t]=std(%rtp_argt(temperature,t,%eval(&lag),%eval(&lag+&window-1)));
					temp_&intnm._avg[t]=mean(%rtp_argt(temperature,t,%eval(&lag),%eval(&lag+&window-1)));
				end;
				%let names={name=%tslit(temp_&intnm._std)}, &names;
				%let names={name=%tslit(temp_&intnm._avg)}, &names;
			%end; 																						
			
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))  																		
		,
		arrayOut={
			table={name='lag_abt4', replace=true, caslib='casuser'},
			arrays={&names}
		}
	;
	run;
	quit;

	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='weather',
			caslib = "&lib.", 
			groupBy = {
				{name = 'pbo_location_id'}
			}
		},
		series = {{name='precipitation'}},
		interval='day',
		timeId = {name='report_dt'},
		trimId = "left", 
		code=
			%unquote(%str(%"))			
			%let names=; 																				
			%let minlag=35; 																			
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30;															
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
				%let window=%scan(&window_list.,&ic.); 													
				%let intnm=%rtp_namet(&window);        													
				%let intnm=%sysfunc(strip(&intnm.));
				do t = %eval(&lag+&window) to _length_; 												
					prec_&intnm._std[t]=std(%rtp_argt(precipitation,t,%eval(&lag),%eval(&lag+&window-1)));
					prec_&intnm._avg[t]=mean(%rtp_argt(precipitation,t,%eval(&lag),%eval(&lag+&window-1)));
				end;
				%let names={name=%tslit(prec_&intnm._std)}, &names;
				%let names={name=%tslit(prec_&intnm._avg)}, &names;
			%end; 																						
			
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))  																		
		,
		arrayOut={
			table={name='lag_abt5', replace=true, caslib='casuser'},
			arrays={&names}
		}
	;
	run;
	quit;
	%mend qqq2;
	%qqq2;
	/* ------------ End. Считаем погоду --------------------- */

	/* ------------ Start. Считаем категорийные признаки магазинов --------------------- */	
	%macro qqq3;
	%if not %sysfunc(exist(casuser.pbo_dictionary_ml)) %then %do;
		data casuser.pbo_dictionary_ml;
			set mn_calc.pbo_dictionary_ml;
		run;	
	%end;


	proc fedsql sessref=casauto;
		create table casuser.pbo_loc_cat{options replace=true} as
			select 
				pbo_location_id
				,lvl3_id as pbo_loc_lvl3
				,lvl2_id as pbo_loc_lvl2
				,case when a_delivery='No' then 'No'
					else 'Yes' end as pbo_loc_delivery_cat
				,a_breakfast as pbo_loc_breakfast_cat
				,a_building_type as pbo_loc_building_cat
				,case when a_mccafe_type='No' then 'No'
					else 'Yes' end as pbo_loc_mccafe_cat
				,a_drive_thru as pbo_loc_drivethru_cat
				,a_window_type as pbo_loc_window_cat
			from casuser.pbo_dictionary_ml
		;	
	quit;
	/* ------------ End. Считаем категорийные признаки магазинов --------------------- */


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
				,t1.MD_lag_7_avg
				,t1.MD_lag_7_med
			
				,abt.target_p10
				,abt.target_p90
			from
				casuser.gc_ml1 as abt
			left join casuser.lag_abt1 as t1
			on
				abt.channel_cd = t1.channel_cd and
				abt.pbo_location_id = t1.pbo_location_id and
				abt.sales_dt = t1.sales_dt
			left join casuser.lag_abt2 as t2
			on
				abt.channel_cd = t2.channel_cd and
				abt.pbo_location_id = t2.pbo_location_id and
				abt.sales_dt = t2.sales_dt
			left join casuser.lag_abt3 as t3
			on
				abt.channel_cd = t3.channel_cd and
				abt.pbo_location_id = t3.pbo_location_id and
				abt.sales_dt = t3.sales_dt
	;
	quit;
	%mend qqq3;
	%qqq3;
	/* ------------ End. Добавляем лаги в витрину --------------------- */
	/* ------------ Start. Поплуярность --------------------- */
/* 	goto Popularity code */	
	/* ------------- End. Поплуярность ---------------------- */
	/* ------------ Start. Считаем фичи из промо --------------------- */	

	%if not %sysfunc(exist(casuser.promo_pbo_enh)) %then %do;
		%add_promotool_marks2(mpOutCaslib=casuser, mpPtCaslib=pt, PromoCalculationRk=);
	%end;


	/*кодируем промо-механики*/
/* 	proc fedsql sessref=casauto; */
/* 			create table casuser.promo_mеch {options replace=true} as */
/* 			select */
/* 				distinct promo_mechanics */
/* 			from casuser.promo_enh */
/* 		;	 */
/* 	quit; */
/* 	data casuser.promo_mеch / single=yes; */
/* 		set casuser.promo_mеch; */
/* 		format PROMO_MECH_SK $char10.; */
/* 		PROMO_MECH_SK = trim(CATS('PM_', put(_N_,3.))); */
/* 	run; */
/* 	%let pv_var_list = ; */
	
	proc fedsql sessref=casauto;
		create table casuser.promo_ml{options replace=true} as
			select
				t1.pbo_location_id
				,t2.channel_cd
				,t2.promo_id
				,t2.promo_group_id
				,t2.platform
				,t2.promo_mechanics
				,t2.np_gift_price_amt
				,t2.from_pt
				,t2.start_dt
				,t2.end_dt
/* 				,t3.PROMO_MECH_SK  */
				, 'PM_' || coalesce(t3.new_mechanic, 'no_mech') as PROMO_MECH_SK
/* 				, 1 as uno */
			from casuser.promo_pbo_enh as t1
			left join casuser.promo_enh as t2
										on	t1.promo_id = t2.promo_id
/* 			left join casuser.promo_mеch as t3  */
/* 										on t2.promo_mechanics = t3.promo_mechanics */
			left join MN_SHORT.PROMO_MECH_TRANSFORMATION  as t3  
										on t2.promo_mechanics = t3.old_mechanic
		;	
	quit;

/* 	схлопываем */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml_agg{options replace=true} as
		select
			t1.sales_dt
			,t1.pbo_location_id
			,t1.channel_cd
			, coalesce(t2.PROMO_MECH_SK,'PM_no_mech') as PROMO_MECH_SK
			, count(*) as cnt
		from  casuser.gc_ml2 as t1 
		left join casuser.promo_ml as t2 on
				t1.pbo_location_id = t2.pbo_location_id 
				and t1.channel_cd = t2.channel_cd
				and t1.sales_dt between t2.start_dt and t2.end_dt
		group by 
			t1.sales_dt
			,t1.pbo_location_id
			,t1.channel_cd
			, coalesce(t2.PROMO_MECH_SK,'PM_no_mech')
	;quit;

	proc sql  noprint;
		select distinct PROMO_MECH_SK into :PV_VAR_LIST separated by ','
		from casuser.promo_ml_agg
	;quit;
	%put &=PV_VAR_LIST;

/* 	транспонируем */
	proc transpose data=casuser.promo_ml_agg
               out=casuser.promo_ml_T
/* 				prefix=PM_ */
		;
	    by pbo_location_id channel_cd sales_dt;
	    id PROMO_MECH_SK;	
	    var cnt;
	run;

	proc fedsql sessref=casauto;
		create table casuser.promo_features{options replace=true} as
			select
				t1.channel_cd
				,t1.pbo_location_id
				,t1.sales_dt
				,count(t2.promo_id) as promo_cnt_all_id
				,count(distinct(t2.promo_id)) as promo_cnt_dist_id
				,count(distinct(t2.promo_group_id)) as promo_cnt_dist_group_id
				,count(distinct(t2.platform)) as promo_cnt_dist_platf
				,count(distinct(t2.promo_mechanics)) as promo_cnt_dist_mech
				
/* 				,sum(t2.from_pt) as promo_cnt_pt */
				,min(t2.np_gift_price_amt) as promo_min_gift_price
				,max(t2.np_gift_price_amt) as promo_max_gift_price
				,mean(t2.np_gift_price_amt) as promo_avg_gift_price

				,min(t2.np_gift_price_amt*t3.q_pct) as promo_min_gift_price_w
				,max(t2.np_gift_price_amt*t3.q_pct) as promo_max_gift_price_w
				,mean(t2.np_gift_price_amt*t3.q_pct) as promo_avg_gift_price_w
				
				,case when count(t2.promo_id) > 0 then 1
					else 0 end as promo_flg
			from casuser.gc_ml2 as t1
	
			left join casuser.promo_ml as t2 on
				t1.pbo_location_id = t2.pbo_location_id
				and	t1.channel_cd = t2.channel_cd
			
			left join casuser.promo_prod_enh as t4 on
					t2.promo_id = t4.promo_id
	
			left join casuser.Popularity as t3
				on t1.pbo_location_id = t3.pbo_location_id
				and t4.product_id = t3.product_id
	
			where t1.sales_dt between t2.start_dt and t2.end_dt
			group by t1.pbo_location_id
				,t1.channel_cd
				,t1.sales_dt
		;	
	quit;

	/* ------------ End. Считаем фичи из промо --------------------- */


/**/
	


		/* ------------ Start. Считаем цены и промо-цены --------------------- */
	%let lmvInPricesTb = price_full_sku_pbo_day;
	%let lmvInPricesLib = MN_DICT;
	/* 	таблица жирная, в кас поднимаем на время */
	proc casutil;
	    droptable casdata="&lmvInPricesTb." incaslib="&lmvInPricesLib." quiet;
	    load casdata="&lmvInPricesTb..sashdat" incaslib="&lmvInPricesLib." 
			casout="&lmvInPricesTb." outcaslib="&lmvInPricesLib.";
	quit;
		
	/* 	цены в тотале на магазин */
	
	proc fedsql sessref=casauto;
		create table casuser.all_prices {options replace=true} as
		select 
			t1.period_dt as sales_dt
			, t1.pbo_location_id
			, min(t1.price_reg_net) as price_reg_net_min
			, max(t1.price_reg_net) as price_reg_net_max
			, mean(t1.price_reg_net) as price_reg_net_avg

			, min(t1.price_reg_net*t2.q_pct) as price_reg_net_min_w
			, max(t1.price_reg_net*t2.q_pct) as price_reg_net_max_w
			, mean(t1.price_reg_net*t2.q_pct) as price_reg_net_avg_w

			, sum(t2.q_pct) as Pop
		from &lmvInPricesLib..&lmvInPricesTb. as t1
		left join casuser.Popularity as t2 
				on t1.pbo_location_id = t2.pbo_location_id
				and t1.product_id = t2.product_id
		group by t1.period_dt, t1.pbo_location_id
	;quit;

	proc fedsql sessref=casauto;
		create table casuser.promo_prices {options replace=true} as
		select 
			t1.period_dt as sales_dt
			, t1.pbo_location_id
			
			, round(min(t1.price_reg_net),0.01) as price_reg_net_min_p
			, round(max(t1.price_reg_net),0.01) as price_reg_net_max_p
			, round(mean(t1.price_reg_net),0.01) as price_reg_net_avg_p

			, round(min(t1.price_promo_net),0.01) as price_promo_net_min_p
			, round(max(t1.price_promo_net),0.01) as price_promo_net_max_p
			, round(mean(t1.price_promo_net),0.01) as price_promo_net_avg_p

			, round(min(t1.discount_net_pct),0.01) as discount_net_pct_min_p
			, round(max(t1.discount_net_pct),0.01) as discount_net_pct_max_p
			, round(mean(t1.discount_net_pct),0.01) as discount_net_pct_avg_p
		
			, round(min(t1.discount_net_rur),0.01) as discount_net_rur_min_p
			, round(max(t1.discount_net_rur),0.01) as discount_net_rur_max_p
			, round(mean(t1.discount_net_rur),0.01) as discount_net_rur_avg_p

			, round(min(t1.price_reg_net*t2.q_pct),0.001) as price_reg_net_min_p_w
			, round(max(t1.price_reg_net*t2.q_pct),0.001) as price_reg_net_max_p_w
			, round(mean(t1.price_reg_net*t2.q_pct),0.01) as price_reg_net_avg_p_w

			, round(min(t1.price_promo_net*t2.q_pct),0.001) as price_promo_net_min_p_w
			, round(max(t1.price_promo_net*t2.q_pct),0.001) as price_promo_net_max_p_w
			, round(mean(t1.price_promo_net*t2.q_pct),0.001) as price_promo_net_avg_p_w

			, round(min(t1.discount_net_pct*t2.q_pct),0.001) as discount_net_pct_min_p_w
			, round(max(t1.discount_net_pct*t2.q_pct),0.001) as discount_net_pct_max_p_w
			, round(mean(t1.discount_net_pct*t2.q_pct),0.001) as discount_net_pct_avg_p_w
		
			, round(min(t1.discount_net_rur*t2.q_pct),0.001) as discount_net_rur_min_p_w
			, round(max(t1.discount_net_rur*t2.q_pct),0.001) as discount_net_rur_max_p_w
			, round(mean(t1.discount_net_rur*t2.q_pct),0.001) as discount_net_rur_avg_p_w

			, sum(t2.q_pct) as Pop_p
		from &lmvInPricesLib..&lmvInPricesTb. as t1
		left join casuser.Popularity as t2 
				on t1.pbo_location_id = t2.pbo_location_id
				and t1.product_id = t2.product_id
		where t1.discount_net_pct > 0
		group by t1.period_dt, t1.pbo_location_id
	;quit;
	proc casutil;
		promote incaslib='casuser' casdata='promo_prices' 
					outcaslib="casuser" casout="promo_prices";
	    droptable casdata="&lmvInPricesTb." incaslib="&lmvInPricesLib." quiet;
	quit;

	/* ------------ End. Считаем цены и промо-цены --------------------- */


	/* 	debug */
	/* 	%let PV_VAR_LIST=PM_1,PM_10,PM_11,PM_12,PM_13,PM_3,PM_4,PM_5,PM_7,PM_8,PM_9,PM_X; */
	/* end debug */
	proc fedsql sessref=casauto;
		create table casuser.gc_ml3{options replace=true} as
			select				
				abt.*,
				t4.temp_week_avg,
				t4.temp_week_std,
				t4.temp_month_avg,
				t4.temp_month_std,
				t4.temperature, /* !!! */
				t5.prec_week_avg,
				t5.prec_week_std,
				t5.prec_month_avg,
				t5.prec_month_std,
				t5.precipitation, /* !!! */
				t6.pbo_loc_lvl2,
				t6.pbo_loc_lvl3,
				t6.pbo_loc_delivery_cat,
				t6.pbo_loc_breakfast_cat,
				t6.pbo_loc_building_cat,
				t6.pbo_loc_mccafe_cat,
				t6.pbo_loc_drivethru_cat,
				t6.pbo_loc_window_cat,
				coalesce(t7.promo_cnt_all_id, 0) as promo_cnt_all_id,
				coalesce(t7.promo_cnt_dist_id, 0) as promo_cnt_dist_id,
				coalesce(t7.promo_cnt_dist_group_id, 0) as promo_cnt_dist_group_id,
				coalesce(t7.promo_cnt_dist_platf, 0) as promo_cnt_dist_platf,
				coalesce(t7.promo_cnt_dist_mech, 0) as promo_cnt_dist_mech
/* 				coalesce(t7.promo_cnt_pt, 0) as promo_cnt_pt, */
				,t7.promo_min_gift_price
				,t7.promo_max_gift_price
				,t7.promo_avg_gift_price
				,t7.promo_min_gift_price_w
				,t7.promo_max_gift_price_w
				,t7.promo_avg_gift_price_w
				,coalesce(t7.promo_flg, 0) as promo_flg

				, &PV_VAR_LIST   /* t8 */
				
				,t9.PRICE_REG_NET_AVG
				,t9.PRICE_REG_NET_MAX
				,t9.PRICE_REG_NET_MIN

				,t9.PRICE_REG_NET_AVG_w
				,t9.PRICE_REG_NET_MAX_w
				,t9.PRICE_REG_NET_MIN_w

				,t9.POP

				,t10.DISCOUNT_NET_PCT_AVG_P
				,t10.DISCOUNT_NET_PCT_MAX_P
				,t10.DISCOUNT_NET_PCT_MIN_P
				,t10.DISCOUNT_NET_RUR_AVG_P
				,t10.DISCOUNT_NET_RUR_MAX_P
				,t10.DISCOUNT_NET_RUR_MIN_P
				,t10.PRICE_PROMO_NET_AVG_P
				,t10.PRICE_PROMO_NET_MAX_P
				,t10.PRICE_PROMO_NET_MIN_P
				,t10.PRICE_REG_NET_AVG_P
				,t10.PRICE_REG_NET_MAX_P
				,t10.PRICE_REG_NET_MIN_P

				,t10.DISCOUNT_NET_PCT_AVG_P_w
				,t10.DISCOUNT_NET_PCT_MAX_P_w
				,t10.DISCOUNT_NET_PCT_MIN_P_w
				,t10.DISCOUNT_NET_RUR_AVG_P_w
				,t10.DISCOUNT_NET_RUR_MAX_P_w
				,t10.DISCOUNT_NET_RUR_MIN_P_w
				,t10.PRICE_PROMO_NET_AVG_P_w
				,t10.PRICE_PROMO_NET_MAX_P_w
				,t10.PRICE_PROMO_NET_MIN_P_w
				,t10.PRICE_REG_NET_AVG_P_w
				,t10.PRICE_REG_NET_MAX_P_w
				,t10.PRICE_REG_NET_MIN_P_w

				,t10.POP_P

				,y1.MAX_CNT_ENC_A_OFFER_TYPE
				,y1.MAX_CNT_ENC_PRODUCT_ID
				,y1.MAX_CNT_ENC_PROD_LVL2_ID
				,y1.MAX_CNT_ENC_PROD_LVL3_ID
				,y1.MAX_CNT_ENC_PROD_LVL4_ID
				,y1.MEAN_CNT_ENC_A_OFFER_TYPE
				,y1.MEAN_CNT_ENC_PRODUCT_ID
				,y1.MEAN_CNT_ENC_PROD_LVL2_ID
				,y1.MEAN_CNT_ENC_PROD_LVL3_ID
				,y1.MEAN_CNT_ENC_PROD_LVL4_ID
				,y1.MIN_CNT_ENC_A_OFFER_TYPE
				,y1.MIN_CNT_ENC_PRODUCT_ID
				,y1.MIN_CNT_ENC_PROD_LVL2_ID
				,y1.MIN_CNT_ENC_PROD_LVL3_ID
				,y1.MIN_CNT_ENC_PROD_LVL4_ID 
			from
				casuser.gc_ml2 as abt
			left join casuser.lag_abt4 as t4
			on
				abt.pbo_location_id = t4.pbo_location_id and
				abt.sales_dt = t4.report_dt
			left join casuser.lag_abt5 as t5
			on
				abt.pbo_location_id = t5.pbo_location_id and
				abt.sales_dt = t5.report_dt
			left join casuser.pbo_loc_cat as t6
			on
				abt.pbo_location_id = t6.pbo_location_id
			left join casuser.promo_features as t7
			on
				abt.channel_cd = t7.channel_cd and
				abt.pbo_location_id = t7.pbo_location_id and
				abt.sales_dt = t7.sales_dt

			left join casuser.promo_ml_T as t8 on
				abt.pbo_location_id = t8.pbo_location_id
				and abt.channel_cd = t8.channel_cd
				and abt.sales_dt = t8.sAles_dt

			left join casuser.all_prices as t9 on	
				abt.pbo_location_id = t9.pbo_location_id
				and abt.sales_dt = t9.sAles_dt

			left join casuser.promo_prices as t10 on	
				abt.pbo_location_id = t10.pbo_location_id
				and abt.sales_dt = t10.sAles_dt

			left join max_casl.promo_product_features as y1
				on abt.pbo_location_id = y1.pbo_location_id
				and abt.channel_cd = y1.channel_cd
				and abt.sales_dt = y1.sAles_dt

	;
	quit;


	/* Удаляем промежуточные таблицы */
	proc casutil;
		droptable casdata="lag_abt1" incaslib="casuser" quiet;
		droptable casdata="lag_abt2" incaslib="casuser" quiet;
		droptable casdata="lag_abt3" incaslib="casuser" quiet;
		droptable casdata="lag_abt4" incaslib="casuser" quiet;
		droptable casdata="lag_abt5" incaslib="casuser" quiet;
		droptable casdata="gc_ml1" incaslib="casuser" quiet;
		droptable casdata="gc_ml2" incaslib="casuser" quiet;
		droptable casdata="pbo_loc_cat" incaslib="casuser" quiet;
		droptable casdata="promo_ml" incaslib="casuser" quiet;
		droptable casdata="promo_features" incaslib="casuser" quiet;
		
		droptable casdata="promo_ml_agg" incaslib="casuser" quiet;	
		droptable casdata="promo_ml_T" incaslib="casuser" quiet;	

		droptable casdata="all_prices" incaslib="casuser" quiet;	
		droptable casdata="promo_prices" incaslib="casuser" quiet;	

	run;

	proc casutil;
		droptable incaslib="&outp_lib." casdata="&outp_dm_nm." quiet;
		promote incaslib='casuser' casdata='gc_ml3' outcaslib="&outp_lib." casout="&outp_dm_nm.";
/* 		save incaslib='casuser' casdata='gc_ml3' outcaslib="&outp_lib." casout="&outp_dm_nm."; */
	run;


proc contents data =casuser.&outp_dm_nm.
;run;quit;


%mend fcst_create_ml_abt_AF;

/* См внимательно какой скрипт использовать! */
/* %include '/opt/sas/mcd_config/macro/step/pt/gc_ml_forecast/fcst_create_abt_pbo_gc2.sas'; */
%include "/opt/sas/mcd_config/macro/step/pt/gc_ml_fcst_md/fcst_create_abt_pbo_gc_mp_COPY.sas";
%fcst_create_abt_pbo_gc_ML(
	  mpMode		  = gc
	, mpSeasonMode	  = 2
	, mpOutTableDmVf  = casuser.DM_TRAIN_TRP_GC_MP	
	, mpOutTableDmABT = casuser.TRAIN_ABT_TRP_GC_MP
);

%fcst_create_ml_abt_AF(inp_dm = casuser.DM_TRAIN_TRP_GC_MP,
								outp_lib = casuser,
								outp_dm_nm = DM_GC_ML2);

/* proc casutil; */
/* 	droptable incaslib='casuser' casdata='DM_GC_ML' quiet; */
/* 	promote incaslib='casuser' casdata='gc_ml3' outcaslib="casuser" casout="DM_GC_ML"; */
/* 	save incaslib='casuser' casdata='gc_ml3' outcaslib="casuser" casout="DM_GC_ML"; */
/* run; */

