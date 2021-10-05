%include '/opt/sas/mcd_config/macro/step/pt/gc_ml_fcst_frantsev/count_encoder.sas';
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

%macro fcst_create_ml_abt_AF(inp_dm = casuser.DM_TRAIN_TRP_GC,
							outp_lib = casuser,
							outp_dm_nm = DM_TRAIN_TRP_GC_ML);

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
	/* ------------ End. Добавляем лаги в витрину --------------------- */


	/* ------------ Start. Считаем погоду ------------------- */
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
	/* ------------ End. Считаем погоду --------------------- */


	/* ------------ Start. Считаем категорийные признаки магазинов --------------------- */	
	
	%if not %sysfunc(exist(casuser.pbo_dictionary_ml)) %then %do;
		data casuser.pbo_dictionary_ml;
			set mn_calc.pbo_dictionary_ml;
		run;	
	%end;


	proc fedsql sessref=casauto;
		create table casuser.pbo_loc_cat{options replace=true} as
			select 
				pbo_location_id
/* 				,case when a_delivery='No' then 'No' */
/* 					else 'Yes' end as pbo_loc_delivery_cat */
/* 				,case when a_mccafe_type='No' then 'No' */
/* 					else 'Yes' end as pbo_loc_mccafe_cat */
				,lvl3_id as lvl3_id
				,lvl2_id as lvl2_id
				,A_AGREEMENT_TYPE_id as agreement_type
				,A_BREAKFAST_id as breakfast
				,A_BUILDING_TYPE_id as building_type
				,A_COMPANY_id as company
				,A_DELIVERY_id as delivery
				,A_DRIVE_THRU_id as drive_thru
				,A_MCCAFE_TYPE_id as mccafe_type
				,A_PRICE_LEVEL_id as price_level
				,A_WINDOW_TYPE_id as window_type
			from casuser.pbo_dictionary_ml
		;	
	quit;

	%let pbo_type_features = %str(lvl3_id,lvl2_id,agreement_type,breakfast,building_type,company,delivery,drive_thru,mccafe_type,price_level,window_type);
	%count_encoder(inp_data = casuser.pbo_loc_cat, 
				target_var_list = &pbo_type_features., 
				group_by_list = %str(pbo_location_id),
				outp_data = casuser.pbo_type_features);

	/* ------------ End. Считаем категорийные признаки магазинов --------------------- */


	/* ------------ Start. Считаем фичи из промо --------------------- */	

	%if not %sysfunc(exist(casuser.promo_pbo_enh)) %then %do;
		%add_promotool_marks2(mpOutCaslib=casuser, mpPtCaslib=pt, PromoCalculationRk=);
	%end;

/* 	TODO: использовать таблицы mn_short.promo_ml, mn_short.promo_mech_transformation */
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
				,t2.start_dt
				,t2.end_dt
			from casuser.promo_pbo_enh as t1
			inner join casuser.promo_enh as t2
			on
				t1.promo_id = t2.promo_id
		;	
	quit;

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
				,min(t2.np_gift_price_amt) as promo_min_gift_price
				,max(t2.np_gift_price_amt) as promo_max_gift_price
				,mean(t2.np_gift_price_amt) as promo_avg_gift_price
				,case when count(t2.promo_id) > 0 then 1
					else 0 end as promo_flg
			from casuser.gc_ml2 as t1
			left join casuser.promo_ml as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and 
				t1.channel_cd = t2.channel_cd
			where t1.sales_dt between t2.start_dt and t2.end_dt
			group by t1.pbo_location_id
				,t1.channel_cd
				,t1.sales_dt
		;	
	quit;

	proc fedsql sessref = casauto;
		create table casuser.promo_ml_by_dt{options replace = true} as 
		select
			t1.PBO_LOCATION_ID
			,t1.CHANNEL_CD
			,t1.SALES_DT
			,t2.promo_id
			,t2.promo_group_id
			,t2.platform
			,t2.promo_mechanics
		from
			casuser.gc_ml2 as t1
		inner join	
			casuser.promo_ml as t2									
		on
			t1.pbo_location_id = t2.pbo_location_id and
			t1.CHANNEL_CD = t2.CHANNEL_CD and
			t1.SALES_DT <= t2.END_DT and
			t1.SALES_DT >= t2.START_DT
		;	
	quit;

	%let promo_type_features = %str(promo_id,promo_group_id,platform,promo_mechanics);
	%count_encoder(inp_data = casuser.promo_ml_by_dt, 
				target_var_list = &promo_type_features., 
				group_by_list = %str(pbo_location_id,channel_cd,sales_dt),
				outp_data = casuser.promo_type_features);

	/* ------------ End. Считаем фичи из промо --------------------- */

	/* ------------ Start.Считаем фичи от конкурентов --------------------- */

	%if not %sysfunc(exist(casuser.media_enh)) %then %do;
		%add_promotool_marks2(mpOutCaslib=casuser, mpPtCaslib=pt, PromoCalculationRk=);
	%end;

	proc fedsql sessref=casauto;
		create table casuser.promo_media_mcd{options replace=true} as
			select
				report_dt 
				,sum(trp) as trp_mcd
				,log(1+sum(trp)) as log_trp_mcd
			from casuser.media_enh as t1
			group by report_dt
		;	
	quit;

	proc fedsql sessref=casauto;
		create table casuser.promo_media_comp{options replace=true} as
			select
				report_dt 
				,sum(case when competitor_cd='BK' then trp else 0 end) as trp_bk
				,log(1+sum(case when competitor_cd='BK' then trp else 0 end)) as log_trp_bk
				,sum(case when competitor_cd='KFC' then trp else 0 end) as trp_kfc
				,log(1+sum(case when competitor_cd='KFC' then trp else 0 end)) as log_trp_kfc
			from mn_short.comp_media as t1
			group by report_dt
		;	
	quit;

	proc fedsql sessref=casauto;
		create table casuser.promo_media_features{options replace=true} as
			select 
				coalesce(t1.report_dt, t2.report_dt) as sales_dt
				,coalesce(t1.trp_mcd, 0) as trp_mcd
				,coalesce(t1.log_trp_mcd, 0) as log_trp_mcd
				,coalesce(t2.trp_bk, 1)/coalesce(t1.trp_mcd, 1) as trp_bk_to_mcd
				,coalesce(t2.log_trp_bk, 0) as log_trp_bk
				,coalesce(t2.trp_kfc, 1)/coalesce(t1.trp_mcd, 1) as trp_kfc_to_mcd
				,coalesce(t2.log_trp_kfc, 0) as log_trp_kfc
			from casuser.promo_media_mcd as t1
			full join casuser.promo_media_comp as t2
			on t1.report_dt = t2.report_dt
		;	
	quit;

	/* ------------ End. Считаем фичи от конкурентов --------------------- */


	/* ------------ Start. Считаем фичи из маркетинговой поддержки  --------------------- */	
	%if not %sysfunc(exist(casuser.ia_promo)) %then %do;
		data casuser.ia_promo;
			set ia.ia_promo;
		run;
	%end;

	proc fedsql sessref=casauto;\
		select max(end_dt) as last_date
		from casuser.ia_promo
		;	
	quit;

	/* ------------ End. Считаем фичи из маркетинговой поддержки --------------------- */	


	/* ------------ Start. Считаем фичи из товаров --------------------- */
	
	proc fedsql sessref = casauto;
		create table casuser.promo_by_product{options replace = true} as 
		select
			t1.PBO_LOCATION_ID,
			t2.product_LEAF_ID as PRODUCT_ID,
			t3.PROD_LVL4_ID,
			t3.PROD_LVL3_ID,
			t3.PROD_LVL2_ID,
			t3.A_OFFER_TYPE,
			t1.CHANNEL_CD,
			t1.SALES_DT
		from
			casuser.gc_ml2 as t1
		inner join	/* Присоединяем транспонированную по типам механик таблицу с индикаторами промо */
			mn_short.promo_transposed as t2									
		on
			t1.pbo_location_id = t2.PBO_LEAF_ID and
			t1.CHANNEL_CD = t2.CHANNEL_CD and
			t1.SALES_DT <= t2.END_DT and
			t1.SALES_DT >= t2.START_DT
		inner join	/* Присоединяем продуктовую иерархию */
			mn_short.product_dictionary_ml as t3								
		on
			t2.product_leaf_id = t3.product_id
		;	
	quit;

	%let promo_product_featurs=%str(product_id,prod_lvl4_id,prod_lvl3_id,prod_lvl2_id,a_offer_type);
	%count_encoder(inp_data = casuser.promo_by_product, 
				target_var_list = &promo_product_featurs., 
				group_by_list = %str(pbo_location_id,channel_cd,sales_dt),
				agg_func_list = %str(min,max,mean),
				outp_data = casuser.promo_product_features);

	/* ------------ End. Считаем фичи из товаров --------------------- */

	/* ------------ Start. Добавляем промо, погоду, осадки, категории магазинов в витрину --------------------- */
	%let agg_func_list=%str(min,max,mean);
	proc fedsql sessref=casauto;
		create table casuser.gc_ml3{options replace=true} as
			select				
				abt.*,
				t4.temp_week_avg,
				t4.temp_week_std,
				t4.temp_month_avg,
				t4.temp_month_std,
				t5.prec_week_avg,
				t5.prec_week_std,
				t5.prec_month_avg,
				t5.prec_month_std,
				t6.pbo_loc_lvl2,
				t6.pbo_loc_lvl3,
				t6.pbo_loc_delivery_cat,
				t6.pbo_loc_breakfast_cat,
				t6.pbo_loc_building_cat,
				t6.pbo_loc_mccafe_cat,
				t6.pbo_loc_drivethru_cat,
				t6.pbo_loc_window_cat
				%do j=1 %to %sysfunc(countw(&promo_product_features.)); 
					%let var = %qscan(&promo_product_features.,&j.,',;');
					,t1.cnt_enc_&var.
					,t1.freq_enc_&var.
				%end;
				,coalesce(t7.promo_cnt_all_id, 0) as promo_cnt_all_id
				,coalesce(t7.promo_cnt_dist_id, 0) as promo_cnt_dist_id
				,coalesce(t7.promo_cnt_dist_group_id, 0) as promo_cnt_dist_group_id
				,coalesce(t7.promo_cnt_dist_platf, 0) as promo_cnt_dist_platf
				,coalesce(t7.promo_cnt_dist_mech, 0) as promo_cnt_dist_mech
				,coalesce(t7.promo_max_gift_price, 0) as promo_max_gift_price
				,coalesce(t7.promo_flg, 0) as promo_flg
				%do j=1 %to %sysfunc(countw(&promo_type_features.)); 
					%let var = %qscan(&promo_type_features.,&j.,',;');
					,t2.cnt_enc_&var.
					,t2.freq_enc_&var.
				%end;
				%do i=1 %to %sysfunc(countw(&agg_func_list.)); 
					%let agg = %qscan(&agg_func_list.,&i.,',;');
					%do j=1 %to %sysfunc(countw(&promo_product_features.)); 
						%let var = %qscan(&promo_product_features.,&j.,',;');
						,coalesce(t3.&agg._cnt_enc_&var., 0) as &agg._cnt_enc_&var.
						,coalesce(t3.&agg._freq_enc_&var., 0) as &agg._freq_enc_&var.
					%end;
				%end;
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
			left join casuser.pbo_type_features as t1
			on
				abt.pbo_location_id = t1.pbo_location_id
			left join casuser.promo_type_features as t2
			on
				abt.channel_cd = t2.channel_cd and
				abt.pbo_location_id = t2.pbo_location_id
			left join casuser.promo_product_features as t3
			on
				abt.channel_cd = t3.channel_cd and
				abt.pbo_location_id = t3.pbo_location_id and
				abt.sales_dt = t3.sales_dt
	;
	quit;
	/* ------------ End. Добавляем промо, погоду, осадки, категории магазинов в витрину --------------------- */

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
	run;

	proc casutil;
/* 		droptable incaslib='&outp_lib.' casdata='&outp_dm_nm.' quiet; */
		promote incaslib='casuser' casdata='gc_ml3' outcaslib="&outp_lib." casout="&outp_dm_nm.";
		save incaslib='casuser' casdata='gc_ml3' outcaslib="&outp_lib." casout="&outp_dm_nm.";
	run;

%mend fcst_create_ml_abt_AF;

/* См внимательно какой скрипт использовать! */
/* %include '/opt/sas/mcd_config/macro/step/pt/gc_ml_forecast/fcst_create_abt_pbo_gc2.sas'; */
%include '/opt/sas/mcd_config/macro/step/pt/short_term/gc_ml_fcst_frantsev/fcst_create_abt_pbo_gc_mp.sas';
%fcst_create_abt_pbo_gc_mp(
	  mpMode		  = gc
	, mpOutTableDmVf  = casuser.DM_TRAIN_TRP_GC_MP	
	, mpOutTableDmABT = casuser.TRAIN_ABT_TRP_GC_MP
);

%fcst_create_ml_abt_AF(inp_dm = casuser.DM_TRAIN_TRP_GC_MP,
								outp_lib = casuser,
								outp_dm_nm = DM_GC_ML);

/* proc casutil; */
/* 	droptable incaslib='casuser' casdata='DM_GC_ML' quiet; */
/* 	promote incaslib='casuser' casdata='gc_ml3' outcaslib="casuser" casout="DM_GC_ML"; */
/* 	save incaslib='casuser' casdata='gc_ml3' outcaslib="casuser" casout="DM_GC_ML"; */
/* run; */