/* Параметры макроса

Параметр				Возможные значения
mpProdLvl 			 	PROD_LVL1_NM, PROD_LVL2_NM, PROD_LVL3_NM, product_nm
mpLocLvl 			 	A_AGREEMENT_TYPE, LVL1_NM, LVL2_NM, LVL3_NM, pbo_location_nm
mpBorderDt 			 	дата в виде числа 22646, любая функция от даты, например intnx('year', today(), 0, 'B')
mpAlertCriterionDamp 	интервал от 0 до 1, исключая границы 
mpAlertCriterionGrowth 	интервал от 0 до 1, исключая границы 
mpOutTableNm 			имя таблицы, например, AVGRCP_ALERT_YEAR_TREND

*/


%macro avgrcp_alert_year_trend(
	      mpProdLvl = PROD_LVL2_NM
		, mpLocLvl = LVL2_NM
		, mpBorderDt = 22333
		, mpAlertCriterionDamp = 0.05
		, mpAlertCriterionGrowth = 0.1
		, mpOutTableNm = AVGRCP_ALERT_YEAR_TREND
	);

	%macro mDummy;
	%mend mDummy;


	/* Переопределение граничной даты на начало полного сезона 
		и расчет количества сезонных циклов */	

/* 	%let mpSeason = qtr; */
/* 	%let mpBorderDt = 22646; */

	data _null_;
		border_dt = &mpBorderDt.;
		curr_season_start_dt = intnx('month', border_dt, 0, 'B');
		next_season_start_dt = intnx('month', border_dt, 1, 'B');
		new_border_dt = ifn(
			  curr_season_start_dt = border_dt
			, border_dt
			, next_season_start_dt
		);		
		call symputx('mpBorderDt', new_border_dt);

	run;
	%put &=mpBorderDt;

	/* ************************************************************************************************ */
	/* Подготовка прогнозов */

	/* 1. Извлечь прогнозы и просуммировать до требуемого уровня */
	proc fedsql sessref=casauto noprint;
		/* Подшаг 1. Внимание! по иерархии SKU чеки нужно усреднять */
		create table CASUSER.AVGRCP_SUM_FCST_PRE {options replace=true} as
		select 
		/* 	  main.ORG as channel_cd */
			  main.LOCATION as pbo_location_id
			, loc.&mpLocLvl.
			, prod.&mpProdLvl.
			, main.DATA as sales_dt			
			, sum(main.OVERRIDED_FCST_SALE	) as sum_predict_rur
			, avg(main.OVERRIDED_FCST_GC	) as sum_predict_gc
		from 
			CASUSER.AVGRCP_FCST_MONTH as main
		left join casuser.pbo_dictionary as loc
			on main.LOCATION = loc.pbo_location_id
		left join casuser.PRODUCT_DICTIONARY as prod
			on main.PROD = prod.product_id
		group by 
		/* 	  main.ORG */
			  main.LOCATION
			, loc.&mpLocLvl.
			, prod.&mpProdLvl.
			, main.DATA
		;
		/* Подшаг 2. Внимание! по иерархии PBO чеки нужно суммировать */
		create table CASUSER.AVGRCP_SUM_FCST {options replace=true} as
		select 
		/* 	  main.ORG as channel_cd */
			  &mpLocLvl.
			, &mpProdLvl.
			, sales_dt			
			, sum(sum_predict_rur	) as sum_predict_rur
			, sum(sum_predict_gc	) as sum_predict_gc
		from 
			CASUSER.AVGRCP_SUM_FCST_PRE as main
		where 
			cast(sales_dt as date) >= cast(&mpBorderDt. as date)
		group by 
		/* 	  main.ORG */
			  &mpLocLvl.
			, &mpProdLvl.
			, sales_dt
		;
	quit;

	/* 2. просуммировать прогноз за год вперед после даты mpBorderDt */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.AVGRCP_SUM_FCST_YEAR {options replace=true} as
		select 
/* 			  channel_cd */
			  &mpLocLvl.
			, &mpProdLvl.
			, sum(sum_predict_rur) as sum_predict_rur_year
			, sum(sum_predict_gc) as sum_predict_gc_year
		from 
			CASUSER.AVGRCP_SUM_FCST
		where 
			cast(sales_dt as date) < cast(intnx('month', &mpBorderDt., 12, 'B') as date)
		group by 
/* 			  channel_cd */
			  &mpLocLvl.
			, &mpProdLvl.
		;
	quit;

	/* 3. Часть прогноза до даты mpBorderDt */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.AVGRCP_SUM_FCST_APNDX {options replace=true} as
		select 
		/* 	  main.ORG as channel_cd */
			  &mpLocLvl.
			, &mpProdLvl.	
			, sum(sum_predict_rur	) as sum_apndx_predict_rur_year
			/* Чеки суммируем по иерархии PBO */
			, sum(sum_predict_gc	) as sum_apndx_predict_gc_year
		from 
			CASUSER.AVGRCP_SUM_FCST_PRE as main
		where 
			cast(sales_dt as date) < cast(&mpBorderDt. as date)
		group by 
		/* 	  main.ORG */
			  &mpLocLvl.
			, &mpProdLvl.
		;
	quit;	


	/* ************************************************************************************************ */
	/* Подготовка фактических/исторических значений */
	
	/* 1. Извлечь факты из ABT */
	proc fedsql sessref=casauto noprint;
		/* Подшаг 1. Внимание! по иерархии SKU чеки нужно усреднять */
		create table CASUSER.AVGRCP_SUM_HIST_PRE {options replace=true} as
		select 
/* 			  main.channel_cd */
			  main.pbo_location_id
			, loc.&mpLocLvl.
			, prod.&mpProdLvl.
			, main.sales_dt		
			, sum(main.sales_rur) as sum_actual_rur
			/* Усредняем чеки */
			, avg(main.receipt_qty) as sum_actual_gc

		from 
			CASUSER.AVGRCP_FACT_MONTH as main
		left join casuser.pbo_dictionary as loc
			on main.pbo_location_id = loc.pbo_location_id
		left join casuser.PRODUCT_DICTIONARY as prod
			on main.product_id = prod.product_id
		where 
			cast(main.sales_dt as date) < cast(&mpBorderDt. as date)
		group by 
/* 			  main.channel_cd		 */
			  main.pbo_location_id
			, loc.&mpLocLvl.
			, prod.&mpProdLvl.
			, main.sales_dt
		;
		/* Подшаг 2. Внимание! по иерархии PBO чеки нужно суммировать */
		create table CASUSER.AVGRCP_SUM_HIST {options replace=true} as
		select 
		/* 	  main.ORG as channel_cd */
			  &mpLocLvl.
			, &mpProdLvl.
			, sales_dt			
			, sum(sum_actual_rur	) as sum_actual_rur
			/* Суммируем чеки */
			, sum(sum_actual_gc		) as sum_actual_gc
		from 
			CASUSER.AVGRCP_SUM_HIST_PRE as main
		group by 
		/* 	  main.ORG */
			  &mpLocLvl.
			, &mpProdLvl.
			, sales_dt
		;
	quit;

	/* 2. просуммировать факты за год назад */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.AVGRCP_SUM_HIST_PREVYEAR {options replace=true} as
		select 
/* 			  channel_cd */
			  &mpLocLvl.
			, &mpProdLvl.
			, sum(sum_actual_rur) as sum_actual_rur_prevyear
			, sum(sum_actual_gc) as sum_actual_gc_prevyear

		from 
			CASUSER.AVGRCP_SUM_HIST
		where 
			sales_dt >= cast(intnx('month', &mpBorderDt., - 12, 'B') as date)
		group by 
/* 			  channel_cd */
			  &mpLocLvl.
			, &mpProdLvl.
		;
	quit;

	/* 3. просуммировать факты за год, перед предыдущим */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.AVGRCP_SUM_HIST_PREVYEAR2 {options replace=true} as
		select 
/* 			  channel_cd */
			  &mpLocLvl.
			, &mpProdLvl.
			, sum(sum_actual_rur) as sum_actual_rur_prevyear2
			, sum(sum_actual_gc) as sum_actual_gc_prevyear2
		from 
			CASUSER.AVGRCP_SUM_HIST
		where 
			sales_dt >= cast(intnx('month', &mpBorderDt., - 2 * 12, 'B') as date)
			and sales_dt < cast(intnx('month', &mpBorderDt., - 12, 'B') as date)
		group by 
/* 			  channel_cd */
			  &mpLocLvl.
			, &mpProdLvl.
		;
	quit;



	/* ************************************************************************************************ */
 	/* Соединение и расчет алертов */	
	
	/* Соединить факты и прогноз за предыдущий год и рассчитать поля для сравнения с критериями*/
	/* Замечание: имеет смысл присоединение части прогноза, если mpBorderDt установлена не на начало прогноза, 
		а, например, на следующий финансовый/календарный год */

	proc fedsql sessref=casauto noprint;
		create table CASUSER.AVGRCP_SUM_YEAR_PRE {options replace=true} as
		select 
/* 			  fcst.channel_cd */
			  fcst.&mpLocLvl.
			, fcst.&mpProdLvl.
	
			, fcst.sum_predict_rur_year
			, fcst.sum_predict_gc_year
			, case 
				when fcst.sum_predict_rur_year > 0 
					and fcst.sum_predict_gc_year > 0
/* 					and fcst.sum_predict_rur_year is not null */
/* 					and fcst.sum_predict_gc_year is not null */
				then fcst.sum_predict_rur_year / fcst.sum_predict_gc_year
				else 0
			  end as sum_predict_avgrcp_year

			, hist2.sum_actual_rur_prevyear2
			, hist2.sum_actual_gc_prevyear2
			, case 
				when hist2.sum_actual_rur_prevyear2 > 0 
					and hist2.sum_actual_gc_prevyear2 > 0
/* 					and hist2.sum_actual_rur_prevyear2 is not null */
/* 					and hist2.sum_actual_gc_prevyear2 is not null */
				then hist2.sum_actual_rur_prevyear2 / hist2.sum_actual_gc_prevyear2
				else 0
			  end as sum_actual_avgrcp_prevyear2
		
			, hist.sum_actual_rur_prevyear
			, hist.sum_actual_gc_prevyear
			, apndx.sum_apndx_predict_rur_year
			, apndx.sum_apndx_predict_gc_year
			, sum(hist.sum_actual_rur_prevyear, apndx.sum_apndx_predict_rur_year) 	as sum_act_n_apndx_rur_prevyear
			, sum(hist.sum_actual_gc_prevyear, apndx.sum_apndx_predict_gc_year) 	as sum_act_n_apndx_gc_prevyear
			, case 
				when sum(hist.sum_actual_rur_prevyear, apndx.sum_apndx_predict_rur_year) > 0 
					and sum(hist.sum_actual_gc_prevyear, apndx.sum_apndx_predict_gc_year) > 0
/* 					and (hist.sum_actual_rur_prevyear + apndx.sum_apndx_predict_rur_year) is not null */
/* 					and (hist.sum_actual_gc_prevyear + apndx.sum_apndx_predict_gc_year) is not null */
				then sum(hist.sum_actual_rur_prevyear, apndx.sum_apndx_predict_rur_year) / sum(hist.sum_actual_gc_prevyear, apndx.sum_apndx_predict_gc_year)
				else 0
			  end as sum_act_n_apndx_avgrcp_prevyear
					
		from 
			CASUSER.AVGRCP_SUM_FCST_YEAR as fcst

		left join 
			CASUSER.AVGRCP_SUM_FCST_APNDX as apndx
/* 		on fcst.channel_cd = apndx.channel_cd */
			on fcst.&mpLocLvl. = apndx.&mpLocLvl.
			and fcst.&mpProdLvl. = apndx.&mpProdLvl.
		
		left join 
			CASUSER.AVGRCP_SUM_HIST_PREVYEAR as hist
/* 		on fcst.channel_cd = hist.channel_cd */
			on fcst.&mpLocLvl. = hist.&mpLocLvl.
			and fcst.&mpProdLvl. = hist.&mpProdLvl.

		left join 
			CASUSER.AVGRCP_SUM_HIST_PREVYEAR2 as hist2
/* 		on fcst.channel_cd = hist2.channel_cd */
			on fcst.&mpLocLvl. = hist2.&mpLocLvl.
			and fcst.&mpProdLvl. = hist2.&mpProdLvl.	
		;
	quit;

	/* Рассчитать отклонения */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.AVGRCP_SUM_YEAR {options replace=true} as
		select *
			, case 
				when sum_actual_avgrcp_prevyear2 > 0 
					and sum_actual_avgrcp_prevyear2 is not null
				then (sum_predict_avgrcp_year - sum_actual_avgrcp_prevyear2) / sum_actual_avgrcp_prevyear2 
				else 0
			  end as rel_change_to_prevyear2
			, case 
				when sum_act_n_apndx_avgrcp_prevyear > 0 
					and sum_act_n_apndx_avgrcp_prevyear is not null
				then (sum_predict_avgrcp_year - sum_act_n_apndx_avgrcp_prevyear) / sum_act_n_apndx_avgrcp_prevyear 
				else 0
			  end as rel_change_to_prevyear
		from 
			CASUSER.AVGRCP_SUM_YEAR_PRE 
		;
	quit;
	
	/* Рассчитать алерты */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.ALERT_YEAR_TREND {options replace=true} as
		select 
 			  &mpLocLvl.
			, &mpProdLvl.

			, sum_predict_rur_year          
			, sum_actual_rur_prevyear2      
			, sum_actual_rur_prevyear       
			, sum_apndx_predict_rur_year 		
			, sum_act_n_apndx_rur_prevyear  

			, sum_predict_gc_year            
			, sum_actual_gc_prevyear2        
			, sum_actual_gc_prevyear         
			, sum_apndx_predict_gc_year   		
			, sum_act_n_apndx_gc_prevyear
    
			, sum_predict_avgrcp_year          	
			, sum_actual_avgrcp_prevyear2      	
			, sum_act_n_apndx_avgrcp_prevyear
  	
			, rel_change_to_prevyear 			
			, rel_change_to_prevyear2 
			  
			/* Прогноз на год вперед ниже, чем факт за прошлый год на X% */
			, case
				when rel_change_to_prevyear < - &mpAlertCriterionDamp.
					and rel_change_to_prevyear is not missing 
						then 1
				else 0
				end as alert_trend_damp_to_prevyear

			/* Прогноз на год вперед ниже, чем факт за прошлый год на X%, и ниже чем факт за позапрошлый год на X% */
			, case
				when rel_change_to_prevyear2 < - &mpAlertCriterionDamp.					
					and rel_change_to_prevyear2 is not missing 
						then 1
				else 0
				end as alert_trend_damp_to_prevyear2

			/* Прогноз на год вперед выше, чем факт за прошлый год на X% */
			, case
				when rel_change_to_prevyear > &mpAlertCriterionGrowth.
					and rel_change_to_prevyear is not missing 					
						then 1
				else 0
				end as alert_trend_growth_to_prevyear

			/* Прогноз на год вперед выше, чем факт за прошлый год на X%, и выше чем факт за позапрошлый год на X% */
			, case
				when rel_change_to_prevyear2 > &mpAlertCriterionGrowth.
					and rel_change_to_prevyear2 is not missing 
						then 1
				else 0
				end as alert_trend_growth_to_prevyear2
			
		from 
			CASUSER.AVGRCP_SUM_YEAR		
		;
	quit;

	proc casutil incaslib="DM_ALERT" ;
		droptable casdata = "&mpOutTableNm" quiet;
	run;

	data DM_ALERT.&mpOutTableNm.(promote=yes);
		set CASUSER.ALERT_YEAR_TREND;
		format sum_predict_rur_year          	commax15.2	;
		format sum_predict_gc_year            	commax15.	;
		format sum_predict_avgrcp_year          commax15.2	;
		format sum_actual_rur_prevyear2      	commax15.2	;
		format sum_actual_gc_prevyear2        	commax15.	;
		format sum_actual_avgrcp_prevyear2      commax15.2	;
		format sum_actual_rur_prevyear       	commax15.2	;
		format sum_actual_gc_prevyear         	commax15.	;
		format sum_apndx_predict_rur_year 		commax15.2	;
		format sum_apndx_predict_gc_year   		commax15.	;
		format sum_act_n_apndx_rur_prevyear  	commax15.2	;
		format sum_act_n_apndx_gc_prevyear    	commax15.	;
		format sum_act_n_apndx_avgrcp_prevyear  commax15.2	;
		format rel_change_to_prevyear 			numx8.2		;
		format rel_change_to_prevyear2 			numx8.2		;
		label 		
				sum_predict_rur_year          	= 'Руб., прогноз на ближайший год'
				sum_actual_rur_prevyear2      	= 'Руб., факт за позапрошлый год'
				sum_actual_rur_prevyear       	= 'Руб., факт за прошлый год'
				sum_apndx_predict_rur_year 		= 'Руб., часть прогноза за прошлый год'
				sum_act_n_apndx_rur_prevyear  	= 'Руб., факт и часть прогноза за прошлый год'
		                                       
				sum_predict_gc_year            	= 'Чеки, прогноз на ближайший год'
				sum_actual_gc_prevyear2        	= 'Чеки, факт за позапрошлый год'
				sum_actual_gc_prevyear         	= 'Чеки, факт за прошлый год'
				sum_apndx_predict_gc_year   	= 'Чеки, часть прогноза за прошлый год'
				sum_act_n_apndx_gc_prevyear    	= 'Чеки, факт и часть прогноза за прошлый год'
				                       
				sum_predict_avgrcp_year         = 'Ср.чек, прогноз на ближайший год'
				sum_actual_avgrcp_prevyear2     = 'Ср.чек, факт за позапрошлый год'
				sum_act_n_apndx_avgrcp_prevyear = 'Ср.чек, факт и часть прогноза за прошлый год'
		
				REL_CHANGE_TO_PREVYEAR			= 'Отклонение к прошлому году' 
				REL_CHANGE_TO_PREVYEAR2			= 'Отклонение к позапрошлому году' 	
		
				ALERT_TREND_DAMP_TO_PREVYEAR	= 'Алерт - падение к прошлому году!'	
				ALERT_TREND_DAMP_TO_PREVYEAR2	= 'Алерт - падение к позапрошлому году!'
	
				ALERT_TREND_GROWTH_TO_PREVYEAR	= 'Алерт - рост к прошлому году!'	
				ALERT_TREND_GROWTH_TO_PREVYEAR2	= 'Алерт - рост к позапрошлому году!'
			;
	run;


	/* Clear CAS */
	proc casutil incaslib="CASUSER" ;
		droptable casdata = "AVGRCP_SUM_FCST" 			quiet;
		droptable casdata = "AVGRCP_SUM_FCST_YEAR" 		quiet;
		droptable casdata = "AVGRCP_SUM_FCST_APNDX" 	quiet;
		droptable casdata = "AVGRCP_SUM_HIST" 			quiet;
		droptable casdata = "AVGRCP_SUM_HIST_PREVYEAR" 	quiet;
		droptable casdata = "AVGRCP_SUM_HIST_PREVYEAR2" quiet;
		droptable casdata = "AVGRCP_SUM_YEAR" 			quiet;
		droptable casdata = "AVGRCP_SUM_YEAR_PRE" 		quiet;
		droptable casdata = "ALERT_YEAR_TREND" 			quiet;
	run;

%mend avgrcp_alert_year_trend;