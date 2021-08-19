/* Параметры макроса

Параметр				Возможные значения
mpProdLvl 			 	PROD_LVL1_NM, PROD_LVL2_NM, PROD_LVL3_NM, product_nm
mpLocLvl 			 	A_AGREEMENT_TYPE, LVL1_NM, LVL2_NM, LVL3_NM, pbo_location_nm
mpBorderDt 			 	дата в виде числа 22646, любая функция от даты, например intnx('year', today(), 0, 'B')
mpAlertCriterionDamp 	интервал от 0 до 1, исключая границы 
mpAlertCriterionGrowth 	интервал от 0 до 1, исключая границы 
mpOutTable 			 	имя таблицы, например, AVGRCP_ALERT_YEAR_TREND

*/


%macro avgrcp_alert_strange_seasonality(
	      mpProdLvl = PROD_LVL2_NM
		, mpLocLvl = LVL2_NM
		, mpBorderDt = 22333
		, mpSeason = month
		, mpAlertCriterionRelChange = 0.3
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
		curr_season_start_dt = intnx(%tslit(&mpSeason.), border_dt, 0, 'B');
		next_season_start_dt = intnx(%tslit(&mpSeason.), border_dt, 1, 'B');
		new_border_dt = ifn(
			  curr_season_start_dt = border_dt
			, border_dt
			, next_season_start_dt
		);		
		call symputx('mpBorderDt', new_border_dt);

		if %tslit(&mpSeason.) = 'qtr' then year_shift = 4;
		if %tslit(&mpSeason.) = 'month' then year_shift = 12;
		call symputx('mpYearShift', year_shift);
	run;
	%put &=mpBorderDt;
	%put &=mpYearShift;


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
			, cast(intnx(%tslit(&mpSeason.), main.DATA, 0, 'B') as date) as season_dt		
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
			, season_dt
			, sum(sum_predict_rur	) as sum_predict_rur
			, sum(sum_predict_gc	) as sum_predict_gc
		from 
			CASUSER.AVGRCP_SUM_FCST_PRE as main
		where 
			cast(sales_dt as date) >= cast(&mpBorderDt. as date)
		group by 1,2,3,4
		;
	quit;

	/* 2. просуммировать прогноз за год вперед после даты mpBorderDt */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.AVGRCP_SUM_FCST_YEAR {options replace=true} as
		select 
			  &mpLocLvl.
			, &mpProdLvl.
			, season_dt
			, %if %tslit(&mpSeason.) = 'semiyear' %then %do;
				ceil(qtr(cast(season_dt as date)) / 2) as season_num
			  %end;
			  %else %do;
				&mpSeason.(cast(season_dt as date)) as season_num
			  %end;
			, sum(sum_predict_rur) as sum_predict_rur_year
			, sum(sum_predict_gc) as sum_predict_gc_year
		from 
			CASUSER.AVGRCP_SUM_FCST
		where 
			sales_dt < cast(intnx('month', &mpBorderDt., 12, 'B') as date)
		group by 1,2,3,4
		;
	quit;

	/* 3. Часть прогноза до даты mpBorderDt */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.AVGRCP_SUM_FCST_APNDX {options replace=true} as
		select 
		/* 	  main.ORG as channel_cd */
			  &mpLocLvl.
			, &mpProdLvl.	
			, season_dt
			, %if %tslit(&mpSeason.) = 'semiyear' %then %do;
				ceil(qtr(cast(season_dt as date)) / 2) as season_num
			  %end;
			  %else %do;
				&mpSeason.(cast(season_dt as date)) as season_num
			  %end;
			, sum(sum_predict_rur	) as sum_apndx_predict_rur_year
			/* Чеки суммируем по иерархии PBO */
			, sum(sum_predict_gc	) as sum_apndx_predict_gc_year
		from 
			CASUSER.AVGRCP_SUM_FCST_PRE as main
		where 
			cast(sales_dt as date) < cast(&mpBorderDt. as date)
		group by 1,2,3,4
		;
	quit;	

	/* 4. Усреднить прогноз за все сезоны на год вперед после даты mpBorderDt */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.AVGRCP_AVG_FCST_YEAR {options replace=true} as
		select 
			  &mpLocLvl.
			, &mpProdLvl.
			, avg(sum_predict_rur_year) as avg_predict_rur_year
			, avg(sum_predict_gc_year) as avg_predict_gc_year
		from 
			CASUSER.AVGRCP_SUM_FCST_YEAR
		group by 1,2
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
			, cast(intnx(%tslit(&mpSeason.), main.sales_dt, 0, 'B') as date) as season_dt	
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
			, season_dt		
			, sum(sum_actual_rur	) as sum_actual_rur
			/* Суммируем чеки */
			, sum(sum_actual_gc	) as sum_actual_gc
		from 
			CASUSER.AVGRCP_SUM_HIST_PRE as main
		group by 1,2,3,4
		;
	quit;

	/* 2. просуммировать факты за год назад */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.AVGRCP_SUM_HIST_PREVYEAR {options replace=true} as
		select 
			  &mpLocLvl.
			, &mpProdLvl.
			, season_dt
			, %if %tslit(&mpSeason.) = 'semiyear' %then %do;
				ceil(qtr(cast(season_dt as date)) / 2) as season_num
			  %end;
			  %else %do;
				&mpSeason.(cast(season_dt as date)) as season_num
			  %end;
			, sum(sum_actual_rur) as sum_actual_rur_prevyear
			, sum(sum_actual_gc) as sum_actual_gc_prevyear

		from 
			CASUSER.AVGRCP_SUM_HIST
		where 
			season_dt >= cast(intnx('month', &mpBorderDt., - 12, 'B') as date)
		group by 1,2,3,4
		;
	quit;

	/* 3. просуммировать факты за год, перед предыдущим */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.AVGRCP_SUM_HIST_PREVYEAR2 {options replace=true} as
		select 
			  &mpLocLvl.
			, &mpProdLvl.
			, season_dt
			, %if %tslit(&mpSeason.) = 'semiyear' %then %do;
				ceil(qtr(cast(season_dt as date)) / 2) as season_num
			  %end;
			  %else %do;
				&mpSeason.(cast(season_dt as date)) as season_num
			  %end;
			, sum(sum_actual_rur) as sum_actual_rur_prevyear2
			, sum(sum_actual_gc) as sum_actual_gc_prevyear2
		from 
			CASUSER.AVGRCP_SUM_HIST
		where 
			season_dt >= cast(intnx('month', &mpBorderDt., - 2 * 12, 'B') as date)
			and season_dt < cast(intnx('month', &mpBorderDt., - 12, 'B') as date)
		group by 1,2,3,4
		;
	quit;

	/* 4. Среднее за предыдущий год с учетом части прогноза */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.AVGRCP_SUM_HIST_N_FCST {options replace=true} as
		select 
		      coalescec(cast(hist.&mpLocLvl. as VARCHAR), cast(fcst.&mpLocLvl. as VARCHAR)) as &mpLocLvl.
			, coalescec(cast(hist.&mpProdLvl. as VARCHAR), cast(fcst.&mpProdLvl. as VARCHAR)) as &mpProdLvl.
			, coalesce(hist.season_dt, fcst.season_dt) as season_dt
			, coalesce(hist.season_num, fcst.season_num) as season_num
			, hist.sum_actual_rur_prevyear
			, hist.sum_actual_gc_prevyear
			, fcst.sum_apndx_predict_rur_year
			, fcst.sum_apndx_predict_gc_year
			, sum(hist.sum_actual_rur_prevyear, fcst.sum_apndx_predict_rur_year) as sum_act_n_apndx_rur_prevyear
			, sum(hist.sum_actual_gc_prevyear, fcst.sum_apndx_predict_gc_year) as sum_act_n_apndx_gc_prevyear
		from CASUSER.AVGRCP_SUM_HIST_PREVYEAR as hist
		full join CASUSER.AVGRCP_SUM_FCST_APNDX as fcst 
			on fcst.&mpLocLvl. = hist.&mpLocLvl.
			and fcst.&mpProdLvl. = hist.&mpProdLvl.
			and fcst.season_num = hist.season_num
		;	
		create table CASUSER.AVGRCP_AVG_HIST_N_FCST {options replace=true} as
		select 		
			  &mpLocLvl.
			, &mpProdLvl.	
			, avg(sum_apndx_predict_rur_year	) as avg_act_n_apndx_rur_prevyear
			/* Чеки суммируем по иерархии PBO */
			, avg(sum_apndx_predict_gc_year	) as avg_act_n_apndx_gc_prevyear
		from 
			CASUSER.AVGRCP_SUM_HIST_N_FCST 
		group by 1,2
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
			  fcst.&mpLocLvl.
			, fcst.&mpProdLvl.
			, fcst.season_dt
			, fcst.season_num
	
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

			, avgfcst.avg_predict_rur_year
			, avgfcst.avg_predict_gc_year
			, case 
				when avgfcst.avg_predict_rur_year > 0 
					and avgfcst.avg_predict_gc_year > 0
				then avgfcst.avg_predict_rur_year / avgfcst.avg_predict_gc_year
				else 0
			  end as avg_predict_avgrcp_year

			, avghist.avg_act_n_apndx_rur_prevyear
			, avghist.avg_act_n_apndx_gc_prevyear
			, case 
				when avghist.avg_act_n_apndx_rur_prevyear > 0 
					and avghist.avg_act_n_apndx_gc_prevyear > 0
				then avghist.avg_act_n_apndx_rur_prevyear / avghist.avg_act_n_apndx_gc_prevyear
				else 0
			  end as avg_act_n_apndx_avgrcp_prevyear

			
		from 
			CASUSER.AVGRCP_SUM_FCST_YEAR as fcst

		left join 
			CASUSER.AVGRCP_SUM_FCST_APNDX as apndx
			on fcst.&mpLocLvl. = apndx.&mpLocLvl.
			and fcst.&mpProdLvl. = apndx.&mpProdLvl.
			and fcst.season_num = apndx.season_num
		
		left join 
			CASUSER.AVGRCP_sum_HIST_PREVYEAR as hist
			on fcst.&mpLocLvl. = hist.&mpLocLvl.
			and fcst.&mpProdLvl. = hist.&mpProdLvl.
			and fcst.season_num = hist.season_num

		left join 
			CASUSER.AVGRCP_sum_HIST_PREVYEAR2 as hist2
			on fcst.&mpLocLvl. = hist2.&mpLocLvl.
			and fcst.&mpProdLvl. = hist2.&mpProdLvl.	
			and fcst.season_num = hist2.season_num

		left join 
			CASUSER.AVGRCP_AVG_FCST_YEAR as avgfcst
			on fcst.&mpLocLvl. = avgfcst.&mpLocLvl.
			and fcst.&mpProdLvl. = avgfcst.&mpProdLvl.	

		left join 
			CASUSER.AVGRCP_AVG_HIST_N_FCST as avghist
			on fcst.&mpLocLvl. = avghist.&mpLocLvl.
			and fcst.&mpProdLvl. = avghist.&mpProdLvl.	
		;
	quit;

	/* Рассчитать отклонения */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.AVGRCP_SUM_YEAR {options replace=true} as
		select *
			, case 
				when sum_actual_avgrcp_prevyear2 > 0 
					and sum_actual_avgrcp_prevyear2 is not null
				then abs(sum_predict_avgrcp_year - sum_actual_avgrcp_prevyear2) / sum_actual_avgrcp_prevyear2 
				else 0
			  end as modrel_change_to_prevyear2
			, case 
				when sum_act_n_apndx_avgrcp_prevyear > 0 
					and sum_act_n_apndx_avgrcp_prevyear is not null
				then abs(sum_predict_avgrcp_year - sum_act_n_apndx_avgrcp_prevyear) / sum_act_n_apndx_avgrcp_prevyear 
				else 0
			  end as modrel_change_to_prevyear
			, case 
				when avg_predict_avgrcp_year > 0 
					and avg_predict_avgrcp_year is not null
				then abs(sum_predict_avgrcp_year - avg_predict_avgrcp_year) / avg_predict_avgrcp_year 
				else 0
			  end as modrel_change_to_avg_curryear
			, case 
				when avg_act_n_apndx_avgrcp_prevyear > 0 
					and avg_act_n_apndx_avgrcp_prevyear is not null
				then abs(sum_predict_avgrcp_year - avg_act_n_apndx_avgrcp_prevyear) / avg_act_n_apndx_avgrcp_prevyear 
				else 0
			  end as modrel_change_to_avg_prevyear
		from 
			CASUSER.AVGRCP_SUM_YEAR_PRE 
		;
	quit;


	/* Рассчитать алерты */
	proc fedsql sessref=casauto noprint;
		create table CASUSER.ALERT_STRANGE_SEASON {options replace=true} as
		select 
		 	  &mpLocLvl.
			, &mpProdLvl.
			, season_dt
  			, season_num

			, sum_predict_rur_year          
			, sum_actual_rur_prevyear2      
			, sum_actual_rur_prevyear       
			, sum_apndx_predict_rur_year 		
			, sum_act_n_apndx_rur_prevyear  
			, avg_act_n_apndx_rur_prevyear 
			, avg_predict_rur_year

			, sum_predict_gc_year            
			, sum_actual_gc_prevyear2        
			, sum_actual_gc_prevyear         
			, sum_apndx_predict_gc_year   		
			, sum_act_n_apndx_gc_prevyear
			, avg_act_n_apndx_gc_prevyear
			, avg_predict_gc_year
    
			, sum_predict_avgrcp_year          	
			, sum_actual_avgrcp_prevyear2      	
			, sum_act_n_apndx_avgrcp_prevyear
			, avg_act_n_apndx_avgrcp_prevyear
			, avg_predict_avgrcp_year
  	
			, modrel_change_to_prevyear 			
			, modrel_change_to_prevyear2 
			, modrel_change_to_avg_curryear
			, modrel_change_to_avg_prevyear
			  
			/* Прогноз на год вперед выше/ниже, чем факт за прошлый год на X% */
			, case
				when modrel_change_to_prevyear > &mpAlertCriterionRelChange.
					and modrel_change_to_prevyear is not missing 
						then 1
				else 0
				end as alert_change_to_prevyear

			/* Прогноз на год вперед выше/ниже, чем факт за прошлый год на X%, и выше/ниже чем факт за позапрошлый год на X% */
			, case
				when modrel_change_to_prevyear2 > &mpAlertCriterionRelChange.					
					and modrel_change_to_prevyear2 is not missing 
						then 1
				else 0
				end as alert_change_to_prevyear2

			/* Прогноз на год вперед выше/ниже, чем средний прогноз по сезонам на год вперед на X% */
			, case
				when modrel_change_to_avg_curryear > &mpAlertCriterionRelChange.					
					and modrel_change_to_avg_curryear is not missing 
						then 1
				else 0
				end as alert_change_to_avg_curryear

			/* Прогноз на год вперед выше/ниже, чем средний факт+прогноз по сезонам за прошлый год на X% */
			, case
				when modrel_change_to_avg_prevyear > &mpAlertCriterionRelChange.					
					and modrel_change_to_avg_prevyear is not missing 
						then 1
				else 0
				end as alert_change_to_avg_prevyear

		from 
			CASUSER.AVGRCP_SUM_YEAR		
		;
	quit;

	proc casutil incaslib="DM_ALERT" ;
		droptable casdata = "&mpOutTableNm" quiet;
	run;

	data DM_ALERT.&mpOutTableNm.(promote=yes);
		set  CASUSER.ALERT_STRANGE_SEASON;
		format season_dt yymmdd10.;

		format sum_predict_rur_year          	commax15.	;
		format sum_actual_rur_prevyear2      	commax15.	;
		format sum_actual_rur_prevyear       	commax15.	;
		format sum_apndx_predict_rur_year 		commax15.	;
		format sum_act_n_apndx_rur_prevyear  	commax15.	;
		format avg_predict_rur_year				commax15.	;
		format avg_act_n_apndx_rur_prevyear		commax15.	;

		format sum_predict_gc_year            	commax15.	;
		format sum_actual_gc_prevyear2        	commax15.	;
		format sum_actual_gc_prevyear         	commax15.	;
		format sum_apndx_predict_gc_year   		commax15.	;
		format sum_act_n_apndx_gc_prevyear    	commax15.	;
		format avg_predict_gc_year				commax15.	;
		format avg_act_n_apndx_gc_prevyear    	commax15.	;
		
		format sum_predict_avgrcp_year          commax15.2	;		
		format sum_actual_avgrcp_prevyear2      commax15.2	;		
		format sum_act_n_apndx_avgrcp_prevyear  commax15.2	;
		format avg_predict_avgrcp_year   		commax15.2	;
		format avg_act_n_apndx_avgrcp_prevyear  commax15.2	;

		format modrel_change_to_prevyear 		numx8.2		;
		format modrel_change_to_prevyear2 		numx8.2		;
		format modrel_change_to_avg_curryear	numx8.2		;
		format modrel_change_to_avg_prevyear	numx8.2		;

		label 		
			sum_predict_rur_year          	= 'Руб., прогноз на ближайший год'
			sum_actual_rur_prevyear2      	= 'Руб., факт за позапрошлый год'
			sum_actual_rur_prevyear       	= 'Руб., факт за прошлый год'
			sum_apndx_predict_rur_year 		= 'Руб., часть прогноза за прошлый год'
			sum_act_n_apndx_rur_prevyear  	= 'Руб., факт и часть прогноза за прошлый год'
			avg_predict_rur_year			= 'Руб., средний прогноз за текущий год'
			avg_act_n_apndx_rur_prevyear	= 'Руб., средние за сезон факт и часть прогноза за прошлый год'
	                                       
			sum_predict_gc_year            	= 'Чеки, прогноз на ближайший год'
			sum_actual_gc_prevyear2        	= 'Чеки, факт за позапрошлый год'
			sum_actual_gc_prevyear         	= 'Чеки, факт за прошлый год'
			sum_apndx_predict_gc_year   	= 'Чеки, часть прогноза за прошлый год'
			sum_act_n_apndx_gc_prevyear    	= 'Чеки, факт и часть прогноза за прошлый год'
			avg_predict_gc_year				= 'Чеки, средний прогноз за текущий год'
			avg_act_n_apndx_gc_prevyear		= 'Чеки, средние за сезон факт и часть прогноза за прошлый год'
			                       
			sum_predict_avgrcp_year         = 'Ср.чек, прогноз на ближайший год'
			sum_actual_avgrcp_prevyear2     = 'Ср.чек, факт за позапрошлый год'
			sum_act_n_apndx_avgrcp_prevyear = 'Ср.чек, факт и часть прогноза за прошлый год'
			avg_predict_avgrcp_year			= 'Ср.чек, средний прогноз за текущий год'
			avg_act_n_apndx_avgrcp_prevyear	= 'Ср.чек, средние за сезон факт и часть прогноза за прошлый год'
	
			modrel_change_to_prevyear		= 'Модуль отклонения к прошлому году' 
			modrel_change_to_prevyear2		= 'Модуль отклонения к позапрошлому году' 	
			modrel_change_to_avg_curryear	= 'Модуль отклонения к среднему текущему году' 	
			modrel_change_to_avg_prevyear	= 'Модуль отклонения к среднему прошлом году' 	
			
			alert_change_to_prevyear		= 'Алерт - изменение к прошлому году!'	
			alert_change_to_prevyear2		= 'Алерт - изменение к позапрошлому году!'
			alert_change_to_avg_curryear	= 'Алерт - изменение к среднему текущему году!'
			alert_change_to_avg_prevyear	= 'Алерт - изменение к среднему прошлому году!'
			
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
		droptable casdata = "ALERT_STRANGE_SEASON"		quiet;
	run;

%mend avgrcp_alert_strange_seasonality;