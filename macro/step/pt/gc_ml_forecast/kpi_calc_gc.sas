%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( cmasauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

options casdatalimit=10G;
libname nac "/data/MN_CALC"; /* Директория в которую складываем результат */



/* Изменяемые параметры */
/*
01MAY2021		31MAY2021
01MAR2021		31MAR2021
01DEC2020		31DEC2020
01JAN2021		31JAN2021
*/

%let lmvToleranceDays = 0;		/* Допустимое отклонение в днях */
%let lmvCompMode	  = NO;	/* YES or NO - по всем компам, или по всем компам и еще доп.ограничениям из методики */

/* Имена импортированных таблиц прогнозов McDonald's в библиотеке MAX_CASL*/
%let lmvGC_SALE_DAY 	= MCD_GC_SALES_COUNTRY_DAY;
%let lmvGC_PBO_MONTH 	= MCD_GC_STORE_MONTH;


/* Неизменяемые параметры */
%let lmvExcludingList 	   = 9908, 1494, 1495, 1496, 1497, 1498, 1499 ;




%macro kpi_calculation(
	lmvStartDate 	= '01DEC2020'd,
	lmvEndDate 	= '31DEC2020'd,
	lmvTableFcstGc 	= gc_ml_december_new3,
	lmvCasLibLaunch	= MN_SHORT, /* MN_SHORT or CASUSER */
	experiment = exp1,
	month_name = december
);

	%let lmvReportDttm 	       = &ETL_CURRENT_DTTM.;
	%let lmvStartDateFormatted = %str(date%')%sysfunc(putn(&lmvStartDate., yymmdd10.))%str(%');
	%let lmvEndDateFormatted   = %str(date%')%sysfunc(putn(&lmvEndDate.  , yymmdd10.))%str(%');
	%let lmvTestMonthDate 	   = %str(date%')%sysfunc(putn(%sysfunc(intnx(month,&lmvStartDate.,0)), yymmdd10.))%str(%');


	/************************************************************************************/
	/******************************* 2.1 Restaurants list *******************************/
	/************************************************************************************/

	/* Step 0. Closed dates */

	/* ------------ Start. Дни когда пбо будет уже закрыт (навсегда) ------------------ */
		data casuser.days_pbo_date_close;
			set &lmvCasLibLaunch..PBO_DICTIONARY;
			format period_dt date9.;
			keep PBO_LOCATION_ID CHANNEL_CD period_dt;
			CHANNEL_CD = "ALL"; 
			if A_CLOSE_DATE ne . and A_CLOSE_DATE <= &lmvEndDate. then 
			do period_dt = max(A_CLOSE_DATE, &lmvStartDate.) to &lmvEndDate.;
				output;
			end;
		run;
	/* ------------ End. Дни когда пбо будет уже закрыт (навсегда) -------------------- */


	/* ------------ Start. Дни когда пбо будет временно закрыт ------------------------ */
		data casuser.days_pbo_close;
			set &lmvCasLibLaunch..PBO_CLOSE_PERIOD;
			format period_dt date9.;
			keep PBO_LOCATION_ID CHANNEL_CD period_dt;
			if channel_cd = "ALL" ;
			if (end_dt >= &lmvStartDate. and end_dt <= &lmvEndDate.) 
			or (start_dt >= &lmvStartDate. and start_dt <= &lmvEndDate.) 
			or (start_dt <= &lmvStartDate. and &lmvStartDate. <= end_dt)
			then
			do period_dt = max(start_dt, &lmvStartDate.) to min(&lmvEndDate., end_dt);
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

	/* ------------ Start. Сколько дней в месяце ресторан закрыт ?  ------------------- */
		proc fedsql sessref = casauto;
			create table casuser.num_days_pbo_close{options replace=true} as
			select 
				  pbo_location_id
				, cast(intnx('month', period_dt, 0, 'B') as date) as month_dt
				, count(period_dt) as num_days_pbo_close
			from casuser.days_pbo_close
			group by 1,2
			;
		quit;
	/* ------------ End. Сколько дней в месяце ресторан закрыт ?  --------------------- */


	/* Step 1. Comparable PBOs */
	/* Расчет комповых ресторанов */
	proc fedsql sessref=casauto;
		create table CASUSER.PBO_LIST_COMP {options replace=true} as
		select
			  pbo_location_id
			, A_OPEN_DATE
			, A_CLOSE_DATE
		from 
			&lmvCasLibLaunch..PBO_DICTIONARY

		where 
			intnx('month', &lmvTestMonthDate. , -12, 'b') >= 
				case 
					when day(A_OPEN_DATE)=1 
						then cast(A_OPEN_DATE as date)
					else 
						cast(intnx('month', A_OPEN_DATE, 1, 'b') as date)
				end
			and &lmvTestMonthDate. <=
				case
					when A_CLOSE_DATE is null 
						then cast(intnx('month',  &lmvTestMonthDate., 12) as date)
					when A_CLOSE_DATE=intnx('month', A_CLOSE_DATE, 0, 'e') 
						then cast(A_CLOSE_DATE as date)
					else 
						cast(intnx('month', A_CLOSE_DATE, -1, 'e') as date)
				end
		;
	quit;

	proc casutil;
		load data=etl_ia.PBO_SALES(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm and
				channel_cd = 'ALL'
			)
		) casout='PBO_SALES' outcaslib='casuser' replace;	
	run;

	/* Step 2. All days higher than 100 gc */
	proc fedsql sessref=casauto;
		create table CASUSER.PBO_GC_OVER100 {options replace=true} as
		select PBO_LOCATION_ID
			, count(SALES_DT) as count_days 								/* Кол-во дней после фильтрации */
		from casuser.PBO_SALES 
		where RECEIPT_QTY > 100												/* Фильтр на 100 чеков */
			and SALES_DT >= &lmvStartDateFormatted. 						/* Фильтр на тестовый период */
			and SALES_DT <= &lmvEndDateFormatted.							
			and CHANNEL_CD = 'ALL'											/* Фильтр на канал !!! */
		group by PBO_LOCATION_ID
		;
	quit;

	proc fedsql sessref=casauto;
		create table CASUSER.PBO_LIST_GC_OVER100 {options replace=true} as
		select main.PBO_LOCATION_ID
			, main.count_days 	
			, cl.num_days_pbo_close
		from CASUSER.PBO_GC_OVER100 as main
		left join casuser.num_days_pbo_close as cl
			on main.PBO_LOCATION_ID = cl.PBO_LOCATION_ID
		/* Проверяем, что после фильтрации кол-во осташихся дней продаж равно кол-во дней в тестовом месяце  
			с учетом выброшенных в закрытиях и временных закрытиях дней*/
		where abs(
				( main.count_days + coalesce(cl.num_days_pbo_close, 0) ) 
				- (1 + intck('day', &lmvStartDateFormatted., &lmvEndDateFormatted.) ) 
				) <= &lmvToleranceDays.
			and main.count_days > 0
		;
	quit;


	/* Step 3. Only with sales history */
	proc fedsql sessref=casauto;
		create table CASUSER.PBO_LIST_QTY_OVER0 {options replace=true} as
		select distinct PBO_LOCATION_ID
		from &lmvCasLibLaunch..PMIX_SALES
		where sales_dt <= &lmvEndDateFormatted.  
		  and sales_dt >= &lmvStartDateFormatted.  
		  and channel_cd = 'ALL'
		  and sum(coalesce(sales_qty, 0), coalesce(sales_qty_promo,0)) > 0
		  and product_id not in (&lmvExcludingList.)
		;
	quit;


	/* final pbo list */
	%macro mPboList;
	proc fedsql sessref=casauto;
		create table casuser.PBO_LIST{options replace=true} as
		select t1.PBO_LOCATION_ID
		from 
			casuser.PBO_LIST_COMP as t1

		%if &lmvCompMode. = NO %then %do;
			inner join 
				casuser.PBO_LIST_GC_OVER100 as t2
			on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
			
			inner join 
				casuser.PBO_LIST_QTY_OVER0 as t3
			on t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID
		%end;
		;
	quit;
	%mend mPboList;

	%mPboList;


	/* PBO_LIST: остается в пересечении:
		дек 2020		698 -> 661
		янв 2021		723 -> 640
		мар 2021		723 -> 662
		май 2021		727 -> 685
	*/




	/**************************************************************************************/
	/*********************** ACTUAL & FORECAST DATA PREPARATION ***************************/
	/**************************************************************************************/
	data casuser.&lmvTableFcstGc.;
		set nac.&lmvTableFcstGc.;
	run;

	proc fedsql sessref=casauto;
		create table CASUSER.GC_SAS_FCST {options replace=true} as
		select pbo.PBO_LOCATION_ID
			, pbo.SALES_DT
			, pbo.GC_PREDICT  as GC_SAS_FCST

		from casuser.&lmvTableFcstGc. as pbo

		inner join CASUSER.PBO_LIST as list
			on pbo.PBO_LOCATION_ID = list.PBO_LOCATION_ID

		where   pbo.SALES_DT >= &lmvStartDateFormatted. 						/* Фильтр на тестовый период */
			and pbo.SALES_DT <= &lmvEndDateFormatted.							
			and pbo.CHANNEL_CD = 'ALL'											/* Фильтр на канал !!! */
		;
	quit;

	proc fedsql sessref=casauto;
		create table CASUSER.GC_ACT {options replace=true} as
		select pbo.PBO_LOCATION_ID
			, pbo.SALES_DT
			, pbo.RECEIPT_QTY as GC_ACT
		
		from casuser.PBO_SALES as pbo
		
		where   pbo.SALES_DT >= &lmvStartDateFormatted. 						/* Фильтр на тестовый период */
			and pbo.SALES_DT <= &lmvEndDateFormatted.							
			and pbo.CHANNEL_CD = 'ALL'											/* Фильтр на канал !!! */
		;
	quit;


	proc fedsql sessref=casauto;
		create table CASUSER.GC_FCST_VS_ACT {options replace=true} as
		select sas.PBO_LOCATION_ID
			, sas.SALES_DT
			, act.GC_ACT
			, sas.GC_SAS_FCST
	 
		from CASUSER.GC_SAS_FCST  as sas

		left join CASUSER.GC_ACT as act	
			on  sas.PBO_LOCATION_ID = act.PBO_LOCATION_ID
			and sas.SALES_DT 		= act.SALES_DT
		;
	quit;


	/**************************************************************************************/
	/**************************** ACCURACY CALCULATION ************************************/
	/**************************************************************************************/


	/* GC aggregatinng to atomic level:
		- PBO 	  / month
		- Country / month
		- Country / week
	*/


	/* 1. Расчет GC_STORE_MONTH - WAPE */

	/* Группировка фактов до уровня ПБО - месяц */
	proc fedsql sessref=casauto;
		create table casuser.FOR_ATOM_PBO_MONTH_WAPE {options replace=true} as
		select PBO_LOCATION_ID
			, intnx('month', SALES_DT, 0, 'B') as month_dt
			, sum(gc_act ) as gc_act
			, sum(gc_sas_fcst) as gc_sas_fcst
			, (sum(gc_sas_fcst) - sum(gc_act)) as gc_sas_err
			, abs(sum(gc_sas_fcst) - sum(gc_act)) as gc_sas_abserr
		from
			casuser.GC_FCST_VS_ACT 
		group by 1,2
		;
	quit;

	/* Добавление прогнозов MCD на уровне ПБО-месяц */
	proc fedsql sessref=casauto;
		create table casuser.ATOM_PBO_MONTH_WAPE {options replace=true} as
		select main.PBO_LOCATION_ID
			, main.month_dt
			, main.gc_act

			, main.gc_sas_fcst 
			, main.gc_sas_err
			, main.gc_sas_abserr	

			, coalesce(mcd.gc_mcd_fcst, 0) as gc_mcd_fcst
			, (coalesce(mcd.gc_mcd_fcst, 0) - main.gc_act) as gc_mcd_err
			, abs(coalesce(mcd.gc_mcd_fcst, 0) - main.gc_act) as gc_mcd_abserr
		
		from
			casuser.FOR_ATOM_PBO_MONTH_WAPE as main
		
		left join MAX_CASL.&lmvGC_PBO_MONTH. as mcd	
			on  main.PBO_LOCATION_ID = mcd.PBO_LOCATION_ID
			and main.month_dt 		 = mcd.month_dt
		;
	quit;


	proc fedsql sessref=casauto;
		create table casuser.KPI_MONTH_WAPE {options replace=true} as
		select month_dt
		
			, sum(gc_sas_abserr ) / sum(gc_act ) as WAPE_SAS
			, sum(gc_mcd_abserr ) / sum(gc_act ) as WAPE_MCD

	/* 		, sum(gc_sas_err    ) / sum(gc_act ) as BIAS_SAS */
	/* 		, sum(gc_mcd_err    ) / sum(gc_act ) as BIAS_MCD */

			, sum(gc_act ) as sum_gc_act

			, sum(gc_sas_fcst) as sum_gc_sas_fcst
			, sum(gc_mcd_fcst) as sum_gc_mcd_fcst
		
			, sum(gc_sas_abserr ) as sum_gc_sas_abserr
			, sum(gc_mcd_abserr ) as sum_gc_mcd_abserr
			
	/* 		, sum(gc_sas_err    ) as sum_gc_sas_err */
	/* 		, sum(gc_mcd_err    ) as sum_gc_mcd_err */
			
		from
			casuser.ATOM_PBO_MONTH_WAPE 
		group by month_dt
		;
	quit;



	/* 2. Расчет GC_MONTH_COUNTRY и GC_WEEK_COUNTRY - BIAS */

	/* Группировка фактов до уровня день */
	proc fedsql sessref=casauto;
		create table casuser.FOR_ATOM_DAY_BIAS {options replace=true} as
		select SALES_DT
			, intnx('week.2', SALES_DT, 0, 'B') as week_dt
			, intnx('month', SALES_DT, 0, 'B') as month_dt
			, sum(gc_act ) as gc_act
			, sum(gc_sas_fcst) as gc_sas_fcst
			, (sum(gc_sas_fcst) - sum(gc_act)) as gc_sas_err
	/* 		, abs(sum(gc_sas_fcst) - sum(gc_act)) as gc_sas_abserr */
		from
			casuser.GC_FCST_VS_ACT 
		group by 1,2,3
		;
	quit;

	/* Добавление прогнозов MCD на уровне ПБО-месяц */
	proc fedsql sessref=casauto;
		create table casuser.ATOM_DAY_BIAS {options replace=true} as
		select main.SALES_DT
			, main.month_dt
			, main.week_dt
			, main.gc_act

			, main.gc_sas_fcst 
			, main.gc_sas_err
	/* 		, main.gc_sas_abserr	 */

			, coalesce(mcd.gc_comp, 0) as gc_mcd_fcst
			, (coalesce(mcd.gc_comp, 0) - main.gc_act) as gc_mcd_err
			, abs(coalesce(mcd.gc_comp, 0) - main.gc_act) as gc_mcd_abserr
		
		from
			casuser.FOR_ATOM_DAY_BIAS as main
		
		left join MAX_CASL.&lmvGC_SALE_DAY. as mcd	
			on  main.SALES_DT 		 = mcd.SALES_DT
		;
	quit;


	proc fedsql sessref=casauto;
		create table casuser.KPI_MONTH_BIAS {options replace=true} as
		select month_dt

	/* 		, sum(gc_sas_abserr ) / sum(gc_act ) as WAPE_SAS */
	/* 		, sum(gc_mcd_abserr ) / sum(gc_act ) as WAPE_MCD */

			, sum(gc_sas_err    ) / sum(gc_act ) as BIAS_SAS
			, sum(gc_mcd_err    ) / sum(gc_act ) as BIAS_MCD

			, sum(gc_act ) as sum_gc_act

			, sum(gc_sas_fcst) as sum_gc_sas_fcst
			, sum(gc_mcd_fcst) as sum_gc_mcd_fcst
		
	/* 		, sum(gc_sas_abserr ) as sum_gc_sas_abserr */
	/* 		, sum(gc_mcd_abserr ) as sum_gc_mcd_abserr */
			
			, sum(gc_sas_err    ) as sum_gc_sas_err
			, sum(gc_mcd_err    ) as sum_gc_mcd_err
			
		from
			casuser.ATOM_DAY_BIAS 
		group by month_dt
		;
	quit;


	proc fedsql sessref=casauto;
		create table casuser.FOR_KPI_WEEK_BIAS {options replace=true} as
		select week_dt

	/* 		, sum(gc_sas_abserr ) / sum(gc_act ) as WAPE_SAS */
	/* 		, sum(gc_mcd_abserr ) / sum(gc_act ) as WAPE_MCD */

			, sum(gc_sas_err    ) / sum(gc_act ) as BIAS_SAS
			, sum(gc_mcd_err    ) / sum(gc_act ) as BIAS_MCD

			, sum(gc_act ) as sum_gc_act

			, sum(gc_sas_fcst) as sum_gc_sas_fcst
			, sum(gc_mcd_fcst) as sum_gc_mcd_fcst
		
	/* 		, sum(gc_sas_abserr ) as sum_gc_sas_abserr */
	/* 		, sum(gc_mcd_abserr ) as sum_gc_mcd_abserr */
			
			, sum(gc_sas_err    ) as sum_gc_sas_err
			, sum(gc_mcd_err    ) as sum_gc_mcd_err
			
		from
			casuser.ATOM_DAY_BIAS 
		group by week_dt
		;
	quit;


	%let lmvExcludeWeekList = 22277; /*22368, 22305, 22277*/
	proc fedsql sessref=casauto;
		create table casuser.KPI_WEEK_BIAS {options replace=true} as
		select avg(abs(BIAS_SAS)) as BIAS_SAS
		from CASUSER.FOR_KPI_WEEK_BIAS
	/* 	where week_dt not in (&lmvExcludeWeekList.) */
		;
	quit;

	proc fedsql sessref=casauto;
		create table casuser.kpi_result{options replace=true} as
			select
				t1.wape_sas as wape,
				t2.bias_sas as bias_month,
				t3.bias_sas as bias_week
			from
				(select WAPE_SAS from CASUSER.KPI_MONTH_WAPE) as t1,
				(select BIAS_SAS from CASUSER.KPI_MONTH_BIAS) as t2,
				(select BIAS_SAS from CASUSER.KPI_WEEK_BIAS) as t3
		;
	quit;
	
	data work.kpi_result_temp;
		set casuser.kpi_result;
	run;
	
	proc sql;
		create table work.kpi_result as
			select
				"&experiment." as experiment_name,
				"&month_name." as month,
				t1.wape,
				t1.bias_month,
				t1.bias_week
			from
				work.kpi_result_temp as t1
		;
	quit;

	proc append base=work.kpi_comparing data=work.kpi_result force;
	run;

%mend;

proc datasets library=work nolist;
	delete kpi_result;
run;

%kpi_calculation(
	lmvStartDate = '01DEC2020'd,
	lmvEndDate = '31DEC2020'd,
	lmvTableFcstGc = gc_ml_december,
	lmvCasLibLaunch	= MN_SHORT, /* MN_SHORT or CASUSER */
	experiment = exp1,
	month_name = december
)

%kpi_calculation(
	lmvStartDate = '01DEC2020'd,
	lmvEndDate = '31DEC2020'd,
	lmvTableFcstGc = gc_ml_december_new,
	lmvCasLibLaunch	= MN_SHORT, /* MN_SHORT or CASUSER */
	experiment = exp2,
	month_name = december
)


%kpi_calculation(
	lmvStartDate 	= '01DEC2020'd,
	lmvEndDate 	= '31DEC2020'd,
	lmvTableFcstGc 	= gc_ml_december_new2,
	lmvCasLibLaunch	= MN_SHORT, /* MN_SHORT or CASUSER */
	experiment = exp3,
	month_name = december
)

%kpi_calculation(
	lmvStartDate 	= '01DEC2020'd,
	lmvEndDate 	= '31DEC2020'd,
	lmvTableFcstGc 	= gc_ml_december_new3,
	lmvCasLibLaunch	= MN_SHORT, /* MN_SHORT or CASUSER */
	experiment = exp4,
	month_name = december
)


%kpi_calculation(
	lmvStartDate 	= '01MAR2021'd,
	lmvEndDate 	= '31MAR2021'd,
	lmvTableFcstGc 	= gc_ml_march_new,
	lmvCasLibLaunch	= MN_SHORT, /* MN_SHORT or CASUSER */
	experiment = exp2,
	month_name = march
)

		
 