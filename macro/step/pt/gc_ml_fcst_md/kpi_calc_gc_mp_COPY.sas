/*TODO1: выкидывать периоды временного закрытия в разрезе рест-день из прогноза*/

%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( cmasauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

options casdatalimit=600000M;



/* Изменяемые параметры */
/*
01MAR2021		31MAR2021
01DEC2020		31DEC2020
01JAN2021		31JAN2021
*/

/* Январь */
/* %let suf = Январь 2021; */
/* %let lmvTableFcstGc 	= max_casl.gc_ml_JAN_MD_v2; */
/* %let tb2 = max_casl.gc_ml_JAN_MD_v0; */
/* %let lmvStartDate 	= '01jan2021'd; */
/* %let lmvEndDate 	= '31jan2021'd; */
/* %let graph_points = ; */


/* Декабрь */
/* %let suf = Декабрь 2020; */
/* %let lmvTableFcstGc 	= max_casl.gc_ml_DEC_MD_v2; */
/* %let lmvStartDate 	= '01dec2020'd; */
/* %let lmvEndDate 	= '31dec2020'd; */
/* %let tb2 = max_casl.gc_ml_DEC_MD_v0; */
/* %let graph_points = ; */

/* Март */
/* %let suf = Март 2021; */
/* %let lmvTableFcstGc 	= max_casl.gc_ml_MAR_MD_v2; */
/* %let lmvStartDate 	= '01mar2021'd; */
/* %let lmvEndDate 	= '31mar2021'd; */
/* %let tb2 = max_casl.gc_ml_MAR_MD_v0; */
/* %let graph_points = refline '08mar2021'd  / axis=x  */
/* 							lineattrs=(thickness=1  pattern=dash ) */
/* 							label = '8march' */
/* 							transparency=0.33; */



/* Май */
%let suf = Май 2021;
%let lmvTableFcstGc 	= max_casl.gc_ml_MAY_MD_v2;
%let lmvStartDate 	= '01may2021'd;
%let lmvEndDate 	= '31may2021'd;
%let tb2 = max_casl.gc_ml_MAY_MD_v0;
%let graph_points = refline '09may2021'd  / axis=x 
							lineattrs=(thickness=1  pattern=dash )
							label = '9may'
							transparency=0.33;

%let lmvToleranceDays 	= 1;		/* Допустимое отклонение в днях */
%let lmvCompMode	  	= NO;		/* YES or NO - по всем компам, или по всем компам и еще доп.ограничениям из методики */
%let lmvFcstVsActMode 	= left;		/* full or left - способ соединения Forecast и Actual */
%let lmvMissingMode 	= .;		/* 0 or . - способ заполнения missing при соединении Forecast и Actual */

/* data casuser.gc_ml_jan; */
/* 	set mn_calc.gc_ml_january_new; */
/* run; */


/* %let lmvTableFcstGc 	= max_casl.fcst_gc_ml_v2; */
%let lmvOutTablePostfix = MD_v1_JAN;		/* Результаты см. в WORK */
%let lmvCasLibLaunch	= MAX_CASL; /* MN_SHORT or CASUSER */

/* Имена импортированных таблиц прогнозов McDonald's в библиотеке MAX_CASL*/
%let lmvGC_SALE_DAY 	= MCD_GC_SALES_COUNTRY_DAY;
%let lmvGC_PBO_MONTH 	= MCD_GC_STORE_MONTH;



/* Неизменяемые параметры */
%let lmvReportDttm 	       = &ETL_CURRENT_DTTM.;
%let lmvStartDateFormatted = %str(date%')%sysfunc(putn(&lmvStartDate., yymmdd10.))%str(%');
%let lmvEndDateFormatted   = %str(date%')%sysfunc(putn(&lmvEndDate.  , yymmdd10.))%str(%');
%let lmvTestMonthDate 	   = %str(date%')%sysfunc(putn(%sysfunc(intnx(month,&lmvStartDate.,0)), yymmdd10.))%str(%');
%let lmvExcludingList 	   = 9908, 1494, 1495, 1496, 1497, 1498, 1499 ;




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
/*TODO1: приджойнить к прогнозу*/
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

/* Step 2. All days higher than 100 gc */
proc fedsql sessref=casauto;
	create table CASUSER.PBO_GC_OVER100 {options replace=true} as
	select PBO_LOCATION_ID
		, count(SALES_DT) as count_days 								/* Кол-во дней после фильтрации */
	from &lmvCasLibLaunch..PBO_SALES 
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

/*TODO1: добавить нерабочие магазины*/
proc fedsql sessref=casauto;
	create table CASUSER.GC_SAS_FCST {options replace=true} as
	select pbo.PBO_LOCATION_ID
		, pbo.SALES_DT
		, pbo.GC_PREDICT as GC_SAS_FCST

	from &lmvTableFcstGc. as pbo

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
	
	from &lmvCasLibLaunch..PBO_SALES as pbo
	
	where   pbo.SALES_DT >= &lmvStartDateFormatted. 						/* Фильтр на тестовый период */
		and pbo.SALES_DT <= &lmvEndDateFormatted.							
		and pbo.CHANNEL_CD = 'ALL'											/* Фильтр на канал !!! */
	;
quit;


proc fedsql sessref=casauto;
	create table CASUSER.GC_FCST_VS_ACT {options replace=true} as
	select 
		  coalesce(sas.PBO_LOCATION_ID	, act.PBO_LOCATION_ID	) as PBO_LOCATION_ID
		, coalesce(sas.SALES_DT			, act.SALES_DT			) as SALES_DT
		, coalesce(act.GC_ACT			, &lmvMissingMode.		) as GC_ACT
		, coalesce(sas.GC_SAS_FCST		, &lmvMissingMode.		) as GC_SAS_FCST
 
	from CASUSER.GC_SAS_FCST  as sas

	&lmvFcstVsActMode. join CASUSER.GC_ACT as act	
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
	select avg(abs(BIAS_SAS))  as BIAS_SAS
	from CASUSER.FOR_KPI_WEEK_BIAS
/* 	where week_dt not in (&lmvExcludeWeekList.) */
	;
quit;

data casuser.t;
	tb = "&lmvTableFcstGc";
run;

proc fedsql sessref=casauto ;
	create table casuser.finals {options replace=true} as
	select 0 as n, 'table_name' as nm, tb as val from casuser.t
	union
	select 1 as n, 'WAPE_SAS', round(WAPE_SAS,0.001)::char(10) from CASUSER.KPI_MONTH_WAPE
	union
	select 2 as n, 'BIAS_SAS (m)', round(BIAS_SAS,0.001)::char(10) from CASUSER.KPI_MONTH_BIAS
	union
	select 3 as n, 'BIAS_SAS (w)', round(BIAS_SAS,0.001)::char(10) from CASUSER.KPI_WEEK_BIAS;
quit;
proc sql; select * from casuser.finals order by n; quit;

/**************************************************************************************/
/*********************************** RESULT SAVING ************************************/
/**************************************************************************************/


/* WAPE * MONTH   and   BIAS * MONTH   and   BIAS * WEEK */
/*
proc casutil;
	droptable 
		casdata		= "KPI_GC_WAPE_MONTH_SAS_&lmvOutTablePostfix." 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run;     

data MAX_CASL.KPI_GC_WAPE_MONTH_SAS_&lmvOutTablePostfix. (promote=yes);
	set casuser.KPI_MONTH_WAPE;
run;

proc casutil;         
	save              
		casdata		= "KPI_GC_WAPE_MONTH_SAS_&lmvOutTablePostfix" 
		incaslib	= "MAX_CASL" 
		casout		= "KPI_GC_WAPE_MONTH_SAS_&lmvOutTablePostfix"  
		outcaslib	= "MAX_CASL"
	;
run;




proc casutil;
	droptable 
		casdata		= "KPI_GC_BIAS_MONTH_SAS_&lmvOutTablePostfix." 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run;     

data MAX_CASL.KPI_GC_BIAS_MONTH_SAS_&lmvOutTablePostfix. (promote=yes);
	set casuser.KPI_MONTH_BIAS;
run;

proc casutil;         
	save              
		casdata		= "KPI_GC_BIAS_MONTH_SAS_&lmvOutTablePostfix" 
		incaslib	= "MAX_CASL" 
		casout		= "KPI_GC_BIAS_MONTH_SAS_&lmvOutTablePostfix"  
		outcaslib	= "MAX_CASL"
	;
run;



proc casutil;
	droptable 
		casdata		= "KPI_GC_BIAS_WEEK_SAS_&lmvOutTablePostfix." 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run;     

data MAX_CASL.KPI_GC_BIAS_WEEK_SAS_&lmvOutTablePostfix. (promote=yes);
	set casuser.FOR_KPI_WEEK_BIAS;
run;

proc casutil;         
	save              
		casdata		= "KPI_GC_BIAS_WEEK_SAS_&lmvOutTablePostfix" 
		incaslib	= "MAX_CASL" 
		casout		= "KPI_GC_BIAS_WEEK_SAS_&lmvOutTablePostfix"  
		outcaslib	= "MAX_CASL"
	;
run;

*/

%put &=lmvTableFcstGc;
/* %let lmvTableFcstGc = max_casl.gc_ml_JAN_MD_v3; */
/* %let tb2 = max_casl.gc_ml_JAN_MD_v0; */

proc fedsql sessref=casauto;
	create table CASUSER.GC_graph {options replace=true} as
	select 
		pbo.SALES_DT
		, sum(pbo.GC_PREDICT) as Predict
		, sum(pbo.GC_ACTUAL) as Actual
/* 		, sum(t2.GC_PREDICT) as Predict_old */

		, sum(pbo.target) as Actual_ds
		, sum(pbo.p_target) as Predict_ds
/* 		, sum(t2.p_target) as Predict_ds_old */
/* 		, max(case when Mcf.s_dt is not missing then 1 else 0 end) as McFest */


	from &lmvTableFcstGc. as pbo

	inner join CASUSER.PBO_LIST as list
		on pbo.PBO_LOCATION_ID = list.PBO_LOCATION_ID

/* 	left join &tb2 as t2 	 */
/* 		on pbo.sales_dt=t2.sales_dt */
/* 		and pbo.pbo_location_id = t2.pbo_location_id */
/* 		and pbo.channel_cd=t2.channel_cd */

/* 	left join Casuser.McFest_2 as McF on pbo.sales_dt between Mcf.s_dt and mcf.e_dt */
	
	where   
		pbo.SALES_DT >=  &lmvStartDateFormatted-0						
		and pbo.SALES_DT <= &lmvEndDateFormatted							
		and pbo.CHANNEL_CD = 'ALL'											
	group by pbo.SALES_DT
		 
;quit;

proc export data=CASUSER.GC_graph dbms=xlsx file = "/home/ru-mdubrovsky/Files/GC_ML_Predict2.xlsx" replace;
	sheet="&suf";
run;quit;
/* 9may */



proc sgplot data =CASUSER.GC_graph ;
	title "&suf";
/* 	series x=sales_dt y=PREDICT_old / lineattrs=(color=darkyellow) transparency=0.3; */
	
	series x=sales_dt y=ACTUAL /  lineattrs=(color=blue) transparency=0.3;
	series x=sales_dt y=PREDICT / lineattrs=(color=darkgreen) transparency=0.3;
	
	
/* 	scatter x=sales_dt y=MCFEST / markerattrs=(color=red size=3) y2axis; */
/* 	y2axis grid values = (1 to 2 by 1) display=(NOVALUES NOTICKS NOLINE NOLABEL);	 */
/* 	 */
/* 	refline &lmvStartDate  / axis=x  */
/* 							lineattrs=(thickness=2 color=black pattern=dash) */
/* 							label='predict'; */
	
/* 	&graph_points; */
;
quit;



proc sgplot data =CASUSER.GC_graph ;
	title "&suf (deseasoned)";
/* 	series x=sales_dt y=PREDICT_DS_OLD / lineattrs=(color=darkyellow)  transparency=0.3; */
	series x=sales_dt y=ACTUAL_DS / lineattrs=(color=blue) transparency=0.3;
	series x=sales_dt y=PREDICT_DS / lineattrs=(color=darkgreen)  transparency=0.3;
		refline &lmvStartDate / axis=x 
							lineattrs=(thickness=2 color=black pattern=dash)
							label='predict';
/* 	&graph_points; */

/* 	scatter x=sales_dt y=MCFEST / markerattrs=(color=red size=3) y2axis; */
/* 	y2axis grid values = (1 to 2 by 1) display=(NOVALUES NOTICKS NOLINE NOLABEL); */
quit;