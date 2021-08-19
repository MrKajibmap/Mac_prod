%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( cmasauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

options casdatalimit=600000M;

/*
1. Расчет точности McDonald's по всем KPI
2. Сведение в одну таблицу вместе с SAS результатами
*/


%let lmvCasLibLaunch	= MAX_CASL; /* MN_SHORT or CASUSER or MAX_CASL */

/* Имена импортированных таблиц прогнозов McDonald's в библиотеке MAX_CASL*/
%let lmvGC_SALE_DAY 	= MCD_GC_SALES_COUNTRY_DAY;
%let lmvGC_PBO_MONTH 	= MCD_GC_STORE_MONTH;
%let lmvUPT_SKU_MONTH 	= MCD_UPT_SKU_MONTH;


/*******************************************************************************************/
/*******************************************************************************************/
/*******************************************************************************************/
/* Расчет комповых ресторанов-месяцев */

/* Календарь по месяцам */
data casuser.calendar(keep=mon_dt);
input mon_dt;
format mon_dt date9.;
datalines;
22250
22281
22340
;
run;
proc fedsql sessref=casauto;
	create table casuser.comp_list{options replace=true} as
	select
		  pbo.pbo_location_id
		, pbo.A_OPEN_DATE
		, pbo.A_CLOSE_DATE
		, cal.mon_dt
	from 
		&lmvCasLibLaunch..PBO_DICTIONARY as pbo
	cross join
		CASUSER.CALENDAR as cal
	where 
		intnx('month', cal.mon_dt, -12, 'b') >= 
      		case 
	   			when day(pbo.A_OPEN_DATE)=1 
					then cast(pbo.A_OPEN_DATE as date)
	   			else 
					cast(intnx('month',pbo.A_OPEN_DATE,1,'b') as date)
      		end
	    and cal.mon_dt <=
			case
				when pbo.A_CLOSE_DATE is null 
					then cast(intnx('month', date '2021-09-01', 12) as date)
				when pbo.A_CLOSE_DATE=intnx('month', pbo.A_CLOSE_DATE, 0, 'e') 
					then cast(pbo.A_CLOSE_DATE as date)
		   		else 
					cast(intnx('month', pbo.A_CLOSE_DATE, -1, 'e') as date)
			end
	;
quit;


/*******************************************************************************************/
/*******************************************************************************************/
/*******************************************************************************************/
/* Подготовка фактических данных */
/* Вопрос: 
	Применяем только фильтр на канал ALL и оставляем компы?
	Что-то еще ???
*/

/* Units & Sale actual */
proc fedsql sessref=casauto;
	create table CASUSER.UNITS_N_SALE_ACT {options replace=true} as
	select 
		  pbo.pbo_location_id
		, pbo.product_id
		, pbo.sales_dt
		, pbo.net_sales_amt as sale_act
		, sum(coalesce(sales_qty, 0), coalesce(sales_qty_promo,0)) as units_act
		, divide(
			  pbo.net_sales_amt
			, sum(coalesce(sales_qty, 0), coalesce(sales_qty_promo,0))
		  ) as net_avg_price
	
	from &lmvCasLibLaunch..PMIX_SALES as pbo
	
	inner join CASUSER.comp_list as cmp
		on intnx('month', pbo.SALES_DT, 0, 'B') = cmp.mon_dt
		and pbo.pbo_location_id = cmp.pbo_location_id
		and pbo.CHANNEL_CD = 'ALL'					
	;
quit;


/* Guest count actual */
proc fedsql sessref=casauto;
	create table CASUSER.GC_ACT {options replace=true} as
	select 
		  pbo.pbo_location_id
		, pbo.sales_dt
		, pbo.RECEIPT_QTY as gc_act
	
	from &lmvCasLibLaunch..PBO_SALES as pbo
	
	inner join CASUSER.comp_list as cmp
		on intnx('month', pbo.SALES_DT, 0, 'B') = cmp.mon_dt
		and pbo.pbo_location_id = cmp.pbo_location_id
		and pbo.CHANNEL_CD = 'ALL'											
	;
quit;



/*******************************************************************************************/
/*******************************************************************************************/
/*******************************************************************************************/
/* Вычисление KPI по GC: 3шт.
	GC_BIAS_MONTH
	GC_BIAS_WEEK
	GC_WAPE_MONTH
 */

/* Агрегируем факты по GC до дней */
proc fedsql sessref=casauto;
	create table casuser.GC_ACT_BY_DAY {options replace=true} as
	select sales_dt
		, intnx('month', SALES_DT, 0, 'B') as month_dt
		, sum(gc_act	) as gc_act		
	from casuser.GC_ACT 
	group by 1,2
	;
quit;

/* Добавление прогнозов MCD на уровне дней */
proc fedsql sessref=casauto;
	create table casuser.GC_ATOM_DAY {options replace=true} as
	select mcd.sales_dt
		, intnx('month', mcd.SALES_DT, 0, 'B') as month_dt
		, intnx('week.2', mcd.SALES_DT, 0, 'B') as week_dt
 
		, mcd.GC_COMP as gc_mcd
		, gc.gc_act
		, ( mcd.GC_COMP - gc.gc_act ) as gc_err_mcd

	from
		MAX_CASL.&lmvGC_SALE_DAY. as mcd

	inner join CASUSER.GC_ACT_BY_DAY as gc	
		on  mcd.SALES_DT = gc.SALES_DT
	;
quit;

/* GC x BIAS x MONTH */
proc fedsql sessref=casauto;
	create table casuser.KPI_GC_BIAS_MONTH_MCD {options replace=true} as
	select month_dt
		, sum(gc_err_mcd) / sum(gc_act) 	as gc_bias_mcd
		, sum(gc_mcd       ) as sum_gc_mcd       
		, sum(gc_act       ) as sum_gc_act       
		, sum(gc_err_mcd   ) as sum_gc_err_mcd   
	from
		casuser.GC_ATOM_DAY
	group by 1
	;
quit;

/*******************************************************************************************/
/* GC x average BIAS x WEEK */
proc fedsql sessref=casauto;
	create table casuser.GC_SUM_TO_WEEK {options replace=true} as
	select month_dt
		, week_dt
		, sum(gc_err_mcd) / sum(gc_act) 	as gc_bias_mcd
		, sum(gc_mcd       ) as sum_gc_mcd       
		, sum(gc_act       ) as sum_gc_act       
		, sum(gc_err_mcd   ) as sum_gc_err_mcd   
	from
		casuser.GC_ATOM_DAY
	group by 1,2
	;
quit;

/* Средний абсолютный BIAS */
proc fedsql sessref=casauto;
	create table casuser.KPI_GC_BIAS_WEEK_MCD {options replace=true} as
	select month_dt
		, avg(abs(gc_bias_mcd)) as gc_bias_mcd
	from casuser.GC_SUM_TO_WEEK
	group by 1
	;
quit;

/*******************************************************************************************/
/* Агрегируем факты по GC до ПБО-месяц */
proc fedsql sessref=casauto;
	create table casuser.GC_ACT_BY_PBO_MONTH {options replace=true} as
	select pbo_location_id
		, intnx('month', SALES_DT, 0, 'B') as month_dt
		, sum(gc_act) as gc_act		
	from casuser.GC_ACT 
	group by 1,2
	;
quit;

/* Добавление прогнозов MCD на уровне ПБО-месяц */
proc fedsql sessref=casauto;
	create table casuser.GC_ATOM_PBO_MONTH {options replace=true} as
	select mcd.pbo_location_id
		, mcd.month_dt
 
		, mcd.GC_MCD_FCST as gc_mcd
		, gc.gc_act
		, abs( mcd.GC_MCD_FCST - gc.gc_act ) as gc_abserr_mcd

	from
		MAX_CASL.&lmvGC_PBO_MONTH. as mcd

	inner join CASUSER.GC_ACT_BY_PBO_MONTH as gc	
		on  mcd.month_dt = gc.month_dt
		and mcd.pbo_location_id = gc.pbo_location_id
	;
quit;

/* GC x WAPE x MONTH */
proc fedsql sessref=casauto;
	create table casuser.KPI_GC_WAPE_MONTH_MCD {options replace=true} as
	select month_dt
		, sum(gc_abserr_mcd) / sum(gc_act) 	as gc_wape_mcd
		, sum(gc_mcd       ) as sum_gc_mcd       
		, sum(gc_act       ) as sum_gc_act       
		, sum(gc_abserr_mcd   ) as sum_gc_abserr_mcd 
	from
		casuser.GC_ATOM_PBO_MONTH
	group by 1 
	;
quit;


/*******************************************************************************************/
/*******************************************************************************************/
/*******************************************************************************************/
/* Вычисление KPI по SALE: 2шт.
	SALE_BIAS_MONTH
	SALE_BIAS_WEEK
 */

/* Агрегируем факты по SALE до дней */
proc fedsql sessref=casauto;
	create table casuser.SALE_ACT_BY_DAY {options replace=true} as
	select sales_dt
		, intnx('month', SALES_DT, 0, 'B') as month_dt
		, sum(sale_act	) as sale_act		
	from casuser.UNITS_N_SALE_ACT 
	group by 1,2
	;
quit;

/* Добавление прогнозов MCD на уровне дней */
proc fedsql sessref=casauto;
	create table casuser.SALE_ATOM_DAY {options replace=true} as
	select mcd.sales_dt
		, casintnx('month', mcd.SALES_DT, 0, 'B') as month_dt
		, intnx('week.2', mcd.SALES_DT, 0, 'B') as week_dt
 
		, mcd.SALES_COMP as sale_mcd
		, sale.sale_act
		, ( mcd.SALES_COMP - sale.sale_act ) as sale_err_mcd

	from
		MAX_CASL.&lmvGC_SALE_DAY. as mcd
	
	inner join CASUSER.SALE_ACT_BY_DAY as sale	
		on  mcd.SALES_DT = sale.SALES_DT
	;
quit;

/* SALE x BIAS x MONTH */
proc fedsql sessref=casauto;
	create table casuser.KPI_SALE_BIAS_MONTH_MCD {options replace=true} as
	select month_dt
		, sum(sale_err_mcd) / sum(sale_act) as sale_bias_mcd
			
		, sum(sale_mcd     ) as sum_sale_mcd     
		, sum(sale_act     ) as sum_sale_act     
		, sum(sale_err_mcd ) as sum_sale_err_mcd 
	from
		casuser.SALE_ATOM_DAY
	group by 1
	;
quit;


/*******************************************************************************************/
/* SALE x BIAS x WEEK */
proc fedsql sessref=casauto;
	create table casuser.SALE_SUM_TO_WEEK {options replace=true} as
	select month_dt
		, week_dt
		, sum(sale_err_mcd) / sum(sale_act) as sale_bias_mcd

		, sum(sale_mcd     ) as sum_sale_mcd     
		, sum(sale_act     ) as sum_sale_act     
		, sum(sale_err_mcd ) as sum_sale_err_mcd 
	from
		casuser.SALE_ATOM_DAY
	group by 1,2
	;
quit;

/* Средний абсолютный BIAS */
proc fedsql sessref=casauto;
	create table casuser.KPI_SALE_BIAS_WEEK_MCD {options replace=true} as
	select month_dt
		, avg(abs(sale_bias_mcd)) as sale_bias_mcd	
	from casuser.SALE_SUM_TO_WEEK
	group by 1
	;
quit;



/*******************************************************************************************/
/*******************************************************************************************/
/*******************************************************************************************/
/* Вычисление KPI по UPT: 1шт.
	UPT_WAPE_MONTH
 */

/* Агрегация до SKU - месяц */
proc fedsql sessref=casauto;
	/* UNITS_ACT */
	create table CASUSER.UNITS_ACT_BY_SKU_MONTH {options replace=true} as
	select 
		  product_id
		, cast(intnx('month', SALES_DT, 0, 'B')	as date) as month_dt
		, sum(UNITS_ACT) as units_act
	from CASUSER.UNITS_N_SALE_ACT						
	group by 1,2
	;
	/* GC_ACT */
	create table CASUSER.GC_ACT_BY_MONTH {options replace=true} as
	select cast(intnx('month', SALES_DT, 0, 'B') as date) as month_dt
		, sum(gc_act) as gc_act
	from CASUSER.GC_ACT		
	group by 1										
	;
	/* UPT_ACT */
	create table CASUSER.UPT_ACT_BY_SKU_MONTH {options replace=true} as
	select units.product_id
		, units.month_dt
		, units.units_act
		, gc.gc_act
		, divide(1000 * units.units_act, gc.gc_act) as upt_act
	from CASUSER.UNITS_ACT_BY_SKU_MONTH as units
	inner join CASUSER.GC_ACT_BY_MONTH as gc
		on units.month_dt = gc.month_dt
	;
quit;


proc fedsql sessref=casauto;
	create table CASUSER.UPT_ATOM_SKU_MONTH {options replace=true} as
	select 
		  act.product_id
		, act.month_dt
		, coalesce(act.UNITS_ACT, 0) as units_act
		, coalesce(act.GC_ACT   , 0) as gc_act   
		, coalesce(act.UPT_ACT  , 0) as upt_act  
		, coalesce(mcd.UPT, 0) as upt_mcd 
		, abs(coalesce(mcd.UPT, 0) -  coalesce(act.UPT_ACT  , 0)) as upt_mcd_err

	from MAX_CASL.&lmvUPT_SKU_MONTH. as mcd	
	inner join CASUSER.UPT_ACT_BY_SKU_MONTH  as act
		on mcd.product_id 		= act.product_id
		and mcd.month_dt 		= act.month_dt
	;
quit;

/* UPT x WAPE x MONTH */
proc fedsql sessref=casauto;
	create table casuser.KPI_UPT_WAPE_MONTH_MCD {options replace=true} as
	select month_dt
		, sum(upt_mcd_err) / sum(upt_mcd) as upt_wape_mcd

		, sum(upt_mcd     ) as sum_upt_mcd     
		, sum(upt_act     ) as sum_upt_act     
		, sum(upt_mcd_err ) as sum_upt_mcd_err
	from
		casuser.UPT_ATOM_SKU_MONTH
	group by 1
	;
quit;


/*******************************************************************************************/
/********************* Сведение всех результатов в одну таблицу ****************************/
/*******************************************************************************************/


/****************************************************************************************************/
/* Сведение SALE - BIAS - WEEK */

	/* 		, avg(abs(BIAS_SAS_SALE_ML)) as AVG_BIAS_SAS_ML */
	/* 		, avg(abs(BIAS_SAS_SALE_REC_BPLM)) as AVG_BIAS_SAS_REC_BPLM */
	/* 		, avg(abs(BIAS_MCD)) as AVG_BIAS_MCD */
proc fedsql sessref=casauto;
	create table CASUSER.KPI_SALE_BIAS_WEEK_SAS_DEC {options replace=true} as
	select 
		  cast('2020-12-01' as date) as month_dt
		, avg(abs(BIAS_SAS_SALE_REC_APLM)) as sale_bias_sas
	from
		MAX_CASL.KPI_SALE_BIAS_WEEK_SAS_DEC
	group by 1
	;
	create table CASUSER.KPI_SALE_BIAS_WEEK_SAS_JAN {options replace=true} as
	select 
		  cast('2021-01-01' as date) as month_dt
		, avg(abs(BIAS_SAS_SALE_REC_APLM)) as sale_bias_sas
	from
		MAX_CASL.KPI_SALE_BIAS_WEEK_SAS_JAN
	group by 1
	;
	create table CASUSER.KPI_SALE_BIAS_WEEK_SAS_MAR {options replace=true} as
	select 
		  cast('2021-03-01' as date) as month_dt
		, avg(abs(BIAS_SAS_SALE_REC_APLM)) as sale_bias_sas
	from
		MAX_CASL.KPI_SALE_BIAS_WEEK_SAS_MAR
	group by 1
	;
quit;

data CASUSER.KPI_SALE_BIAS_WEEK_SAS;
	set 
		CASUSER.KPI_SALE_BIAS_WEEK_SAS_DEC
		CASUSER.KPI_SALE_BIAS_WEEK_SAS_JAN
		CASUSER.KPI_SALE_BIAS_WEEK_SAS_MAR
	;
	KPI = 'SALE_ABSBIAS_WEEK';
run;

proc fedsql sessref=casauto;
	create table CASUSER.KPI_SALE_BIAS_WEEK {options replace=true} as
	select 
		  sas.month_dt
		, sas.KPI
		, sas.sale_bias_sas as SAS
		, abs(mcd.sale_bias_mcd) as MCD
		, 0.035 as BENCHMARK
	from CASUSER.KPI_SALE_BIAS_WEEK_SAS as sas
	inner join CASUSER.KPI_SALE_BIAS_WEEK_MCD as mcd
	on sas.month_dt = mcd.month_dt
	;
quit;


/****************************************************************************************************/
/* Сведение SALE - BIAS - MONTH*/
data CASUSER.KPI_SALE_BIAS_MONTH_SAS;
	set 
		MAX_CASL.KPI_SALE_BIAS_MONTH_SAS_DEC
		MAX_CASL.KPI_SALE_BIAS_MONTH_SAS_JAN
		MAX_CASL.KPI_SALE_BIAS_MONTH_SAS_MAR
	;
	KPI = 'SALE_ABSBIAS_MONTH';
run;

proc fedsql sessref=casauto;
	create table CASUSER.KPI_SALE_BIAS_MONTH {options replace=true} as
	select 
		  sas.month_dt 
		, sas.KPI
		, abs(sas.BIAS_SAS_SALE_REC_APLM) as SAS
		, abs(mcd.sale_bias_mcd) as MCD
		, 0.030 as BENCHMARK
	from CASUSER.KPI_SALE_BIAS_MONTH_SAS as sas
	inner join CASUSER.KPI_SALE_BIAS_MONTH_MCD as mcd
	on sas.month_dt = mcd.month_dt
	;
quit;


/****************************************************************************************************/
/* Сведение GC - WAPE - MONTH*/
data CASUSER.KPI_GC_WAPE_MONTH_SAS;
	set 
		MAX_CASL.KPI_GC_WAPE_MONTH_SAS_DEC
		MAX_CASL.KPI_GC_WAPE_MONTH_SAS_JAN
		MAX_CASL.KPI_GC_WAPE_MONTH_SAS_MAR
	;
	KPI = 'GC_WAPE_MONTH';
run;

proc fedsql sessref=casauto;
	create table CASUSER.KPI_GC_WAPE_MONTH {options replace=true} as
	select 
		  sas.month_dt 
		, sas.KPI
		, sas.WAPE_SAS as SAS
		, mcd.gc_wape_mcd as MCD
		, 0.060 as BENCHMARK
	from CASUSER.KPI_GC_WAPE_MONTH_SAS as sas
	inner join CASUSER.KPI_GC_WAPE_MONTH_MCD as mcd
	on sas.month_dt = mcd.month_dt
	;
quit;


/****************************************************************************************************/
/* Сведение GC - BIAS - MONTH*/
data CASUSER.KPI_GC_BIAS_MONTH_SAS;
	set 
		MAX_CASL.KPI_GC_BIAS_MONTH_SAS_DEC
		MAX_CASL.KPI_GC_BIAS_MONTH_SAS_JAN
		MAX_CASL.KPI_GC_BIAS_MONTH_SAS_MAR
	;
	KPI = 'GC_ABSBIAS_MONTH';
run;

proc fedsql sessref=casauto;
	create table CASUSER.KPI_GC_BIAS_MONTH {options replace=true} as
	select 
		  sas.month_dt 
		, sas.KPI
		, abs(sas.BIAS_SAS) as SAS
		, abs(mcd.gc_bias_mcd) as MCD
		, 0.020 as BENCHMARK
	from CASUSER.KPI_GC_BIAS_MONTH_SAS as sas
	inner join CASUSER.KPI_GC_BIAS_MONTH_MCD as mcd
	on sas.month_dt = mcd.month_dt
	;
quit;


/****************************************************************************************************/
/* Сведение GC - BIAS - WEEK*/
proc fedsql sessref=casauto;
	create table CASUSER.KPI_GC_BIAS_WEEK_SAS_DEC {options replace=true} as
	select 
		  cast('2020-12-01' as date) as month_dt
		, avg(abs(BIAS_SAS)) as GC_bias_sas
	from
		MAX_CASL.KPI_GC_BIAS_WEEK_SAS_DEC
	group by 1
	;
	create table CASUSER.KPI_GC_BIAS_WEEK_SAS_JAN {options replace=true} as
	select 
		  cast('2021-01-01' as date) as month_dt
		, avg(abs(BIAS_SAS)) as GC_bias_sas
	from
		MAX_CASL.KPI_GC_BIAS_WEEK_SAS_JAN
	group by 1
	;
	create table CASUSER.KPI_GC_BIAS_WEEK_SAS_MAR {options replace=true} as
	select 
		  cast('2021-03-01' as date) as month_dt
		, avg(abs(BIAS_SAS)) as GC_bias_sas
	from
		MAX_CASL.KPI_GC_BIAS_WEEK_SAS_MAR
	group by 1
	;
quit;

data CASUSER.KPI_GC_BIAS_WEEK_SAS;
	set 
		CASUSER.KPI_GC_BIAS_WEEK_SAS_DEC
		CASUSER.KPI_GC_BIAS_WEEK_SAS_JAN
		CASUSER.KPI_GC_BIAS_WEEK_SAS_MAR
	;
	KPI = 'GC_ABSBIAS_WEEK';
run;

proc fedsql sessref=casauto;
	create table CASUSER.KPI_GC_BIAS_WEEK {options replace=true} as
	select 
		  sas.month_dt
		, sas.KPI
		, sas.GC_bias_sas as SAS
		, abs(mcd.GC_bias_mcd) as MCD
		, 0.025 as BENCHMARK
	from CASUSER.KPI_GC_BIAS_WEEK_SAS as sas
	inner join CASUSER.KPI_GC_BIAS_WEEK_MCD as mcd
	on sas.month_dt = mcd.month_dt
	;
quit;


/****************************************************************************************************/
/* Сведение UPT - WAPE - MONTH*/
data CASUSER.KPI_UPT_WAPE_MONTH_SAS;
	set 
		MAX_CASL.KPI_UPT_WAPE_MONTH_SAS_DEC
		MAX_CASL.KPI_UPT_WAPE_MONTH_SAS_JAN
		MAX_CASL.KPI_UPT_WAPE_MONTH_SAS_MAR
	;
	KPI = 'UPT_WAPE_MONTH';
run;

proc fedsql sessref=casauto;
	create table CASUSER.KPI_UPT_WAPE_MONTH {options replace=true} as
	select 
		  sas.month_dt 
		, sas.KPI
		, sas.WAPE_SAS_UPT_REC_APLM as SAS
		, mcd.upt_wape_mcd as MCD
		, 0.15 as BENCHMARK
	from CASUSER.KPI_UPT_WAPE_MONTH_SAS as sas
	inner join CASUSER.KPI_UPT_WAPE_MONTH_MCD as mcd
	on sas.month_dt = mcd.month_dt
	;
quit;



/****************************************************************************************************/
/* Объединение всех результатов */

data CASUSER.KPI_PIVOT;
	set  
		CASUSER.KPI_SALE_BIAS_MONTH
		CASUSER.KPI_SALE_BIAS_WEEK
		CASUSER.KPI_GC_BIAS_MONTH
		CASUSER.KPI_GC_BIAS_WEEK
		CASUSER.KPI_GC_WAPE_MONTH
		CASUSER.KPI_UPT_WAPE_MONTH
	;
run;

data WORK.KPI_PIVOT;
	set casuser.KPI_PIVOT;
	format
		month_dt 	date9.
		SAS 		PERCENTN8.2
		MCD			PERCENTN8.2
		BENCHMARK	PERCENTN8.2
	;
run;

proc sort data = WORK.KPI_PIVOT;
by MONTH_DT KPI;
run;


/*

%let common_path = /opt/sas/mcd_config/macro/step/pt/short_term;

ods excel file="&common_path./KPI_GC_&lmvOutTablePostfix..xlsx"  style=statistical;

ods excel options(sheet_interval = 'none' sheet_name = "KPI_GC_WEEK"	);
proc print data = WORK.KPI_WEEK_&lmvOutTablePostfix. 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "KPI_GC_MONTH"	);
proc print data = WORK.KPI_MONTH_&lmvOutTablePostfix.	label; run;

ods excel close;

