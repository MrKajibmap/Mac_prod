/*
Сравнить два графика истории
Посчитать прогнозы
Наложить сезонность обратно (изменить скрипт restore_seasanality)
Посмотреть прогнозы вместе с фактом
*/
/* Календарь по месяцам */
data casuser.calendar(keep=mon_dt);
input mon_dt;
format mon_dt date9.;
datalines;
22250
22281
22340
22401
;
run;

/* Расчет комповых ресторанов-месяцев */
proc fedsql sessref=casauto;
	create table casuser.comp_list{options replace=true} as
	select
		  pbo.pbo_location_id
		, pbo.A_OPEN_DATE
		, pbo.A_CLOSE_DATE
		, cal.mon_dt
	from 
		MAX_CASL.PBO_DICTIONARY as pbo
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

PROC FEDSQL sessref=casauto;
	CREATE TABLE CASUSER.compare{options replace=true} AS 
	SELECT act.SALES_DT
		, sum(coalesce(act.receipt_qty, 0)) as actual
		, sum(coalesce(fcst1.GC_PREDICT, 0)) as fcst_ml_v0
		, sum(coalesce(fcst2.GC_PREDICT, 0)) as fcst_ml_v2
		, sum(coalesce(fcst3.GC_PREDICT, 0)) as fcst_ml_v3
	FROM MAX_CASL.PBO_SALES as act
	inner join CASUSER.comp_list as cmp
		on intnx('month', act.SALES_DT, 0, 'B') = cmp.mon_dt
		and act.pbo_location_id = cmp.pbo_location_id
	left join max_casl.fcst_gc_ml_v0 as fcst1
		on fcst1.CHANNEL_CD = act.CHANNEL_CD
		and fcst1.pbo_location_id = act.pbo_location_id	
		and fcst1.SALES_DT = act.SALES_DT	
	left join max_casl.fcst_gc_ml_v2 as fcst2
		on fcst2.CHANNEL_CD = act.CHANNEL_CD
		and fcst2.pbo_location_id = act.pbo_location_id	
		and fcst2.SALES_DT = act.SALES_DT	
	left join max_casl.fcst_gc_ml_v3 as fcst3
		on fcst3.CHANNEL_CD = act.CHANNEL_CD
		and fcst3.pbo_location_id = act.pbo_location_id	
		and fcst3.SALES_DT = act.SALES_DT	
	where act.CHANNEL_CD = 'ALL'
	group by act.SALES_DT
	;
QUIT;

proc sgplot data=casuser.compare
/* 	(where=(sales_dt>='1may2021'd and sales_dt<'1jun2021'd)) */
	;
	series x=sales_dt y=actual;
	series x=sales_dt y=fcst_ml_v0;
	series x=sales_dt y=fcst_ml_v2;
	series x=sales_dt y=fcst_ml_v3;
run;


/* PROC FEDSQL sessref=casauto; */
/* 	CREATE TABLE CASUSER.actual{options replace=true} AS  */
/* 	SELECT act.CHANNEL_CD */
/* 		, act.PBO_LOCATION_ID */
/* 		, act.SALES_DT */
/* 		, coalesce(act.receipt_qty, 0) as GC */
/* 	FROM MAX_CASL.PBO_SALES as act */
/* 	inner join CASUSER.comp_list as cmp */
/* 		on intnx('month', act.SALES_DT, 0, 'B') = cmp.mon_dt */
/* 		and act.pbo_location_id = cmp.pbo_location_id */
/* 	where act.CHANNEL_CD = 'ALL' */
/* 	; */
/* QUIT; */
/*  */
/* data CASUSER.actual; */
/* 	set CASUSER.actual; */
/* 	GROUP = "ACTUAL"; */
/* run; */
/*  */
/* PROC FEDSQL sessref=casauto; */
/* 	CREATE TABLE CASUSER.FCSTM1{options replace=true} AS  */
/* 	SELECT fcst.CHANNEL_CD */
/* 		, fcst.PBO_LOCATION_ID */
/* 		, fcst.SALES_DT */
/* 		, coalesce(fcst.GC_PREDICT, 0) as GC */
/* 	FROM casuser.fcst_gc_ml_v0 as fcst */
/* 	inner join CASUSER.comp_list as cmp */
/* 		on intnx('month', fcst.SALES_DT, 0, 'B') = cmp.mon_dt */
/* 		and fcst.pbo_location_id = cmp.pbo_location_id */
/* 	where fcst.CHANNEL_CD = 'ALL' */
/* 	; */
/* QUIT; */
/*  */
/* data CASUSER.FCSTM1; */
/* 	set CASUSER.FCSTM1; */
/* 	GROUP = "FCST_ML_v0"; */
/* run; */
/*  */
/* PROC FEDSQL sessref=casauto; */
/* 	CREATE TABLE CASUSER.FCSTM2{options replace=true} AS  */
/* 	SELECT fcst.CHANNEL_CD */
/* 		, fcst.PBO_LOCATION_ID */
/* 		, fcst.SALES_DT */
/* 		, coalesce(fcst.GC_PREDICT, 0) as GC */
/* 	FROM casuser.fcst_gc_ml_v3 as fcst */
/* 	inner join CASUSER.comp_list as cmp */
/* 		on intnx('month', fcst.SALES_DT, 0, 'B') = cmp.mon_dt */
/* 		and fcst.pbo_location_id = cmp.pbo_location_id */
/* 	where fcst.CHANNEL_CD = 'ALL' */
/* 	; */
/* QUIT; */
/*  */
/* data CASUSER.FCSTM2; */
/* 	set CASUSER.FCSTM2; */
/* 	GROUP = "FCST_ML_v3"; */
/* run; */
/*  */
/* proc casutil; */
/* 	droptable  */
/* 		casdata		= "COMPARE_FCST"  */
/* 		incaslib	= "CASUSER"  */
/* 		quiet          */
/* 	;                  */
/* run;    */
/*  */
/* data CASUSER.COMPARE_FCST; */
/* 	set  */
/* 		casuser.actual */
/* 		casuser.FCSTM1 */
/* 		casuser.FCSTM2 */
/* 	; */
/* run; */
/*  */
/* proc casutil;          */
/* 	promote            */
/* 		casdata		= "COMPARE_FCST"  */
/* 		incaslib	= "CASUSER"  */
/* 		casout		= "COMPARE_FCST"   */
/* 		outcaslib	= "CASUSER" */
/* 	;                  */
/* run;    */