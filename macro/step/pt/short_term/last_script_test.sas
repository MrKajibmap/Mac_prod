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
	CREATE TABLE CASUSER.pre_data{options replace=true} AS 
	SELECT m1.CHANNEL_CD
		, m1.PBO_LOCATION_ID
		, m1.SALES_DT
		, m1.GC_FCST as GC_FCST_M1
		, m2.GC_FCST as GC_FCST_M2
		, coalesce(act.receipt_qty, 0) as ACTUAL
	FROM MAX_CASL.GC_FORECAST_RESTORED_A3M1_JAN as m1
	inner join MAX_CASL.GC_FORECAST_RESTORED_A3M2_JAN as m2
		on m1.CHANNEL_CD 		= m2.CHANNEL_CD
		and m1.pbo_location_id 	= m2.pbo_location_id	
		and m1.SALES_DT 		= m2.SALES_DT	
	left join MAX_CASL.PBO_SALES as act
		on m1.CHANNEL_CD 		= act.CHANNEL_CD
		and m1.pbo_location_id 	= act.pbo_location_id	
		and m1.SALES_DT 		= act.SALES_DT	
	inner join CASUSER.comp_list as cmp
		on intnx('month', m1.SALES_DT, 0, 'B') = cmp.mon_dt
		and m1.pbo_location_id = cmp.pbo_location_id
	where m1.CHANNEL_CD = 'ALL'
	;
QUIT;

/* data casuser.actual; */
/* set MAX_CASL.PBO_SALES; */
/* where CHANNEL_CD = 'ALL'; */
/* GC = RECEIPT_QTY; */
/* GROUP = 'ACTUAL'; */
/* run; */

data casuser.actual;
set casuser.pre_data;
GC = ACTUAL;
GROUP = 'ACTUAL';
drop GC_FCST_M1 GC_FCST_M2 ACTUAL;
run;


data casuser.FCSTM1;
set casuser.pre_data;
GC = GC_FCST_M1;
GROUP = 'FCSTM1';
drop GC_FCST_M1 GC_FCST_M2 ACTUAL;
run;

data casuser.FCSTM2;
set casuser.pre_data;
GC = GC_FCST_M2;
GROUP = 'FCSTM2';
drop GC_FCST_M1 GC_FCST_M2 ACTUAL;
run;

proc casutil;
	droptable 
		casdata		= "COMPARE_FCST_M1_VS_M2" 
		incaslib	= "CASUSER" 
		quiet         
	;                 
run;   


data CASUSER.COMPARE_FCST_M1_VS_M2;
	set 
		casuser.actual
		casuser.FCSTM1
		casuser.FCSTM2
	;
run;

proc casutil;         
	promote           
		casdata		= "COMPARE_FCST_M1_VS_M2" 
		incaslib	= "CASUSER" 
		casout		= "COMPARE_FCST_M1_VS_M2"  
		outcaslib	= "CASUSER"
	;                 
run;   