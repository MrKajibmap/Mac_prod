cas casauto;
caslib _all_ assign;

/*****************************************************************/
/* Структура для истории */
proc fedsql sessref=casauto;
	create table casuser.min_dt{options replace=true} as 
	select pbo_location_id
		, min(sales_dt) as min_dt
	from MAX_CASL.PBO_SALES
	where channel_cd = 'ALL'
	group by 1
	;
quit;

data casuser.structure;
set casuser.min_dt;
by pbo_location_id;
format sales_dt date9.;
do sales_dt = max(min_dt, '01nov2019'd) to '31jan2021'd;
	output;
end;
run;

proc fedsql sessref=casauto;
	create table casuser.hist{options replace=true} as 
	select 
		  pbo_location_id
		, sales_dt
		, receipt_qty
	from MAX_CASL.PBO_SALES
	where channel_cd = 'ALL'
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.hist_ext{options replace=true} as 
	select 
		  str.pbo_location_id
		, str.sales_dt
		, coalesce(hst.receipt_qty, 0) as gc
	from casuser.structure as str
	left join casuser.hist as hst
	on str.pbo_location_id = hst.pbo_location_id
	and str.sales_dt = hst.sales_dt
	;
quit;

data casuser.history;
set casuser.hist_ext;
GROUP = 'HISTORY';
run;

/*****************************************************************/
/* История из обучающей выборки */
proc fedsql sessref=casauto;
	create table casuser.hist_rs{options replace=true} as 
	select 
		  pbo_location_id
		, sales_dt
		, receipt_qty as gc
	from MAX_CASL.TRAIN_ABT_TRP_GC_DEC
	where channel_cd = 'ALL'
	;
quit;

data casuser.hist_rs;
set casuser.hist_rs(where=(sales_dt>='01nov2019'd));;
GROUP = 'HIST_RS';
run;

/*****************************************************************/
/* История TARGET */
proc fedsql sessref=casauto;
	create table casuser.hist_vf{options replace=true} as 
	select 
		  pbo_location_id
		, sales_dt
		, TARGET as gc
	from MAX_CASL.DM_TRAIN_TRP_GC_DEC
	where channel_cd = 'ALL'
	;
quit;

data casuser.hist_vf;
set casuser.hist_vf (where=(sales_dt>='01nov2019'd));
GROUP = 'HIST_VF';
run;

/*****************************************************************/
/* Прогноз VF */
proc fedsql sessref=casauto;
	create table casuser.fcst_vf{options replace=true} as 
	select 
		  pbo_location_id
		, sales_dt
		, divide(GC_FCST, DETREND_MULTI) as gc
	from MAX_CASL.GC_FORECAST_RESTORED_DEC_2
	where channel_cd = 'ALL'
	;
quit;

data casuser.fcst_vf;
set casuser.fcst_vf(where=(sales_dt<='31jan2021'd));;
GROUP = 'FCST_VF';
run;


/*****************************************************************/
/* Прогноз восстановленный финальный */
proc fedsql sessref=casauto;
	create table casuser.fcst_rs{options replace=true} as 
	select 
		  pbo_location_id
		, sales_dt
		, GC_FCST as gc
	from MAX_CASL.GC_FORECAST_RESTORED_DEC_2
	where channel_cd = 'ALL'
	;
quit;

data casuser.fcst_rs;
set casuser.fcst_rs (where=(sales_dt<='31jan2021'd));;
GROUP = 'FCST_RS';
run;


/*****************************************************************/
/* Сборка витрины отчета */

proc casutil;
	droptable 
		casdata		= "gc_dec_analysis" 
		incaslib	= "CASUSER" 
		quiet         
	;                 
run; 

data casuser.gc_dec_analysis (promote=yes);
	set 
		casuser.history
		casuser.fcst_vf
		casuser.fcst_rs
		casuser.hist_vf
		casuser.hist_rs
	;
run;
