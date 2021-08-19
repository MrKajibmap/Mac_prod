%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( cmasauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

options casdatalimit=600000M; 


/* СНАЧАЛА ЗАПУСТИТЬ СНАЧАЛА ИНИЦИАЛИЗАЦИЮ СКРИПТА 
	%fcst_create_abt_pbo_gc В ЖЕЛАЕМОЙ КОНФИГУРАЦИИ */

/* !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! */
%let ETL_CURRENT_DT	= '26dec2020'd;
%let lmvPostfix 	= JAN_BT;
/* !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! */


/* Сборка витрины GC */
%fcst_create_abt_pbo_gc(
			  mpMode		  = gc
			, mpOutTableDmVf  = MAX_CASL.DM_TRAIN_TRP_GC_&lmvPostfix.
			, mpOutTableDmABT = MAX_CASL.TRAIN_ABT_TRP_GC_&lmvPostfix.
		);

proc fedsql sessref=casauto;
select min(sales_dt) from MAX_CASL.DM_TRAIN_TRP_GC_&lmvPostfix. where TARGET is not null;
select max(sales_dt) from MAX_CASL.DM_TRAIN_TRP_GC_&lmvPostfix. where TARGET is not null;
quit;

/* Запуск прогнозирования GC */
proc casutil;
	droptable 
		casdata		= "DM_TRAIN_TRP_GC" 
		incaslib	= "MN_DICT" 
		quiet         
	;                 
run;                  

data MN_DICT.DM_TRAIN_TRP_GC(promote=yes);
	set MAX_CASL.DM_TRAIN_TRP_GC_&lmvPostfix.;
run;

%vf_run_project(mpProjectName=mn_gc_shortterm);

%fcst_restore_seasonality(
		  mpInputTbl	= MAX_CASL.TRAIN_ABT_TRP_GC_&lmvPostfix.
		, mpMode 		= GC									
		, mpOutTableNm 	= MAX_CASL.GC_FORECAST_RESTORED_&lmvPostfix.
		, mpAuth 		= NO
	);

proc fedsql sessref=casauto;
select sum(GC_FCST) from MAX_CASL.GC_FORECAST_RESTORED_&lmvPostfix.;
quit;