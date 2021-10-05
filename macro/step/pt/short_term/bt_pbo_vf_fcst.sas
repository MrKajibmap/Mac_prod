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
/*
01dec2020	for DEC
26dec2020	for JAN
27feb2021	for MAR		
*/
%let ETL_CURRENT_DT	= '26dec2020'd;
%let lmvPostfix 	= JANSH;
/* !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! */

/* Сборка витрины PBO */
%fcst_create_abt_pbo_gc(
			  mpMode		  = pbo
			, mpOutTableDmVf  = MAX_CASL.DM_TRAIN_TRP_PBO_&lmvPostfix.
			, mpOutTableDmABT = MAX_CASL.TRAIN_ABT_TRP_PBO_&lmvPostfix.
		);

proc fedsql sessref=casauto;
select min(sales_dt) from MAX_CASL.DM_TRAIN_TRP_PBO_&lmvPostfix. where TARGET is not null;
select max(sales_dt) from MAX_CASL.DM_TRAIN_TRP_PBO_&lmvPostfix. where TARGET is not null;
quit;

/* Запуск прогнозирования PBO */
proc casutil;
	droptable 
		casdata		= "DM_TRAIN_TRP_PBO" 
		incaslib	= "MN_DICT" 
		quiet         
	;                 
run;                  

data MN_DICT.DM_TRAIN_TRP_PBO(promote=yes);
	set MAX_CASL.DM_TRAIN_TRP_PBO_&lmvPostfix.;
run;

%vf_run_project(mpProjectName=mn_pbo_shortterm);

%fcst_restore_seasonality(
		  mpInputTbl	= MAX_CASL.TRAIN_ABT_TRP_PBO_&lmvPostfix.
		, mpMode 		= pbo									
		, mpOutTableNm 	= MAX_CASL.PBO_FORECAST_RESTORED_&lmvPostfix.
		, mpAuth 		= NO
	);

proc fedsql sessref=casauto;
select sum(PBO_FCST) from MAX_CASL.PBO_FORECAST_RESTORED_&lmvPostfix. where channel_cd = 'ALL';
quit;