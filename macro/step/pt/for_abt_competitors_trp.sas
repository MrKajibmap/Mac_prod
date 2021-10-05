cas casauto;
caslib _all_ assign;

%let lmvInLib 		= ETL_IA;
%let lmvReportDttm	= &ETL_CURRENT_DTTM.;

data CASUSER.COMP_MEDIA (replace=yes  drop=valid_from_dttm valid_to_dttm);
	set &lmvInLib..COMP_MEDIA(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
run;

proc fedsql sessref=casauto;
	create table CASUSER.COMP_MEDIA_ML{options replace=true} as 
		select
			  COMPETITOR_CD
			, TRP
			, datepart(cast(report_dt as timestamp)) as report_dt
		from 
			CASUSER.COMP_MEDIA
	;
quit;

/* Транспонируем таблицу */
proc cas;
	transpose.transpose /
   		table = { 
				  name 	  = "COMP_MEDIA_ML"
				, caslib  = "CASUSER"
				, groupby = {"REPORT_DT"}
			}
		transpose 	= {"TRP"} 
   		prefix		= "comp_trp_" 
   		id			= {"COMPETITOR_CD"} 
   		casout = {
				  name 	  = "COMP_TRANSPOSED_ML"
				, caslib  = "CASUSER"
				, replace = true
			}
	;
quit;

/* Протягиваем trp на всю неделю вперед */
data casuser.comp_transposed_ml_expand;
	set casuser.comp_transposed_ml;
	by REPORT_DT;
	do i = 1 to 7;
	   output;
	   REPORT_DT + 1;
	end;
run;

/*
	Пока в данных есть ошибка, все интевалы report_dt указаны
	с интервалом в неделю, но есть одно наблюдение
	в котором этот порядок рушится 16dec2019 и 22dec2019 (6 Дней)
	Поэтому, пока в таблице есть дубль, который мы убираем путем усреднения
*/
proc fedsql sessref=casauto;
	create table casuser.comp_transposed_ml_expand{options replace=true} as
		select
			REPORT_DT,
			mean(comp_trp_BK) as comp_trp_BK,
			mean(comp_trp_KFC) as comp_trp_KFC
		from
			casuser.comp_transposed_ml_expand
		group by report_dt
	;
quit;

