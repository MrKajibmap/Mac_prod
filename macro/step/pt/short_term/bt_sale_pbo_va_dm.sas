cas casauto;
caslib _all_ assign;

/*****************************************************************/
/* Структура для истории */
proc fedsql sessref=casauto;
	create table casuser.min_dt{options replace=true} as 
	select pbo_location_id
		, min(sales_dt) as min_dt
	from MAX_CASL.PMIX_SALES
	where channel_cd = 'ALL'
	group by 1
	;
quit;

data casuser.structure;
set casuser.min_dt;
by pbo_location_id;
format sales_dt date9.;
do sales_dt = max(min_dt, '01jan2018'd) to '31may2021'd;
	output;
end;
run;

proc fedsql sessref=casauto;
	create table casuser.hist{options replace=true} as 
	select 
		  pbo_location_id
		, sales_dt
		, sum(net_sales_amt) as sum_net_sales_amt
	from MAX_CASL.PMIX_SALES
	where channel_cd = 'ALL'
	group by 1,2
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.hist_ext{options replace=true} as 
	select 
		  str.pbo_location_id
		, str.sales_dt
		, coalesce(hst.sum_net_sales_amt, 0) as sale
	from casuser.structure as str
	left join casuser.hist as hst
	on str.pbo_location_id = hst.pbo_location_id
	and str.sales_dt = hst.sales_dt
	;
quit;

data casuser.history;
set casuser.hist_ext;
GROUP = 'HISTORY_';
run;


/*****************************************************************/
/* ДЕКАБРЬ */
/* Прогноз восстановленный финальный */
proc fedsql sessref=casauto;
	create table casuser.fcst_dec{options replace=true} as 
	select 
		  main.pbo_location_id
		, main.sales_dt
		, sum( main.FINAL_FCST_UNITS_REC_APLM * coalesce(pr.PRICE_NET , 0) ) as sale
	from MAX_CASL.FCST_UNITS_DEC as main
	left join MAX_CASL.KPI_PRICES_ENH as pr
		on  main.pbo_location_id = pr.pbo_location_id
		and main.product_id 	 = pr.product_id
		and main.sales_dt 		 = pr.period_dt
	where month(main.sales_dt) = 12
	group by 1,2
	;
quit;

data casuser.fcst_dec;
set casuser.fcst_dec;
GROUP = 'FCST_DEC';
run;

/* Список компов  */
%let lmvTestMonthDateDec = %str(date%')%sysfunc(putn(%sysfunc(intnx(month,'01dec2020'd,0)), yymmdd10.))%str(%');
proc fedsql sessref=casauto;
	create table CASUSER.PBO_LIST_COMP_DEC {options replace=true} as
	select
		  pbo_location_id
		, A_OPEN_DATE
		, A_CLOSE_DATE
		, 1 as comp_flag
	from 
		MAX_CASL.PBO_DICTIONARY

	where 
		intnx('month', &lmvTestMonthDateDec. , -12, 'b') >= 
      		case 
	   			when day(A_OPEN_DATE)=1 
					then cast(A_OPEN_DATE as date)
	   			else 
					cast(intnx('month', A_OPEN_DATE, 1, 'b') as date)
      		end
	    and &lmvTestMonthDateDec. <=
			case
				when A_CLOSE_DATE is null 
					then cast(intnx('month',  &lmvTestMonthDateDec., 12) as date)
				when A_CLOSE_DATE=intnx('month', A_CLOSE_DATE, 0, 'e') 
					then cast(A_CLOSE_DATE as date)
		   		else 
					cast(intnx('month', A_CLOSE_DATE, -1, 'e') as date)
			end
	;
quit;


/*****************************************************************/
/* ЯНВАРЬ */

/* Прогноз восстановленный финальный */
proc fedsql sessref=casauto;
	create table casuser.fcst_jan{options replace=true} as 
	select 
		  main.pbo_location_id
		, main.sales_dt
		, sum( main.FINAL_FCST_UNITS_REC_APLM * coalesce(pr.PRICE_NET , 0) ) as sale
	from MAX_CASL.FCST_UNITS_JAN as main
	left join MAX_CASL.KPI_PRICES_ENH as pr
		on  main.pbo_location_id = pr.pbo_location_id
		and main.product_id 	 = pr.product_id
		and main.sales_dt 		 = pr.period_dt
	where month(main.sales_dt) = 1
	group by 1,2
	;
quit;

data casuser.fcst_jan;
set casuser.fcst_jan;
GROUP = 'FCST_JAN';
run;

/* Список компов  */
%let lmvTestMonthDateJan = %str(date%')%sysfunc(putn(%sysfunc(intnx(month,'01jan2021'd,0)), yymmdd10.))%str(%');
proc fedsql sessref=casauto;
	create table CASUSER.PBO_LIST_COMP_JAN {options replace=true} as
	select
		  pbo_location_id
		, A_OPEN_DATE
		, A_CLOSE_DATE
		, 1 as comp_flag
	from 
		MAX_CASL.PBO_DICTIONARY

	where 
		intnx('month', &lmvTestMonthDateJan. , -12, 'b') >= 
      		case 
	   			when day(A_OPEN_DATE)=1 
					then cast(A_OPEN_DATE as date)
	   			else 
					cast(intnx('month', A_OPEN_DATE, 1, 'b') as date)
      		end
	    and &lmvTestMonthDateJan. <=
			case
				when A_CLOSE_DATE is null 
					then cast(intnx('month',  &lmvTestMonthDateJan., 12) as date)
				when A_CLOSE_DATE=intnx('month', A_CLOSE_DATE, 0, 'e') 
					then cast(A_CLOSE_DATE as date)
		   		else 
					cast(intnx('month', A_CLOSE_DATE, -1, 'e') as date)
			end
	;
quit;

/*****************************************************************/
/* МАРТ */

/* Прогноз восстановленный финальный */
proc fedsql sessref=casauto;
	create table casuser.fcst_mar{options replace=true} as 
	select 
		  main.pbo_location_id
		, main.sales_dt
		, sum( main.FINAL_FCST_UNITS_REC_APLM * coalesce(pr.PRICE_NET , 0) ) as sale
	from MAX_CASL.FCST_UNITS_MAR as main
	left join MAX_CASL.KPI_PRICES_ENH as pr
		on  main.pbo_location_id = pr.pbo_location_id
		and main.product_id 	 = pr.product_id
		and main.sales_dt 		 = pr.period_dt
	where month(main.sales_dt) = 3
	group by 1,2
	;
quit;

data casuser.fcst_mar;
set casuser.fcst_mar;
GROUP = 'FCST_MAR';
run;

/* Список компов  */
%let lmvTestMonthDateMar = %str(date%')%sysfunc(putn(%sysfunc(intnx(month,'01mar2021'd,0)), yymmdd10.))%str(%');
proc fedsql sessref=casauto;
	create table CASUSER.PBO_LIST_COMP_MAR {options replace=true} as
	select
		  pbo_location_id
		, A_OPEN_DATE
		, A_CLOSE_DATE
		, 1 as comp_flag
	from 
		MAX_CASL.PBO_DICTIONARY

	where 
		intnx('month', &lmvTestMonthDateMar. , -12, 'b') >= 
      		case 
	   			when day(A_OPEN_DATE)=1 
					then cast(A_OPEN_DATE as date)
	   			else 
					cast(intnx('month', A_OPEN_DATE, 1, 'b') as date)
      		end
	    and &lmvTestMonthDateMar. <=
			case
				when A_CLOSE_DATE is null 
					then cast(intnx('month',  &lmvTestMonthDateMar., 12) as date)
				when A_CLOSE_DATE=intnx('month', A_CLOSE_DATE, 0, 'e') 
					then cast(A_CLOSE_DATE as date)
		   		else 
					cast(intnx('month', A_CLOSE_DATE, -1, 'e') as date)
			end
	;
quit;

/*****************************************************************/
/* МАЙ */

/* Прогноз восстановленный финальный */
proc fedsql sessref=casauto;
	create table casuser.fcst_may{options replace=true} as 
	select 
		  main.pbo_location_id
		, main.sales_dt
		, sum( main.FINAL_FCST_UNITS_REC_APLM * coalesce(pr.PRICE_NET , 0) ) as sale
	from MAX_CASL.FCST_UNITS_MAY as main
	left join MAX_CASL.KPI_PRICES_ENH as pr
		on  main.pbo_location_id = pr.pbo_location_id
		and main.product_id 	 = pr.product_id
		and main.sales_dt 		 = pr.period_dt
	where month(main.sales_dt) = 5
	group by 1,2
	;
quit;

data casuser.fcst_may;
set casuser.fcst_may;
GROUP = 'FCST_MAY';
run;

/* Список компов  */
%let lmvTestMonthDateMay = %str(date%')%sysfunc(putn(%sysfunc(intnx(month,'01may2021'd,0)), yymmdd10.))%str(%');
proc fedsql sessref=casauto;
	create table CASUSER.PBO_LIST_COMP_MAY {options replace=true} as
	select
		  pbo_location_id
		, A_OPEN_DATE
		, A_CLOSE_DATE
		, 1 as comp_flag
	from 
		MAX_CASL.PBO_DICTIONARY

	where 
		intnx('month', &lmvTestMonthDateMay. , -12, 'b') >= 
      		case 
	   			when day(A_OPEN_DATE)=1 
					then cast(A_OPEN_DATE as date)
	   			else 
					cast(intnx('month', A_OPEN_DATE, 1, 'b') as date)
      		end
	    and &lmvTestMonthDateMay. <=
			case
				when A_CLOSE_DATE is null 
					then cast(intnx('month',  &lmvTestMonthDateMay., 12) as date)
				when A_CLOSE_DATE=intnx('month', A_CLOSE_DATE, 0, 'e') 
					then cast(A_CLOSE_DATE as date)
		   		else 
					cast(intnx('month', A_CLOSE_DATE, -1, 'e') as date)
			end
	;
quit;


/*****************************************************************/
/* Сборка витрины отчета */

data CASUSER.VA_SALE_ANALYSIS_APPENDED;
	set 
		casuser.history
		casuser.fcst_dec
		casuser.fcst_jan
		casuser.fcst_mar
		casuser.fcst_may
	;
run;

proc fedsql sessref=casauto;
	create table CASUSER.VA_SALE_ANALYSIS {options replace=true} as
	select main.*
		, coalesce(dec.comp_flag, 0) as comp_flag_dec
		, coalesce(jan.comp_flag, 0) as comp_flag_jan
		, coalesce(mar.comp_flag, 0) as comp_flag_mar
		, coalesce(may.comp_flag, 0) as comp_flag_may
	from CASUSER.VA_SALE_ANALYSIS_APPENDED as main
	left join CASUSER.PBO_LIST_COMP_DEC as dec
		on main.pbo_location_id = dec.pbo_location_id
	left join CASUSER.PBO_LIST_COMP_JAN as jan
		on main.pbo_location_id = jan.pbo_location_id
	left join CASUSER.PBO_LIST_COMP_MAR as mar
		on main.pbo_location_id = mar.pbo_location_id
	left join CASUSER.PBO_LIST_COMP_MAY as may
		on main.pbo_location_id = may.pbo_location_id
	;
quit;

proc casutil;
	droptable 
		casdata		= "VA_SALE_ANALYSIS" 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run; 

data MAX_CASL.VA_SALE_ANALYSIS (promote=yes);
	set CASUSER.VA_SALE_ANALYSIS;
run;
