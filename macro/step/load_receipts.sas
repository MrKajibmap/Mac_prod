%macro load_receipts();
	LIBNAME tda ORACLE &IA_CONNECT_OPTIONS SCHEMA=TDA;
	libname MN_CALC '/data/MN_CALC';

	%local
		lmvCurrYear        /* текущий год 	*/
		lmvLoadDates;
	
	%let lmvCurrYear = %sysfunc(year("&SYSDATE."d));

	proc sql noprint;
		connect using tda;
		create table work.ia_pmx_dates as
		select order_date from connection to tda (
			select distinct ORDER_DATE 
			from tda.TDA_PMX_&lmvCurrYear.
		);
		disconnect from tda;
	quit;

	proc sql noprint;
		create table work.mn_calc_pmx_dates as
		select distinct order_date
		from mn_calc.tda_pmx_&lmvCurrYear.;
	quit;

	proc sql noprint;
		create table work.dates_to_append_pmx as
		select ORDER_DATE
		from work.ia_pmx_dates
		except
		select ORDER_DATE
		from work.mn_calc_pmx_dates;
	quit;

	proc format;
		picture ordate
		other = '%0d.%0m.%0y'(datatype=datetime);
	run;	

	data work.dates_to_append_pmx_f;
		set work.dates_to_append_pmx;
		format order_date ordate.;
	run;

	proc sql noprint;
		SELECT order_date INTO :lmvLoadDates separated by ' '
		FROM work.dates_to_append_pmx_f;
	quit;	

	%let lmvDatesCnt = %sysfunc(countw(&lmvLoadDates., %str( )));	
	
	%if &lmvDatesCnt. gt 0 %then %do;
		%do i=1 %to &lmvDatesCnt.;
			%let lmvLoadDate = %scan(&lmvLoadDates., &i., %str( ));
			%if &i. eq 1 %then %do;
				%let lmvDatesComma = %str(%')&lmvLoadDate.%str(%');
			%end;
			%else %do;
				%let lmvDatesComma = &lmvDatesComma.%str(,) %str(%')&lmvLoadDate.%str(%');
			%end;
		%end;

		proc sql noprint;
			connect using tda;
			create table work.data_to_append_pmx as
			select * from connection to tda (
				select
					STORE_ID,
					ORDER_DATE,
					ORDER_NUMBER,
					MENU_CODE,
					QTY,
					QTY_PROMO,
					SALES_REGISTRY_TYPE
				from tda.TDA_PMX_&lmvCurrYear.
				where ORDER_DATE in (&lmvDatesComma.)
			);
		quit;

		proc append base=MN_CALC.TDA_PMX_&lmvCurrYear. data=work.data_to_append_pmx;
		run;
	%end;
	%else %do;
	/* Обновлений нет нечего нам тут делать, варнинг в бота ТГ */
		filename resp temp;
		proc http 
			 method="POST"
			  url="https://api.telegram.org/bot&TG_BOT_TOKEN./sendMessage?chat_id=-1001360913796&text=LOAD_RECEIPTS_WARNING:There are no updates for TDA_PMX_&lmvCurrYear."
			 ct="application/json"
			 out=resp; 
		run;
	%end;
%mend load_receipts;