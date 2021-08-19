/*для Маттео. Факт за прошлый год.*/

/*вместо сортировки по версии данных, иногда появлялись пропуски*/

%macro hlvl_report;
	%tech_cas_session(mpMode = start
					,mpCasSessNm = casauto
					,mpAssignFlg= y
					,mpAuthinfoUsr=
					);
					
					
   %let lmvReportDttm=&ETL_CURRENT_DTTM.;

	proc sql;
		create table work.gc_last_year_date as
		select distinct 
			t1.PBO_LOCATION_ID
			, mdy(month(t1.SALES_DT),day(t1.SALES_DT),year(today())) as SALES_DT format=date9.
			, receipt_qty
		from etl_ia.pbo_sales t1
		where channel_cd='ALL' 
			and t1.sales_dt between intnx('month', today(),-13,'m') and intnx('month', today(),-9,'e')
			and (valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.);
	quit;

	proc sql;
		create table work.gc_last_year_day as
		select distinct 
			PBO_LOCATION_ID 
			, mdy(month(SALES_DT+52*7), day(SALES_DT+52*7), year(today())) as SALES_DT format=date9.
			, receipt_qty
		from work.gc_last_year_date t1
		;
	quit;

	/*////////////////////////////////////////////*/

	proc sql;
		create table work.pmix_last_year_date as
		select distinct  
			t1.PBO_LOCATION_ID
			, mdy(month(t1.SALES_DT),day(t1.SALES_DT),year(today())) as SALES_DT format=date9.
			, sum(net_sales_amt) as net_sales
		from etl_ia.pmix_sales t1
		where channel_cd='ALL' 
			and t1.sales_dt between intnx('month', today(),-13,'m') and intnx('month', today(),-9,'e')
			and (valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.)
		group by t1.pbo_location_id, t1.sales_dt
		;
	quit;

	proc sql;
		create table work.pmix_last_year_day as
		select distinct 
			PBO_LOCATION_ID
			, mdy(month(SALES_DT+52*7), day(SALES_DT+52*7), year(today())) as SALES_DT format=date9.
			, net_sales
		from work.pmix_last_year_date t1
		;
	quit;

	/*прогноз. Переименовать название источника на регламентные таблицы
	Сегодня-1 - принудительная чистка, что бы до этого подгрузить историю продаж. Проверить по датам, если факт доступен, то по идее прогноз должен начинаться от сегодня. На момент добавления заглушки не было факта за последнюю дату*/

	proc fedsql sessref=casauto;
		create table casuser.gc_forc {option replace = true} as 
		select distinct 
			location
			, data
			, final_fcst_gc
		from mn_short.plan_gc_day
		where data>=(today()-1)

		;
	quit;


	/*********************************************/
	proc fedsql sessref=casauto;
		create table casuser.pmix_forc {option replace = true} as 
		select distinct 
			location
			, data
			, sum(FINAL_FCST_SALE) as fin_sale
		from mn_short.plan_pmix_day
		where data>=(today()-1)
		group by location, data
		;
	quit;

	/*подготовить список пересечений локация-дата к которому буду джойнить все данные*/
	/*календарь на 2 месяца, в перспективе должен остаться один, текущий)*/
	%let x=(intnx("month",today(),3,'e')-intnx("month",today(),0,'b'));
	data calendar;
		start = intnx("month",today(),0,'b');
			do i = 0 to &x.;
				sales_dt = start + i;
				output;
			end;
		format sales_dt DATE9.;
		drop start i;
	run;

	proc sql;
		create table work.list_prep as 
		select distinct 
			t1.PBO_LOCATION_ID
			, t2.Sales_DT 
		from etl_ia.pbo_location t1, work.calendar t2
		;
	quit;

	data CASUSER.list_prep (replace=yes);
		set work.list_prep
	;
	run;

	%load_komp_matrix;

	/*продолжаем расчет пересечений и собираем все данные в кучу*/

	proc fedsql sessref=casauto;
		create table casuser.list {option replace = true} as 
		select distinct 
			t1.*
			, (case when t2.komp_attrib=1 then 2 else 3 end) as LOCATION
		from casuser.list_prep t1
		inner join casuser.komp_matrix t2 on t1.pbo_location_id=t2.pbo_location_id 
						and intnx('month',t1.sales_dt,0,'b')=t2.month
		;
	quit;


	/*факт за текущий год с начала месяца для прогноза*/

	proc sql;
		create table work.gc_fact as
		select distinct 
			t1.PBO_LOCATION_ID
			, t1.SALES_DT
			, receipt_qty
		from etl_ia.pbo_sales t1
		where channel_cd='ALL' and  t1.sales_dt between  intnx('month', today(), 0, 'b') and intnx('month', today(), 0, 'e')
		and (valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.);
	quit;

	proc sql;
		create table work.pmix_fact as
		select distinct  
			t1.PBO_LOCATION_ID
			, t1.SALES_DT
			, sum(net_sales_amt) as net_sales
		from etl_ia.pmix_sales t1
		where channel_cd='ALL' and  t1.sales_dt between  intnx('month', today(), 0, 'b') and intnx('month', today(), 0, 'e')
		and (valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.)
		group by t1.pbo_location_id, t1.sales_dt
		;
	quit;

	/*перетащить все в КАС*/
	data CASUSER.gc_last_year_date (replace=yes);
		set work.gc_last_year_date
	;
	run;

	data CASUSER.gc_last_year_day (replace=yes);
		set work.gc_last_year_day
	;
	run;

	data CASUSER.pmix_last_year_date (replace=yes);
		set work.pmix_last_year_date
	;
	run;
	data CASUSER.pmix_last_year_day (replace=yes);
		set work.pmix_last_year_day
	;
	run;
	data CASUSER.gc_fact (replace=yes);
		set work.gc_fact
	;
	run;
	data CASUSER.pmix_fact (replace=yes);
		set work.pmix_fact
	;
	run;

	/*собираю итоговую табличку по дням*/
	proc fedsql sessref=casauto;
		create table casuser.data_prep {option replace = true} as
		select distinct 
			1 as PROD, 'RUR' as CURRENCY
			, t1.LOCATION, t1.PBO_location_id
			,t1.sales_dt as DATA
			,t2.receipt_qty as DATE_GC_LAST_YEAR
			, t3.receipt_qty as DAILY_GC_LAST_YEAR
			,t4.net_sales as DATE_SALES_LAST_YEAR
			, t5.net_sales as DAILY_SALES_LAST_YEAR
			, coalesce(t6.final_fcst_gc, t8.receipt_qty) as BASE_GC_FCST
			, coalesce(t6.final_fcst_gc, t8.receipt_qty) as DAILY_GC
			, coalesce(t7.fin_sale, t9.net_sales) as BASE_SALES_FCST
			, coalesce(t7.fin_sale, t9.net_sales) as DAILY_SALES
			, . as AVG_CHECK_FCST
		from  CASUSER.list t1
		left join CASUSER.gc_last_year_date t2 
			on t1.pbo_location_id = t2.pbo_location_id 
			and t1.sales_dt=t2.sales_dt
		left join CASUSER.gc_last_year_day t3 
			on t1.pbo_location_id = t3.pbo_location_id 
			and t1.sales_dt=t3.sales_dt
		left join CASUSER.pmix_last_year_date t4 
			on t1.pbo_location_id = t4.pbo_location_id 
			and t1.sales_dt=t4.sales_dt
		left join CASUSER.pmix_last_year_day t5 
			on t1.pbo_location_id = t5.pbo_location_id 
			and t1.sales_dt=t5.sales_dt
		left join casuser.gc_forc t6 
			on t1.pbo_location_id = t6.location 
			and t1.sales_dt=t6.data
		left join casuser.pmix_forc t7 
			on t1.pbo_location_id = t7.location
			and t1.sales_dt=t7.data
		left join CASUSER.gc_fact t8 
			on t1.pbo_location_id = t8.PBO_location_id 
			and t1.sales_dt=t8.sales_dt
		left join CASUSER.pmix_fact t9 
			on t1.pbo_location_id = t9.pbo_location_id 
			and t1.sales_dt=t9.sales_dt
	;
	quit;


	proc fedsql sessref=casauto;
		create table casuser.data {option replace = true} as
		select distinct 
			PROD, CURRENCY, LOCATION, DATA
			, sum(DATE_GC_LAST_YEAR) as DATE_GC_LAST_YEAR
			, sum(DAILY_GC_LAST_YEAR) as DAILY_GC_LAST_YEAR
			, sum(DATE_SALES_LAST_YEAR) as DATE_SALES_LAST_YEAR
			, sum(DAILY_SALES_LAST_YEAR) as DAILY_SALES_LAST_YEAR
			, sum(BASE_GC_FCST) as BASE_GC_FCST
			, sum(DAILY_GC) as DAILY_GC
			, sum(BASE_SALES_FCST) as BASE_SALES_FCST
			, sum(DAILY_SALES) as DAILY_SALES
			, sum(AVG_CHECK_FCST) as AVG_CHECK_FCST
		from  casuser.data_prep
		group by PROD, CURRENCY, LOCATION, DATA
		;
	quit;
	
	%dp_export_csv(mpInput=casuser.data
				, mpTHREAD_CNT=1
				, mpPath=/data/files/output/dp_files/);
								
%mend hlvl_report;