%macro dp_load_facts(mpFactGcMnth = mn_dict.fact_gc_month,
					mpFactPmixMnth = mn_dict.fact_pmix_month,
					mpFactUptMnth = mn_dict.fact_upt_month,
					mpPath = /data/files/output/dp_files/
					);
	
	options notes symbolgen mlogic mprint casdatalimit=all;
	
	%local lmvLibrefGc 
			lmvTabNmGc 
			lmvLibrefPmix
			lmvTabNmPmix
			lmvLibrefUpt
			lmvTabNmUpt
			;
			
	%member_names(mpTable=&mpFactGcMnth, mpLibrefNameKey=lmvLibrefGc, mpMemberNameKey=lmvTabNmGc);
	%member_names(mpTable=&mpFactPmixMnth, mpLibrefNameKey=lmvLibrefPmix, mpMemberNameKey=lmvTabNmPmix);
	%member_names(mpTable=&mpFactUptMnth, mpLibrefNameKey=lmvLibrefUpt, mpMemberNameKey=lmvTabNmUpt);

	cas casauto sessopts=(metrics=true);
	caslib _all_ assign;

	%let lmvInLib=ETL_IA;
	%let etl_current_dt = %sysfunc(today());
	%let ETL_CURRENT_DTTM = %sysfunc(datetime());
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	%let BeginOfYear = %sysfunc(intnx(year,&etl_current_dt.,-1,b));
	%put &=BeginOfYear;

	proc casutil;
		droptable casdata="&lmvTabNmGc." incaslib="&lmvLibrefGc." quiet;
	run;

	/* LOAD fact_gc_month */
	data CASUSER.pbo_sales (replace=yes);
		set &lmvInLib..pbo_sales(where=( (valid_to_dttm>=&lmvReportDttm.) and sales_dt>=&BeginOfYear.));
	run;
	
	proc fedsql sessref=casauto; 
		create table casuser.PBO_SALES_day{options replace=true} as 
			select distinct pbo_location_id
						, sales_dt
						, receipt_qty
			from CASUSER.pbo_sales
	;
	quit;

	proc fedsql sessref=casauto; 
		create table casuser.PBO_sales_prep_m{options replace=true} as
			select distinct *
						, intnx('month', sales_dt, 0, 'b') as month
			from casuser.PBO_SALES_day;
	quit;

	proc fedsql sessref=casauto; 
		create table casuser.fact_gc_month{options replace=true} as
			select distinct '1' as PROD
							, pbo_location_id as LOCATION
							, month as DATA
							, 'RUR' as CURRENCY
							, sum(receipt_qty) as ACTUAL_GC_MONTH
			from casuser.PBO_sales_prep_m
			group by pbo_location_id
					, month;
	quit;
	/* сброс форматов */
	data casuser.&lmvTabNmGc.(replace=yes);
		set casuser.fact_gc_month ;
		format data yymon7. LOCATION PROD 8.;
	;
	run;

	/* LOAD fact_pmix_month */
	proc casutil;
		droptable casdata="&lmvTabNmPmix." incaslib="&lmvLibrefPmix." quiet;
	run;

	data CASUSER.pmix_sales (replace=yes);
		set &lmvInLib..pmix_sales(where=( valid_to_dttm>=&lmvReportDttm. and sales_dt>=&BeginOfYear.));
	run;
	
	proc fedsql sessref=casauto; 
		create table casuser.sales_prep_m{options replace=true} as
			select distinct product_id
							,pbo_location_id
							,sales_dt
							,sales_qty
							,intnx('month', sales_dt, 0, 'b') as month
			 from CASUSER.pmix_sales
		 ;
	 quit;

	proc fedsql sessref=casauto; 
		create table casuser.fact_pmix_month{options replace=true} as
			select product_id as PROD
					, pbo_location_id as LOCATION
					, month as DATA
					,'RUR' as CURRENCY
					, sum(sales_qty) as ACTUAL_QNT_MONTH
			from casuser.sales_prep_m
			group by product_id
					, pbo_location_id
					, month;
	quit;
	/* Сброс форматов */
	data casuser.&lmvTabNmPmix.(replace=yes);
		set casuser.fact_pmix_month;
		format data yymon7. LOCATION PROD 8.;
	;
	run;

	/* LOAD fact_upt_month */
	proc fedsql sessref=casauto; 
		Create table casuser.fact_upt_month{options replace=true} as
			Select distinct t1.PROD
				, t1.LOCATION
				, t1.DATA
				, 'RUR' as CURRENCY
				, (ACTUAL_QNT_MONTH/ACTUAL_GC_MONTH*1000) as ACTUAL_UPT
				From casuser.&lmvTabNmPmix. t1
				Inner join casuser.&lmvTabNmGc. t2
					on t1.LOCATION=t2.LOCATION
					and t1.DATA=t2.DATA
				;
	quit;

	data casuser.&lmvTabNmUpt.(replace=yes);
		set casuser.fact_upt_month;
		format data yymon7. LOCATION PROD 8.;
	run;

	%load_komp_matrix;
	/* non-komp*/
	proc fedsql sessref=casauto;
		create table casuser.&lmvTabNmGc._nonkomp{options replace=true} as
			select distinct t1.* 
			from casuser.&lmvTabNmGc. t1
			inner join casuser.komp_matrix t2
				on t1.LOCATION = t2.pbo_location_id
				and t1.DATA = t2.month and t2.KOMP_ATTRIB =0
			;
	quit;
	
	proc casutil;
		droptable casdata="&lmvTabNmGc._nonkomp" incaslib="&lmvLibrefGc." quiet;
		promote casdata="&lmvTabNmGc._nonkomp" incaslib="casuser" outcaslib="&lmvLibrefGc.";
	quit;
	
	*%dp_export_csv(mpInput=&lmvLibrefGc..&lmvTabNmGc._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);
	
	%if %sysfunc(exist(&lmvLibrefGc..&lmvTabNmGc._nonkomp)) %then %do;
		proc export data=&lmvLibrefGc..&lmvTabNmGc._nonkomp(datalimit=all)
					outfile="&mpPath.&lmvTabNmGc._nonkomp.csv"
					dbms=dlm
					replace
					;
					delimiter='|'
					;
		run;
	%end;
				
	proc casutil;
		droptable casdata="&lmvTabNmGc._nonkomp" incaslib="&lmvLibrefGc." quiet;
	quit;
	
	
	proc fedsql sessref=casauto;
		create table casuser.&lmvTabNmPmix._nonkomp{options replace=true} as
		select distinct t1.* 
		from casuser.&lmvTabNmPmix. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB =0
		;
	quit;
	
	proc casutil;
		droptable casdata="&lmvTabNmPmix._nonkomp" incaslib="&lmvLibrefPmix." quiet;
		promote casdata="&lmvTabNmPmix._nonkomp" incaslib="casuser" outcaslib="&lmvLibrefPmix.";
	quit;
	
	*%dp_export_csv(mpInput=&lmvLibrefPmix..&lmvTabNmPmix._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);
	
	%if %sysfunc(exist(&lmvLibrefPmix..&lmvTabNmPmix._nonkomp)) %then %do;
		proc export data=&lmvLibrefPmix..&lmvTabNmPmix._nonkomp(datalimit=all)
					outfile="&mpPath.&lmvTabNmPmix._nonkomp.csv"
					dbms=dlm
					replace
					;
					delimiter='|'
					;
		run;
	%end;
				
	proc casutil;
		droptable casdata="&lmvTabNmPmix._nonkomp" incaslib="&lmvLibrefPmix." quiet;
	quit;
	
	
	proc fedsql sessref=casauto;
		create table casuser.&lmvTabNmUpt._nonkomp{options replace=true} as
		select distinct t1.* 
		from casuser.&lmvTabNmUpt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB =0
		;
	quit;
	
	proc casutil;
		droptable casdata="&lmvTabNmUpt._nonkomp" incaslib="&lmvLibrefUpt." quiet;
		promote casdata="&lmvTabNmUpt._nonkomp" incaslib="casuser" outcaslib="&lmvLibrefUpt.";
	quit;
	
	*%dp_export_csv(mpInput=&lmvLibrefUpt..&lmvTabNmUpt._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);
	
	%if %sysfunc(exist(&lmvLibrefUpt..&lmvTabNmUpt._nonkomp)) %then %do;
		proc export data=&lmvLibrefUpt..&lmvTabNmUpt._nonkomp(datalimit=all)
					outfile="&mpPath.&lmvTabNmUpt._nonkomp.csv"
					dbms=dlm
					replace
					;
					delimiter='|'
					;
		run;
	%end;
				
	proc casutil;
		droptable casdata="&lmvTabNmUpt._nonkomp" incaslib="&lmvLibrefUpt." quiet;
	quit;
	
	
	proc fedsql sessref=casauto;
		create table casuser.&lmvTabNmGc._komp{options replace=true} as
		select distinct t1.* 
		from casuser.&lmvTabNmGc. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB =1
		;
	quit;

	proc casutil;
		droptable casdata="&lmvTabNmGc._komp" incaslib="&lmvLibrefGc." quiet;
		promote casdata="&lmvTabNmGc._komp" incaslib="casuser" outcaslib="&lmvLibrefGc.";
	quit;
	
	*%dp_export_csv(mpInput=&lmvLibrefGc..&lmvTabNmGc._komp
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);
	
	%if %sysfunc(exist(&lmvLibrefGc..&lmvTabNmGc._komp)) %then %do;
		proc export data=&lmvLibrefGc..&lmvTabNmGc._komp(datalimit=all)
					outfile="&mpPath.&lmvTabNmGc._komp.csv"
					dbms=dlm
					replace
					;
					delimiter='|'
					;
		run;
	%end;
				
	proc casutil;
		droptable casdata="&lmvTabNmGc._komp" incaslib="&lmvLibrefGc." quiet;
	quit;
	

	proc fedsql sessref=casauto;
		create table casuser.&lmvTabNmPmix._komp{options replace=true} as
		select distinct t1.* 
		from casuser.&lmvTabNmPmix. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB =1
		;
	quit;
	
	proc casutil;
		droptable casdata="&lmvTabNmPmix._komp" incaslib="&lmvLibrefPmix." quiet;
		promote casdata="&lmvTabNmPmix._komp" incaslib="casuser" outcaslib="&lmvLibrefPmix.";
	quit;
	
	*%dp_export_csv(mpInput=&lmvLibrefPmix..&lmvTabNmPmix._komp
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);
	
	%if %sysfunc(exist(&lmvLibrefPmix..&lmvTabNmPmix._komp)) %then %do;
		proc export data=&lmvLibrefPmix..&lmvTabNmPmix._komp(datalimit=all)
					outfile="&mpPath.&lmvTabNmPmix._komp.csv"
					dbms=dlm
					replace
					;
					delimiter='|'
					;
		run;
	%end;
			
	proc casutil;
		droptable casdata="&lmvTabNmPmix._komp" incaslib="&lmvLibrefPmix." quiet;
	quit;
	
	
	proc fedsql sessref=casauto;
		create table casuser.&lmvTabNmUpt._komp{options replace=true} as
		select distinct t1.* 
		from casuser.&lmvTabNmUpt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB =1
		;
	quit;
	
	proc casutil;
		droptable casdata="&lmvTabNmUpt._komp" incaslib="&lmvLibrefUpt." quiet;
		promote casdata="&lmvTabNmUpt._komp" incaslib="casuser" outcaslib="&lmvLibrefUpt.";
	quit;
	
	*%dp_export_csv(mpInput=&lmvLibrefUpt..&lmvTabNmUpt._komp
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);
	
	%if %sysfunc(exist(&lmvLibrefUpt..&lmvTabNmUpt._komp)) %then %do;
		proc export data=&lmvLibrefUpt..&lmvTabNmUpt._komp(datalimit=all)
					outfile="&mpPath.&lmvTabNmUpt._komp.csv"
					dbms=dlm
					replace
					;
					delimiter='|'
					;
		run;
	%end;
			
	proc casutil;
		droptable casdata="&lmvTabNmUpt._komp" incaslib="&lmvLibrefUpt." quiet;
	quit;
	
	%load_loc_dim;
	%load_prod_dim;
	
%mend dp_load_facts;