
%macro load_gc_fact_last_year(
		mpOutput=
		mpOutPath=/data/files/output/dp_files/
	);
	%local
		lmvReportDttm
		lmvOutput
		lmvOutLib
		lmvOutTable
	;

   %let lmvReportDttm=&ETL_CURRENT_DTTM.;
   %member_names(mpTable=&lmvOutput., mpLibrefNameKey=lmvOutLib, mpMemberNameKey=lmvOutTable);


	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						);

	proc sql;
		create table work.gc_last_year123 as
		select distinct
			t1.PBO_LOCATION_ID,
			t1.SALES_DT,
			intnx('month', t1.sales_dt, 12, 'b') as month format=date9.,
			receipt_qty
		from etl_ia.pbo_sales t1
		where
			channel_cd='ALL'
			and (valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.);
	quit;

	proc sql;
		create table work.fin_gc_last123 as
		select
			1 as PROD,
			PBO_LOCATION_ID as LOCATION,
			month as DATA format=yymon7.,
			'RUR' as CURRENCY,
			sum(receipt_qty) as LAST_YEAR_GC
		from work.gc_last_year123
		group by PBO_LOCATION_ID, month;
	quit;


	proc fedsql sessref=casauto;
		create table casuser.gc_forc {option replace = true} as
		select distinct
			prod,
			location,
			intnx('month',data, 12, 'b') as data,
			currency,
			base_fcst_gc as last_year_gc
		from mn_short.plan_gc_month
		;
	quit;

	data casuser.fin_gc (replace=yes);
		set work.fin_gc_last123;
	run;

	data casuser.gc_forc_form;
		set casuser.gc_forc;
		format data yymon7.;
	run;


	proc fedsql sessref=casauto;
		create table casuser.gc_data_prep {options replace=true} as
		select distinct * from casuser.gc_forc_form
		union
		select distinct * from casuser.fin_gc;
	quit;

	proc fedsql sessref=casauto;
		create table casuser.gc_data_LY {option replace = true} as
		select distinct
			prod,
			location,
			data,
			currency,
			sum(last_year_gc) as last_year_gc
		from casuser.gc_data_prep
		group by  prod, location, data, currency
		;
	quit;

   %let lmvReportDttm=&ETL_CURRENT_DTTM.;

	proc sql /*noprint*/;
		create table pbo_dt as
			select distinct
					pbo_location_id
					, input(pbo_loc_attr_value, DDMMYY10.) format=date9. as OPEN_DATE
					, PBO_LOC_ATTR_NM

			from etl_ia.pbo_loc_attributes
			where (PBO_LOC_ATTR_NM = 'OPEN_DATE' or PBO_LOC_ATTR_NM = 'CLOSE_DATE')
					and  valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
			;
	quit;

	proc transpose data=pbo_dt
		out=pbo_open_dt;
		by pbo_location_id;
		var  open_date;
		id PBO_LOC_ATTR_NM;
	run;

	data casuser.pbo_open_dt (replace=yes drop=_name_);
		set pbo_open_dt;
	run;

	data casuser.calendar(drop=i);
		format day month week date9.;
		do i=intnx("year", date(), 0, "b") to intnx("year", date(), 3, "e");
			day = i;
			week = intnx("week.2", day, 0, "b");
			month = intnx("month", day, 0, "b");
			output;
		end;
	run;

	proc fedsql sessref=casauto;
		create table casuser.pbo_date_list{options replace=true} as
			select distinct pbo_location_id
							, OPEN_DATE
							, CLOSE_DATE
							, month
			from casuser.pbo_open_dt t1
				cross join
			(select month
				from casuser.calendar) t2
		;
	quit;

	proc fedsql sessref=casauto;
		create table casuser.komp_matrix{options replace=true} as
			select pbo_location_id
					, month
					, OPEN_DATE
					, CLOSE_DATE
					,(case when intnx('month', month,-12,'b')>=(case
						when day(OPEN_DATE)=1 then
							   cast(OPEN_DATE as date)
				  else cast(intnx('month',OPEN_DATE,1,'b') as date)
	  end)
	  and month <=
				 (case
				  when CLOSE_DATE is null then cast(intnx('month',month,12) as date)
				  when CLOSE_DATE=intnx('month', CLOSE_DATE,0,'e') then cast(CLOSE_DATE as date)
	   else cast(intnx('month', CLOSE_DATE,-1,'e') as date)
				 end) then 1 else 0 end) as KOMP_ATTRIB

			from casuser.pbo_date_list
	;
	quit;


	proc fedsql sesref=casauto;
		create table casuser.gc_data_komp {options replace=true} as
		select t1.*
		from casuser.gc_data_LY t1
			inner join casuser.komp_matrix t2
				on t1.location = t2.pbo_location_id
				and t1.data = t2.month
		where t2.KOMP_ATTRIB = 1;
	quit;

	data casuser.gc_data_komp_form;
		set casuser.gc_data_komp;
		format data yymon7.;
	run;

	proc casutil;
		droptable incaslib="&lmvOutLib" casdata="&lmvOutTable." quiet;
		promote incaslib="casuser" casdata="gc_data_komp_form" outcaslib="&lmvOutLib." casout="&lmvOutTable.";
		save incaslib="&lmvOutLib." casdata="&lmvOutTable." outcaslib="&lmvOutLib." casout="&lmvOutTable..sashdat" replace;
	run;
	
	%dp_export_csv(
		mpInput=&mpOutput.,
		mpTHREAD_CNT=1,
		mpPath=&mpOutPath.,
		mpAuthFlag=Y
	);

%mend load_gc_fact_last_year;