%macro rtp_komp_sep(mpInPmixLt=dm_abt.plan_pmix_month,
					mpInGcLt=dm_abt.plan_gc_month, 
					mpInUptLt=dm_abt.plan_upt_month, 
					mpInPmixSt=dm_abt.plan_pmix_day,
					mpInGcSt=dm_abt.plan_gc_day, 
					mpInUptSt=dm_abt.plan_upt_day, 
					mpPathOut=/data/files/output/dp_files/);
							
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	/* Проверка на существование входных наборов данных с прогнозами разных разрезов */
	%member_exists_list(mpMemberList=&mpInPmixLt.
									&mpInGcLt.
									&mpInUptLt.
									&mpInPmixSt.
									&mpInGcSt.
									&mpInUptSt.
									);
						
	%local	lmvOutLibrefPmixSt 
			lmvOutTabNamePmixSt 
			lmvOutLibrefGcSt 
			lmvOutTabNameGcSt 
			lmvOutLibrefUptSt 
			lmvOutTabNameUptSt 
			lmvOutLibrefPmixLt 
			lmvOutTabNamePmixLt 
			lmvOutLibrefGcLt 
			lmvOutTabNameGcLt
			lmvOutLibrefUptLt 
			lmvOutTabNameUptLt  
			lmvPathOut
			;
			
	%let lmvPathOut=&mpPathOut.;
	
	%member_names (mpTable=&mpInGcSt, mpLibrefNameKey=lmvOutLibrefGcSt, mpMemberNameKey=lmvOutTabNameGcSt); 
	%member_names (mpTable=&mpInPmixSt, mpLibrefNameKey=lmvOutLibrefPmixSt, mpMemberNameKey=lmvOutTabNamePmixSt); 
	%member_names (mpTable=&mpInUptSt, mpLibrefNameKey=lmvOutLibrefUptSt, mpMemberNameKey=lmvOutTabNameUptSt); 
	%member_names (mpTable=&mpInGcLt, mpLibrefNameKey=lmvOutLibrefGcLt, mpMemberNameKey=lmvOutTabNameGcLt); 
	%member_names (mpTable=&mpInPmixLt, mpLibrefNameKey=lmvOutLibrefPmixLt, mpMemberNameKey=lmvOutTabNamePmixLt); 
	%member_names (mpTable=&mpInUptLt, mpLibrefNameKey=lmvOutLibrefUptLt, mpMemberNameKey=lmvOutTabNameUptLt); 
		
	/* Создание разметки по комп-некомп ресторанам */
	%load_komp_matrix;
	/* non-komp*/
	
	/* plan_upt_month */
	proc casutil;
		droptable casdata="&lmvOutTabNameUptLt._nonkomp" incaslib="&lmvOutLibrefUptLt." quiet;
	quit;
	
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._nonkomp{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefUptLt..&lmvOutTabNameUptLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month
			and t2.KOMP_ATTRIB = 1
		;
	quit;

	data &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._nonkomp(replace=yes);
		set &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._nonkomp;
		format DATA yymon7.;
	run;

	proc casutil;
		promote casdata="&lmvOutTabNameUptLt._nonkomp" incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt.";
	quit;

	*%dp_export_csv_prod(mpInput=&lmvOutLibrefUptLt..&lmvOutTabNameUptLt._nonkomp
					, mpPath=&lmvPathOut.);
	%dp_export_csv(mpInput=&lmvOutLibrefUptLt..&lmvOutTabNameUptLt._nonkomp, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);
	proc casutil;
		droptable casdata="&lmvOutTabNameUptLt._nonkomp" incaslib="&lmvOutLibrefUptLt." quiet;
	quit;


	/* plan_pmix_month */
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixLt._nonkomp" incaslib="&lmvOutLibrefPmixLt." quiet;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._nonkomp{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB = 0
		;
	quit;

	data &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._nonkomp (replace=yes);
		set &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._nonkomp;
		format DATA yymon7.;
	run;

	proc casutil;
			promote casdata="&lmvOutTabNamePmixLt._nonkomp" incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt.";
	quit;
	*%dp_export_csv_prod(mpInput=&lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._nonkomp
					, mpPath=&lmvPathOut.);
	%dp_export_csv(mpInput=&lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._nonkomp, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixLt._nonkomp" incaslib="&lmvOutLibrefPmixLt." quiet;
	quit;


	/* plan_gc_month */
	proc casutil;
		droptable casdata="&lmvOutTabNameGcLt._nonkomp" incaslib="&lmvOutLibrefGcLt." quiet;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._nonkomp{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefGcLt..&lmvOutTabNameGcLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB = 0
		;
	quit;

	data &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._nonkomp (replace=yes);
		set &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._nonkomp;
		format DATA yymon7.;
	run;

	proc casutil;
			promote casdata="&lmvOutTabNameGcLt._nonkomp" incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt.";
	quit;
	*%dp_export_csv_prod(mpInput=&lmvOutLibrefGcLt..&lmvOutTabNameGcLt._nonkomp
					, mpPath=&lmvPathOut.);
	%dp_export_csv(mpInput=&lmvOutLibrefGcLt..&lmvOutTabNameGcLt._nonkomp, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);
	proc casutil;
		droptable casdata="&lmvOutTabNameGcLt._nonkomp" incaslib="&lmvOutLibrefGcLt." quiet;
	quit;



	proc casutil;
		droptable casdata="&lmvOutTabNameUptSt._nonkomp" incaslib="&lmvOutLibrefUptSt." quiet;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptSt..&lmvOutTabNameUptSt._nonkomp{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefUptSt..&lmvOutTabNameUptSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 0
		;
	quit;

	proc casutil;
			promote casdata="&lmvOutTabNameUptSt._nonkomp" incaslib="&lmvOutLibrefUptSt." outcaslib="&lmvOutLibrefUptSt.";
	quit;
	*%dp_export_csv_prod(mpInput=&lmvOutLibrefUptSt..&lmvOutTabNameUptSt._nonkomp, mpPath=&lmvPathOut.);
	%dp_export_csv(mpInput=&lmvOutLibrefUptSt..&lmvOutTabNameUptSt._nonkomp, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);
	proc casutil;
		droptable casdata="&lmvOutTabNameUptSt._nonkomp" incaslib="&lmvOutLibrefUptSt." quiet;
	quit;

	proc casutil;
		droptable casdata="&lmvOutTabNamePmixSt._nonkomp" incaslib="&lmvOutLibrefPmixSt." quiet;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._nonkomp{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 0
		;
	quit;

	proc casutil;
			promote casdata="&lmvOutTabNamePmixSt._nonkomp" incaslib="&lmvOutLibrefPmixSt." outcaslib="&lmvOutLibrefPmixSt.";
	quit;
	*%dp_export_csv_prod(mpInput=&lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._nonkomp
					, mpPath=&lmvPathOut.);
	%dp_export_csv(mpInput=&lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._nonkomp, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixSt._nonkomp" incaslib="&lmvOutLibrefPmixSt." quiet;
	quit;
	
	proc casutil;
		droptable casdata="&lmvOutTabNameGcSt._nonkomp" incaslib="&lmvOutLibrefGcSt." quiet;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcSt..&lmvOutTabNameGcSt._nonkomp{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefGcSt..&lmvOutTabNameGcSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 0
		;
	quit;	
		
	proc casutil;
			promote casdata="&lmvOutTabNameGcSt._nonkomp" incaslib="&lmvOutLibrefGcSt." outcaslib="&lmvOutLibrefGcSt.";
	quit;
	*%dp_export_csv_prod(mpInput=&lmvOutLibrefGcSt..&lmvOutTabNameGcSt._nonkomp
					, mpPath=&lmvPathOut.);
	%dp_export_csv(mpInput=&lmvOutLibrefGcSt..&lmvOutTabNameGcSt._nonkomp, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);
	proc casutil;
		droptable casdata="&lmvOutTabNameGcSt._nonkomp" incaslib="&lmvOutLibrefGcSt." quiet;
	quit;	
	
		
	/* komp*/
	proc casutil;
		droptable casdata="&lmvOutTabNameUptLt._komp" incaslib="&lmvOutLibrefUptLt." quiet;
	quit;
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._komp{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefUptLt..&lmvOutTabNameUptLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB = 1
		;
	quit;

	data &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._komp(replace=yes);
		set &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._komp;
		format DATA yymon7.;
	run;

	proc casutil;
			promote casdata="&lmvOutTabNameUptLt._komp" incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt.";
	quit;
	*%dp_export_csv_prod(mpInput=&lmvOutLibrefUptLt..&lmvOutTabNameUptLt._komp
					, mpPath=&lmvPathOut.);
	%dp_export_csv(mpInput=&lmvOutLibrefUptLt..&lmvOutTabNameUptLt._komp, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);
	proc casutil;
		droptable casdata="&lmvOutTabNameUptLt._komp" incaslib="&lmvOutLibrefUptLt." quiet;
	quit;

	proc casutil;
		droptable casdata="&lmvOutTabNamePmixLt._komp" incaslib="&lmvOutLibrefPmixLt." quiet;
	quit;
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._komp{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB = 1
		;
	quit;
	data &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._komp (replace=yes);
		set &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._komp;
		format DATA yymon7.;
	run;
	proc casutil;
		promote casdata="&lmvOutTabNamePmixLt._komp" incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt.";
	quit;
	*%dp_export_csv_prod(mpInput=&lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._komp
					, mpPath=&lmvPathOut.);
	%dp_export_csv(mpInput=&lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._komp, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixLt._komp" incaslib="&lmvOutLibrefPmixLt." quiet;
	quit;


	proc casutil;
		droptable casdata="&lmvOutTabNameGcLt._komp" incaslib="&lmvOutLibrefGcLt." quiet;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._komp{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefGcLt..&lmvOutTabNameGcLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB = 1
		;
	quit;

	data &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._komp (replace=yes);
		set &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._komp;
		format DATA yymon7.;
	run;
	proc casutil;
			promote casdata="&lmvOutTabNameGcLt._komp" incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt.";
	quit;
	*%dp_export_csv_prod(mpInput=&lmvOutLibrefGcLt..&lmvOutTabNameGcLt._komp
					, mpPath=&lmvPathOut.);
	%dp_export_csv(mpInput=&lmvOutLibrefGcLt..&lmvOutTabNameGcLt._komp, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);
	proc casutil;
		droptable casdata="&lmvOutTabNameGcLt._komp" incaslib="&lmvOutLibrefGcLt." quiet;
	quit;


		/*
	proc casutil;
		droptable casdata="&lmvOutTabNameUptSt._komp" incaslib="&lmvOutLibrefUptSt." quiet;
	quit;
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptSt..&lmvOutTabNameUptSt._komp{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefUptSt..&lmvOutTabNameUptSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB = 1
		;
	quit;
	proc casutil;
		promote casdata="&lmvOutTabNameUptSt._komp" incaslib="&lmvOutLibrefUptSt." outcaslib="&lmvOutLibrefUptSt.";
	quit;
	%dp_export_csv_prod(mpInput=&lmvOutLibrefUptSt..&lmvOutTabNameUptSt._komp
					, mpPath=&lmvPathOut.);
	proc casutil;
		droptable casdata="&lmvOutTabNameUptSt._komp" incaslib="&lmvOutLibrefUptSt." quiet;
	quit;
	*/
	/* Для первого месяца */
	/*
	proc casutil;
		droptable casdata="&lmvOutTabNameUptSt._komp1" incaslib="&lmvOutLibrefUptSt." quiet;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptSt..&lmvOutTabNameUptSt._komp1{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefUptSt..&lmvOutTabNameUptSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 1
		where DATA <= INTNX( 'MONTH', today(), 1, 'SAMEDAY')
		;
	quit;	

	proc casutil;
			promote casdata="&lmvOutTabNameUptSt._komp1" incaslib="&lmvOutLibrefUptSt." outcaslib="&lmvOutLibrefUptSt.";
	quit;


	%dp_export_csv(mpInput=&lmvOutLibrefUptSt..&lmvOutTabNameUptSt._komp1, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);	
	proc casutil;
		droptable casdata="&lmvOutTabNameUptSt._komp1" incaslib="&lmvOutLibrefUptSt." quiet;
	quit;
	*/
	/* Для второго месяца */
	/*
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptSt..&lmvOutTabNameUptSt._komp2{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefUptSt..&lmvOutTabNameUptSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 1
		where DATA > INTNX( 'MONTH', today(), 1, 'SAMEDAY')
		and DATA <= INTNX( 'MONTH', today(), 2, 'SAMEDAY')
		;
	quit;	

	proc casutil;
			promote casdata="&lmvOutTabNameUptSt._komp2" incaslib="&lmvOutLibrefUptSt." outcaslib="&lmvOutLibrefUptSt.";
	quit;

	%dp_export_csv(mpInput=&lmvOutLibrefUptSt..&lmvOutTabNameUptSt._komp2, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);	
	proc casutil;
		droptable casdata="&lmvOutTabNameUptSt._komp2" incaslib="&lmvOutLibrefUptSt." quiet;
	quit;
	*/
	/* Для третьего месяца */
/*
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptSt..&lmvOutTabNameUptSt._komp3{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefUptSt..&lmvOutTabNameUptSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 1
		where DATA > INTNX( 'MONTH', today(), 2, 'SAMEDAY')
		;
	quit;	

	proc casutil;
			promote casdata="&lmvOutTabNameUptSt._komp3" incaslib="&lmvOutLibrefUptSt." outcaslib="&lmvOutLibrefUptSt.";
	quit;

	%dp_export_csv(mpInput=&lmvOutLibrefUptSt..&lmvOutTabNameUptSt._komp3, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);	
	proc casutil;
		droptable casdata="&lmvOutTabNameUptSt._komp3" incaslib="&lmvOutLibrefUptSt." quiet;
	quit;
	*/

	proc casutil;
		droptable casdata="&lmvOutTabNamePmixSt._komp" incaslib="&lmvOutLibrefPmixSt." quiet;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 1
		;
	quit;
	proc casutil;
			promote casdata="&lmvOutTabNamePmixSt._komp" incaslib="&lmvOutLibrefPmixSt." outcaslib="&lmvOutLibrefPmixSt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._komp
					, mpTHREAD_CNT=1 
					, mpPath=&lmvPathOut.);
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixSt._komp" incaslib="&lmvOutLibrefPmixSt." quiet;
	quit;



	/* Для первого месяца */
/* proc casutil;
		droptable casdata="&lmvOutTabNamePmixSt._komp1" incaslib="&lmvOutLibrefPmixSt." quiet;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._komp1{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 1
		where DATA <= INTNX( 'MONTH', today(), 1, 'SAMEDAY')
		;
	quit;	

	proc casutil;
			promote casdata="&lmvOutTabNamePmixSt._komp1" incaslib="&lmvOutLibrefPmixSt." outcaslib="&lmvOutLibrefPmixSt.";
	quit;
*/

	/*%dp_export_csv(mpInput=&lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._komp1, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);	
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixSt._komp1" incaslib="&lmvOutLibrefPmixSt." quiet;
	quit;
	*/
	/* Для второго месяца */
/*
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._komp2{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 1
		where DATA > INTNX( 'MONTH', today(), 1, 'SAMEDAY')
		and DATA <= INTNX( 'MONTH', today(), 2, 'SAMEDAY')
		;
	quit;	

	proc casutil;
			promote casdata="&lmvOutTabNamePmixSt._komp2" incaslib="&lmvOutLibrefPmixSt." outcaslib="&lmvOutLibrefPmixSt.";
	quit;
					, mpPath=&lmvPathOut.);
	%dp_export_csv(mpInput=&lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._komp2, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);		
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixSt._komp2" incaslib="&lmvOutLibrefPmixSt." quiet;
	quit;
*/	
	/* Для третьего месяца */
/*
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._komp3{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 1
		where DATA > INTNX( 'MONTH', today(), 2, 'SAMEDAY')
		;
	quit;	

	proc casutil;
			promote casdata="&lmvOutTabNamePmixSt._komp3" incaslib="&lmvOutLibrefPmixSt." outcaslib="&lmvOutLibrefPmixSt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._komp3, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixSt._komp3" incaslib="&lmvOutLibrefPmixSt." quiet;
	quit;
*/
	
	proc casutil;
		droptable casdata="&lmvOutTabNameGcSt._komp" incaslib="&lmvOutLibrefGcSt." quiet;
	quit;
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcSt..&lmvOutTabNameGcSt._komp{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefGcSt..&lmvOutTabNameGcSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 1
		;
	quit;	
 
	proc casutil;
			promote casdata="&lmvOutTabNameGcSt._komp" incaslib="&lmvOutLibrefGcSt." outcaslib="&lmvOutLibrefGcSt.";
	quit;
	*%dp_export_csv_prod(mpInput=&lmvOutLibrefGcSt..&lmvOutTabNameGcSt._komp
					, mpPath=&lmvPathOut.);
	%dp_export_csv(mpInput=&lmvOutLibrefGcSt..&lmvOutTabNameGcSt._komp, mpTHREAD_CNT=1, mpPath=&lmvPathOut.);				
	proc casutil;
		droptable casdata="&lmvOutTabNameGcSt._komp" incaslib="&lmvOutLibrefGcSt." quiet;
	quit;

%mend rtp_komp_sep;