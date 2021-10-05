/* Макрос разбивает прогнозы (в шести разрезах) на komp|nonkomp и выгружает их в указанную каслибу */

%macro rtp_load_fcst_sep_x_load(mpInPmixLt=dm_abt.plan_pmix_month,
							mpInGcLt=dm_abt.plan_gc_month, 
							mpInUptLt=dm_abt.plan_upt_month, 
							mpInPmixSt=dm_abt.plan_pmix_day,
							mpInGcSt=dm_abt.plan_gc_day, 
							mpInUptSt=dm_abt.plan_upt_day,
							mpOutCaslib=dm_abt
						);
							
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	
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
			lmvOutLibrefOutforgc 
			lmvOutTabNameOutforgc 
			lmvOutLibrefOutfor 
			lmvOutTabNameOutfor 
			lmvOutCaslib
			;
			
	%let lmvOutCaslib =  &mpOutCaslib.;
	
	%member_names (mpTable=&mpOutOutfor, mpLibrefNameKey=lmvOutLibrefOutfor, mpMemberNameKey=lmvOutTabNameOutfor);
	%member_names (mpTable=&mpOutOutforgc, mpLibrefNameKey=lmvOutLibrefOutforgc, mpMemberNameKey=lmvOutTabNameOutforgc); 
	%member_names (mpTable=&mpInGcSt, mpLibrefNameKey=lmvOutLibrefGcSt, mpMemberNameKey=lmvOutTabNameGcSt); 
	%member_names (mpTable=&mpInPmixSt, mpLibrefNameKey=lmvOutLibrefPmixSt, mpMemberNameKey=lmvOutTabNamePmixSt); 
	%member_names (mpTable=&mpInUptSt, mpLibrefNameKey=lmvOutLibrefUptSt, mpMemberNameKey=lmvOutTabNameUptSt); 
	%member_names (mpTable=&mpInGcLt, mpLibrefNameKey=lmvOutLibrefGcLt, mpMemberNameKey=lmvOutTabNameGcLt); 
	%member_names (mpTable=&mpInPmixLt, mpLibrefNameKey=lmvOutLibrefPmixLt, mpMemberNameKey=lmvOutTabNamePmixLt); 
	%member_names (mpTable=&mpInUptLt, mpLibrefNameKey=lmvOutLibrefUptLt, mpMemberNameKey=lmvOutTabNameUptLt); 
		
	/* Создание разметки по комп-некомп ресторанам */
	%load_komp_matrix;
	/* non-komp*/
	proc casutil;
		droptable casdata="&lmvOutTabNameUptLt._nonkomp" incaslib="&lmvOutCaslib." quiet;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..&lmvOutTabNameUptLt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefUptLt..&lmvOutTabNameUptLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB = 0
		;
	quit;

	data &lmvOutCaslib..&lmvOutTabNameUptLt._nonkomp(replace=yes);
		set &lmvOutCaslib..&lmvOutTabNameUptLt._nonkomp;
		format DATA yymon7.;
	run;

	proc casutil;
		promote casdata="&lmvOutTabNameUptLt._nonkomp" incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib.";
		save incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib." casdata="&lmvOutTabNameUptLt._nonkomp" casout="&lmvOutTabNameUptLt._nonkomp.sashdat" replace;
	quit;
	
	
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixLt._nonkomp" incaslib="&lmvOutCaslib." quiet;
	quit;

	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..&lmvOutTabNamePmixLt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB = 0
		;
	quit;

	data &lmvOutCaslib..&lmvOutTabNamePmixLt._nonkomp (replace=yes);
		set &lmvOutCaslib..&lmvOutTabNamePmixLt._nonkomp;
		format DATA yymon7.;
	run;

	proc casutil;
			promote casdata="&lmvOutTabNamePmixLt._nonkomp" incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib.";
			save incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib." casdata="&lmvOutTabNamePmixLt._nonkomp" casout="&lmvOutTabNamePmixLt._nonkomp.sashdat" replace;
	quit;


	proc casutil;
		droptable casdata="&lmvOutTabNameGcLt._nonkomp" incaslib="&lmvOutCaslib." quiet;
	quit;
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..&lmvOutTabNameGcLt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefGcLt..&lmvOutTabNameGcLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB = 0
		;
	quit;

	data &lmvOutCaslib..&lmvOutTabNameGcLt._nonkomp (replace=yes);
		set &lmvOutCaslib..&lmvOutTabNameGcLt._nonkomp;
		format DATA yymon7.;
	run;

	proc casutil;
			promote casdata="&lmvOutTabNameGcLt._nonkomp" incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib.";
			save incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib." casdata="&lmvOutTabNameGcLt._nonkomp" casout="&lmvOutTabNameGcLt._nonkomp.sashdat" replace;
	quit;


	proc casutil;
		droptable casdata="&lmvOutTabNameUptSt._nonkomp" incaslib="&lmvOutCaslib." quiet;
	quit;
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..&lmvOutTabNameUptSt._nonkomp{options replace=true} as
		select distinct t1.* 
		from &lmvOutLibrefUptSt..&lmvOutTabNameUptSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 0
		;
	quit;
	

	proc casutil;
			promote casdata="&lmvOutTabNameUptSt._nonkomp" incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib.";
			save incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib." casdata="&lmvOutTabNameUptSt._nonkomp" casout="&lmvOutTabNameUptSt._nonkomp.sashdat" replace;
	quit;


	proc casutil;
		droptable casdata="&lmvOutTabNamePmixSt._nonkomp" incaslib="&lmvOutCaslib." quiet;
	quit;
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..&lmvOutTabNamePmixSt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 0
		;
	quit;


	proc casutil;
		promote casdata="&lmvOutTabNamePmixSt._nonkomp" incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib.";
		save incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib." casdata="&lmvOutTabNamePmixSt._nonkomp" casout="&lmvOutTabNamePmixSt._nonkomp.sashdat" replace;
	quit;


	proc casutil;
		droptable casdata="&lmvOutTabNameGcSt._nonkomp" incaslib="&lmvOutCaslib." quiet;
	quit;
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..&lmvOutTabNameGcSt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefGcSt..&lmvOutTabNameGcSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 0
		;
	quit;	
	
		
	proc casutil;
			promote casdata="&lmvOutTabNameGcSt._nonkomp" incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib.";
			save incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib." casdata="&lmvOutTabNameGcSt._nonkomp" casout="&lmvOutTabNameGcSt._nonkomp.sashdat" replace;
	quit;
	/* komp*/
	proc casutil;
		droptable casdata="&lmvOutTabNameUptLt._komp" incaslib="&lmvOutCaslib." quiet;
	quit;
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..&lmvOutTabNameUptLt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefUptLt..&lmvOutTabNameUptLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB = 0
		;
	quit;

	data &lmvOutCaslib..&lmvOutTabNameUptLt._komp(replace=yes);
		set &lmvOutCaslib..&lmvOutTabNameUptLt._komp;
		format DATA yymon7.;
	run;

	proc casutil;
			promote casdata="&lmvOutTabNameUptLt._komp" incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib.";
			save incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib." casdata="&lmvOutTabNameUptLt._komp" casout="&lmvOutTabNameUptLt._komp.sashdat" replace;
	quit;
		
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixLt._komp" incaslib="&lmvOutCaslib." quiet;
	quit;
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..&lmvOutTabNamePmixLt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB = 1
		;
	quit;
	data &lmvOutCaslib..&lmvOutTabNamePmixLt._komp (replace=yes);
		set &lmvOutCaslib..&lmvOutTabNamePmixLt._komp;
		format DATA yymon7.;
	run;
	proc casutil;
		promote casdata="&lmvOutTabNamePmixLt._komp" incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib.";
		save incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib." casdata="&lmvOutTabNamePmixLt._komp" casout="&lmvOutTabNamePmixLt._komp.sashdat" replace;
	quit;

	proc casutil;
		droptable casdata="&lmvOutTabNameGcLt._komp" incaslib="&lmvOutCaslib." quiet;
	quit;
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..&lmvOutTabNameGcLt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefGcLt..&lmvOutTabNameGcLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB = 1
		;
	quit;

	data &lmvOutCaslib..&lmvOutTabNameGcLt._komp (replace=yes);
		set &lmvOutCaslib..&lmvOutTabNameGcLt._komp;
		format DATA yymon7.;
	run;
	proc casutil;
			promote casdata="&lmvOutTabNameGcLt._komp" incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib.";
			save incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib." casdata="&lmvOutTabNameGcLt._komp" casout="&lmvOutTabNameGcLt._komp.sashdat" replace;
	quit;
	
	proc casutil;
		droptable casdata="&lmvOutTabNameUptSt._komp" incaslib="&lmvOutCaslib." quiet;
	quit;
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..&lmvOutTabNameUptSt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefUptSt..&lmvOutTabNameUptSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t1.DATA = t2.month and t2.KOMP_ATTRIB = 1
		;
	quit;
	proc casutil;
		promote casdata="&lmvOutTabNameUptSt._komp" incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib.";
		save incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib." casdata="&lmvOutTabNameUptSt._komp" casout="&lmvOutTabNameUptSt._komp.sashdat" replace;
	quit;

	proc casutil;
		droptable casdata="&lmvOutTabNamePmixSt._komp" incaslib="&lmvOutCaslib." quiet;
	quit;
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..&lmvOutTabNamePmixSt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. t1
		inner join casuser.komp_matrix t2
				on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 1
		;
	quit;
	
	proc casutil;
			promote casdata="&lmvOutTabNamePmixSt._komp" incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib.";
			save incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib." casdata="&lmvOutTabNamePmixSt._komp" casout="&lmvOutTabNamePmixSt._komp.sashdat" replace;
	quit;

	proc casutil;
		droptable casdata="&lmvOutTabNameGcSt._komp" incaslib="&lmvOutCaslib." quiet;
	quit;
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..&lmvOutTabNameGcSt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefGcSt..&lmvOutTabNameGcSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and intnx('month',t1.DATA,0,'b') = t2.month and t2.KOMP_ATTRIB = 1
		;
	quit;	
 
	proc casutil;
		promote casdata="&lmvOutTabNameGcSt._komp" incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib.";
		save incaslib="&lmvOutCaslib." outcaslib="&lmvOutCaslib." casdata="&lmvOutTabNameGcSt._komp" casout="&lmvOutTabNameGcSt._komp.sashdat" replace;
	quit;
	
%mend rtp_load_fcst_sep_x_load;
						