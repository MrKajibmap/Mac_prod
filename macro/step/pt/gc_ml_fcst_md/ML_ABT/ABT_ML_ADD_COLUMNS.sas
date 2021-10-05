%macro ABT_ML_ADD_COLUMNS(lib_main = casuser
						, tb_main = 
						, lib_add = casuser
						, tb_add =
						, lib_out = casuser
						, tb_out =
						);

	/* 	запоминаем сколько строк было */
	proc sql noprint;
		select sum(1) into :rows_in from &lib_main..&tb_main.;
	quit;
	
/* 	выкачиваем список имеющихся столбцов */
	proc contents data=&lib_add..&tb_add;
		ods output variables=casuser.INIT_VAR;
	;quit;

	/* 	выкачиваем список новыех столбцов	 */
	%let var_list = ;
	proc sql noprint;
		select feature_nm into : var_list separated by ' ,t2.'
		from &lib_add..&tb_add._FL as t1.
		left join casuser.INIT_VAR as t2 on upcase(t1.feature_nm) = upcase(t2.Variable)
		where t1.use = 1 and t2.Variable is missing
	;
	quit;
	
	/* 	если нечего добавлять - ничего не делаем */
	/* 	ToDo Сделать перезапись целевой таблицы */
	%if var_list NE %str() %then %do;
		%let var_list = ,t2.&var_list;
	

	/* 	определяем, по каким столбцам джойнить */
		proc contents data=&lib_add..&tb_add;
			ods output variables=casuser.A_VAR;
		;quit;
		proc sql noprint;
			select 
				sum(case when upcase(Variable) = 'SALES_DT' then 1 else 0 end) as SALES_DT
				,sum(case when upcase(Variable) = 'PBO_LOCATION_ID' then 1 else 0 end) as PBO_LOCATION_ID
				,sum(case when upcase(Variable) = 'CHANNEL_CD' then 1 else 0 end) as CHANNEL_CD
				into :s1, :s2, :s3
			from casuser.A_VAR;
		quit;
		%let w1 = ; %let w2 =; %let w3 =;
		%if &s1 = 1 %then %do; %let w1 = %str(and t1.sales_dt = t2.sales_dt); %end;
		%if &s2 = 1 %then %do; %let w2 = %str(and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID); %end;
		%if &s3 = 1 %then %do; %let w3 = %str(and t1.CHANNEL_CD = t2.CHANNEL_CD); %end;
		%let join_cond = %str(1=1) &w1 &w2 &w3;	
		%put &=join_cond;
	
		%if &s1 = 0 and &s2 = 0 and &s3 = 0 %then %put ERROR: джойн ща кекнет! ;
	
		/* 	джойн */
		proc fedsql sessref=casauto;
			create table casuser.tmp {options replace=true} as
			select t1.*
				&var_list.
			from &lib_main..&tb_main. as t1 
			left join &lib_add..&tb_add. as t2 on &join_cond.
		;
		quit;
		
		proc sql noprint;
			select sum(1) into :rows_out from casuser.tmp;
		quit;
		
		%if &rows_in = &rows_out %then %do;		
			proc casutil;
				droptable casdata="&tb_out." incaslib="&lib_out." quiet;
			run;
			proc casutil;
				promote incaslib='casuser' casdata="tmp" outcaslib="&lib_out." casout="&tb_out";
			run;		
		%end; %else %do;
			%put WARNING: ЧТО-ТО ПОШЛО НЕ ТАК, КОЛИЧЕСТВО СТРОК ИЗМЕНИЛОСЬ &rows_in => &rows_out;
			%put WARNING: casuser.tmp;
		%end; /* if-else */

	%end; /* var_list NE %str() */

	proc casutil;
		droptable casdata="A_VAR" incaslib="casuser" quiet;
		droptable casdata="INIT_VAR" incaslib="casuser" quiet;
	run;
%mend ABT_ML_ADD_COLUMNS;

