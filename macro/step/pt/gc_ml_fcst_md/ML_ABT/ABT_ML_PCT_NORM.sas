%macro ABT_ML_PCT_NORM(lib_in, tb_in, lib_out, tb_out);

	%if %sysfunc(exist(&lib_out..&tb_out.)) %then %do;
		proc casutil;
			droptable casdata="&tb_out." incaslib="&lib_out." quiet;
		run;
	%end;
	proc means data = &lib_in..&tb_in. noprint;
		by pbo_location_id channel_cd;
		var target;
		output out=casuser.TARGET_PCT10_90 p10= p90= / autoname;
	run;

/* 	на случай коротких рядов, чтобы не было деления на 0  */
	data casuser.TARGET_PCT10_90;
		set casuser.TARGET_PCT10_90;
		if (target_p10 = target_p90) then do;
			target_p10=0;
			target_p90=1;
		end;
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.gc_ml2{options replace=true} as
		select	
			t1.channel_cd,
			t1.pbo_location_id,
			t1.sales_dt,
			t1.covid_pattern,
			t1.covid_level,
			t1.covid_lockdown,
			t1.sum_trp_log,
			t1.target as target_init
			,(t1.target - t2.target_p10) / (t2.target_p90 - t2.target_p10) as target
			,t2.target_p10
			,t2.target_p90
		from &lib_in..&tb_in. as t1
		inner join casuser.TARGET_PCT10_90 as t2
			on t1.pbo_location_id = t2.pbo_location_id
			and t1.channel_cd = t2.channel_cd

	;quit;

	proc casutil;
		droptable casdata="TARGET_PCT10_90" incaslib="casuser" quiet;	
	run;
	proc casutil;
		promote incaslib='casuser' casdata="gc_ml2" outcaslib="&lib_out." casout="&tb_out";
	run;

%mend;