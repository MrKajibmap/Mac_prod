%macro ABT_ML_clear_PBO(tb_in, lib_out, tb_out);
	
	proc casutil;
		droptable casdata="&tb_out." incaslib="&lib_out." quiet;
		droptable casdata="&tb_out._FL" incaslib="&lib_out." quiet;
	run;

	proc casutil;
		load data=etl_ia.pbo_close_period(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm and
				channel_cd = 'ALL'
			)
		) casout='pbo_close_period' outcaslib='casuser' replace;	
	run;

	/* Убираем эти интервалы из витрины	 */
	proc fedsql sessref=casauto;
		create table casuser.&tb_out. {options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.sales_dt,
				t1.covid_pattern,
				t1.covid_level,
				t1.covid_lockdown,
				t1.sum_trp_log,
				t1.deseason_multi as target
			from &tb_in. as t1
			left join casuser.pbo_close_period as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt <= t2.end_dt + 3 and
				t1.sales_dt >= t2.start_dt - 3
			where
				t2.pbo_location_id is missing
				and t1.channel_cd = 'ALL'
		;	
	quit;

	/* Удаляем промежуточные таблицы */		
	proc casutil;
		droptable casdata="pbo_close_period" incaslib="casuser" 
				quiet;
	run;
	proc casutil;
		promote incaslib='casuser' casdata="&tb_out." outcaslib="&lib_out." casout="&tb_out";
	run;

	data casuser.&tb_out._FL;
		format feature_nm $40. feature_type $10. use 2.;
		feature_nm='covid_pattern'; feature_type = 'num'; use=1;output;
		feature_nm='covid_level'; feature_type = 'cat'; use=1;output;
		feature_nm='covid_lockdown'; feature_type = 'cat'; use=1;output;
		feature_nm='sum_trp_log'; feature_type = 'num'; use=1;output;
	run;
	proc casutil;
		promote incaslib="casuser" casdata="&tb_out._FL" 
					outcaslib="&lib_out." casout="&tb_out._FL";
	quit;
%mend ABT_ML_clear_PBO;