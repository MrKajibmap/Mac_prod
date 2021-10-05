%macro ABT_ML_PROMO_MECH(lib_in,tb_in,lib_out,tb_out);
	proc casutil;
		droptable incaslib="casuser" casdata="promo_ml" quiet;
		droptable casdata="&tb_out." incaslib="&lib_out." quiet;
		droptable casdata="&tb_out._FL" incaslib="&lib_out." quiet;
	run;
	proc fedsql sessref=casauto;
		create table casuser.promo_ml{options replace=true} as
			select
				t1.pbo_location_id
				,t2.channel_cd
				,t2.start_dt
				,t2.end_dt
				, 'PM_' || coalesce(t3.new_mechanic, 'no_mech') as PROMO_MECH_SK

			from casuser.promo_pbo_enh as t1
			left join casuser.promo_enh as t2
										on	t1.promo_id = t2.promo_id
			left join MN_SHORT.PROMO_MECH_TRANSFORMATION  as t3  
										on t2.promo_mechanics = t3.old_mechanic
		;	
	quit;

/* 	схлопываем */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml_agg{options replace=true} as
		select
			t1.sales_dt
			,t1.pbo_location_id
			,t1.channel_cd
			, coalesce(t2.PROMO_MECH_SK,'PM_no_mech') as PROMO_MECH_SK
			, count(*) as cnt
		from  &lib_in..&tb_in. as t1 
		left join casuser.promo_ml as t2 on
				t1.pbo_location_id = t2.pbo_location_id 
				and t1.channel_cd = t2.channel_cd
				and t1.sales_dt between t2.start_dt and t2.end_dt
		group by 
			t1.sales_dt
			,t1.pbo_location_id
			,t1.channel_cd
			, coalesce(t2.PROMO_MECH_SK,'PM_no_mech')
	;quit;

	proc sql  noprint;
		select distinct PROMO_MECH_SK into :PV_VAR_LIST separated by ','
		from casuser.promo_ml_agg
	;quit;
	%put &=PV_VAR_LIST;

/* 	транспонируем */
	proc transpose data=casuser.promo_ml_agg
               out=casuser.promo_ml_T
/* 				prefix=PM_ */
		;
	    by pbo_location_id channel_cd sales_dt;
	    id PROMO_MECH_SK;	
	    var cnt;
	run;
	
	proc casutil;
		promote incaslib='casuser' casdata='promo_ml_T' outcaslib="&lib_out." casout="&tb_out";/* 		save incaslib='casuser' casdata='gc_ml3' outcaslib="&outp_lib." casout="&outp_dm_nm."; */
	run;
	proc casutil;
		droptable incaslib="casuser" casdata="promo_ml" quiet;
		droptable incaslib="casuser" casdata="promo_ml_agg" quiet;
/* 		droptable incaslib="casuser" casdata="promo_ml_T" quiet; */
	run;

		data casuser.&tb_out._FL;
		format feature_nm $40. feature_type $10. use 2.;
		feature_nm='PM_bogo'; feature_type = 'num'; use=1;output;
		feature_nm='PM_discount'; feature_type = 'num'; use=1;output;
		feature_nm='PM_evm_set'; feature_type = 'num'; use=1;output;
		feature_nm='PM_no_mech'; feature_type = 'num'; use=1;output;
		feature_nm='PM_non_product_gift'; feature_type = 'num'; use=1;output;
		feature_nm='PM_other_digital'; feature_type = 'num'; use=1;output;
		feature_nm='PM_pairs'; feature_type = 'num'; use=1;output;
		feature_nm='PM_product_gift'; feature_type = 'num'; use=1;output;
		feature_nm='PM_support'; feature_type = 'num'; use=1;output;
	run;
	proc casutil;
		promote incaslib="casuser" casdata="&tb_out._FL" 
					outcaslib="&lib_out." casout="&tb_out._FL";
	quit;

%mend ABT_ML_PROMO_MECH;