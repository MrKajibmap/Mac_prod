%macro ABT_ML_PROMO_st2(lib_in,tb_in,lib_out,tb_out);
	
	proc casutil;
		droptable casdata="&tb_out." incaslib="&lib_out." quiet;
		droptable casdata="&tb_out._FL" incaslib="&lib_out." quiet;
	run;

	%if not %sysfunc(exist(casuser.promo_ml)) %then %do;
		
		%if not %sysfunc(exist(casuser.promo_pbo_enh)) %then %do;
			%add_promotool_marks2(mpOutCaslib=casuser, mpPtCaslib=pt, PromoCalculationRk=);
		%end;

		proc fedsql sessref=casauto;
			create table casuser.promo_ml{options replace=true} as
				select
					t1.pbo_location_id
					,t2.channel_cd
					,t2.promo_id
					,t2.promo_group_id
					,t2.platform
					,t2.promo_mechanics
					,t2.np_gift_price_amt
					,t2.start_dt
					,t2.end_dt
				from casuser.promo_pbo_enh as t1
				inner join casuser.promo_enh as t2
				on
					t1.promo_id = t2.promo_id
			;	
		quit;
	%end;
	%include '/opt/sas/mcd_config/macro/step/pt/gc_ml_fcst_frantsev/count_encoder.sas';
	
	%let promo_type_features = %str(promo_id,promo_group_id,platform,promo_mechanics);
	%count_encoder(inp_data = casuser.promo_ml, 
				target_var_list = &promo_type_features., 
				group_by_list = %str(pbo_location_id,channel_cd),
				outp_data = casuser.&tb_out.);

	proc casutil;
		promote incaslib='casuser' casdata="&tb_out." outcaslib="&lib_out." casout="&tb_out";
	run;

	data casuser.&tb_out._FL;
		format feature_nm $40. feature_type $10. use 2.;
		feature_nm='CNT_ENC_PLATFORM'; feature_type = 'num'; use=1;output;
		feature_nm='CNT_ENC_PROMO_GROUP_ID'; feature_type = 'num'; use=1;output;
		feature_nm='CNT_ENC_PROMO_ID'; feature_type = 'num'; use=1;output;
		feature_nm='CNT_ENC_PROMO_MECHANICS'; feature_type = 'num'; use=1;output;
		feature_nm='FREQ_ENC_PLATFORM'; feature_type = 'num'; use=1;output;
		feature_nm='FREQ_ENC_PROMO_GROUP_ID'; feature_type = 'num'; use=1;output;
		feature_nm='FREQ_ENC_PROMO_ID'; feature_type = 'num'; use=1;output;
		feature_nm='FREQ_ENC_PROMO_MECHANICS'; feature_type = 'num'; use=1;output;


	run;
	proc casutil;
		promote incaslib="casuser" casdata="&tb_out._FL" 
					outcaslib="&lib_out." casout="&tb_out._FL";
	quit;
%mend;