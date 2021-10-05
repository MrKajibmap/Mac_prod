%macro ABT_ML_PROMO_ST1(lib_in,tb_in,lib_out,tb_out);
/* 	%let lib_in=casuser; */
/* 	%let tb_in=gc_ml1; */
	
	proc casutil;
		droptable casdata="&tb_out." incaslib="&lib_out." quiet;
		droptable casdata="&tb_out._FL" incaslib="&lib_out." quiet;
	run;
	

/* 	%include '/opt/sas/mcd_config/macro/step/pt/gc_ml_fcst_frantsev/count_encoder.sas'; */
	
	%if not %sysfunc(exist(casuser.promo_ml)) %then %do;

		%if not %sysfunc(exist(casuser.promo_pbo_enh)) %then %do;
			%add_promotool_marks2(mpOutCaslib=casuser, mpPtCaslib=pt, PromoCalculationRk=);
		%end;
		
		proc casutil;
			droptable incaslib="casuser" casdata="promo_ml" quiet;
		run;
	
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
						on	t1.promo_id = t2.promo_id
			;	
		quit;
	%end;

	proc fedsql sessref=casauto;
			create table casuser.&tb_out. {options replace=true} as
		
			select
				t1.channel_cd
				,t1.pbo_location_id
				,t1.sales_dt
				,count(t2.promo_id) as promo_cnt_all_id
				,count(distinct(t2.promo_id)) as promo_cnt_dist_id
				,count(distinct(t2.promo_group_id)) as promo_cnt_dist_group_id
				,count(distinct(t2.platform)) as promo_cnt_dist_platf
				,count(distinct(t2.promo_mechanics)) as promo_cnt_dist_mech
				
/* 				,sum(t2.from_pt) as promo_cnt_pt */
				,min(t2.np_gift_price_amt) as promo_min_gift_price
				,max(t2.np_gift_price_amt) as promo_max_gift_price
				,mean(t2.np_gift_price_amt) as promo_avg_gift_price

				,min(t2.np_gift_price_amt*t3.q_pct) as promo_min_gift_price_w
				,max(t2.np_gift_price_amt*t3.q_pct) as promo_max_gift_price_w
				,mean(t2.np_gift_price_amt*t3.q_pct) as promo_avg_gift_price_w
				
				,case when count(t2.promo_id) > 0 then 1
					else 0 end as promo_flg
			from &lib_in..&tb_in. as t1
	
			left join casuser.promo_ml as t2 on
				t1.pbo_location_id = t2.pbo_location_id
				and	t1.channel_cd = t2.channel_cd
			
			left join casuser.promo_prod_enh as t4 on
					t2.promo_id = t4.promo_id
	
			left join casuser.Popularity as t3
				on t1.pbo_location_id = t3.pbo_location_id
				and t4.product_id = t3.product_id
	
			where t1.sales_dt between t2.start_dt and t2.end_dt
			group by t1.pbo_location_id
				,t1.channel_cd
				,t1.sales_dt
		;	
	quit;

	proc casutil;
		promote incaslib='casuser' casdata="&tb_out." outcaslib="&lib_out." casout="&tb_out.";
	run;
	
	data casuser.&tb_out._FL;
		format feature_nm $40. feature_type $10. use 2.;
		feature_nm='PROMO_AVG_GIFT_PRICE'; feature_type = 'num'; use=1;output;
		feature_nm='PROMO_AVG_GIFT_PRICE_W'; feature_type = 'num'; use=1;output;
		feature_nm='PROMO_CNT_ALL_ID'; feature_type = 'num'; use=1;output;
		feature_nm='PROMO_CNT_DIST_GROUP_ID'; feature_type = 'num'; use=1;output;
		feature_nm='PROMO_CNT_DIST_ID'; feature_type = 'num'; use=1;output;
		feature_nm='PROMO_CNT_DIST_MECH'; feature_type = 'num'; use=1;output;
		feature_nm='PROMO_CNT_DIST_PLATF'; feature_type = 'num'; use=1;output;
		feature_nm='PROMO_FLG'; feature_type = 'cat'; use=1;output;
		feature_nm='PROMO_MAX_GIFT_PRICE'; feature_type = 'num'; use=1;output;
		feature_nm='PROMO_MAX_GIFT_PRICE_W'; feature_type = 'num'; use=1;output;
		feature_nm='PROMO_MIN_GIFT_PRICE'; feature_type = 'num'; use=1;output;
		feature_nm='PROMO_MIN_GIFT_PRICE_W'; feature_type = 'num'; use=1;output;


	run;
	proc casutil;
		promote incaslib="casuser" casdata="&tb_out._FL" 
					outcaslib="&lib_out." casout="&tb_out._FL";
	quit;

%mend;

