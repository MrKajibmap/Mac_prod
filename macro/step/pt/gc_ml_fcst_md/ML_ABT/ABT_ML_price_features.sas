%macro ABT_ML_price_features(lib_out, tb1_out, tb2_out);	
	%let lmvInPricesTb = price_full_sku_pbo_day;
	%let lmvInPricesLib = MN_DICT;
	/* 	таблица жирная, в кас поднимаем на время */
	proc casutil;
	    droptable casdata="&lmvInPricesTb." incaslib="&lmvInPricesLib." quiet;
		droptable casdata="all_prices" incaslib="casuser" quiet;
		droptable casdata="promo_prices" incaslib="casuser" quiet;	
		droptable casdata="&tb1_out." incaslib="&lib_out." quiet;
		droptable casdata="&tb1_out._FL" incaslib="&lib_out." quiet;
		droptable casdata="&tb2_out." incaslib="&lib_out." quiet;
		droptable casdata="&tb2_out._FL" incaslib="&lib_out." quiet;
	    load casdata="&lmvInPricesTb..sashdat" incaslib="&lmvInPricesLib." 
			casout="&lmvInPricesTb." outcaslib="&lmvInPricesLib.";
	quit;
		
	/* 	цены в тотале на магазин */
	
	proc fedsql sessref=casauto;
		create table casuser.all_prices {options replace=true} as
		select 
			t1.period_dt as sales_dt
			, t1.pbo_location_id
			, min(t1.price_reg_net) as price_reg_net_min
			, max(t1.price_reg_net) as price_reg_net_max
			, mean(t1.price_reg_net) as price_reg_net_avg

			, min(t1.price_reg_net*t2.q_pct) as price_reg_net_min_w
			, max(t1.price_reg_net*t2.q_pct) as price_reg_net_max_w
			, mean(t1.price_reg_net*t2.q_pct) as price_reg_net_avg_w

			, sum(t2.q_pct) as Pop
		from &lmvInPricesLib..&lmvInPricesTb. as t1
		left join casuser.Popularity as t2 
				on t1.pbo_location_id = t2.pbo_location_id
				and t1.product_id = t2.product_id
		group by t1.period_dt, t1.pbo_location_id
	;quit;

	proc casutil;
		promote incaslib='casuser' casdata='all_prices' 
					outcaslib="&lib_out." casout="&tb1_out";
	quit;

	proc fedsql sessref=casauto;
		create table casuser.promo_prices {options replace=true} as
		select 
			t1.period_dt as sales_dt
			, t1.pbo_location_id
			
			, round(min(t1.price_reg_net),0.01) as price_reg_net_min_p
			, round(max(t1.price_reg_net),0.01) as price_reg_net_max_p
			, round(mean(t1.price_reg_net),0.01) as price_reg_net_avg_p

			, round(min(t1.price_promo_net),0.01) as price_promo_net_min_p
			, round(max(t1.price_promo_net),0.01) as price_promo_net_max_p
			, round(mean(t1.price_promo_net),0.01) as price_promo_net_avg_p

			, round(min(t1.discount_net_pct),0.01) as discount_net_pct_min_p
			, round(max(t1.discount_net_pct),0.01) as discount_net_pct_max_p
			, round(mean(t1.discount_net_pct),0.01) as discount_net_pct_avg_p
		
			, round(min(t1.discount_net_rur),0.01) as discount_net_rur_min_p
			, round(max(t1.discount_net_rur),0.01) as discount_net_rur_max_p
			, round(mean(t1.discount_net_rur),0.01) as discount_net_rur_avg_p

			, round(min(t1.price_reg_net*t2.q_pct),0.001) as price_reg_net_min_p_w
			, round(max(t1.price_reg_net*t2.q_pct),0.001) as price_reg_net_max_p_w
			, round(mean(t1.price_reg_net*t2.q_pct),0.01) as price_reg_net_avg_p_w

			, round(min(t1.price_promo_net*t2.q_pct),0.001) as price_promo_net_min_p_w
			, round(max(t1.price_promo_net*t2.q_pct),0.001) as price_promo_net_max_p_w
			, round(mean(t1.price_promo_net*t2.q_pct),0.001) as price_promo_net_avg_p_w

			, round(min(t1.discount_net_pct*t2.q_pct),0.001) as discount_net_pct_min_p_w
			, round(max(t1.discount_net_pct*t2.q_pct),0.001) as discount_net_pct_max_p_w
			, round(mean(t1.discount_net_pct*t2.q_pct),0.001) as discount_net_pct_avg_p_w
		
			, round(min(t1.discount_net_rur*t2.q_pct),0.001) as discount_net_rur_min_p_w
			, round(max(t1.discount_net_rur*t2.q_pct),0.001) as discount_net_rur_max_p_w
			, round(mean(t1.discount_net_rur*t2.q_pct),0.001) as discount_net_rur_avg_p_w

			, sum(t2.q_pct) as Pop_p
		from &lmvInPricesLib..&lmvInPricesTb. as t1
		left join casuser.Popularity as t2 
				on t1.pbo_location_id = t2.pbo_location_id
				and t1.product_id = t2.product_id
		
		where t1.discount_net_pct > 0
		group by t1.period_dt, t1.pbo_location_id
	;quit;
	proc casutil;
		promote incaslib='casuser' casdata='promo_prices' 
					outcaslib="&lib_out." casout="&tb2_out";
	    droptable casdata="&lmvInPricesTb." incaslib="&lmvInPricesLib." quiet;
	quit;

	data casuser.&tb1_out._FL;
		format feature_nm $40. feature_type $10. use 2.;
		feature_nm='POP'; feature_type = 'num'; use=1;output;
		feature_nm='PRICE_REG_NET_AVG'; feature_type = 'num'; use=1;output;
		feature_nm='PRICE_REG_NET_AVG_W'; feature_type = 'num'; use=1;output;
		feature_nm='PRICE_REG_NET_MAX'; feature_type = 'num'; use=1;output;
		feature_nm='PRICE_REG_NET_MAX_W'; feature_type = 'num'; use=1;output;
		feature_nm='PRICE_REG_NET_MIN'; feature_type = 'num'; use=1;output;
		feature_nm='PRICE_REG_NET_MIN_W'; feature_type = 'num'; use=1;output;


	run;
	proc casutil;
		promote incaslib="casuser" casdata="&tb1_out._FL" 
					outcaslib="&lib_out." casout="&tb1_out._FL";
	quit;

	data casuser.&tb2_out._FL;
		format feature_nm $40. feature_type $10. use 2.;
		feature_nm='price_reg_net_min_p'; feature_type = 'num'; use=1;output;
		feature_nm='price_reg_net_max_p'; feature_type = 'num'; use=1;output;
		feature_nm='price_reg_net_avg_p'; feature_type = 'num'; use=1;output;
		feature_nm='price_promo_net_min_p'; feature_type = 'num'; use=1;output;
		feature_nm='price_promo_net_max_p'; feature_type = 'num'; use=1;output;
		feature_nm='price_promo_net_avg_p'; feature_type = 'num'; use=1;output;
		feature_nm='discount_net_pct_min_p'; feature_type = 'num'; use=1;output;
		feature_nm='discount_net_pct_max_p'; feature_type = 'num'; use=1;output;
		feature_nm='discount_net_pct_avg_p'; feature_type = 'num'; use=1;output;
		feature_nm='discount_net_rur_min_p'; feature_type = 'num'; use=1;output;
		feature_nm='discount_net_rur_max_p'; feature_type = 'num'; use=1;output;
		feature_nm='discount_net_rur_avg_p'; feature_type = 'num'; use=1;output;
		feature_nm='price_reg_net_min_p_w'; feature_type = 'num'; use=1;output;
		feature_nm='price_reg_net_max_p_w'; feature_type = 'num'; use=1;output;
		feature_nm='price_reg_net_avg_p_w'; feature_type = 'num'; use=1;output;
		feature_nm='price_promo_net_min_p_w'; feature_type = 'num'; use=1;output;
		feature_nm='price_promo_net_max_p_w'; feature_type = 'num'; use=1;output;
		feature_nm='price_promo_net_avg_p_w'; feature_type = 'num'; use=1;output;
		feature_nm='discount_net_pct_min_p_w'; feature_type = 'num'; use=1;output;
		feature_nm='discount_net_pct_max_p_w'; feature_type = 'num'; use=1;output;
		feature_nm='discount_net_pct_avg_p_w'; feature_type = 'num'; use=1;output;
		feature_nm='discount_net_rur_min_p_w'; feature_type = 'num'; use=1;output;
		feature_nm='discount_net_rur_max_p_w'; feature_type = 'num'; use=1;output;
		feature_nm='discount_net_rur_avg_p_w'; feature_type = 'num'; use=1;output;
		feature_nm='Pop_p'; feature_type = 'num'; use=1;output;
	run;
	proc casutil;
		promote incaslib="casuser" casdata="&tb2_out._FL" 
					outcaslib="&lib_out." casout="&tb2_out._FL";
	quit;
%mend ABT_ML_price_features;