%macro init_ml_features;
	%put Initializing feature list for ML model;
	%global mv_nominal_feature_list mv_interval_feature_list;

	%let mv_nominal_feature_list = 
		covid_lockdown
		covid_level
		pbo_loc_lvl2
		pbo_loc_lvl3
		pbo_loc_delivery_cat
		pbo_loc_breakfast_cat
		pbo_loc_building_cat
		pbo_loc_mccafe_cat
		pbo_loc_drivethru_cat
		pbo_loc_window_cat
		promo_flg

	;

	%let mv_interval_feature_list = 
/* 		sum_trp_log */
		covid_pattern
		lag_halfyear_avg
		lag_halfyear_med
		lag_month_avg
		lag_month_med
		lag_qtr_avg
		lag_qtr_med
		lag_week_avg
		lag_week_med
		lag_year_avg
		lag_year_med
		lag_halfyear_std
		lag_month_std
		lag_qtr_std
		lag_week_std
		lag_year_std
		lag_halfyear_pct10		 
		lag_halfyear_pct90		 
		lag_month_pct10
		lag_month_pct90
		lag_qtr_pct10	
		lag_qtr_pct90	
		lag_week_pct10	
		lag_week_pct90	
		lag_year_pct10	
		lag_year_pct90

		temperature
		temp_week_avg
		temp_week_std
		temp_month_avg
		temp_month_std

		precipitation
		prec_week_avg
		prec_week_std
		prec_month_avg
		prec_month_std

		promo_cnt_all_id
		promo_cnt_dist_id
		promo_cnt_dist_group_id
		promo_cnt_dist_platf
/* ! 		promo_cnt_dist_mech */
/* ! 		promo_cnt_pt */
		promo_min_gift_price
		promo_max_gift_price
		promo_avg_gift_price
		promo_min_gift_price_w
		promo_max_gift_price_w
		promo_avg_gift_price_w
/*  */
		PM_bogo
		PM_discount
		PM_evm_set
		PM_no_mech
		PM_non_product_gift
		PM_other_digital
		PM_pairs
		PM_product_gift
		PM_support
/*  */
/*  */
		PRICE_REG_NET_AVG
		PRICE_REG_NET_MAX
		PRICE_REG_NET_MIN

		PRICE_REG_NET_AVG_w
		PRICE_REG_NET_MAX_w
		PRICE_REG_NET_MIN_w

		DISCOUNT_NET_PCT_AVG_P
		DISCOUNT_NET_PCT_MAX_P
		DISCOUNT_NET_PCT_MIN_P
		DISCOUNT_NET_RUR_AVG_P
		DISCOUNT_NET_RUR_MAX_P
		DISCOUNT_NET_RUR_MIN_P
		PRICE_PROMO_NET_AVG_P
		PRICE_PROMO_NET_MAX_P
		PRICE_PROMO_NET_MIN_P
		PRICE_REG_NET_AVG_P
		PRICE_REG_NET_MAX_P
		PRICE_REG_NET_MIN_P

		DISCOUNT_NET_PCT_AVG_P_w
		DISCOUNT_NET_PCT_MAX_P_w
		DISCOUNT_NET_PCT_MIN_P_w
		DISCOUNT_NET_RUR_AVG_P_w
		DISCOUNT_NET_RUR_MAX_P_w
		DISCOUNT_NET_RUR_MIN_P_w
		PRICE_PROMO_NET_AVG_P_w
		PRICE_PROMO_NET_MAX_P_w
		PRICE_PROMO_NET_MIN_P_w
		PRICE_REG_NET_AVG_P_w
		PRICE_REG_NET_MAX_P_w
		PRICE_REG_NET_MIN_P_w

		MD_lag_7_avg
		MD_lag_7_med
	
/* 		POP */
/* 		POP_P */
/*  */
/* 		MAX_CNT_ENC_A_OFFER_TYPE */
/* 		MAX_CNT_ENC_PRODUCT_ID */
/* 		MAX_CNT_ENC_PROD_LVL2_ID */
/* 		MAX_CNT_ENC_PROD_LVL3_ID */
/* 		MAX_CNT_ENC_PROD_LVL4_ID */
/* 		MEAN_CNT_ENC_A_OFFER_TYPE */
/* 		MEAN_CNT_ENC_PRODUCT_ID */
/* 		MEAN_CNT_ENC_PROD_LVL2_ID */
/* 		MEAN_CNT_ENC_PROD_LVL3_ID */
/* 		MEAN_CNT_ENC_PROD_LVL4_ID */
/* 		MIN_CNT_ENC_A_OFFER_TYPE */
/* 		MIN_CNT_ENC_PRODUCT_ID */
/* 		MIN_CNT_ENC_PROD_LVL2_ID */
/* 		MIN_CNT_ENC_PROD_LVL3_ID */
/* 		MIN_CNT_ENC_PROD_LVL4_ID	 */
/*  */
/* 		target_p10 */
/* 		target_p90	 */
/* 	 */
/*! 		CNT_ENC_PLATFORM */
/* !		CNT_ENC_PROMO_GROUP_ID */
/* 	!	CNT_ENC_PROMO_ID */
/* 	!	CNT_ENC_PROMO_MECHANICS */
		LOG_TRP_BK
		LOG_TRP_KFC
		LOG_TRP_MCD
		
		TRP_BK_TO_MCD
		TRP_KFC_TO_MCD
		TRP_MCD


/* 	!	CNT_ENC_PLATFORM */
/* 	!	CNT_ENC_PROMO_GROUP_ID */
/* 	!	CNT_ENC_PROMO_ID */
/* 	!	CNT_ENC_PROMO_MECHANICS */


;

%mend init_ml_features;

%init_ml_features;
