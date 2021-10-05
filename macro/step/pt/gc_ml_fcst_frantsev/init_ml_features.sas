%macro init_ml_features;
	%put Initializing feature list for ML model;
	%global mv_nominal_feature_list mv_interval_feature_list;

	%let mv_nominal_feature_list = 
		covid_lockdown
		covid_level
		pbo_loc_lvl2
		pbo_loc_delivery_cat
		pbo_loc_breakfast_cat
		pbo_loc_building_cat
		pbo_loc_mccafe_cat
		pbo_loc_drivethru_cat
		pbo_loc_window_cat
		promo_flg
	;
/* 		pbo_loc_lvl3 */

	%let mv_interval_feature_list = 
		sum_trp_log
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
		temp_week_avg
		temp_week_std
		temp_month_avg
		temp_month_std
		prec_week_avg
		prec_week_std
		prec_month_avg
		prec_month_std
		promo_cnt_all_id
		promo_cnt_dist_id
		promo_cnt_dist_group_id
		promo_cnt_dist_platf
		promo_cnt_dist_mech
		promo_cnt_pt
		promo_max_gift_price
	;
%mend init_ml_features;

%init_ml_features;
