%macro rtp_full_process;
	
	%tech_redirect_log(mpMode=START, mpJobName=rtp_load_data_to_caslib, mpArea=Main);
	%tech_log_event(mpMode=START, mpProcess_Nm=rtp_load_data_to_caslib);
		%rtp_load_data_to_caslib(mpWorkCaslib=mn_short);
	%tech_redirect_log(mpMode=END, mpJobName=rtp_load_data_to_caslib, mpArea=Main);
	%tech_log_event(mpMode=END, mpProcess_Nm=rtp_load_data_to_caslib);
	
	%tech_redirect_log(mpMode=START, mpJobName=rtp_1_load_data_product, mpArea=Main);
	%tech_log_event(mpMode=START, mpProcess_Nm=rtp_1_load_data_product);
	/*	%rtp_1_load_data_product(mpMode=A,
					 mpOutTrain=mn_short.all_ml_train,
					 mpOutScore=mn_short.all_ml_scoring,
					 mpWorkCaslib=mn_short);  */
		%rtp_1_load_data_product(
					  mpMode		= A
					, mpRetroLaunch	= N
					, mpOutTrain	= mn_short.all_ml_train
					, mpOutScore	= mn_short.all_ml_scoring
					, mpWorkCaslib	= mn_short
				);			 
					 
					 
	%tech_redirect_log(mpMode=END, mpJobName=rtp_1_load_data_product, mpArea=Main);
	%tech_log_event(mpMode=END, mpProcess_Nm=rtp_1_load_data_product);						

	/*
	%tech_redirect_log(mpMode=START, mpJobName=rtp_4_modeling_train_pmix, mpArea=Main);
	%tech_log_event(mpMode=START, mpProcess_Nm=rtp_4_modeling_train_pmix);
	%rtp_4_modeling(mode=TRAIN,
						external=1,
						ids = product_id pbo_location_id sales_dt,
						target=sum_qty,
						categories=lvl2_id prod_lvl2_id, 
						external_modeltable=/data/files/input/PMIX_MODEL_TABLE.csv, 
						modeltable=PMIX_MODEL_TABLE,				
						traintable=mn_short.all_ml_train,
						scoretable=mn_short.all_ml_scoring,
						resulttable=mn_short.pmix_days_result, 
						default_params=seed=12345 loh=0 binmethod=QUANTILE maxbranch=2 assignmissing=useinsearch minuseinsearch=5 ntrees=10 maxdepth=10 inbagfraction=0.6 minleafsize=5 numbin=50 printtarget,				
						default_interval=GROSS_PRICE_AMT lag_month_avg lag_month_med lag_qtr_avg lag_qtr_med lag_week_avg lag_week_med lag_month_std lag_qtr_std lag_week_std lag_month_pct10 lag_month_pct90 lag_qtr_pct10 lag_qtr_pct90 lag_week_pct10 lag_week_pct90 PRICE_RANK PRICE_INDEX,				
						default_nominal=OTHER_PROMO SUPPORT BOGO DISCOUNT EVM_SET NON_PRODUCT_GIFT PAIRS PRODUCT_GIFT SIDE_PROMO_FLAG HERO ITEM_SIZE OFFER_TYPE PRICE_TIER AGREEMENT_TYPE BREAKFAST BUILDING_TYPE COMPANY DELIVERY DRIVE_THRU MCCAFE_TYPE PRICE_LEVEL WINDOW_TYPE week weekday month weekend_flag DEFENDER_DAY FEMALE_DAY MAY_HOLIDAY NEW_YEAR RUSSIA_DAY SCHOOL_START STUDENT_DAY SUMMER_START VALENTINE_DAY,
					model_prefix=FOREST);	
	%tech_redirect_log(mpMode=END, mpJobName=rtp_4_modeling_train_pmix, mpArea=Main);
	%tech_log_event(mpMode=END, mpProcess_Nm=rtp_4_modeling_train_pmix);
	*/		
	
	%tech_redirect_log(mpMode=START, mpJobName=rtp_4_modeling_pmix, mpArea=Main);
	%tech_log_event(mpMode=START, mpProcess_Nm=rtp_4_modeling_pmix);
		%rtp_4_modeling(mode=SCORE,
					external=1,
					ids = product_id pbo_location_id sales_dt,
					target=sum_qty,
					categories=lvl2_id prod_lvl2_id, 
					external_modeltable=/data/files/input/PMIX_MODEL_TABLE.csv, 
					modeltable=PMIX_MODEL_TABLE,				
					traintable=mn_short.all_ml_train,
					scoretable=mn_short.all_ml_scoring,
					resulttable=mn_short.pmix_days_result, 
					default_params=seed=12345 loh=0 binmethod=QUANTILE maxbranch=2 assignmissing=useinsearch minuseinsearch=5 ntrees=10 maxdepth=10 inbagfraction=0.6 minleafsize=5 numbin=50 printtarget,				
					default_interval=GROSS_PRICE_AMT lag_month_avg lag_month_med lag_qtr_avg lag_qtr_med lag_week_avg lag_week_med lag_month_std lag_qtr_std lag_week_std lag_month_pct10 lag_month_pct90 lag_qtr_pct10 lag_qtr_pct90 lag_week_pct10 lag_week_pct90 PRICE_RANK PRICE_INDEX,				
					default_nominal=OTHER_PROMO SUPPORT BOGO DISCOUNT EVM_SET NON_PRODUCT_GIFT PAIRS PRODUCT_GIFT SIDE_PROMO_FLAG HERO ITEM_SIZE OFFER_TYPE PRICE_TIER AGREEMENT_TYPE BREAKFAST BUILDING_TYPE COMPANY DELIVERY DRIVE_THRU MCCAFE_TYPE PRICE_LEVEL WINDOW_TYPE week weekday month weekend_flag DEFENDER_DAY FEMALE_DAY MAY_HOLIDAY NEW_YEAR RUSSIA_DAY SCHOOL_START STUDENT_DAY SUMMER_START VALENTINE_DAY,
				model_prefix=FOREST);	
	%tech_redirect_log(mpMode=END, mpJobName=rtp_4_modeling_pmix, mpArea=Main);
	%tech_log_event(mpMode=END, mpProcess_Nm=rtp_4_modeling_pmix);

	/* Обратная интеграция + ПЛМ */
	%tech_redirect_log(mpMode=START, mpJobName=rtp_7_out_integration, mpArea=Main);
	%tech_log_event(mpMode=START, mpProcess_Nm=rtp_7_out_integration);
		%rtp_7_out_integration(mpVfPmixProjName=&VF_PMIX_PROJ_NM.,
									mpVfPboProjName=&VF_PBO_PROJ_NM.,
									mpMLPmixTabName=mn_short.pmix_days_result,
									mpInEventsMkup=mn_long.events_mkup,
									mpInWpGc=mn_dict.wp_gc,
									mpOutPmixLt=mn_short.plan_pmix_month,
									mpOutGcLt=mn_short.plan_gc_month, 
									mpOutUptLt=mn_short.plan_upt_month, 
									mpOutPmixSt=mn_short.plan_pmix_day,
									mpOutGcSt=mn_short.plan_gc_day, 
									mpOutUptSt=mn_short.plan_upt_day, 
									mpOutOutforgc=mn_short.TS_OUTFORGC,
									mpOutOutfor=mn_short.TS_OUTFOR, 
									mpOutNnetWp=mn_dict.nnet_wp1,
									mpPrmt=Y,
									mpInLibref=mn_short,
									mpAuth = NO);
	%tech_redirect_log(mpMode=END, mpJobName=rtp_7_out_integration, mpArea=Main);
	%tech_log_event(mpMode=END, mpProcess_Nm=rtp_7_out_integration);
	
	%tech_redirect_log(mpMode=START, mpJobName=rtp_komp_sep, mpArea=Main);
	%tech_log_event(mpMode=START, mpProcess_Nm=rtp_komp_sep);
		%rtp_komp_sep(mpInPmixLt=mn_short.plan_pmix_month,
						mpInGcLt=mn_short.plan_gc_month, 
						mpInUptLt=mn_short.plan_upt_month, 
						mpInPmixSt=mn_short.plan_pmix_day,
						mpInGcSt=mn_short.plan_gc_day, 
						mpInUptSt=mn_short.plan_upt_day, 
						mpPathOut=/data/files/output/dp_files/);
	%tech_redirect_log(mpMode=END, mpJobName=rtp_komp_sep, mpArea=Main);
	%tech_log_event(mpMode=END, mpProcess_Nm=rtp_komp_sep);
	
%mend rtp_full_process;