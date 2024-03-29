/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для обучения моделей PMIX
*	
*
*  ПАРАМЕТРЫ:
*     Нет
*
******************************************************************
*  Использует: 
*	  нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %rtp004_train_pmix;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp004_train_pmix;
	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_log_event(mpMode=START, mpProcess_Nm=rtp_4_modeling_train_pmix);					
	
	%tech_update_resource_status(mpStatus=P, mpResource=rtp_load_data);
	/* 1. загрузка данных в CAS */
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
	
	%tech_update_resource_status(mpStatus=L, mpResource=rtp_load_data);
	%tech_open_resource(mpResource=rtp_train_pmix);
	
	%tech_log_event(mpMode=END, mpProcess_Nm=rtp_4_modeling_train_pmix);		
	
%mend rtp004_train_pmix;