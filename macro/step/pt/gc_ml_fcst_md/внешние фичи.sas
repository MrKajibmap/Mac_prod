proc contents data = max_casl.promo_product_features;
quit;


proc fedsql sessref=casauto;
	create table casuser.DM_GC_ML7{options replace=true} as
	select abt.*
/* 		,y1.MAX_CNT_ENC_A_OFFER_TYPE */
/* 		,y1.MAX_CNT_ENC_PRODUCT_ID */
/* 		,y1.MAX_CNT_ENC_PROD_LVL2_ID */
/* 		,y1.MAX_CNT_ENC_PROD_LVL3_ID */
/* 		,y1.MAX_CNT_ENC_PROD_LVL4_ID */
/* 		,y1.MEAN_CNT_ENC_A_OFFER_TYPE */
/* 		,y1.MEAN_CNT_ENC_PRODUCT_ID */
/* 		,y1.MEAN_CNT_ENC_PROD_LVL2_ID */
/* 		,y1.MEAN_CNT_ENC_PROD_LVL3_ID */
/* 		,y1.MEAN_CNT_ENC_PROD_LVL4_ID */
/* 		,y1.MIN_CNT_ENC_A_OFFER_TYPE */
/* 		,y1.MIN_CNT_ENC_PRODUCT_ID */
/* 		,y1.MIN_CNT_ENC_PROD_LVL2_ID */
/* 		,y1.MIN_CNT_ENC_PROD_LVL3_ID */
/* 		,y1.MIN_CNT_ENC_PROD_LVL4_ID */

/* 		,y2.CNT_ENC_PLATFORM */
/* 		,y2.CNT_ENC_PROMO_GROUP_ID */
/* 		,y2.CNT_ENC_PROMO_ID */
/* 		,y2.CNT_ENC_PROMO_MECHANICS */
		, y3.LOG_TRP_BK
		, y3.LOG_TRP_KFC
		, y3.LOG_TRP_MCD
		
		, y3.TRP_BK_TO_MCD
		, y3.TRP_KFC_TO_MCD
		, y3.TRP_MCD
	from 	 casuser.DM_GC_ML6 as abt
/* 	left join max_casl.promo_product_features as y1 */
/* 				on abt.pbo_location_id = y1.pbo_location_id */
/* 				and abt.channel_cd = y1.channel_cd */
/* 				and abt.sales_dt = y1.sAles_dt */

/* 	left join max_casl.promo_type_features as y2 */
/* 			on abt.pbo_location_id = y2.pbo_location_id */
/* 				and abt.channel_cd = y2.channel_cd */
/* 				and abt.sales_dt = y2.sales_dt */

	left join MAX_CASL.PROMO_MEDIA_FEATURES as y3
			on abt.sales_dt = y3.sales_dt
;quit;

proc casutil;
		droptable casdata="GC_ML_TRAIN_240921" incaslib="max_casl" quiet;
	run;
	proc casutil;
		promote incaslib='casuser' 
			casdata='DM_GC_ML7' 
			outcaslib="MAX_CASL" 
			casout="GC_ML_TRAIN_240921";
/* 		save incaslib='casuser' casdata='gc_ml3' outcaslib="&outp_lib." casout="&outp_dm_nm."; */
	run;