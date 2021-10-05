/* %include "/opt/sas/mcd_config/macro/step/pt/gc_ml_fcst_frantsev/count_encoder.sas"; */
%let POP_MACRO_PATH = /opt/sas/mcd_config/macro/step/pt/gc_ml_fcst_md/Popularity.sas;
%let ABT_SCRIPT_PATH = /opt/sas/mcd_config/macro/step/pt/gc_ml_fcst_md/ML_ABT;

options casdatalimit=20G;

%macro assign;
	%let casauto_ok = %sysfunc(SESSFOUND ( casauto)) ;
	%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
	 cas casauto SESSOPTS=(TIMEOUT=31536000);
	 caslib _all_ assign;
	%end;
%mend;
%assign

%if not %sysfunc(exist(casuser.popularity)) %then %do;
	%include "&POP_MACRO_PATH";
	%Popularity;
%end;

/* входная таблица */
%let inp_dm = casuser.TRAIN_ABT_TRP_GC_MP;

/* Флаги пересчета фичей*/
%let recalc_pct_norm = 1; 	/* не выключать, нормировка на квантили */
%let recalc_pbo = 1; 		/* фильтрация по актуальным ПБО, +-3 дня тут */
%let recalc_lags = 1; 		/* расчет лагов */
%let recalc_tempr = 1; 		/* температура */
%let recalc_perc = 1; 		/* осадки */
%let recalc_prices = 1;		/* ценовые фичи, на выходе 2 таблицы! */
%let recalc_promo_st1 = 1;	/* простые промо-фичи */
%let recalc_promo_st2 = 0; 	/* выключено */
%let recalc_promo_mech = 1;	/* промо-механики */
%let recalc_pbo_cat = 1;	/* простые ПБО-фичи (категориальные)*/

/* Флаги добавления в итоговую витрину */
%let add_pbo = 0; /* приходят из первоначальной витрины */
%let add_lags = 1;
%let add_tempr = 1;
%let add_perc = 1;
%let add_prices = 1;
%let add_promo_st1 = 1;
%let add_promo_st2 = 0; /* проблемы с уникальностью */
%let add_promo_mech = 1;
%let add_pbo_cat = 1;


/* Загружаем таблицу с временными закрытиями */
%if &recalc_pbo = 1 %then %do;
	%include "&ABT_SCRIPT_PATH./ABT_ML_clear_PBO.sas";
	%ABT_ML_clear_PBO(&inp_dm., casuser , gc_ml0); 
%end;

/* номировка на 10%-90% квантильный размах */
%if &recalc_pct_norm = 1 %then %do;
	%include "&ABT_SCRIPT_PATH./ABT_ML_PCT_NORM.sas";
	%ABT_ML_PCT_NORM(casuser, gc_ml0,casuser, gc_ml1);
%end;

/* считаем лаги */
%if &recalc_lags = 1 %then %do;
	%include "&ABT_SCRIPT_PATH./ABT_ML_LAGS_MEAN_AVG.sas";
	%ABT_ML_LAGS_MEAN_AVG(casuser, gc_ml1, casuser, f_lag_abt1);
	
	%include "&ABT_SCRIPT_PATH./ABT_ML_LAGS_STD.sas";
	%ABT_ML_LAGS_STD(casuser, gc_ml1, casuser, f_lag_abt2);
	
	%include "&ABT_SCRIPT_PATH./ABT_ML_LAGS_PCT.sas";
	%ABT_ML_LAGS_PCT(casuser, gc_ml1, casuser, f_lag_abt3);
%end;

/* температура */
%if &recalc_tempr = 1 %then %do;
	%include "&ABT_SCRIPT_PATH./ABT_ML_TEMPERATURE.sas";
	%ABT_ML_TEMPERATURE(casuser, f_tempr);
%end;

/* осадки */
%if &recalc_perc = 1 %then %do;
	%include "&ABT_SCRIPT_PATH./ABT_ML_PERC.sas";
	%ABT_ML_PERC(casuser, f_perc);
%end;

/* простые промо-фичи */
%if &recalc_promo_st1 = 1 %then %do;
	%include "&ABT_SCRIPT_PATH./ABT_ML_PROMO_ST1.sas";
	%ABT_ML_PROMO_ST1(casuser, gc_ml1, casuser, f_promo_st1);
%end;

/* CE промо-фичи */
%if &recalc_promo_st2 = 1 %then %do;
	%include "&ABT_SCRIPT_PATH./ABT_ML_PROMO_ST2.sas";
	%ABT_ML_PROMO_ST2(casuser, gc_ml1, casuser, f_promo_st2);
%end;

/* промо-механики */
%if &recalc_promo_mech = 1 %then %do;
	%include "&ABT_SCRIPT_PATH./ABT_ML_PROMO_MECH.sas";
	%ABT_ML_PROMO_MECH(casuser, gc_ml1, casuser, f_promo_mech)
%end;

/* Цены */
%if &recalc_prices = 1 %then %do;
	%include "&ABT_SCRIPT_PATH./ABT_ML_price_features.sas";
	%ABT_ML_price_features(casuser, F_ALL_PRC, F_PROMO_PRC);
%end;

/* Категориальные ПБО фичи */
%if &recalc_pbo_cat = 1 %then %do;
	%include "&ABT_SCRIPT_PATH./ABT_ML_PBO_CAT.sas";
	%ABT_ML_PBO_CAT(casuser, F_PBO_CAT);
%end;



/* ============================================= */
/* ======  Формируем итоговую витрину   ======== */
/* ============================================= */
%include "&ABT_SCRIPT_PATH./ABT_ML_ADD_COLUMNS.sas";

%if &add_lags = 1 %then %do;
	%ABT_ML_ADD_COLUMNS(tb_main = gc_ml1, tb_add = f_lag_abt1, tb_out = ABT_ML);
	%ABT_ML_ADD_COLUMNS(tb_main = ABT_ML, tb_add = f_lag_abt2, tb_out = ABT_ML);
	%ABT_ML_ADD_COLUMNS(tb_main = ABT_ML, tb_add = f_lag_abt3, tb_out = ABT_ML);
%end;
%if &add_tempr = 1 %then %do;
	%ABT_ML_ADD_COLUMNS(tb_main = ABT_ML, tb_add = F_TEMPR, tb_out = ABT_ML);
%end;
%if &add_perc = 1 %then %do;
	%ABT_ML_ADD_COLUMNS(tb_main = ABT_ML, tb_add = F_PERC, tb_out = ABT_ML);
%end;
%if &add_prices = 1 %then %do;
	%ABT_ML_ADD_COLUMNS(tb_main = ABT_ML, tb_add = F_PROMO_PRC, tb_out = ABT_ML);
%end;
%if &add_promo_st1 = 1 %then %do;
	%ABT_ML_ADD_COLUMNS(tb_main = ABT_ML, tb_add = f_promo_st1, tb_out = ABT_ML);
%end;
%if &add_promo_st2 = 1 %then %do;
	%ABT_ML_ADD_COLUMNS(tb_main = ABT_ML, tb_add = f_promo_st2, tb_out = ABT_ML);
%end;
%if &add_promo_mech = 1 %then %do;
	%ABT_ML_ADD_COLUMNS(tb_main = ABT_ML, tb_add = f_promo_mech, tb_out = ABT_ML);
%end;
%if &add_pbo_cat = 1 %then %do;
	%ABT_ML_ADD_COLUMNS(tb_main = ABT_ML, tb_add = F_PBO_CAT, tb_out = ABT_ML);
%end;
%if &add_pbo = 1 %then %do;
	%ABT_ML_ADD_COLUMNS(tb_main = gc_ml1, tb_add = gc_ml0, tb_out = ABT_ML);
%end;

/* костыли */
/* 1 */
/* ПБО-фичи */
data casuser.F_PBO_TYPE_CNT;
	set max_casl.pbo_type_features;
run;
data casuser.F_PBO_TYPE_CNT_FL;
	format feature_nm $40. feature_type $10. use 2.;
	feature_nm='CNT_ENC_AGREEMENT_TYPE'; feature_type = 'cat'; use=0;output;
	feature_nm='CNT_ENC_BREAKFAST'; feature_type = 'cat'; use=1;output;
	feature_nm='CNT_ENC_BUILDING_TYPE'; feature_type = 'cat'; use=1;output;
	feature_nm='CNT_ENC_COMPANY'; feature_type = 'cat'; use=0;output;
	feature_nm='CNT_ENC_DELIVERY'; feature_type = 'cat'; use=1;output;
	feature_nm='CNT_ENC_DRIVE_THRU'; feature_type = 'cat'; use=1;output;
	feature_nm='CNT_ENC_LVL2_ID'; feature_type = 'cat'; use=1;output;
	feature_nm='CNT_ENC_LVL3_ID'; feature_type = 'cat'; use=1;output;
	feature_nm='CNT_ENC_MCCAFE_TYPE'; feature_type = 'num'; use=1;output;
	feature_nm='CNT_ENC_PRICE_LEVEL'; feature_type = 'num'; use=0;output;
	feature_nm='CNT_ENC_WINDOW_TYPE'; feature_type = 'num'; use=1;output;
	feature_nm='FREQ_ENC_AGREEMENT_TYPE'; feature_type = 'num'; use=0;output;
	feature_nm='FREQ_ENC_BREAKFAST'; feature_type = 'num'; use=0;output;
	feature_nm='FREQ_ENC_BUILDING_TYPE'; feature_type = 'num'; use=0;output;
	feature_nm='FREQ_ENC_COMPANY'; feature_type = 'num'; use=0;output;
	feature_nm='FREQ_ENC_DELIVERY'; feature_type = 'num'; use=0;output;
	feature_nm='FREQ_ENC_DRIVE_THRU'; feature_type = 'num'; use=0;output;
	feature_nm='FREQ_ENC_LVL2_ID'; feature_type = 'num'; use=0;output;
	feature_nm='FREQ_ENC_LVL3_ID'; feature_type = 'num'; use=0;output;
	feature_nm='FREQ_ENC_MCCAFE_TYPE'; feature_type = 'num'; use=0;output;
	feature_nm='FREQ_ENC_PRICE_LEVEL'; feature_type = 'num'; use=0;output;
	feature_nm='FREQ_ENC_WINDOW_TYPE'; feature_type = 'num'; use=0;output;
run;
%ABT_ML_ADD_COLUMNS(tb_main = ABT_ML, tb_add = F_PBO_TYPE_CNT, tb_out = ABT_ML);

/* 2 */
/* ТРП */
data casuser.F_PROMO_MEDIA;
	set MAX_CASL.PROMO_MEDIA_FEATURES;
run;
data casuser.F_PROMO_MEDIA_FL;
	format feature_nm $40. feature_type $10. use 2.;
	feature_nm='LOG_TRP_BK'; feature_type = 'num'; use=1;output;
	feature_nm='LOG_TRP_KFC'; feature_type = 'num'; use=1;output;
	feature_nm='LOG_TRP_MCD'; feature_type = 'num'; use=1;output;
	feature_nm='TRP_BK_TO_MCD'; feature_type = 'num'; use=0;output;
	feature_nm='TRP_KFC_TO_MCD'; feature_type = 'num'; use=0;output;
	feature_nm='TRP_MCD'; feature_type = 'num'; use=0;output;
run;
%ABT_ML_ADD_COLUMNS(tb_main = ABT_ML, tb_add = F_PROMO_MEDIA, tb_out = ABT_ML);

/* 3 */
/* продукты в промо */
data casuser.F_promo_product;
	set MAX_CASL.promo_product_features;
run;
data casuser.F_promo_product_FL;
	format feature_nm $40. feature_type $10. use 2.;
	feature_nm='MEAN_CNT_ENC_PROD_LVL2_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MEAN_CNT_ENC_PROD_LVL3_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MEAN_CNT_ENC_PROD_LVL4_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MEAN_FREQ_ENC_A_OFFER_TYPE'; feature_type = 'num'; use=1;output;
	feature_nm='MEAN_FREQ_ENC_PRODUCT_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MEAN_FREQ_ENC_PROD_LVL2_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MEAN_FREQ_ENC_PROD_LVL3_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MEAN_FREQ_ENC_PROD_LVL4_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MIN_CNT_ENC_A_OFFER_TYPE'; feature_type = 'num'; use=1;output;
	feature_nm='MIN_CNT_ENC_PRODUCT_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MIN_CNT_ENC_PROD_LVL2_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MIN_CNT_ENC_PROD_LVL3_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MIN_CNT_ENC_PROD_LVL4_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MIN_FREQ_ENC_A_OFFER_TYPE'; feature_type = 'num'; use=1;output;
	feature_nm='MIN_FREQ_ENC_PRODUCT_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MIN_FREQ_ENC_PROD_LVL2_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MIN_FREQ_ENC_PROD_LVL3_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MIN_FREQ_ENC_PROD_LVL4_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MAX_CNT_ENC_A_OFFER_TYPE'; feature_type = 'num'; use=1;output;
	feature_nm='MAX_CNT_ENC_PRODUCT_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MAX_CNT_ENC_PROD_LVL2_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MAX_CNT_ENC_PROD_LVL3_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MAX_CNT_ENC_PROD_LVL4_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MAX_FREQ_ENC_A_OFFER_TYPE'; feature_type = 'num'; use=1;output;
	feature_nm='MAX_FREQ_ENC_PRODUCT_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MAX_FREQ_ENC_PROD_LVL2_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MAX_FREQ_ENC_PROD_LVL3_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MAX_FREQ_ENC_PROD_LVL4_ID'; feature_type = 'num'; use=1;output;
	feature_nm='MEAN_CNT_ENC_A_OFFER_TYPE'; feature_type = 'num'; use=1;output;
	feature_nm='MEAN_CNT_ENC_PRODUCT_ID'; feature_type = 'num'; use=1;output;
run;
%ABT_ML_ADD_COLUMNS(tb_main = ABT_ML, tb_add = F_promo_product, tb_out = ABT_ML);

proc casutil;
/* 		droptable casdata="&tb_out." incaslib="&lib_out." quiet; */
		promote incaslib="casuser" 	casdata="ABT_ML"
			outcaslib="casuser" casout="ABT_ML";
	run;