/* 
	TODO. Выгрузить витрину с прогнозом в Python.
		Посмотреть, что не так, где ошибаемся
*/


%let test_threshold = '30nov2020'd;
/* Будущие промо */
proc sql;
	create table work.past_promo as
		select distinct
			promo_id
		from
			nac.na_abt17
		where
			sales_dt <= &test_threshold.
	;
quit;

/* Тест */
proc sql;
	create table work.test as
		select
			t1.*
		from
			nac.na_abt17 as t1
		left join
			work.past_promo as t2
		on
			t1.promo_id = t2.promo_id
		where
			t2.promo_id is missing
	;
quit;

/* Трейн */
proc sql;
	create table work.train as
		select
			t1.*
		from
			nac.na_abt17 as t1
		inner join
			work.past_promo as t2
		on
			t1.promo_id = t2.promo_id
	;
quit;

proc surveyselect data=work.train out=work.train_sample sampsize=500000;
run;


/* Гиперпараметры моделей */
%let default_hyper_params = seed=12345 loh=0 binmethod=QUANTILE 
	 maxbranch=2 
     assignmissing=useinsearch 
	 minuseinsearch=5
     ntrees=50
     maxdepth=20
     inbagfraction=0.7
     minleafsize=5
     numbin=100
     printtarget
;


data casuser.train_sample;
	set work.train_sample;
run;

%let data = casuser.train_sample;
%let target = n_a;
%let output = test_quality500_ntree10;


/* Стираем результирующие таблицы с обученными моделями */
proc casutil;
	droptable casdata="&output." incaslib="public" quiet;
run;
	
/* Обучение модели */
proc forest data=&data.
	&default_hyper_params.;
	input 
		NUMBER_OF_OPTIONS
		NUMBER_OF_PRODUCTS
		NECESSARY_AMOUNT
		Breakfast
		ColdDrinks
		Condiments
		Desserts
		Fries
		HotDrinks
		McCafe
		Nonproduct
		Nuggets
		SNCORE
		SNEDAP
		SNPREMIUM
		Shakes
		StartersSalad
		UndefinedProductGroup
		ValueMeal
		week
		weekday
		month
		year
		MEAN_RECEIPT_QTY
		STD_RECEIPT_QTY
		mean_sales_qty
		std_sales_qty
		mean_past_n_a
		temperature
		precipitation
			/ level = interval;
	input
		promo_group_id
		promo_mechanics_name
/* 		pbo_location_id */
		AGREEMENT_TYPE_ID
		BREAKFAST_ID
		BUILDING_TYPE_ID
		COMPANY_ID
		DELIVERY_ID
		DRIVE_THRU_ID
		MCCAFE_TYPE_ID
/* 		PRICE_LEVEL_ID */
		WINDOW_TYPE_ID
		regular_weekend_flag
		weekend_flag
/* 		Christmas */
		Christmas_Day
		Day_After_New_Year
/* 		Day_of_Unity */
/* 		Defendence_of_the_Fatherland */
/* 		International_Womens_Day */
/* 		Labour_Day */
/* 		National_Day */
		New_Year_shift
		New_year
/* 		Victory_Day */
		 / level = nominal;
	id promo_id pbo_location_id sales_dt;
	target &target. / level = interval;
	savestate rstore=public.&output.;
	;
run;

proc casutil;
    promote casdata="&output." incaslib="public" outcaslib="public";
run;

data casuser.test;
	set work.test;
run;

proc astore;
	score data=casuser.train_sample
	copyvars=(_all_)
	rstore=public.test_quality100_ntree10
	out=casuser.train_prediction
	;
quit;

proc astore;
	score data=casuser.test
	copyvars=(_all_)
	rstore=public.test_quality500_ntree10
	out=casuser.test_prediction
	;
quit;

options casdatalimit=20G;
data nac.test_prediction;
	set casuser.test_prediction;
run; 

data nac.train_prediction;
	set  casuser.train_prediction;
run;


%let bad_promo = 1506, 1504, 1092, 1116, 1526;
%let bad_pbo = 21034,21069, 21097, 21054, 70331, 21062;

/* Check quality metric */
proc fedsql sessref=casauto;
	select
		mean(divide(abs(p_n_a-n_a), n_a)) as mape_without_filter,
		sum(abs(p_n_a-n_a))/sum(n_a) as wape_without_filter
	from
		casuser.test_prediction
	;

	select
		mean(divide(abs(p_n_a-n_a), n_a)) as mape_with_filter,
		sum(abs(p_n_a-n_a))/sum(n_a) as wape_with_filter
	from
		casuser.test_prediction
	where 
		promo_id not in (&bad_promo.) and
		pbo_location_id not in (&bad_pbo.)
	;

	select
		pbo_location_id,
		mean(divide(abs(p_n_a-n_a), n_a)) as mape,
		sum(abs(p_n_a-n_a))/sum(n_a) as wape
	from
		casuser.test_prediction
	where 
		promo_id not in (&bad_promo.) 
	group by
		pbo_location_id
	order by
		mape
	;
	select
		promo_id,
		mean(divide(abs(p_n_a-n_a), n_a)) as mape,
		sum(abs(p_n_a-n_a))/sum(n_a) as wape
	from
		casuser.test_prediction
	group by
		promo_id
	order by
		WAPE
	;
quit;

proc fedsql sessref=casauto;
	select
		t1.promo_id,
		t2.promo_nm,
		t2.PROMO_MECHANICS,	
		divide(abs(p_n_a-n_a), n_a) as wape
	from (
		select
			promo_id,
			sum(p_n_a) as p_n_a,
			sum(n_a) as n_a
		from
			casuser.test_prediction
		group by
			promo_id
	) as t1
	inner join
		casuser.promo_enh as t2
	on
		t1.promo_id = t2.promo_id
	order by
		divide(abs(p_n_a-n_a), n_a) desc
	;
quit;

/* 6 mape without filter, 3 with filter */



/************************* Тест 2 **************************/
%let PromoCalculationRk = 124;

/*** 1. Инициализация окружения ***/
%include '/opt/sas/mcd_config/config/initialize_global.sas';
options casdatalimit=10G;

libname cheque "/data/backup/"; /* Директория с чеками */
libname nac "/data/MN_CALC"; /* Директория в которую складываем результат */

/* Текущий день */
%let ETL_CURRENT_DT_DB = date %str(%')%sysfunc(putn(%sysfunc(datepart(%sysfunc(datetime()))),yymmdd10.))%str(%');

%macro assign;
	%let casauto_ok = %sysfunc(SESSFOUND ( casauto)) ;
	%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
	 cas casauto SESSOPTS=(TIMEOUT=31536000);
	 caslib _all_ assign;
	%end;
%mend;

%assign


/*** 2. Получение информации из промо тула ***/
*%include '/opt/sas/mcd_config/macro/step/add_promotool_marks2.sas';
%add_promotool_marks2(
	mpOutCaslib=casuser,
	mpPtCaslib=pt,
	PromoCalculationRk=&PromoCalculationRk.
);

proc fedsql sessref=casauto;
	create table casuser.promo_tool_promo{options replace=true} as
		select
			*
		from
			casuser.promo_enh
		where (
			year(start_dt) = year(&ETL_CURRENT_DT_DB) or
			year(end_dt) = year(&ETL_CURRENT_DT_DB) or
			(
				year(start_dt) < year(&ETL_CURRENT_DT_DB) and
				year(end_dt) > year(&ETL_CURRENT_DT_DB)
			)
		) and channel_cd = 'ALL'
	;
quit;
	


%scoring_building(
	promo_lib = casuser, 
	ia_promo = promo_tool_promo,
	ia_promo_x_pbo = promo_pbo_enh,
	ia_promo_x_product = promo_prod_enh,
	ia_media = media_enh,
	calendar_start = '01jan2017'd,
	calendar_end = '01jan2022'd
	)


/* Сэмплируем обучающую выборку */
proc surveyselect data=nac.na_abt17 out=nac.na_train_sample sampsize=500000;
run;

data casuser.na_train_sample;
	set nac.na_train_sample;
run;

%promo_effectiveness_model_fit(
	data = casuser.na_train_sample,
	target = n_a,
	output = na_prediction_model_test,
	hyper_params = &default_hyper_params.
)

%promo_effectivness_predict(
	model = na_prediction_model_test,
	target = na,
	data = casuser.promo_effectivness_scoring
)


