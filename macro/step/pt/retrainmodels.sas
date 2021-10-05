/*
	Внерегламентный процесс.

	1. Инициализация окружения
	2. Получение информации из промо тула [add_promotool_marks2.sas]
		* Выделение уже прошедших промо акций
	3. Рассчет эффективности промо акций на истории
		a. Подсчет из чеков n_a (число срабатываний акций) и
			 t_a(количество чеков с промо) [na_calculation.sas]
		b. Сборка витрины для модели прогнозирования n_a (t_a)
			[promo_effectiveness_abt_building.sas]
		c. Обучение модели для прогнозирования n_a (t_a)
			[promo_effectiveness_model_fitting.sas]
	4. Линейная модель для GC
		a. Сборка витрины [gc_abt_building.sas]
		b. Обучение моделей [gc_model_fitting.sas]
	5. Линейная модель для UPT
		a. Сборка витрины [upt_abt_building.sas]
		b. Обучение моделей [upt_model_fitting.sas]
*/


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
%include '/opt/sas/mcd_config/macro/step/add_promotool_marks2.sas';
%add_promotool_marks2(
	mpOutCaslib=casuser,
	mpPtCaslib=pt
)

/* Выделим только прошедшие промо акции */
proc fedsql sessref=casauto;
	create table casuser.past_promo{options replace=true} as
		select
			*,
			trim(coalesce(cast(promo_txt_id as char(36)), put(PROMO_ID, 12.))) as hybrid_promo_id
		from
			casuser.promo_enh
		where
			channel_cd = 'ALL' and
			start_dt >= date '2019-01-01' and
			end_dt <= &ETL_CURRENT_DT_DB. and
			trim(promo_mechanics) ^= 'Other: Collaboration' and 
			promo_nm not in ( /* акции не обсчитались из-за проблем в данных */
				'2020 30 years Anniversary: : Big Mac @ 3 rubles',
				'2020 McFest: Summer: Gift for subscription 3 (Strips)',
				'2020 : EDAP: Omletter + Cappucino 0,2 @129 rub',
				'TOW Italy: Panini Chicken Pair'
			) and
			promo_nm not in ( /* акции на zr товары, которые не могут быть рассчитаны */
				'2020 TOW: : 2 Beef a-la Rus @279',
				'2020 : : 2 Royals @199',
				'2020 : Royal: 2 Royals @199',
				'2020 : Big Tasty: 2 Big Tasty @ 299',
				'2019 Royal+Royal BOGO',
				'2019 McFest: wave 1 w3- 2Big Mac @ 199'
			)
	;
quit;


/*** 3. Рассчет эффективности промо акций на истории ***/

/* a. Подсчет из чеков n_a и t_a */
%include '/opt/sas/mcd_config/macro/step/pt/na_calculation_dev.sas';
%na_calculation(
	promo_lib = casuser, 
	ia_promo = past_promo,
	ia_promo_x_pbo = promo_pbo_enh,
	ia_promo_x_product = promo_prod_enh,
);

/* b. Сборка витрины для модели прогнозирования n_a и t_a */
%include '/opt/sas/mcd_config/macro/step/pt/promo_effectiveness_abt_building_dev.sas';
%promo_effectiveness_abt_building(
	promo_lib = casuser, 
	ia_promo = past_promo,
	ia_promo_x_pbo = promo_pbo_enh,
	ia_promo_x_product = promo_prod_enh,
	ia_media = media_enh,
	calendar_start = '01jan2017'd,
	calendar_end = '01jan2022'd,
	na_history_table = nac.na_calculation_result_new
);

/* c. Обучение модели для прогнозирования n_a  и t_a */

/* Сэмплируем обучающую выборку */
/* proc surveyselect data=nac.na_abt17 out=nac.na_train_sample sampsize=500000; */
/* run; */

data casuser.na_abt17;
	set nac.na_abt17;
run;

%include '/opt/sas/mcd_config/macro/step/pt/promo_effectiveness_model_fitting.sas';
%promo_effectiveness_model_fit(
	data = casuser.na_abt17,
	target = n_a,
	output = na_prediction_model_test,
	hyper_params = &default_hyper_params.
)

%promo_effectiveness_model_fit(
	data = casuser.na_abt17,
	target = t_a,
	output = ta_prediction_model_test,
	hyper_params = &default_hyper_params.
)


/*** 4. Линейная модель для GC ***/

/* a. Сборка витрины */
%include '/opt/sas/mcd_config/macro/step/pt/gc_abt_building.sas';
%gc_prepare_abt(
	covid_start = '26mar2020'd,
	covid_end = '1jul2020'd,
	history_end = '1apr2021'd,
	num_of_changepoint = 10,
	promo_efficiency_table = na_calculation_result,
	promo_lib = nac,
	output_table = work.gc6
)

/* Нормировка промо и целевой переменной */
%gc_normalize_train(
	train = work.gc6,
	out_train = nac.train_n,
	out_target_max = nac.receipt_qty_max,
	out_promo_max = nac.promo_max
)

/* b. Обучение модели */
%include '/opt/sas/mcd_config/macro/step/pt/gc_model_fitting.sas';
%gc_fit(
	train = nac.train_n,
	posterior_samples = nac.gc_out_train,
	num_of_changepoint = 10,
	nbi_value=500,
	nmc_value=100,
	b=100,
	seed = 123	
)


/**	5. Линейная модель для UPT **/

/* a. Сборка витрины [upt_abt_building.sas] */
%include '/opt/sas/mcd_config/macro/step/pt/upt_abt_building.sas';
%upt_abt_building(
	promo_lib = casuser, 
	ia_promo = past_promo,
	ia_promo_x_pbo = promo_pbo_enh,
	ia_promo_x_product = promo_prod_enh,
	period_start_dt = '1jan2019'd,
	period_end_dt = '12apr2021'd
)

/* b. Обучение моделей [upt_model_fitting.sas] */
%include '/opt/sas/mcd_config/macro/step/pt/upt_model_fitting.sas';
%upt_fit(
	train = nac.upt_train,
	upt_promo_max = nac.upt_train_max,
	upt_est = nac.upt_parameters
)