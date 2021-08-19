 /* 
	Регламентный процесс.
	
	1. Инициализация окружения
	2. Получение информации из промо тула [add_promotool_marks2.sas]
		* Выделение будущий акций
	3. Прогнозирование n_a и t_a для будущий акций [promo_effectiveness_model_scoring.sas]
	4. Разложение GC на промо компоненты [gc_model_scoring.sas]
	5. Разложение UPT на промо компоненты [upt_model_scoring.sas]
	6. Объединение результатов для отчетности
*/

%macro scoremodels(PromoCalculationRk);

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
	
	
	/*** 3. Прогнозирование n_a и t_a для будущий акций ***/
	%include '/opt/sas/mcd_config/macro/step/pt/promo_effectiveness_model_scoring_dev.sas';
	%scoring_building(
		promo_lib = casuser, 
		ia_promo = promo_tool_promo,
		ia_promo_x_pbo = promo_pbo_enh,
		ia_promo_x_product = promo_prod_enh,
		ia_media = media_enh,
		calendar_start = '01jan2017'd,
		calendar_end = '01jan2022'd
	);
	
	/* Скоринг t_a */
	%promo_effectivness_predict(
		model = ta_prediction_model_test,
		target = ta,
		data = casuser.promo_effectivness_scoring
	);

	/* Скоринг n_a */
	%promo_effectivness_predict(
		model = na_prediction_model_test,
		target = na,
		data = casuser.promo_effectivness_scoring
	);
	
	
	/*** 4. Разложение GC на промо компоненты ***/
	%include '/opt/sas/mcd_config/macro/step/pt/gc_model_scoring.sas';
	/* Собираем скоринговую витрину */
	%gc_scoring_builing(
		data = nac.promo_effectivness_ta_predict,
		promo_lib = nac,
		num_of_changepoint = 10,
		history_end = '1apr2021'd
	)
	
	/* Создаем прогноз */
	%let gc_predict_out = nac.gc_prediction;
	
	%gc_predict(
		data = work.gc_scoring6,
		out = &gc_predict_out.,
		num_of_changepoint = 10,
		posterior_samples = nac.gc_out_train,
		train_target_max = nac.receipt_qty_max
	);

	
	/*** 5. Разложение UPT на промо компоненты ***/
	%include '/opt/sas/mcd_config/macro/step/pt/upt_model_scoring.sas';
	/* Собираем скоринговую витрину и выдаем прогноз (сохраняется на диск nac.upt_scoring + поднимается в касюзер (без промоута) в таблицу public.upt_scoring */
	%upt_model_scoring(
		data = nac.promo_effectivness_na_predict,
		upt_promo_max = nac.upt_train_max
	);
	
	/* Поднимаем данные в память для формирования отчета ВА*/
	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto;
		caslib _all_ assign;
	%end;

	%let GcLibref=%scan(&gc_predict_out,1,'.');
	%let GcOutTableNm=%scan(&gc_predict_out.,2,'.');
	
	data casuser.&GcOutTableNm.(replace=yes);
		set &gc_predict_out.;
	run;
		
	data casuser.upt_scoring(replace=yes);
		set nac.upt_scoring;
	run;
	
	proc casutil;
		droptable casdata="&GcOutTableNm." incaslib="public" quiet;
		droptable casdata="upt_scoring" incaslib="public" quiet;
		promote incaslib="casuser" outcaslib="public" casdata="&GcOutTableNm." casout="&GcOutTableNm.";
		promote incaslib="casuser" outcaslib="public" casdata="upt_scoring" casout="upt_scoring";
	run;
	quit;
	
%mend;