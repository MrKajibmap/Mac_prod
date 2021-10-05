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
	
	/* Список промо для скоринга */
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
			) and channel_cd = 'ALL' and FROM_PT = 1
		;
	quit;
	
	/* Загружаем в CAS таблицу */
	data casuser.na_calculation_schedule;
		set nac.na_calculation_schedule;
	run;
	
	/* Оставляем только будущие промо */
	proc fedsql sessref=casauto;
		create table casuser.future_promo_tool_promo{options replace=true} as
			select
				t1.*
			from
				casuser.promo_tool_promo as t1
			left join
				(select distinct promo_txt_id from casuser.na_calculation_schedule) as t2
			on
				t1.promo_txt_id = t2.promo_txt_id
			where
				t2.promo_txt_id is missing
		;
	quit;
	
	
	/*** 3. Прогнозирование n_a и t_a для будущий акций ***/
	%include '/opt/sas/mcd_config/macro/step/pt/promo_effectiveness_model_scoring_dev.sas';
	%scoring_building(
		promo_lib = casuser, 
		ia_promo = future_promo_tool_promo,
		ia_promo_x_pbo = promo_pbo_enh,
		ia_promo_x_product = promo_prod_enh,
		ia_media = media_enh,
		calendar_start = '01jan2017'd,
		calendar_end = '01jan2022'd
	);  /* Заменить подсчитанные таблицы pbo_lvl_all, product_lvl_all на 
		считаемые на ходу, потому что справочники могут обновится и
		данные в этих таблицах будут неактуальными */
	
	/* Скоринг t_a */
	%promo_effectivness_predict(
		model = ta_prediction_model,
		target = ta,
		data = casuser.promo_effectivness_scoring
	);

	/* Скоринг n_a */
	%promo_effectivness_predict(
		model = na_prediction_model,
		target = na,
		data = casuser.promo_effectivness_scoring
	);
	
	/* Добавляем фактические значения промо эффективности к прогнозным */
		
		/* Дабавляем обычный промо ID */
		proc fedsql sessref=casauto;
			create table casuser.na_calculation_schedule_id{options replace=true} as
				select
					t2.promo_id,
					t1.pbo_location_id,
					t1.sales_dt,
					t1.n_a,
					t1.t_a
				from
					casuser.na_calculation_schedule as t1
				inner join
					casuser.promo_tool_promo as t2
				on
					t1.promo_txt_id = t2.promo_txt_id
			;
		quit;
	
		/* Загружаем в CAS */
		data casuser.pbo_loc_attributes;
			set etl_ia.pbo_loc_attributes;
		run;

		/* Дабавляем обычный pbo_location_id */
		proc fedsql sessref=casauto;
			create table casuser.na_calculation_schedule_id2{options replace=true} as
				select
					t1.promo_id,
					t2.pbo_location_id,
					t1.sales_dt,
					t1.n_a,
					t1.t_a
				from
					casuser.na_calculation_schedule_id as t1
				inner join (
					select distinct
						PBO_LOCATION_ID,
						PBO_LOC_ATTR_VALUE
					from
						casuser.pbo_loc_attributes
					where
						PBO_LOC_ATTR_NM = 'STORE_ID' and
						&ETL_CURRENT_DTTM. <= valid_to_dttm and
						&ETL_CURRENT_DTTM. >= valid_from_dttm
				) as t2 
				on
					t1.pbo_location_id = t2.PBO_LOC_ATTR_VALUE
			;
		quit;

		/* Таблица для UPT */
		data work.na_history(rename=(n_a=p_n_a) drop=t_a);
			set casuser.na_calculation_schedule_id2;
		run;
	
		/* Таблица для GC */
		data work.ta_history(rename=(t_a=p_t_a) drop=n_a);
			set casuser.na_calculation_schedule_id2;
		run;
				
		/* Делаем append для GC */
		proc append base=nac.promo_effectivness_ta_predict
			data = work.ta_history force;
		run; 
		
		/* Делаем append для UPT */
		proc append base=nac.promo_effectivness_na_predict
			data = work.na_history force;
		run;
		
		
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
		droptable casdata="na_calculation_schedule_id" incaslib="casuser" quiet;
		droptable casdata="na_calculation_schedule_id2" incaslib="casuser" quiet;
		droptable casdata="pbo_loc_attributes" incaslib="casuser" quiet;
		droptable casdata="&GcOutTableNm." incaslib="public" quiet;
		droptable casdata="upt_scoring" incaslib="public" quiet;
		promote incaslib="casuser" outcaslib="public" casdata="&GcOutTableNm." casout="&GcOutTableNm.";
		promote incaslib="casuser" outcaslib="public" casdata="upt_scoring" casout="upt_scoring";
	run;
	quit;
	
%mend;