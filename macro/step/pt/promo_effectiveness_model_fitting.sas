/* 
	Обучение модели для прогнозирования n_a (t_a) 
*/

/* Гиперпараметры моделей */
%let default_hyper_params = seed=12345 loh=0 binmethod=QUANTILE 
	 maxbranch=2 
     assignmissing=useinsearch 
	 minuseinsearch=5
     ntrees=20
     maxdepth=20
     inbagfraction=0.7
     minleafsize=5
     numbin=100
     printtarget
;



%macro promo_effectiveness_model_fit(
	data = public.na_train,
	target = n_a,
	output = na_prediction_model,
	hyper_params = &default_hyper_params.
);
/* 
	Макрос обучение модель для прогнозирования n_a (t_a).
	Параметры:
	----------
		* data : Обучающий набор данных
		* target : Название целевой переменной (n_a или t_a)
		* output : Название таблицы, куда будет сохранена обученная модель
			(сохраняются в public)
		* hyper_params : Гиперпараметры модели
*/

	/* Стираем результирующую таблицу с обученной моделью */
	proc casutil;
		droptable casdata="&output." incaslib="public" quiet;
	run;
	
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
			mean_past_&target.
			temperature
			precipitation
				/ level = interval;
		input
			promo_group_id
			promo_mechanics_name
		/* 	pbo_location_id */
			AGREEMENT_TYPE_ID
			BREAKFAST_ID
			BUILDING_TYPE_ID
			COMPANY_ID
			DELIVERY_ID
			DRIVE_THRU_ID
			MCCAFE_TYPE_ID
		/*  PRICE_LEVEL_ID */
			WINDOW_TYPE_ID
			regular_weekend_flag
			weekend_flag
			Christmas_Day
			Day_After_New_Year
			New_Year_shift
			New_year
			 / level = nominal;
		id promo_id pbo_location_id sales_dt;
		target &target. / level = interval;
		savestate rstore=public.&output.;
		;
	run;

	/* Промоутим модель */
	proc casutil;
	    promote casdata="&output." incaslib="public" outcaslib="public";
	run;

	/* Сохраняем на диск на случай падейния среды */
    proc astore;
        download RSTORE=public.&output. store="/data/ETL_BKP/&output.";
    run;

%mend;