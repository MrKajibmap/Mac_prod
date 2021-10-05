%let test_threshold = '31mar2021'd;
%let data = casuser.train;
%let target = n_a;
%let output = basic_quality_model;

/* Промо акции для обучения */
data work.past_promo;
	set casuser.past_promo;
run;


proc sql;
	create table work.train_promo as
		select distinct 
			hybrid_promo_id
		from work.past_promo
		where end_dt <= &test_threshold.
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
			work.train_promo as t2
		on
			t1.hybrid_promo_id = t2.hybrid_promo_id
		where
			t2.hybrid_promo_id is missing
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
			work.train_promo as t2
		on
			t1.hybrid_promo_id = t2.hybrid_promo_id
	;
quit;

data casuser.train;
	set work.train;
run;

data casuser.test;
	set work.test;
run;

/* proc surveyselect data=work.train out=work.train_sample sampsize=500000; */
/* run; */

/* Гиперпараметры моделей */
%let default_hyper_params = seed=12345 loh=0 binmethod=QUANTILE 
	 maxbranch=2 
     assignmissing=useinsearch 
	 minuseinsearch=5
     ntrees=40
     maxdepth=20
     inbagfraction=0.7
     minleafsize=5
     numbin=100
     printtarget
;


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
/* 			Condiments только одна акция на эту категорию товаров, плохой признак */
			Desserts
			Fries
			HotDrinks
/* 			McCafe константное значение */ 
/* 			Nonproduct  константное значение */
			Nuggets
			SNCORE
			SNEDAP
			SNPREMIUM
/* 			Shakes всего два промо имеют ненулевое значение */
			StartersSalad
/* 			UndefinedProductGroup только одна акция на эту категорию товаров, плохой признак */
			np_gift_price_amt /* 83% миссингов */
/* 			ValueMeal константное значение */
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
/* 			precipitation  Есть выбросы, когда осадки больше 100 мм в день */
				/ level = interval;
		input
/* 			promo_group_id не понятно, стоит ли использовать этот признак 
							надо отдельно с ним и без него провести эксперимент */			
			promo_mechanics_name
		/* 	pbo_location_id я бы хотел включить этот признак в модель, но есть ощущение
							что переменная с таким количеством принимаемых значений
							не может быть обработана в SAS */
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
			New_year
			 / level = nominal;
		id hybrid_promo_id pbo_location_id sales_dt;
		target &target. / level = interval;
		savestate rstore=public.&output.;
		;
	run;

proc casutil;
    promote casdata="&output." incaslib="public" outcaslib="public";
run;



/* Scoring */
proc astore;
	score data=casuser.train
	copyvars=(_all_)
	rstore=public.&output.
	out=casuser.train_prediction
	;
quit;

proc astore;
	score data=casuser.test
	copyvars=(_all_)
	rstore=public.&output.
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



data casuser.test_prediction;
	set nac.test_prediction;
run; 

data casuser.train_prediction;
	set nac.train_prediction;
run;


proc fedsql sessref=casauto;
	select
		t2.promo_nm,
		t2.promo_mechanics,
		t1.mape, 
		t1.wape
	from (
		select
			hybrid_promo_id,
			mean(divide(abs(p_n_a-n_a), n_a)) as mape,
			sum(abs(p_n_a-n_a))/sum(n_a) as wape
		from
			casuser.test_prediction
		group by
			hybrid_promo_id
	) as t1
	inner join 
		casuser.past_promo as t2
	on
		t1.hybrid_promo_id = t2.hybrid_promo_id
	order by
		t2.promo_nm
	;
quit;

%macro wape(df, out);
	
	data past_promo;
		set casuser.past_promo;
	run;
	
	proc sql;
		create table &out. as
			select
				t2.promo_nm,
				t2.promo_mechanics,
				t1.week_start,
				divide(abs(t1.p_n_a - t1.n_a), t1.n_a) as wape,
				divide(t1.p_n_a - t1.n_a, t1.n_a) as bias
			from (
				select
					hybrid_promo_id,
					intnx('week.2', sales_dt, 0, 'B') as week_start format date9.,
					sum(p_n_a) as p_n_a,
					sum(n_a) as n_a
				from
					nac.&df.
				group by
					hybrid_promo_id,
					calculated week_start
			) as t1
			inner join 
				past_promo as t2
			on
				t1.hybrid_promo_id = t2.hybrid_promo_id
			order by
				t2.promo_mechanics,
				t2.promo_nm,
				t1.week_start
		;
	quit;

%mend;

%wape(test_prediction, new_launch_test);
