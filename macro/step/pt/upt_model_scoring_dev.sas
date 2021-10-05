/* 	
	1. Добавляем product_id в ключ витрины промо эффективности
	2. В цикле по промо акциям вызываем скрипт рассчета промо эффекта и добавляем его результат к общему результату:
		Макрос calculate_upt_promo_effect(promo_id), которая для промо акции выводит информацию об эффективности прошедшей акции в виде таблицы с полями
			* promo_id
			* pbo_location_id
			* product_id
			* sales_dt
			* delta
			* baseline
	
		product_id - промо товары + все регулряные товары, продающиеся в день промо или в последний день истории
*/

/* Список уникальных категорий товаров */
data work.unique_caterogy;
input category_name $40.;
datalines;
positive_promo_na
mastercode_promo_na
Undefined_Product_Group
Cold_Drinks
Hot_Drinks
Breakfast
Condiments
Desserts
Fries
Starters___Salad
SN_CORE
McCafe
Non_product
SN_EDAP
SN_PREMIUM
Value_Meal
Nuggets
Shakes
;


%macro upt_model_scoring(
	data = nac.promo_effectivness_na_predict,
	upt_promo_max = nac.upt_train_max
);
	
	/*
		Скрипт, который считает UPT промо эффективность
		Параметры:
		----------
			* data : Таблица с прогнозами (или фактом) n_a
			* upt_promo_max : Таблица с нормировочными константами
		Выход:
			Таблица nac.upt_scoring
	*/


	/************************************************************************************
	 * 1. Добавляем product_id в ключ витрины промо эффективности						*
	 ************************************************************************************/

	/* Выгружаем таблицу casuser.promo_prod_enh в work */
	data work.promo_prod_enh;
		set casuser.promo_prod_enh;
	run;
	
	/* Добавляем информацию о товарах в промо */
	proc sql noprint;
		create table work.upt_scoring1 as
			select
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				t2.product_id,
				t2.option_number,
				t2.product_qty,
				t2.product_qty * t1.p_n_a as p_n_a
			from
				&data. as t1
			inner join
				work.promo_prod_enh as t2
			on
				t1.promo_id = t2.promo_id
		;
	quit;
	
	/* Считаем количество товаров в рамках каждого option number */
	proc sql noprint;
		create table work.number_of_products_per_option as
			select
				promo_id,
				option_number,
				count(distinct product_id) as cnt
			from
				work.promo_prod_enh
			group by
				promo_id,
				option_number
		;
	quit;
	
	/* Распределяем n_a равномерно по option number */
	proc sql noprint;
		create table work.upt_scoring2 as
			select
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				t1.product_id,
				t1.option_number,
				t1.product_qty,
				divide(t1.p_n_a, t2.cnt) as p_n_a
			from
				work.upt_scoring1 as t1
			inner join
				work.number_of_products_per_option as t2
			on
				t1.promo_id = t2.promo_id and
				t1.option_number = t2.option_number
		;
	quit;

	/* 	--- End. Добавляем product_id в ключ витрины промо эффективности --- */	
	
	
	/************************************************************************************
	 * 2. В цикле по промо акциям вызываем скрипт рассчета промо эффекта и добавляем    *
	 *	    его результат к общему результату:											*
	 ************************************************************************************/
	
	/* Список промо акций */
	proc sql;
		create table work.unique_promo_list as
			select distinct
				promo_id
			from
				work.upt_scoring2
		;
	quit;
	
	/* Удаляем таблицу с результатом */
	proc datasets library=nac nolist;
	   delete upt_scoring;
	run;
	
	options nomlogic nomprint nosymbolgen nosource nonotes;

	/* Пройдем в цикле по товарам и будем вызывать макрос */
	data _null_;
	   set work.unique_promo_list;
  	   call execute('%nrstr(%calculate_upt_promo_effect)('||promo_id||')');
	run;

	options mlogic mprint symbolgen source notes;
	
	/* Удаляем промежуточные таблицы */
	proc datasets library=work nolist;
		delete unique_promo_list;
		delete upt_scoring2;
		delete upt_scoring1;
		delete number_of_products_per_option;
		delete promo_prod_enh;
		delete unique_caterogy;
	run;
		
	/* 	--- End. В цикле по промо акциям вызываем скрипт рассчета промо эффекта --- */	

%mend;

%macro calculate_upt_promo_effect(promo_id);

	/*
		Макрос calculate_upt_promo_effect(promo_id), которая для промо акции
		выводит информацию об эффективности прошедшей акции в виде таблицы с полями
			* promo_id
			* pbo_location_id
			* product_id
			* sales_dt
			* delta
			* baseline
	
		product_id - промо товары + все регулряные товары, продающиеся в день промо или в последний день истории

		Параметры:
		----------
			* promo_id : ID промо акции
		Выход:
			Таблица work.one_promo_upt_scoring
			
		Логика расчета:
		----------------
			1. Промо акция действует на множество товаров X = Q + Z
				где Q - регулярные товары
					Z - промо товары
				первым шагом получаем эти два множества
				
			2. Рассчет для множетсва Q:
				пока q в Q:
					- умножаем спрогнозированную промо эффективность для q
						на коэффциент positive_promo
					- достаем для q baseline
					
			3. Рассчет для множества Z:
				пока z в Z:
					- просто берем UPT' всех товаров из Z, где UPT' = N_a / mean_gc * 1000
					- baseline = 0
					
			4. Формируем множество товаров Y - регулярные товары продающиеся
					на момент акции (или продавались в последний день истории)
					
			5. Расчет для множества Y
			
			6. Объединяем результаты, добавляем к финальному результату.
	*/
	
	/************************************************************************************
	 * 1. Формируем множетсва Q и Z 												    *
	 ************************************************************************************/
	
	/* Список промо товаров */
	proc sql noprint;
		create table work.X as
			select distinct
				product_id
			from
				work.upt_scoring2
			where
				promo_id = &promo_id.
		;
	quit;
	
	/* Регулярные товары */
	proc sql noprint;
		create table work.Q as
			select
				t1.product_id
			from
				etl_ia.product_attributes as t1
			inner join
				work.X as t2
			on
				t1.product_id = t2.product_id
			where
				t1.product_attr_nm = 'REGULAR_ID' and
				&ETL_CURRENT_DTTM. <= t1.valid_to_dttm and
				&ETL_CURRENT_DTTM. >= t1.valid_from_dttm and
				t1.product_id = input(t1.product_attr_value, best32.)	
		;
	quit;
	
	/* Промо товары */
	proc sql noprint;
		create table work.Z as
			select
				t1.product_id
			from
				work.X as t1
			left join
				work.Q as t2
			on
				t1.product_id = t2.product_id
			where
				t2.product_id is missing
		;
	quit;

	/* Считаем количество элементов в Q, Z */
	proc sql noprint;
		select count(*) into :nobs_q from work.Q;
		select count(*) into :nobs_z from work.Z;
	quit;
	
	/* 	----------------------- End. Формируем множетсва Q и Z ----------------------- */	



	/************************************************************************************
	 * 2. Рассчет для множетсва Q													    *
	 ************************************************************************************/
	
	/* Если множество регулярных товаров не пусто */
	%if %eval(&nobs_q. > 0) %then 
		%do;
			/* Пересекаем регулярные товары и промо эффективность */
			proc sql noprint;
				create table work.q_promo_efficiency as
					select
						t1.promo_id,
						t1.pbo_location_id,
						t1.sales_dt,
						t1.product_id,
						t1.option_number,
						t1.product_qty,
						t1.p_n_a
					from
						work.upt_scoring2 as t1
					inner join
						work.Q as t2
					on
						t1.product_id = t2.product_id
					where
						t1.promo_id = &promo_id.
				;		
			quit;
			
			/* Добавляем коэффциент delta и baseline */
			proc sql noprint;
				create table work.q_promo_efficiency2 as
					select
						t1.promo_id,
						t1.pbo_location_id,
						t1.sales_dt,
						t1.product_id,
						t3.max_upt * t1.p_n_a * coalesce(divide(t2.positive_promo_na, t3.max_positive_promo_na), 0) as delta,
						((t1.sales_dt - t4.min_date + 1) * coalesce(divide(t2.t, t3.max_t), 0) + t2.intercept) * t3.max_upt as baseline
					from
						work.q_promo_efficiency as t1
					inner join
						nac.upt_parameters as t2
					on
						t1.product_id = t2.product_id
					inner join
						nac.upt_train_max as t3
					on
						t1.product_id = t3.product_id
					inner join
						(select product_id, min(sales_dt) as min_date from nac.upt_train group by product_id) as t4
					on
						t1.product_id = t4.product_id
					where
						t2._TYPE_ = 'RIDGE'
				;
			quit;
			
		%end;
	
	/* 	----------------------- End. Рассчет для множетсва Q ----------------------- */	



	/************************************************************************************
	 * 3. Рассчет для множетсва Z													    *
	 ************************************************************************************/

	/* Если множество регулярных товаров не пусто */
	%if %eval(&nobs_z. > 0) %then 
		%do;
			/* Пересекаем регулярные товары и промо эффективность */
			proc sql noprint;
				create table work.z_promo_efficiency as
					select
						t1.promo_id,
						t1.pbo_location_id,
						t1.sales_dt,
						t1.product_id,
						divide(t1.p_n_a, t3.mean_gc) * 1000 as delta,
						0 as baseline
					from
						work.upt_scoring2 as t1
					inner join
						work.Z as t2
					on
						t1.product_id = t2.product_id
					inner join
						nac.history_mean_gc as t3
					on
						t1.pbo_location_id = t3.pbo_location_id
					where
						t1.promo_id = &promo_id.
				;		
			quit;
			
		%end;

	/* 	----------------------- End. Рассчет для множетсва Z ----------------------- */	
	
	

	/************************************************************************************
	 * 4. Формируем множество товаров Y													    *
	 ************************************************************************************/
	
	/* Формируем список продаваемых регулярных товаров на момент промо */
	proc sql noprint;
		create table work.Y as
			select distinct
				t1.product_id
			from
				nac.active_regular_product as t1
			inner join
				(select distinct sales_dt from work.upt_scoring2 where promo_id = &promo_id.) as t2
			on
				t1.sales_dt = t2.sales_dt
			left join
				work.Q as t3 /* исключаем уже посчитанные товары Q */
			on
				t1.product_id = t3.product_id
			where
				t3.product_id is missing
		;	
	quit;
	
	/* Считаем количество строк в Y */
	proc sql noprint;
		select count(*) into :nobs_y from work.Y;
	quit;
	
	/* Если множество Y пусто, то формируем его по последнему дню в истории */
	%if %eval(&nobs_y. = 0) %then 
		%do;
			proc sql noprint;
				create table work.Y as
					select
						t1.product_id
					from (
						select
							t1.product_id
						from
							nac.active_regular_product as t1,
							(select max(sales_dt) as max_sales_dt from nac.active_regular_product) as t2
						where
							t1.sales_dt = t2.max_sales_dt
					) as t1
					left join
						work.Q as t2 /* исключаем уже посчитанные товары Q */
					on
						t1.product_id = t2.product_id
					where
						t2.product_id is missing
				;		
			quit;
		%end;

	/* 	--------------------- End. Формируем множество товаров Y --------------------- */	
	
	
	
	/************************************************************************************
	 * 5. Расчет для множества Y													    *
	 ************************************************************************************/

	/* Добавляем категории товаров и суммируем n_a */
	proc sql noprint;
		create table work.y_promo_efficiency as
			select
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				t2.category_name,
				sum(t1.p_n_a) as p_n_a
			from
				work.upt_scoring2 as t1
			inner join
				nac.product_dictionary_ml as t2
			on
				t1.product_id = t2.product_id
			where
				t1.promo_id = &promo_id.
			group by
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				t2.category_name
		;
	quit;
	
	
	/* Создаем таблицу для транспонирования */
	proc sql noprint;
		create table work.y_promo_efficiency2 as
			select distinct
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				trim(t2.category_name) as category_name
			from 
				(select * from nac.promo_effectivness_na_predict where promo_id = &promo_id.) as t1,
				work.unique_caterogy as t2
		;
	quit;
	
	/* Соединяем результаты с каркасом */
	proc sql noprint;
		create table work.y_promo_efficiency3 as
			select distinct
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				t1.category_name,
				coalesce(t2.p_n_a, 0) as p_n_a
			from
				work.y_promo_efficiency2 as t1
			left join
				work.y_promo_efficiency as t2
			on
				t1.promo_id = t2.promo_id and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt and
				t1.category_name = t2.category_name
		;
	quit;
	
	/* Сортируем таблицу */
	proc sort data=work.y_promo_efficiency3;
		by pbo_location_id sales_dt promo_id;
	run;
	
	/* Транспонируем промо механики */
	proc transpose data=work.y_promo_efficiency3 
		out=work.y_promo_efficiency4;
		var p_n_a;
		id category_name;
		by pbo_location_id sales_dt promo_id;
	run;

	/* Фильтруем таблицу с коэффциентами модели */
	proc sql noprint;
		create table work.simple_upt_parameters as
			select
				t1.product_id,
				t1.intercept,
				coalesce(divide(t1.t, t3.max_t), 0)  as t,
				coalesce(divide(t1.positive_promo_na, t3.max_positive_promo_na), 0) as positive_promo_na,
				coalesce(divide(t1.mastercode_promo_na, t3.max_mastercode_promo_na), 0) as mastercode_promo_na,
				coalesce(divide(t1.Undefined_Product_Group, t3.max_Undefined_Product_Group), 0) as Undefined_Product_Group,
				coalesce(divide(t1.Cold_Drinks, t3.max_Cold_Drinks), 0) as Cold_Drinks,
				coalesce(divide(t1.Hot_Drinks, t3.max_Hot_Drinks), 0) as Hot_Drinks,
				coalesce(divide(t1.Breakfast, t3.max_Breakfast), 0) as Breakfast,
				coalesce(divide(t1.Condiments, t3.max_Condiments), 0) as Condiments,
				coalesce(divide(t1.Desserts, t3.max_Desserts), 0) as Desserts,
				coalesce(divide(t1.Fries, t3.max_Fries), 0) as Fries,
				coalesce(divide(t1.Starters___Salad, t3.max_Starters___Salad), 0) as Starters___Salad,
				coalesce(divide(t1.SN_CORE, t3.max_SN_CORE), 0) as SN_CORE,
				coalesce(divide(t1.McCafe, t3.max_McCafe), 0) as McCafe,
				coalesce(divide(t1.Non_product, t3.max_Non_product), 0) as Non_product,
				coalesce(divide(t1.SN_EDAP, t3.max_SN_EDAP), 0) as SN_EDAP,
				coalesce(divide(t1.SN_PREMIUM, t3.max_SN_PREMIUM), 0) as SN_PREMIUM,
				coalesce(divide(t1.Value_Meal, t3.max_Value_Meal), 0) as Value_Meal,
				coalesce(divide(t1.Nuggets, t3.max_Nuggets), 0) as Nuggets,
				coalesce(divide(t1.Shakes, t3.max_Shakes), 0) as Shakes,		
				t3.max_upt	
			from
				nac.upt_parameters as t1
			inner join
				work.Y as t2
			on
				t1.product_id = t2.product_id
			inner join
				nac.upt_train_max as t3
			on
				t1.product_id = t3.product_id
			where
				t1._TYPE_ = 'RIDGE'
		;
	quit;
	
	
	/* Добавляем к коэффциентам дату начала продаж товара */
	proc sql noprint;
		create table work.simple_upt_parameters2 as
			select
				t1.*,
				t2.min_date
			from
				work.simple_upt_parameters as t1
			inner join
				(select product_id, min(sales_dt) as min_date from nac.upt_train group by product_id) as t2
			on
				t1.product_id = t2.product_id
		;
	quit;
	
	
	/* Джоиним с таблицей коэффициентов */
	proc sql noprint;
		create table work.y_promo_efficiency5 as
			select
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				t2.product_id,
				((t1.sales_dt - t2.min_date + 1) * t2.t + t2.intercept) * t2.max_upt as baseline,
				sum(
					t1.positive_promo_na * t2.positive_promo_na, 
					t1.mastercode_promo_na * t2.mastercode_promo_na,
					t1.Undefined_Product_Group * t2.Undefined_Product_Group,
					t1.Cold_Drinks * t2.Cold_Drinks,
					t1.Hot_Drinks * t2.Hot_Drinks,
					t1.Breakfast * t2.Breakfast,
					t1.Condiments * t2.Condiments,
					t1.Desserts * t2.Desserts,
					t1.Fries * t2.Fries,
					t1.Starters___Salad * t2.Starters___Salad,
					t1.SN_CORE * t2.SN_CORE,
					t1.McCafe * t2.McCafe,
					t1.Non_product * t2.Non_product,
					t1.SN_EDAP * t2.SN_EDAP,
					t1.SN_PREMIUM * t2.SN_PREMIUM,
					t1.Value_Meal * t2.Value_Meal,
					t1.Nuggets * t2.Nuggets,
					t1.Shakes * t2.Shakes
				) * t2.max_upt as delta
			from
				work.y_promo_efficiency4 as t1,
				work.simple_upt_parameters2 as t2
		;
	quit;

	/* 	------------------------ End. Расчет для множества Y ------------------------ */	



	/************************************************************************************
	 * 6. Объединяем результаты, добавляем к финальному результату												    *
	 ************************************************************************************/
	
	/* Добавляем к резульатату если таблица не пустая */
	%if %eval(&nobs_q. > 0) %then 
		%do;
			proc append base = work.promo_efficiency
				data = work.q_promo_efficiency2 force;
			run;
		%end;

	/* Добавляем к резульатату если таблица не пустая */
	%if %eval(&nobs_z. > 0) %then 
		%do;
			proc append base = work.promo_efficiency
				data = work.z_promo_efficiency force;
			run;
		%end;
		
	proc append base = work.promo_efficiency
		data = work.y_promo_efficiency5 force;
	run;

	/* Добавим результат к витрине */
	proc append base=nac.upt_scoring
		data = work.promo_efficiency force;
	run;

	/* Удаляем промежуточные таблицы */
	proc datasets library=work nolist;
		delete X;
		delete Y;
		delete Z;
		delete Q;
		
		delete q_promo_efficiency;
		delete q_promo_efficiency2;
		
		delete z_promo_efficiency;
		
		delete y_promo_efficiency;
		delete y_promo_efficiency2;
		delete y_promo_efficiency3;
		delete y_promo_efficiency4;
		delete y_promo_efficiency5;
		
		delete promo_efficiency;
		
		delete simple_upt_parameters2;
		delete simple_upt_parameters;
	run;

	/* 	------- End.  Объединяем результаты, добавляем к финальному результату ------- */	


%mend;


/* %calculate_upt_promo_effect(12288); */
/* %calculate_upt_promo_effect(12292); */
/* %calculate_upt_promo_effect(12293); */
/* %calculate_upt_promo_effect(12294); */
/* %calculate_upt_promo_effect(12296); */
/* %calculate_upt_promo_effect(12298); */
/* %calculate_upt_promo_effect(12300); */


%upt_model_scoring(
	data = nac.promo_effectivness_na_predict,
	upt_promo_max = nac.upt_train_max
)