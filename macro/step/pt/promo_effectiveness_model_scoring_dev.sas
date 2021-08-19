/* 
	Сборка скоринговых витрин для моделей n_a и t_a.

	1. Сборка скоринговой витрины.
	2. Прогнозирование обученной моделью.  

*/

data casuser.unique_promo_mechanics_name;
input promo_mechanics_name $40.;
datalines;
Bundle
Discount
EVMSet
Giftforpurchaseforproduct
GiftforpurchaseNonProduct
GiftforpurchaseSampling
NPPromoSupport
OtherDiscountforvolume
Pairs
Pairsdifferentcategories
Productlineextension
ProductnewlaunchLTO
ProductnewlaunchPermanentinclite
Productrehitsameproductnolineext
Temppricereductiondiscount
Undefined
;

/*** 1. Сборка скоринговой витрины ***/
%macro scoring_building(
	promo_lib = casuser, 
	ia_promo = promo_tool_promo,
	ia_promo_x_pbo = promo_pbo_enh,
	ia_promo_x_product = promo_prod_enh,
	ia_media = media_enh,
	calendar_start = '01jan2017'd,
	calendar_end = '01jan2022'd
	);

	/*
		Макрос, который собирает обучающую выборку для модели прогнозирующей
			na (и ta).
		Порядок действий:

		1. Вычисление каркаса таблицы промо акций: промо, ПБО, товар, интервал, механика
		2. Количество товаров, участвующих в промо (количество уникальных product_id),
			количество позиций (количество уникальных option_number), 
			количество единиц товара, необходимое для покупки
		3. TRP
		4. Цены <--- TODO
		5. Пускай у нас имеется k товарных категорий, тогда создадим вектор размерности k.
			Каждая компонента этого вектора описывает количество товаров данной 
			категории участвующих в промо
		6. Атрибуты ПБО
		7. Календарные признаки и праздники
		8. Признаки описывающие трафик ресторана (количество чеков)
		9. Признаки описывающие продажи промо товаров
		10. Дополнительная кодировка промо категорий
		11. Добавляем погоду
	
		Параметры:
		----------
			* promo_lib: библиотека, где лежат таблицы с промо (предполагается,
				что таблицы лежат в cas)
			* ia_promo: название таблицы с информацией о промо 
			* ia_promo_x_pbo: название таблицы с привязкой промо к ресторнам
			* ia_promo_x_product: название таблицы с привязкой промо к товарам
			* calendar_start : старт интервала формирования календарных признаков
			* calendar_end : конец интервала формирования календарных признаков
		Выход:
		------
			* Запромоученая в casuser и скопированная в nac таблица na_train
	*/	


	/************************************************************************************
	 * 1. Вычисление каркаса таблицы промо акций										*
	 ************************************************************************************/
	
	/* Загружаем сохраненные на этапе сборки обучающей витрины таблицы */
	data casuser.pbo_lvl_all;
		set nac.pbo_lvl_all;
	run;

	data casuser.product_lvl_all;
		set nac.product_lvl_all;
	run;
	
	/* Добавляем к таблице промо ПБО и товары */
	proc fedsql sessref = casauto;
		create table casuser.ia_promo_x_pbo_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t2.PBO_LEAF_ID as pbo_location_id
			from
				&promo_lib..&ia_promo_x_pbo. as t1,
				casuser.pbo_lvl_all as t2
			where
				t1.pbo_location_id = t2.PBO_LOCATION_ID
		;
		create table casuser.ia_promo_x_product_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t1.OPTION_NUMBER,
				t1.PRODUCT_QTY,
				t2.product_LEAF_ID as product_id
			from
				&promo_lib..&ia_promo_x_product. as t1,
				casuser.product_lvl_all as t2
			where
				t1.product_id = t2.product_id
		;
	quit;

	/* Формируем каркас витрины */
	proc fedsql sessref=casauto;
		create table casuser.promo_skelet{options replace = true} as 
			select
				t1.PROMO_ID,
				t2.pbo_location_id,
				t1.START_DT,
				t1.END_DT,
				(t1.END_DT - t1.START_DT) as promo_lifetime,
				t1.CHANNEL_CD,
				t1.PROMO_GROUP_ID,
				t1.NP_GIFT_PRICE_AMT,
				compress(promo_mechanics,'', 'ak') as promo_mechanics_name
			from
				&promo_lib..&ia_promo. as t1
			inner join
				casuser.ia_promo_x_pbo_leaf as t2
			on 
				t1.PROMO_ID = t2.PROMO_ID
		;
	quit;
	
	/* Расшиваем интервалы по дням */
	data casuser.na_abt0;
		set casuser.promo_skelet;
		format sales_dt DATE9.;
		do sales_dt=start_dt to end_dt;
			output;
		end;
	run;

	/* Оставляем только текущий год */
	proc fedsql sessref=casauto;
		create table casuser.na_abt1{options replace=true} as
			select
				*
			from
				casuser.na_abt0
			where
				year(sales_dt) = year(&ETL_CURRENT_DT_DB)
		;
	quit;
	
	proc casutil;
		droptable casdata="na_abt0" incaslib="casuser" quiet;
		droptable casdata="pbo_lvl_all" incaslib="casuser" quiet;
		droptable casdata="product_lvl_all" incaslib="casuser" quiet;
	run;
	
	/* 	------------ End. Вычисление каркаса таблицы промо акций ------------- */


	/************************************************************************************
	 * 2. Считаем количество товаров, участвующих в промо (количество уникальных  		*
	 *		product_id), количество позиций (количество уникальных option_number),      *
	 *		количество единиц товара, необходимое для покупки						    *
	 ************************************************************************************/
	 
	proc fedsql sessref=casauto;
		/* Количество товаров, позиций участвующих в промо */
		create table casuser.product_characteristics{options replace=true} as
			select
				promo_id,
				max(option_number) as number_of_options,
				count(distinct product_id) as number_of_products
			from
				casuser.ia_promo_x_product_leaf
			group by
				promo_id
		;
		/* Количество единиц товара, необходимое для покупки */
		create table casuser.product_characteristics2{options replace=true} as
			select
				t1.promo_id,
				sum(product_qty) as necessary_amount
			from (
				select distinct
					promo_id,
					option_number,
					PRODUCT_QTY
				from
					casuser.ia_promo_x_product_leaf
			) as t1
			group by
				t1.promo_id
		;
	quit;
	
	/* Добавляем признаки в витрину */
	proc fedsql sessref=casauto;
		create table casuser.na_abt2{options replace=true} as
			select
				t1.*,
				intnx('week.2',t1.sales_dt, 0, 'b') as week_start,
				t2.number_of_options,
				t2.number_of_products,
				t3.necessary_amount
			from
				casuser.na_abt1 as t1
			left join
				casuser.product_characteristics as t2
			on
				t1.promo_id = t2.promo_id
			left join
				casuser.product_characteristics2 as t3
			on
				t1.promo_id = t3.promo_id	
		;
	quit;
	
	proc casutil;
		droptable casdata="na_abt1" incaslib="casuser" quiet;
		droptable casdata="product_characteristics" incaslib="casuser" quiet;
		droptable casdata="product_characteristics2" incaslib="casuser" quiet;
	run;
	
	/* 	------------ End. Подсчет количества промо товаров в акции ------------- */



	/************************************************************************************
	 * 3. Добавление TRP																*
	 ************************************************************************************/

	/* Считаем сколько дней в неделе было промо */
	proc fedsql sessref=casauto;
		create table casuser.number_of_promo_days{option replace=true} as
			select
				t1.promo_id,
				t1.week_start,
				count(distinct sales_dt) as number_of_promo_days
			from 
				casuser.na_abt2 as t1
			group by
				t1.promo_id,
				t1.week_start
		;
	quit;

	/* Добавляем TRP и делим на количество промо дней в неделе */
	proc fedsql sessref=casauto;
		create table casuser.na_abt3{option replace=true} as
			select
				t1.*,
				coalesce(divide(t2.trp, t3.number_of_promo_days), 0) as trp
			from
				casuser.na_abt2 as t1
			left join
				&promo_lib..&ia_media. as t2
			on
				t1.promo_group_id = t2.promo_group_id and
				t1.week_start = t2.report_dt
			left join
				casuser.number_of_promo_days as t3
			on
				t1.promo_id = t3.promo_id and
				t1.week_start = t3.week_start
		;	
	quit;

	/* Удаляем промежуточные таблицы */
	proc casutil;
		droptable casdata="na_abt2" incaslib="casuser" quiet;
		droptable casdata="number_of_promo_days" incaslib="casuser" quiet;
	run;

	/* 	------------ End. Добавление TRP ------------- */
	
		/************************************************************************************
	 * 4. Добавление цены: TODO															*
	 ************************************************************************************/
	/* 
		тут на самом деле вопрос, какие ценовые признаки добавлять в модель 
			* есть промо без изменения цены
			* есть промо с изменением цены на один товар
			* есть промо с изменением цены на несколько товаров
			* а если в промо вводят новый товар, сможем ли мы легко по
				таблице с ценами посчитать глубину скидки 
			
	*/
	
	/* 	------------ End. Добавление цены ------------- */	

	/************************************************************************************
	 * 5. Пускай у нас имеется k товарных категорий,									*
	 *	  	тогда создадим вектор размерности k. Каждая компонента этого				*
	 *	 	вектора описывает количество товаров данной категории участвующих в промо.	*
	 ************************************************************************************/
	
	/* Копируем из nac в  casuser справочник товаров */
	data casuser.product_dictionary_ml(replace=yes drop=prod_lvl2_name);
		set nac.product_dictionary_ml;
	run;

	proc fedsql sessref=casauto;
		create table casuser.product_dictionary_ml{options replace=true} as
			select
				t1.*,
				compress(t1.prod_lvl2_nm,'', 'ak') as prod_lvl2_name
			from
				casuser.product_dictionary_ml as t1
		;
	quit;

	/* Считаем количество товаров в категории */
	proc fedsql sessref=casauto;
		create table casuser.promo_category{options replace=true} as
			select
				t1.promo_id,
				t2.prod_lvl2_name,
				count(distinct t1.product_id) as count_promo
			from
				casuser.ia_promo_x_product_leaf as t1
			inner join
				casuser.product_dictionary_ml as t2
			on
				t1.product_id = t2.product_id
			group by
				t1.promo_id,
				t2.prod_lvl2_name
		;
	quit;
	
	/* Транспонируем таблицу */
	proc cas;
	transpose.transpose /
	   table={name="promo_category", caslib="casuser", groupby={"promo_id"}} 
	   transpose={"count_promo"} 
	   id={"prod_lvl2_name"} 
	   casout={name="promo_category_transposed", caslib="casuser", replace=true};
	quit;
	
	/* Заменяем пропуски на нули */
	data casuser.promo_category_transposed_zero;
		set casuser.promo_category_transposed;
		drop _name_;
		array change _numeric_;
	    	do over change;
	            if change=. then change=0;
	        end;
	run;
	
	/* Добавляем признаки в витрину */
	proc fedsql sessref=casauto;
		create table casuser.na_abt5{options replace=true} as
			select
				t1.*,
				t2.Breakfast,
				t2.ColdDrinks,
				t2.Condiments,
				t2.Desserts,
				t2.Fries,
				t2.HotDrinks,
				t2.McCafe,
				t2.Nonproduct,
				t2.Nuggets,
				t2.SNCORE,
				t2.SNEDAP,
				t2.SNPREMIUM,
				t2.Shakes,
				t2.StartersSalad,
				t2.UndefinedProductGroup,
				t2.ValueMeal
			from
				casuser.na_abt3 as t1
			left join
				casuser.promo_category_transposed_zero as t2
			on
				t1.promo_id = t2.promo_id
		;
	quit;
	
	proc casutil;
		droptable casdata="promo_category" incaslib="casuser" quiet;
		droptable casdata="promo_category_transposed" incaslib="casuser" quiet;
		droptable casdata="promo_category_transposed_zero" incaslib="casuser" quiet;
		droptable casdata="na_abt3" incaslib="casuser" quiet;
	run;
	
	/* 	------------ End. Добавление информации о товарах в промо ------------- */	


	/************************************************************************************
	 * 6. Добавляем атрибуты ПБО														*
	 ************************************************************************************/
	 
	data casuser.pbo_dictionary_ml;
		set nac.pbo_dictionary_ml;
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.na_abt6{options replace=true} as
			select
				t1.*,
				t2.lvl3_id,
				t2.lvl2_id,
				t2.A_AGREEMENT_TYPE_id as agreement_type_id,
				t2.A_BREAKFAST_id as breakfast_id,
				t2.A_BUILDING_TYPE_id as building_type_id,
				t2.A_COMPANY_id as company_id,
				t2.A_DELIVERY_id as delivery_id,
				t2.A_DRIVE_THRU_id as drive_thru_id,
				t2.A_MCCAFE_TYPE_id as mccafe_type_id,
				t2.A_PRICE_LEVEL_id as price_level_id,
				t2.A_WINDOW_TYPE_id as window_type_id
			from
				casuser.na_abt5 as t1
			left join
				casuser.pbo_dictionary_ml as t2
			on
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;
	
	proc casutil;
		droptable casdata="na_abt5" incaslib="casuser" quiet;
	run;
	
	/* 	------------ End. Добавление атрибутов ПБО ------------- */	


	/************************************************************************************
	 * 7. Календарные признаки и праздники												*
	 ************************************************************************************/

	data work.cldr_prep;
		retain date &calendar_start.;
		do while(date <= &calendar_end.);
			output;
			date + 1;		
		end;
		format date ddmmyy10.;
	run;
	
	proc sql;
		create table work.cldr_prep_features as 
			select
				date, 
				week(date) as week,
				weekday(date) as weekday,
				month(date) as month,
				year(date) as year,
				(case
					when weekday(date) in (1, 7) then 1
					else 0
				end) as weekend_flag
			from
				work.cldr_prep
		;
	quit;
	
	/* загружаем в cas */
	data casuser.russia_weekend;
	set nac.russia_weekend;
	weekend_flag=1;
	run;
	
	/* транспонируем russia_weekend */
	proc cas;
	transpose.transpose /
	   table={name="russia_weekend", caslib="casuser", groupby={"date"}} 
	   transpose={"weekend_flag"} 
	   id={"weekend_name"} 
	   casout={name="russia_weekend_transposed", caslib="casuser", replace=true};
	quit;
	
	/* Заменяем пропуски на нули */
	data casuser.russia_weekend_transposed_zero;
		set casuser.russia_weekend_transposed;
		drop _name_;
		array change _numeric_;
	    	do over change;
	            if change=. then change=0;
	        end;
	run;
	
	/* Объединяем государственные выходные с субботой и воскресеньем */
	proc sql;
		create table work.cldr_prep_features2 as 
			select
				t1.date,
				t1.week,
				t1.weekday,
				t1.month,
				t1.year,
				t1.weekend_flag as regular_weekend_flag,
				case
					when t2.date is not missing then 1
					else t1.weekend_flag
				end as weekend_flag
			from
				work.cldr_prep_features as t1
			left join
				nac.russia_weekend as t2
			on
				t1.date = t2.date
		;
	quit;
	
	/* Загружаем в cas */
	data casuser.cldr_prep_features2;
		set work.cldr_prep_features2;
	run;
	
	/* Добавляем к витрине */
	proc fedsql sessref = casauto;
		create table casuser.na_abt7{options replace = true} as
			select
				t1.*,
				t2.week,
				t2.weekday,
				t2.month,
				t2.year,
				t2.regular_weekend_flag,
				t2.weekend_flag,
				coalesce(t3.Christmas, 0) as Christmas,
				coalesce(t3.Christmas_Day, 0) as Christmas_Day,
				coalesce(t3.Day_After_New_Year, 0) as Day_After_New_Year,
				coalesce(t3.Day_of_Unity, 0) as Day_of_Unity,
				coalesce(t3.Defendence_of_the_Fatherland, 0) as Defendence_of_the_Fatherland,
				coalesce(t3.International_Womens_Day, 0) as International_Womens_Day,
				coalesce(t3.Labour_Day, 0) as Labour_Day,
				coalesce(t3.National_Day, 0) as National_Day,
				coalesce(t3.New_Year_shift, 0) as New_Year_shift, 
 				coalesce(t3.New_year, 0) as New_year,
				coalesce(t3.Victory_Day, 0) as Victory_Day		 
			from
				casuser.na_abt6 as t1
			left join
				casuser.cldr_prep_features2 as t2
			on
				t1.sales_dt = t2.date
			left join
				casuser.russia_weekend_transposed as t3
			on
				t1.sales_dt = t3.date
		;
	quit;
	
	/* Удаляем промежуточные таблицы */		
	proc casutil;
		droptable casdata="na_abt6" incaslib="casuser" quiet;
		droptable casdata="russia_weekend_transposed" incaslib="casuser" quiet;
		droptable casdata="russia_weekend_transposed_zero" incaslib="casuser" quiet;
		droptable casdata="cldr_prep_features2" incaslib="casuser" quiet;
		droptable casdata="russia_weekend" incaslib="casuser" quiet;
	run;
	
	/* Удаляем промежуточные таблицы */		
	proc datasets library=work nolist;
		delete cldr_prep;
		delete cldr_prep_features;
		delete cldr_prep_features2;
	run;

	/* 	------------ End. Календарные признаки и праздники ------------- */	
	

	/************************************************************************************
	 * 8. Признаки описывающие трафик ресторана											*
	 ************************************************************************************/

	/* Сохраняем таблицы для сборки скоринговой выборки */
	data casuser.gc_aggr_smart;
		set nac.gc_aggr_smart;
	run;

	data casuser.gc_aggr_dump;
		set nac.gc_aggr_dump;
	run;

	/* Загружаем продажи ресторанов */
	proc casutil;
		load data=etl_ia.pbo_sales(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm and
				(sales_dt <= '1mar2020'd or sales_dt >= '1jul2020'd)
			)
		) casout='ia_pbo_sales_history' outcaslib='casuser' replace;
	run;
	
	/* Среднее количество чеков за год до начала промо в ресторане */
	proc fedsql sessref=casauto;
		create table casuser.promo_skelet_meat{option replace=true} as
			select
				t1.PROMO_ID,
				t1.pbo_location_id,
				t1.START_DT,
				t1.CHANNEL_CD,
				mean(t2.receipt_qty) as mean_receipt_qty
			from
				casuser.promo_skelet as t1
			inner join
				casuser.ia_pbo_sales_history as t2
			on
				(t1.pbo_location_id = t2.pbo_location_id) and
				(t2.sales_dt < t1.start_dt) and
				(t2.sales_dt >= t1.start_dt - 365)
			group by
				t1.PROMO_ID,
				t1.pbo_location_id,
				t1.START_DT,
				t1.CHANNEL_CD
		;	
	quit;

	/* Добавляем к витрине характеристики трафика ресторана */
	proc fedsql sessref=casauto;
		create table casuser.na_abt8{options replace=true} as
			select
				t1.*,
				coalesce(t2.mean_receipt_qty, t4.mean_receipt_qty, t3.mean_receipt_qty) as mean_receipt_qty,
				coalesce(t2.std_receipt_qty, t3.std_receipt_qty) as std_receipt_qty	
			from
				casuser.na_abt7 as t1
			left join
				casuser.promo_skelet_meat as t4
			on
				t1.pbo_location_id = t4.pbo_location_id and
				t1.promo_id = t4.promo_id
			left join
				casuser.gc_aggr_smart as t2
			on
				(t1.year - 1) = t2.year and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.month = t2.month and
				t1.weekday = t2.weekday
			left join
				casuser.gc_aggr_dump as t3
			on
				(t1.year - 1) = t3.year and
				t1.month = t3.month and
				t1.weekday = t3.weekday
		;
	quit;
	
	proc casutil;
		droptable casdata="gc_aggr_smart" incaslib="casuser" quiet;
		droptable casdata="gc_aggr_dump" incaslib="casuser" quiet;
		droptable casdata="na_abt7" incaslib="casuser" quiet;
		droptable casdata="promo_skelet" incaslib="casuser" quiet;
		droptable casdata="promo_skelet_meat" incaslib="casuser" quiet;
		droptable casdata="ia_pbo_sales_history" incaslib="casuser" quiet;
	run;

	/* 	------------ End. Признаки описывающие трафик ресторана ------------- */	


	/************************************************************************************
	 * 9. Признаки описывающие продажи промо товаров									*
	 ************************************************************************************/
	
	/* Проверяем, был ли сделан промоут таблицы pmix_mastercode_sum */
	%if not(%member_exists(public.pmix_mastercode_sum)) %then 
		%do;
			/* Выгружаем таблицу в cas */
			data public.pmix_mastercode_sum;
				set nac.pmix_mastercode_sum;
			run;
		%end;
	
	/* Снова создадим таблицу с промо акциями */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml{options replace = true} as 
			select
				t1.PROMO_ID,
				t1.start_dt,
				t1.end_dt,
				t3.option_number,
				t1.promo_mechanics,
				t3.product_id,
				t2.pbo_location_id
			from
				&promo_lib..&ia_promo. as t1 
			inner join
				casuser.ia_promo_x_pbo_leaf as t2
			on 
				t1.PROMO_ID = t2.PROMO_ID
			inner join
				casuser.ia_promo_x_product_leaf as t3
			on
				t1.PROMO_ID = t3.PROMO_ID
		;
	quit;
	
	/* Меняем товары на мастеркоды  */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml2{options replace = true} as 
			select distinct
				t1.PROMO_ID,
				t1.start_dt,
				t1.option_number,
				t2.PROD_LVL4_ID,
				t1.pbo_location_id
			from
				casuser.promo_ml as t1
			inner join
				casuser.product_dictionary_ml as t2
			on
				t1.product_id = t2.product_id
		;
	quit;
	
	/* 	Соединяем продажи с промо */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml3{options replace = true} as 
			select
				t1.promo_id,
				t1.pbo_location_id,
				t1.start_dt,
				t1.option_number,
				t2.sales_dt,
				sum(t2.sales_qty) as mean_sales_qty
			from
				casuser.promo_ml2 as t1
			inner join
				public.pmix_mastercode_sum as t2
			on
				t1.PROD_LVL4_ID = t2.PROD_LVL4_ID and
				t1.pbo_location_id = t2.pbo_location_id
			group by
				t1.promo_id,
				t1.pbo_location_id,
				t1.start_dt,
				t1.option_number,
				t2.sales_dt			
		;
	quit;
	
	/* Берем минимальные продажи из всех option number */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml4{options replace = true} as 
			select
				t1.promo_id,
				t1.pbo_location_id,
				t1.start_dt,
				t1.sales_dt,
				min(t1.mean_sales_qty) as mean_sales_qty
			from
				casuser.promo_ml3 as t1
			group by
				t1.promo_id,
				t1.pbo_location_id,
				t1.start_dt,
				t1.sales_dt			
		;
	quit;
	
	/* Считаем агрегаты Промо, ПБО, год, месяц, день недели */
	proc fedsql sessref=casauto;
		create table casuser.pmix_aggr_smart{options replace=true} as
			select
				t1.promo_id,
				t1.pbo_location_id,
				t1.year,
				t1.month,
				t1.weekday,
				mean(t1.mean_sales_qty) as mean_sales_qty,
				std(t1.mean_sales_qty) as std_sales_qty
			from (
				select
					t1.promo_id,
					t1.pbo_location_id,
					year(t1.sales_dt) as year,
					month(t1.sales_dt) as month,
					weekday(t1.sales_dt) as weekday,
					t1.mean_sales_qty
				from
					casuser.promo_ml4 as t1
			) as t1
			group by
				t1.promo_id,
				t1.pbo_location_id,
				t1.year,
				t1.month,
				t1.weekday
		;
	quit;
	
	/* Считаем агрегаты Промо, год, месяц, день недели */
	proc fedsql sessref=casauto;
		create table casuser.pmix_aggr_dump{options replace=true} as
			select
				t1.promo_id,
				t1.year,
				t1.month,
				t1.weekday,
				mean(t1.mean_sales_qty) as mean_sales_qty,
				std(t1.mean_sales_qty) as std_sales_qty
			from (
				select
					t1.promo_id,
					year(t1.sales_dt) as year,
					month(t1.sales_dt) as month,
					weekday(t1.sales_dt) as weekday,
					t1.mean_sales_qty
				from
					casuser.promo_ml4 as t1
			) as t1
			group by
				t1.promo_id,
				t1.year,
				t1.month,
				t1.weekday
		;
	quit;
	
	/* 
		Возможно, год назад мастеркод не продавался в ресторане.
		Например, если ресторан новый. В таком случае просто усредним
		продажи мастеркода до даты начала промо
	*/
	proc fedsql sessref=casauto;
		create table casuser.pmix_basic_aggr_smart{options replace=true} as
			select
				t1.promo_id,
				t1.pbo_location_id,
				mean(t1.mean_sales_qty) as mean_sales_qty,
				std(t1.mean_sales_qty) as std_sales_qty
			from 
				casuser.promo_ml4 as t1
			where
				sales_dt < start_dt
			group by
				t1.promo_id,
				t1.pbo_location_id
		;
	quit;

	/* 
		Возможно, год назад мастеркод не продавался во всей сети.
		Например, товар был временно выведен.
	*/
	proc fedsql sessref=casauto;
		create table casuser.pmix_basic_aggr_dump{options replace=true} as
			select
				t1.promo_id,
				mean(t1.mean_sales_qty) as mean_sales_qty,
				std(t1.mean_sales_qty) as std_sales_qty
			from 
				casuser.promo_ml4 as t1
			where
				sales_dt < start_dt
			group by
				t1.promo_id
		;
	quit;
	
	/* 	Добавляем к витрине характеристики характеристики продаж мастеркодов */
	proc fedsql sessref=casauto;
		create table casuser.na_abt9{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PROMO_LIFETIME,
				put(t1.channel_cd, $12.) as channel_cd,
				t1.promo_mechanics_name,
				t1.np_gift_price_amt,
				t1.promo_group_id,
				t1.sales_dt,
				t1.promo_id,
				t1.NUMBER_OF_OPTIONS,
				t1.NUMBER_OF_PRODUCTS,
				t1.NECESSARY_AMOUNT,
				t1.Breakfast,
				t1.ColdDrinks,
				t1.Condiments,
				t1.Desserts,
				t1.Fries,
				t1.HotDrinks,
				t1.McCafe,
				t1.Nonproduct,
				t1.Nuggets,
				t1.SNCORE,
				t1.SNEDAP,
				t1.SNPREMIUM,
				t1.Shakes,
				t1.StartersSalad,
				t1.UndefinedProductGroup,
				t1.ValueMeal,
				t1.LVL3_ID,
				t1.LVL2_ID,
				t1.AGREEMENT_TYPE_ID,
				t1.BREAKFAST_ID,
				t1.BUILDING_TYPE_ID,
				t1.COMPANY_ID,
				t1.DELIVERY_ID,
				t1.DRIVE_THRU_ID,
				t1.MCCAFE_TYPE_ID,
				t1.PRICE_LEVEL_ID,
				t1.WINDOW_TYPE_ID,
				t1.week,
				t1.weekday,
				t1.month,
				t1.year,
				t1.regular_weekend_flag,
				t1.weekend_flag,
				t1.CHRISTMAS,
				t1.CHRISTMAS_DAY,
				t1.DAY_AFTER_NEW_YEAR,
				t1.DAY_OF_UNITY,
				t1.DEFENDENCE_OF_THE_FATHERLAND,
				t1.INTERNATIONAL_WOMENS_DAY,
				t1.LABOUR_DAY,
				t1.NATIONAL_DAY,
				t1.NEW_YEAR_SHIFT,
				t1.NEW_YEAR,
				t1.VICTORY_DAY,
				t1.MEAN_RECEIPT_QTY,
				t1.STD_RECEIPT_QTY,
				coalesce(t2.mean_sales_qty, t3.mean_sales_qty, t4.mean_sales_qty, t5.mean_sales_qty) as mean_sales_qty,
				coalesce(t2.std_sales_qty, t3.std_sales_qty, t4.std_sales_qty, t5.std_sales_qty) as std_sales_qty,
				. as n_a,
				. as t_a
			from
				casuser.na_abt8 as t1
			left join
				casuser.pmix_aggr_smart as t2
			on
				t1.promo_id = t2.promo_id and
				(t1.year - 1) = t2.year and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.month = t2.month and
				t1.weekday = t2.weekday
			left join
				casuser.pmix_aggr_dump as t3
			on
				t1.promo_id = t3.promo_id and
				(t1.year - 1) = t3.year and
				t1.month = t3.month and
				t1.weekday = t3.weekday
			left join
				casuser.pmix_basic_aggr_smart as t4
			on
				t1.promo_id = t4.promo_id and
				t1.pbo_location_id = t4.pbo_location_id
			left join
				casuser.pmix_basic_aggr_dump as t5
			on
				t1.promo_id = t5.promo_id	
		;
	quit;
	
	proc casutil;
		droptable casdata="na_abt8" incaslib="casuser" quiet;

		droptable casdata="promo_ml" incaslib="casuser" quiet;
		droptable casdata="promo_ml2" incaslib="casuser" quiet;
		droptable casdata="promo_ml3" incaslib="casuser" quiet;
		droptable casdata="promo_ml4" incaslib="casuser" quiet;

		droptable casdata="pmix_aggr_smart" incaslib="casuser" quiet;
		droptable casdata="pmix_aggr_dump" incaslib="casuser" quiet;
		droptable casdata="pmix_basic_aggr_smart" incaslib="casuser" quiet;
		droptable casdata="pmix_basic_aggr_dump" incaslib="casuser" quiet;
		
	run;
	
	/* 	------------ End. Признаки описывающие продажи промо товаров ------------- */	

	/************************************************************************************
	 * 10. Дополнительная кодировка промо категорий										*
	 ************************************************************************************/
	/*
			Закодируем промо категории через целевую переменную.
		Для каждой акции на истории усредним n_a (t_a) по всем промо акциям в том
		же ресторане с той же механикой. Если ресторан новый
		и у него недостает акций для усреднения, то усредним по всем ресторанам.
	*/
	
	/* Проверяем, был ли сделан промоут таблицы score_mean_target_variable_pbo */
	%if not(%member_exists(public.score_mean_target_variable_pbo)) %then 
		%do;
			/* Загружаем из NAC таблицы */
			data public.score_mean_target_variable_pbo;
				set nac.score_mean_target_variable_pbo;
			run;			
		%end;

	/* Проверяем, был ли сделан промоут таблицы score_mean_target_variable */
	%if not(%member_exists(public.score_mean_target_variable)) %then 
		%do;
			/* Загружаем из NAC таблицы */
			data public.score_mean_target_variable;
				set nac.score_mean_target_variable;
			run;			
		%end;
	
	/* Добавляем к витрине */
	proc fedsql sessref=casauto;
		create table casuser.na_abt10{option replace=true} as
			select
				t1.*,
				coalesce(t2.mean_n_a, t3.mean_n_a) as mean_past_n_a,
				coalesce(t2.mean_t_a, t3.mean_t_a) as mean_past_t_a
			from
				casuser.na_abt9 as t1
			left join
				public.score_mean_target_variable_pbo as t2
			on
				t1.promo_mechanics_name = t2.promo_mechanics_name and
				t1.pbo_location_id = t2.pbo_location_id
			left join
				public.score_mean_target_variable as t3
			on
				t1.promo_mechanics_name = t3.promo_mechanics_name

		;	
	quit;

	/* Удаляем промежуточные таблицы */		
	proc casutil;
		droptable casdata="na_abt9" incaslib="casuser" quiet;
	run;

	/* 	------------ End. Дополнительная кодировка промо категорий ------------- */	
	
	
	/************************************************************************************
	 * 11. Добавляем погоду																*
	 ************************************************************************************/

	/* Загружаем таблицу с погодой */
	proc casutil;
		load data=etl_ia.weather(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm
			)
		) casout='ia_weather' outcaslib='casuser' replace;	
	run;
	
	/* Соединяем с витриной	 */
	proc fedsql sessref=casauto;
		create table casuser.na_abt11{option replace=true} as
			select
				t1.*,
				t2.temperature,
				t2.precipitation
			from
				casuser.na_abt10 as t1
			left join
				casuser.ia_weather as t2
			on
				t1.sales_dt = t2.report_dt and
				t1.pbo_location_id = t2.pbo_location_id
		;	
	quit;

	/* Удаляем промежуточные таблицы */		
	proc casutil;
		droptable casdata="na_abt10" incaslib="casuser" quiet;
		droptable casdata="ia_weather" incaslib="casuser" quiet;
	run;

	/* ------------ End. Добавляем погоду ------------- */	

	/************************************************************************************
	 * 12. Добавляем one hot закодированную механику промо для разложения GC																*
	 ************************************************************************************/

	proc fedsql sessref=casauto;
		create table casuser.promo_mechanics{options replace=true} as
			select distinct
				promo_id,
				promo_mechanics_name,
				1 as promo_flag
			from
				casuser.na_abt11
		;
	quit;

	/* Джоиним одно с другим */
	proc fedsql sessref=casauto;
		create table casuser.all_combination{options replace=true} as
			select
				t1.promo_id,
				trim(t2.promo_mechanics_name) as promo_mechanics_name
			from
				(select distinct promo_id from casuser.promo_mechanics) as t1,
				casuser.unique_promo_mechanics_name as t2
		;
	quit;
		
	/* Заполняем пропуски нулями */
	proc fedsql sessref=casauto;
		create table casuser.promo_mechanics_zero{options replace=true} as
			select
				t1.promo_id,
				t1.promo_mechanics_name,
				coalesce(t2.promo_flag, 0) as promo_flag
			from
				casuser.all_combination as t1
			left join
				casuser.promo_mechanics as t2
			on
				t1.promo_id = t2.promo_id and
				t1.promo_mechanics_name = t2.promo_mechanics_name
		;
	quit;
	
	/* Транспонируем механику промо в вектор */
	proc cas;
	transpose.transpose /
		table = {
			name="promo_mechanics_zero",
			caslib="casuser",
			groupby={"promo_id"}}
		transpose={"promo_flag"} 
		id={"promo_mechanics_name"} 
		casout={name="promo_mechanics_one_hot", caslib="casuser", replace=true};
	quit;

	
	/* Добавляем переменные к витрине */
	proc fedsql sessref=casauto;
		create table casuser.promo_effectivness_scoring{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PROMO_LIFETIME,
				t1.channel_cd,
				t1.promo_mechanics_name,
				t1.np_gift_price_amt,
				t1.promo_group_id,
				t1.sales_dt,
				t1.NUMBER_OF_OPTIONS,
				t1.NUMBER_OF_PRODUCTS,
				t1.NECESSARY_AMOUNT,
				t1.Breakfast,
				t1.ColdDrinks,
				t1.Condiments,
				t1.Desserts,
				t1.Fries,
				t1.HotDrinks,
				t1.McCafe,
				t1.Nonproduct,
				t1.Nuggets,
				t1.SNCORE,
				t1.SNEDAP,
				t1.SNPREMIUM,
				t1.Shakes,
				t1.StartersSalad,
				t1.UndefinedProductGroup,
				t1.ValueMeal,
				t1.LVL3_ID,
				t1.LVL2_ID,
				t1.AGREEMENT_TYPE_ID,
				t1.BREAKFAST_ID,
				t1.BUILDING_TYPE_ID,
				t1.COMPANY_ID,
				t1.DELIVERY_ID,
				t1.DRIVE_THRU_ID,
				t1.MCCAFE_TYPE_ID,
				t1.PRICE_LEVEL_ID,
				t1.WINDOW_TYPE_ID,
				t1.week,
				t1.weekday,
				t1.month,
				t1.year,
				t1.regular_weekend_flag,
				t1.weekend_flag,
				t1.CHRISTMAS,
				t1.CHRISTMAS_DAY,
				t1.DAY_AFTER_NEW_YEAR,
				t1.DAY_OF_UNITY,
				t1.DEFENDENCE_OF_THE_FATHERLAND,
				t1.INTERNATIONAL_WOMENS_DAY,
				t1.LABOUR_DAY,
				t1.NATIONAL_DAY,
				t1.NEW_YEAR_SHIFT,
				t1.NEW_YEAR,
				t1.VICTORY_DAY,
				t1.MEAN_RECEIPT_QTY,
				t1.STD_RECEIPT_QTY,
				t1.mean_sales_qty,
				t1.std_sales_qty,
				t1.n_a,
				t1.t_a,
				t1.mean_past_n_a,
				t1.mean_past_t_a,
				t1.temperature,
				t1.precipitation,
				t2.*
			from
				casuser.na_abt11 as t1
			left join
				casuser.promo_mechanics_one_hot as t2
			on
				t1.promo_id = t2.promo_id
		;
	quit;

	/* Удаляем промежуточные таблицы */		
	proc casutil;
		droptable casdata="all_combination" incaslib="casuser" quiet;
		droptable casdata="promo_mechanics_zero" incaslib="casuser" quiet;
		droptable casdata="promo_mechanics_one_hot" incaslib="casuser" quiet;
		droptable casdata="promo_mechanics" incaslib="casuser" quiet;
		droptable casdata="na_abt11" incaslib="casuser" quiet;

	run;





%mend;


%macro promo_effectivness_predict(
	model = na_prediction_model,
	target = na,
	data = casuser.promo_effectivness_scoring
	);
	/*
		Макрос, который прогнозирует эффективность промо акций при
		помощи обученных моеделей.
		Параметры:
		----------
			* model : Название для модели
			* target : Название целевой переменной (na или ta)
			* data : скоринговая выборка
	*/
	/****** Скоринг ******/
    proc astore;
        upload RSTORE=casuser.&model. store="/data/ETL_BKP/&model";
    run;

	proc casutil;
	    droptable casdata="promo_effectivness_&target._predict" incaslib="casuser" quiet;
	run;

	proc astore;
		score data=&data.
		copyvars=(_all_)
		rstore=casuser.&model
		out=casuser.promo_effectivness_&target._predict;
	quit;
	
	proc casutil;
	    promote casdata="promo_effectivness_&target._predict" incaslib="casuser" outcaslib="casuser";
	run;

	/* Сохраняем прогноз в nac */
	data nac.promo_effectivness_&target._predict;
		set casuser.promo_effectivness_&target._predict;
	run;

%mend;

