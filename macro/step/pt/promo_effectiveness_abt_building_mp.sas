/* Скрипт собирающий обучающую выборку для модели, прогнозирующей Na */

cas casauto;
caslib _all_ assign;

/* 
	Список выходных дней в РФ с 2018 по 2023.
	Внутрь макроса это data step положить не получается, поэтому
	вынесен это шаг отдельно
*/
data casuser.russia_weekend;
input date :yymmdd10. weekend_name $64.;
format date yymmddd10.;
datalines;
2018-01-01 New_year
2018-01-02 Day_After_New_Year
2018-01-07 Christmas
2018-02-23 Defendence_of_the_Fatherland
2018-03-08 International_Womens_Day
2018-05-09 Victory_Day
2018-06-12 National_Day
2018-11-04 Day_of_Unity
2018-05-01 Labour_Day
2018-12-25 Christmas_Day
2019-01-01 New_year
2019-01-02 Day_After_New_Year
2019-01-07 Christmas
2019-02-23 Defendence_of_the_Fatherland
2019-03-08 International_Womens_Day
2019-05-09 Victory_Day
2019-06-12 National_Day
2019-11-04 Day_of_Unity
2019-05-01 Labour_Day
2019-12-25 Christmas_Day
2020-01-01 New_year
2020-01-02 Day_After_New_Year
2020-01-07 Christmas
2020-02-23 Defendence_of_the_Fatherland
2020-03-08 International_Womens_Day
2020-05-09 Victory_Day
2020-06-12 National_Day
2020-11-04 Day_of_Unity
2020-05-01 Labour_Day
2020-12-25 Christmas_Day
2021-01-01 New_year
2021-01-02 Day_After_New_Year
2021-01-07 Christmas
2021-02-23 Defendence_of_the_Fatherland
2021-03-08 International_Womens_Day
2021-05-09 Victory_Day
2021-06-12 National_Day
2021-11-04 Day_of_Unity
2021-05-01 Labour_Day
2021-12-25 Christmas_Day
2022-01-01 New_year
2022-01-02 Day_After_New_Year
2022-01-07 Christmas
2022-02-23 Defendence_of_the_Fatherland
2022-03-08 International_Womens_Day
2022-05-09 Victory_Day
2022-06-12 National_Day
2022-11-04 Day_of_Unity
2022-05-01 Labour_Day
2022-01-03 New_Year_shift
2022-12-25 Christmas_Day
2023-01-01 New_year
2023-01-02 Day_After_New_Year
2023-01-07 Christmas
2023-02-23 Defendence_of_the_Fatherland
2023-03-08 International_Womens_Day
2023-05-09 Victory_Day
2023-06-12 National_Day
2023-11-04 Day_of_Unity
2023-05-01 Labour_Day
2023-01-02 New_Year_shift
2023-12-25 Christmas_Day
;
run;


%macro promo_effectiveness_abt_building(
	promo_lib = casuser, 
	ia_promo = past_promo,
	ia_promo_x_pbo = promo_pbo_enh,
	ia_promo_x_product = promo_prod_enh,
	hist_start_dt = date '2019-01-01',
	ia_media = media_enh,
	filter = t1.channel_cd = 'ALL',
	calendar_start = '01jan2017'd,
	calendar_end = '01jan2022'd
);
/*
	Скрипт, который собирает обучающую выборку для модели прогнозирующей na (и ta).

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
	10. Добавление целевой переменной
	11.	Убираем временные закрытия
	12.	Удалем пары промо-ресторан, в которых одни пропуски
	13. Обрезаем концы (если промо в ресторане закончилось раньше,
		 либо началось позже)
	14. Заменеям на миссинги значения, которые отклоняются на 2,5 стандартных 
		 отклонений от среднего.
	15. Если среднее t_a < 10, то пропуски заменяем нулями, иначе заменяем средним
	16. Дополнительная кодировка промо категорий
	17. Добавляем погоду
	

	Параметры:
	----------
		* promo_lib: библиотека, где лежат таблицы с промо (предполагается,
			что таблицы лежат в cas)
		* ia_promo: название таблицы с информацией о промо 
		* ia_promo_x_pbo: название таблицы с привязкой промо к ресторнам
		* ia_promo_x_product: название таблицы с привязкой промо к товарам
		* hist_start_dt: рассматриваем все промо, начавшиеся после этой даты
		* filter : фильтр для таблицы с промо (например, убрать каналы)
		* calendar_start : старт интервала формирования календарных признаков
		* calendar_end : конец интервала формирования календарных признаков
	Выход:
	------
		* Запромоученая в casuser и скопированная в casuser таблица na_train
*/


	/************************************************************************************
	 * 1. Вычисление каркаса таблицы промо акций										*
	 ************************************************************************************/

	/* Загружаем товарную и географическую иерархии */
	proc casutil;
		load data=etl_ia.pbo_loc_hierarchy(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm
			)
		) casout='ia_pbo_loc_hierarchy' outcaslib='casuser' replace;	
	
		load data=etl_ia.product_hierarchy(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm
			)
		) casout='ia_product_hierarchy' outcaslib='casuser' replace;
	run;
	
	/* Создаем таблицу связывающую PBO на листовом уровне и на любом другом */
	proc fedsql sessref=casauto;
		create table casuser.pbo_hier_flat{options replace=true} as
			select
				t1.pbo_location_id, 
				t2.PBO_LOCATION_ID as LVL3_ID,
				t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				1 as LVL1_ID
			from 
				(select * from casuser.ia_pbo_loc_hierarchy where pbo_location_lvl=4) as t1
			left join 
				(select * from casuser.ia_pbo_loc_hierarchy where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
		;
		create table casuser.lvl4{options replace=true} as 
			select distinct
				pbo_location_id as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_hier_flat
		;
		create table casuser.lvl3{options replace=true} as 
			select distinct
				LVL3_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_hier_flat
		;
		create table casuser.lvl2{options replace=true} as 
			select distinct
				LVL2_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_hier_flat
		;
		create table casuser.lvl1{options replace=true} as 
			select 
				1 as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_hier_flat
		;
	quit;
	
	/* Соединяем в единый справочник ПБО */
	data casuser.pbo_lvl_all;
		set casuser.lvl4 casuser.lvl3 casuser.lvl2 casuser.lvl1;
	run;
	
	/* Создаем таблицу связывающую товары на листовом уровне и на любом другом */
	proc fedsql sessref=casauto;
	   create table casuser.product_hier_flat{options replace=true} as
			select t1.product_id, 
				   t2.product_id  as LVL4_ID,
				   t3.product_id  as LVL3_ID,
				   t3.PARENT_product_id as LVL2_ID, 
				   1 as LVL1_IDpr
			from 
			(select * from casuser.ia_product_hierarchy where product_lvl=5) as t1
			left join 
			(select * from casuser.ia_product_hierarchy where product_lvl=4) as t2
			on t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
			left join 
			(select * from casuser.ia_product_hierarchy where product_lvl=3) as t3
			on t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
	 	;
		create table casuser.lvl5{options replace=true} as 
			select distinct
				product_id as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
		create table casuser.lvl4{options replace=true} as 
			select distinct
				LVL4_ID as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
		create table casuser.lvl3{options replace=true} as 
			select distinct
				LVL3_ID as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
		create table casuser.lvl2{options replace=true} as 
			select distinct
				LVL2_ID as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
		create table casuser.lvl1{options replace=true} as 
			select distinct
				1 as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
	quit;
	
	/* Соединяем в единый справочник ПБО */
	data casuser.product_lvl_all;
		set casuser.lvl5 casuser.lvl4 casuser.lvl3 casuser.lvl2 casuser.lvl1;
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

	/* Формируем каркас витрины.
		 Inner join  потому что если нет списка ПБО для акции,
		 такие акции не рассматриваем.
	*/
	proc fedsql sessref=casauto;
		create table casuser.promo_skelet{options replace = true} as 
			select
				t1.PROMO_ID,
				t2.pbo_location_id,
				t1.START_DT,
				t1.END_DT,
				(t1.END_DT - t1.START_DT + 1) as promo_lifetime,
				t1.CHANNEL_CD,
				t1.NP_GIFT_PRICE_AMT,
				t1.PROMO_GROUP_ID,
				compress(promo_mechanics,'', 'ak') as promo_mechanics_name
			from
				&promo_lib..&ia_promo. as t1
			inner join
				casuser.ia_promo_x_pbo_leaf as t2
			on 
				t1.PROMO_ID = t2.PROMO_ID
			where
				&filter.
				and t1.start_dt >= &hist_start_dt.
		;
	quit;
		
	/* Расшиваем интервалы по дням */
	data casuser.na_abt1;
		set casuser.promo_skelet;
		format sales_dt DATE9.;
		do sales_dt=start_dt to end_dt;
			output;
		end;
	run;
	
	/* Сохраняем связующие таблицы для скоринга */
	data casuser.pbo_lvl_all;
		set casuser.pbo_lvl_all;
	run;

	/* Сохраняем связующие таблицы для скоринга */
	data casuser.product_lvl_all;
		set casuser.product_lvl_all;
	run;

	/* Удаляем промежуточные таблицы */
	proc casutil;
		droptable casdata="ia_pbo_loc_hierarchy" incaslib="casuser" quiet;
		droptable casdata="ia_product_hierarchy" incaslib="casuser" quiet;
		droptable casdata="pbo_hier_flat" incaslib="casuser" quiet;
		droptable casdata="product_hier_flat" incaslib="casuser" quiet;
		droptable casdata="lvl5" incaslib="casuser" quiet;
		droptable casdata="lvl4" incaslib="casuser" quiet;
		droptable casdata="lvl3" incaslib="casuser" quiet;
		droptable casdata="lvl2" incaslib="casuser" quiet;
		droptable casdata="lvl1" incaslib="casuser" quiet;
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
	
	/* Добавляем признаки в витрину и добавляем номер недели для следующего шага */
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
	
	/* Удаляем промежуточные таблицы */
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

	/* Загружаем информацию о товарах */
	proc casutil;
		load data=etl_ia.product(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm
			)
		) casout='ia_product' outcaslib='casuser' replace;
		
		load data=etl_ia.product_HIERARCHY(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm
			)
		) casout='IA_product_HIERARCHY' outcaslib='casuser' replace;
		
		load data=etl_ia.product_ATTRIBUTES(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm
			)
		) casout='IA_product_ATTRIBUTES' outcaslib='casuser' replace;
	run;
	  
	proc cas;
	transpose.transpose /
	   table={name="ia_product_attributes", caslib="casuser", groupby={"product_id"}} 
	   attributes={{name="product_id"}} 
	   transpose={"PRODUCT_ATTR_VALUE"} 
	   prefix="A_" 
	   id={"PRODUCT_ATTR_NM"} 
	   casout={name="attr_transposed", caslib="casuser", replace=true};
	quit;
	
	proc fedsql sessref=casauto;
		create table casuser.product_hier_flat{options replace=true} as
			select 
				t1.product_id, 
				t2.product_id  as LVL4_ID,
				t3.product_id  as LVL3_ID,
				t3.PARENT_product_id as LVL2_ID, 
				1 as LVL1_ID
			from 
				(select * from casuser.ia_product_hierarchy where product_lvl=5) as t1
			left join 
				(select * from casuser.ia_product_hierarchy where product_lvl=4) as t2
			on 
				t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
			left join 
				(select * from casuser.ia_product_hierarchy where product_lvl=3) as t3
			on
				t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
	 	;
	quit;
	
	proc fedsql sessref=casauto;
		create table casuser.product_dictionary_ml{options replace=true} as
			select 
				t1.product_id, 
				coalesce(t1.lvl4_id,-9999) as prod_lvl4_id,
				coalesce(t1.lvl3_id,-999) as prod_lvl3_id,
				coalesce(t1.lvl2_id,-99) as prod_lvl2_id,
				coalesce(t15.product_nm,'NA') as product_nm,
				coalesce(t14.product_nm,'NA') as prod_lvl4_nm,
				coalesce(t13.product_nm,'NA') as prod_lvl3_nm,
				coalesce(t12.product_nm,'NA') as prod_lvl2_nm,
				compress(coalesce(t12.product_nm,'NA'),'', 'ak') as prod_lvl2_name,
				t3.A_HERO,
				t3.A_ITEM_SIZE,
				t3.A_OFFER_TYPE,
				t3.A_PRICE_TIER
			from
				casuser.product_hier_flat as t1
			left join
				casuser.attr_transposed as t3
			on
				t1.product_id = t3.product_id
			left join
				casuser.IA_product as t15
			on
				t1.product_id = t15.product_id
			left join
				casuser.IA_product as t14
			on
				t1.lvl4_id = t14.product_id
			left join
				casuser.IA_product as t13
			on
				t1.lvl3_id = t13.product_id
			left join
				casuser.IA_product as t12
			on
				t1.lvl2_id = t12.product_id
		;
	quit;

	/* Копируем таблицу product_dictionary_ml в casuser */
	data casuser.product_dictionary_ml;
		set casuser.product_dictionary_ml;
		category_name = translate(trim(prod_lvl2_nm),'_',' ', '_', '&', '_', '-');
	run;
	
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
	
	/* Добавляем признаки в витрину. Inner join потому,
			что если нет списка товаров для акции,
			такие акции не рассматриваем. */
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
			inner join
				casuser.promo_category_transposed_zero as t2
			on
				t1.promo_id = t2.promo_id
		;
	quit;
	
	/* Удаляем промежуточные таблицы */
	proc casutil;
		droptable casdata="promo_category" incaslib="casuser" quiet;
		droptable casdata="promo_category_transposed" incaslib="casuser" quiet;
		droptable casdata="promo_category_transposed_zero" incaslib="casuser" quiet;
		droptable casdata="attr_transposed" incaslib="casuser" quiet;
		droptable casdata="product_hier_flat" incaslib="casuser" quiet;
		droptable casdata="IA_product" incaslib="casuser" quiet;
		droptable casdata="IA_product_HIERARCHY" incaslib="casuser" quiet;
		droptable casdata="IA_product_ATTRIBUTES" incaslib="casuser" quiet;
		droptable casdata="na_abt3" incaslib="casuser" quiet;
	run;

	/* 	------------ End. Добавление информации о товарах в промо ------------- */	


	/************************************************************************************
	 * 6. Добавляем атрибуты ПБО														*
	 ************************************************************************************/

	/* Загружаем справочники ПБО */
	proc casutil;	
		load data=etl_ia.pbo_location(
				where=(
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
				)
			) casout='ia_pbo_location' outcaslib='casuser' replace;
	
		load data=etl_ia.PBO_LOC_HIERARCHY(
				where=(
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
				)
			) casout='ia_pbo_loc_hierarchy' outcaslib='casuser' replace;
	
		load data=etl_ia.PBO_LOC_ATTRIBUTES(
				where=(
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
				)
			) casout='ia_pbo_loc_attributes' outcaslib='casuser' replace;
	run;
	
	proc cas;
	transpose.transpose /
	   table={name="ia_pbo_loc_attributes", caslib="casuser", groupby={"pbo_location_id"}} 
	   attributes={{name="pbo_location_id"}} 
	   transpose={"PBO_LOC_ATTR_VALUE"} 
	   prefix="A_" 
	   id={"PBO_LOC_ATTR_NM"} 
	   casout={name="attr_transposed", caslib="casuser", replace=true};
	quit;
	
	proc fedsql sessref=casauto;
	   create table casuser.pbo_hier_flat{options replace=true} as
			select t1.pbo_location_id, 
				   t2.PBO_LOCATION_ID as LVL3_ID,
				   t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				   1 as LVL1_ID
			from 
			(select * from casuser.ia_pbo_loc_hierarchy where pbo_location_lvl=4) as t1
			left join 
			(select * from casuser.ia_pbo_loc_hierarchy where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
	 		;
	quit;
	
	proc fedsql sessref=casauto;
		create table casuser.pbo_dictionary_ml{options replace=true} as
			select 
				t2.pbo_location_id, 
				coalesce(t2.lvl3_id,-999) as lvl3_id,
				coalesce(t2.lvl2_id,-99) as lvl2_id,
				coalesce(t14.pbo_location_nm,'NA') as pbo_location_nm,
				coalesce(t13.pbo_location_nm,'NA') as lvl3_nm,
				coalesce(t12.pbo_location_nm,'NA') as lvl2_nm,
				t3.A_AGREEMENT_TYPE,
				t3.A_BREAKFAST,
				t3.A_BUILDING_TYPE,
				t3.A_COMPANY,
				t3.A_DELIVERY,
				t3.A_DRIVE_THRU,
				t3.A_MCCAFE_TYPE,
 				t3.A_PRICE_LEVEL,  /* странные значения принимает переменная */
				t3.A_WINDOW_TYPE
			from 
				casuser.pbo_hier_flat t2
			left join
				casuser.attr_transposed t3
			on
				t2.pbo_location_id=t3.pbo_location_id
			left join
				casuser.IA_PBO_LOCATION t14
			on 
				t2.pbo_location_id=t14.pbo_location_id
			left join
				casuser.IA_PBO_LOCATION t13
			on 
				t2.lvl3_id=t13.pbo_location_id
			left join
				casuser.IA_PBO_LOCATION t12
			on
				t2.lvl2_id=t12.pbo_location_id;
	quit;
	
	/* Перекодировка текстовых переменных. */
	%macro text_encoding(table, variable);
		/*
		Параметры:
			table : таблица в которой хотим заненить текстовую переменную
			variable : название текстовой переменной
		Выход:
			* Таблица table с дополнительным столбцом variable_id
			* Таблица encoding_variable с привозкой id к старым значениям
		*/
		proc casutil;
	 		droptable casdata="encoding_&variable." incaslib="casuser" quiet;
	 	run;
	
		proc fedsql sessref=casauto;
			create table casuser.unique{options replace=true} as
				select distinct
					&variable
				from
					&table. 
				;
		quit;
	
		data work.unique;
			set casuser.unique;
		run;
	
		data casuser.encoding_&variable.;
			set work.unique;
			&variable._id = _N_;
		run;
	
		data casuser.encoding_&variable.;
			set casuser.encoding_&variable.;
		run;
	
		proc fedsql sessref = casauto;
			create table casuser.&table.{options replace=true} as 
				select
					t1.*,
					t2.&variable._id
				from
					&table. as t1
				left join
					casuser.encoding_&variable. as t2
				on
					t1.&variable = t2.&variable
			;
		quit;

		/* Удаляем временные таблицы */
		proc casutil;
	 		droptable casdata="encoding_&variable." incaslib="casuser" quiet;
	 	run;

		proc datasets library=work nolist;
			delete unique;
		run;
	
	%mend;
	
	%text_encoding(casuser.pbo_dictionary_ml, A_AGREEMENT_TYPE);
	%text_encoding(casuser.pbo_dictionary_ml, A_BREAKFAST);
	%text_encoding(casuser.pbo_dictionary_ml, A_BUILDING_TYPE);
	%text_encoding(casuser.pbo_dictionary_ml, A_COMPANY);
	%text_encoding(casuser.pbo_dictionary_ml, A_DELIVERY);
	%text_encoding(casuser.pbo_dictionary_ml, A_MCCAFE_TYPE);
	%text_encoding(casuser.pbo_dictionary_ml, A_PRICE_LEVEL);
	%text_encoding(casuser.pbo_dictionary_ml, A_DRIVE_THRU);
	%text_encoding(casuser.pbo_dictionary_ml, A_WINDOW_TYPE);
	
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
/* 				t2.A_PRICE_LEVEL_id as price_level_id, */
				t2.A_WINDOW_TYPE_id as window_type_id
			from
				casuser.na_abt5 as t1
			left join
				casuser.pbo_dictionary_ml as t2
			on
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;

	/* Копируем таблицу pbo_dictionary_ml в casuser для сборки скоринга */
	data casuser.pbo_dictionary_ml;
		set casuser.pbo_dictionary_ml;
	run;
	
	/* Удаляем промежуточные таблицы */
	proc casutil;
		droptable casdata="na_abt5" incaslib="casuser" quiet;
		droptable casdata="attr_transposed" incaslib="casuser" quiet;
		droptable casdata="pbo_hier_flat" incaslib="casuser" quiet;
		droptable casdata="pbo_dictionary_ml" incaslib="casuser" quiet;
		droptable casdata='ia_pbo_location' incaslib='casuser' quiet;
		droptable casdata='IA_PBO_LOC_HIERARCHY' incaslib='casuser' quiet;
		droptable casdata='IA_PBO_LOC_ATTRIBUTES' incaslib='casuser' quiet;
	run;

	/* 	------------ End. Добавление атрибутов ПБО ------------- */	


	/************************************************************************************
	 * 7. Календарные признаки и праздники												*
	 ************************************************************************************/

	/* Создаем каркас с датами */
	data work.cldr_prep;
		retain date &calendar_start.;
		do while(date <= &calendar_end.);
			output;
			date + 1;		
		end;
		format date ddmmyy10.;
	run;
	
	/* Считаем календарные признаки */
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
	set casuser.russia_weekend;
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
				casuser.russia_weekend as t2
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

	/* 	------------ End. Календарные признаки и праздники ------------- */	


	/************************************************************************************
	 * 8. Признаки описывающие трафик ресторана											*
	 ************************************************************************************/

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
	
	/* Агрегируем чеки до ПБО, год, месяц, день недели */
	proc fedsql sessref=casauto;
		create table casuser.gc_aggr_smart{options replace=true} as
			select
				t1.pbo_location_id,
				t1.year,
				t1.month,
				t1.weekday,
				mean(t1.receipt_qty) as mean_receipt_qty,
				std(t1.receipt_qty) as std_receipt_qty
			from (
				select
					pbo_location_id,
					year(sales_dt) as year,
					month(sales_dt) as month,
					weekday(sales_dt) as weekday,
					receipt_qty
				from
					casuser.ia_pbo_sales_history
				where
					channel_cd = 'ALL'
			) as t1
			group by
				t1.pbo_location_id,
				t1.year,
				t1.month,
				t1.weekday			
		;
	quit;
	
	/* Агрегируем чеки до год, месяц, день недели */
	proc fedsql sessref=casauto;
		create table casuser.gc_aggr_dump{options replace=true} as
			select
				t1.year,
				t1.month,
				t1.weekday,
				mean(t1.receipt_qty) as mean_receipt_qty,
				std(t1.receipt_qty) as std_receipt_qty
			from (
				select
					year(sales_dt) as year,
					month(sales_dt) as month,
					weekday(sales_dt) as weekday,
					receipt_qty
				from
					casuser.ia_pbo_sales_history
				where
					channel_cd = 'ALL'
			) as t1
			group by
				t1.year,
				t1.month,
				t1.weekday			
		;
	quit;

	/* Сохраняем таблицы для сборки скоринговой выборки */
	data casuser.gc_aggr_smart;
		set casuser.gc_aggr_smart;
	run;

	data casuser.gc_aggr_dump;
		set casuser.gc_aggr_dump;
	run;

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

	/* Удаляем промежуточные таблицы */	
	proc casutil;
		droptable casdata="gc_aggr_smart" incaslib="casuser" quiet;
		droptable casdata="gc_aggr_dump" incaslib="casuser" quiet;
		droptable casdata="na_abt7" incaslib="casuser" quiet;
		droptable casdata="ia_pbo_sales_history" incaslib="casuser" quiet;
		droptable casdata="promo_skelet" incaslib="casuser" quiet;		
		droptable casdata="promo_skelet_meat" incaslib="casuser" quiet;		
	run;

	/* 	------------ End. Признаки описывающие трафик ресторана ------------- */	


	/************************************************************************************
	 * 9. Признаки описывающие продажи промо товаров									*
	 ************************************************************************************/

	/* Если до этого уже был промоут таблицы pmix_mastercode_sum, удалим ее */	
	proc casutil;
		droptable casdata="pmix_mastercode_sum" incaslib="casuser" quiet;
	run;

	/* Создаем временные ряды продаж мастеркодов */
	proc fedsql sessref=casauto;
		create table casuser.pmix_mastercode_sum{options replace=true} as
			select
				t2.PROD_LVL4_ID,
				t1.sales_dt,
				t1.pbo_location_id,
				sum(sum(t1.sales_qty, t1.sales_qty_promo)) as sales_qty	
			from 
				MN_SHORT.PMIX_SALES as t1 
			inner join
				casuser.product_dictionary_ml as t2
			on
				t1.product_id = t2.product_id
				and t1.channel_cd = 'ALL'
			group by 1,2,3
			;	
	quit;

	/* Промоутим таблицу для сборки скоринговой витрины */
	proc casutil;
		promote casdata="pmix_mastercode_sum" incaslib="casuser" outcaslib="casuser";
	run;
	
	/* 	Снова создадим таблицу с промо акциями */
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
			where
				&filter.
		;
	quit;
	
	/* 	Меняем товары на мастеркоды  */
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
				casuser.pmix_mastercode_sum as t2
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
	
	
	/* 	Считаем агрегаты Промо, ПБО, год, месяц, день недели */
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
	
	/* 	Считаем агрегаты Промо, год, месяц, день недели */
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
				t1.*,
				coalesce(t2.mean_sales_qty, t3.mean_sales_qty, t4.mean_sales_qty, t5.mean_sales_qty) as mean_sales_qty,
				coalesce(t2.std_sales_qty, t3.std_sales_qty, t4.std_sales_qty, t5.std_sales_qty) as std_sales_qty
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

	/* Удаляем промежуточные таблицы */		
	proc casutil;
		droptable casdata="na_abt8" incaslib="casuser" quiet;
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
	 * 10. Добавление целевой переменной и фильтрации									*
	 ************************************************************************************/
		
	/* Меняем store_id на pbo_location_id */
	proc sql;
		create table work.na_calculation_result as
			select
				t1.promo_id,
				t2.pbo_location_id,
				t1.sales_dt,
				t1.n_a,
				t1.t_a
			from
				NAC.na_calculation_result as t1
			inner join (
				select distinct
					PBO_LOCATION_ID,
					input(PBO_LOC_ATTR_VALUE, best32.) as store_id
				from
					etl_ia.pbo_loc_attributes
				where
					PBO_LOC_ATTR_NM='STORE_ID' and
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
			) as t2
			on
				t1.PBO_LOCATION_ID = t2.store_id
		;
	quit;
	
	/* Выгружаем в cas таблицу */
	data casuser.na_calculation_result;
		set work.na_calculation_result;
	run;

	/* Добавляем целевую переменную */
	proc fedsql sessref=casauto;
		create table casuser.na_abt10{options replace=true} as
			select
				t1.*,
				t3.n_a,
				t3.t_a	
			from
				casuser.na_abt9 as t1
			inner join
				(select distinct promo_id from casuser.na_calculation_result) as t2
			on
				t1.promo_id = t2.promo_id
				/* 
					Если акцию не обсчитывали в чеках, то нет смысла
					добавлять ее в обучающую выборку 
				*/
			left join
				casuser.na_calculation_result as t3
			on
				t1.promo_id = t3.promo_id and
				t1.pbo_location_id = t3.pbo_location_id and
				t1.sales_dt = t3.sales_dt
		;
	quit;

	/* Удаляем промежуточные таблицы */		
	proc casutil;
/* 		droptable casdata="na_calculation_result" incaslib="casuser" quiet; */
/* 		droptable casdata="na_abt9" incaslib="casuser" quiet; */
	run;

	/* Удаляем промежуточные таблицы */		
	proc datasets library=work nolist;
		delete na_calculation_result;
	run;

	/* 	------------ End. Добавление целевой переменной и фильтрации ------------- */	


	/************************************************************************************
	 * 11. 		Убираем временные закрытия												*
	 ************************************************************************************/
	/* 
		Не важно в каком канале закрыт ресторан,
	 	от любого канала страдает ALL.
	*/

	/* Загружаем таблицу с временными закрытиями */
	proc casutil;
		load data=etl_ia.pbo_close_period(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm
			)
		) casout='pbo_close_period' outcaslib='casuser' replace;	
	run;

	/* Убираем эти интервалы из витрины	 */
	proc fedsql sessref=casauto;
		create table casuser.na_abt11{options replace=true} as
			select
				t1.*
			from
				casuser.na_abt10 as t1
			left join
				casuser.pbo_close_period as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt <= t2.end_dt and
				t1.sales_dt >= t2.start_dt
			where
				t2.pbo_location_id is missing
		;	
	quit;

	/* Удаляем промежуточные таблицы */		
	proc casutil;
		droptable casdata="pbo_close_period" incaslib="casuser" quiet;
	run;

	/* 	------------ End. Убираем временные закрытия ------------- */	


	/************************************************************************************
	 * 12. 	Удалем пары промо-ресторан, в которых количество пропусков равно			*
	 *		 количеству промо дней												        *
	 ************************************************************************************/

	/* 
		Пары промо-ресторна, про которые совсем нет информации
		(все даты проведения акции в ресторане по чекам одни мисинги)
	*/
	proc fedsql sessref=casauto;
		create table casuser.promo_pbo_miss{options replace=true} as
			select
				t1.promo_id,
				t1.pbo_location_id
			from (
				select
					promo_id,
					pbo_location_id,
					nmiss(n_a) as nmiss
				from
					casuser.na_abt10
				group by
					promo_id,
					pbo_location_id
			) as t1
			inner join (
				select
					promo_id,
					(end_dt - start_dt + 1) as len
				from
					casuser.past_promo
			) as t2
			on
				t1.promo_id = t2.promo_id
			where
				t1.nmiss = t2.len
		;	
	quit;

	/* Убираем такие пары пары промо-ресторан */
	proc fedsql sessref=casauto;
		create table casuser.na_abt12{options replace=true} as
			select
				t1.*
			from
				casuser.na_abt11 as t1
			left join
				casuser.promo_pbo_miss as t2
			on
				t1.promo_id= t2.promo_id and
				t1.pbo_location_id = t2.pbo_location_id
			where
				t2.promo_id is missing
		;	
	quit;

	/* Удаляем промежуточные таблицы */		
	proc casutil;
		droptable casdata="promo_pbo_miss" incaslib="casuser" quiet;
		droptable casdata="na_abt10" incaslib="casuser" quiet;
		droptable casdata="na_abt11" incaslib="casuser" quiet;
	run;

	/* 	------------ End. Удалем пары промо-ресторан, в которых одни пропуски ------------- */	


	/************************************************************************************
	 * 13. 	Обрезаем концы (промо в ресторане закончилось раньше, либо началось позже)  *
	 ************************************************************************************/

	proc fedsql sessref=casauto;
		/* Определяем концы интрвала */
		create table casuser.ts_edges{option replace=true} as
			select
				promo_id,
				pbo_location_id,
				min(sales_dt) as left_side,
				max(sales_dt) as right_side
			from
				casuser.na_abt12
			where
				n_a is not missing
			group by
				promo_id,
				pbo_location_id

		;
		/* Удаляем записи за пределами этих интервалов */
		create table casuser.na_abt13{option replace=true} as
			select
				t1.*
			from
				casuser.na_abt12 as t1
			inner join
				casuser.ts_edges as t2
			on
				t1.promo_id = t2.promo_id and
				t1.pbo_location_id = t2.pbo_location_id
			where
				t1.sales_dt <= t2.right_side and
				t1.sales_dt >= t2.left_side
		;
	quit;

	/* Удаляем промежуточные таблицы */		
	proc casutil;
		droptable casdata="ts_edges" incaslib="casuser" quiet;
		droptable casdata="na_abt12" incaslib="casuser" quiet;

	run;

	/* 	------------ End. Обрезаем концы ------------- */	


	/************************************************************************************
	 * 14. Заменеям на миссинги значения, которые отклоняются на 2,5 стандартных 		*
	 *		отклонений от среднего.													    *
	 ************************************************************************************/

	/* Уберем выбросы */
	proc fedsql sessref=casauto;
		/* Читаем среднее и стандартное отклонение */
		create table casuser.na_stat{option replace=true} as
			select
				promo_id,
				pbo_location_id,
				mean(t_a) as mean_t_a,
				mean(n_a) as mean_n_a,
				std(t_a) as std_t_a
			from
				casuser.na_abt13
			group by
				promo_id,
				pbo_location_id
		;
		/* Заменяем на миссинги выбросы */
		create table casuser.na_abt14{option replace=true} as
			select
				t1.PBO_LOCATION_ID,
				PROMO_LIFETIME, 
				CHANNEL_CD, 
				NP_GIFT_PRICE_AMT, 
				PROMO_GROUP_ID, 
				sales_dt, 
				PROMO_MECHANICS_NAME, 
				t1.PROMO_ID, 
				WEEK_START, 
				NUMBER_OF_OPTIONS, 
				NUMBER_OF_PRODUCTS, 
				NECESSARY_AMOUNT, 
				TRP, 
				Breakfast, 
				ColdDrinks, 
				Condiments, 
				Desserts, 
				Fries, 
				HotDrinks, 
				McCafe, 
				Nonproduct, 
				Nuggets, 
				SNCORE, 
				SNEDAP, 
				SNPREMIUM, 
				Shakes, 
				StartersSalad, 
				UndefinedProductGroup, 
				ValueMeal, 
				LVL3_ID, 
				LVL2_ID, 
				AGREEMENT_TYPE_ID, 
				BREAKFAST_ID, 
				BUILDING_TYPE_ID, 
				COMPANY_ID, 
				DELIVERY_ID, 
				DRIVE_THRU_ID, 
				MCCAFE_TYPE_ID, 
/* 				PRICE_LEVEL_ID,  */
				WINDOW_TYPE_ID, 
				week, 
				weekday, 
				month, 
				year, 
				regular_weekend_flag, 
				weekend_flag, 
				CHRISTMAS, 
				CHRISTMAS_DAY, 
				DAY_AFTER_NEW_YEAR, 
				DAY_OF_UNITY, 
				DEFENDENCE_OF_THE_FATHERLAND, 
				INTERNATIONAL_WOMENS_DAY, 
				LABOUR_DAY, 
				NATIONAL_DAY, 
				NEW_YEAR_SHIFT, 
				NEW_YEAR, 
				VICTORY_DAY, 
				MEAN_RECEIPT_QTY, 
				STD_RECEIPT_QTY, 
				mean_sales_qty,
				std_sales_qty,
				(case
					when abs(t_a - mean_t_a) <= 2.5*coalesce(std_t_a, 0) then t_a
					else .
				end) as t_a,
				(case
					when abs(t_a - mean_t_a) <= 2.5*coalesce(std_t_a, 0) then n_a
					else .
				end) as n_a
			from
				casuser.na_abt13 as t1
			inner join
				casuser.na_stat as t2
			on
				t1.promo_id = t2.promo_id and
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;

	/* Удаляем промежуточные таблицы */		
	proc casutil;
		droptable casdata="na_abt13" incaslib="casuser" quiet;
	run;

	/* 	------------ End. Заменяем на миссинги выбросы ------------- */	


	/************************************************************************************
	 * 15. Если среднее t_a < 10, то пропуски заменяем нулями, иначе заменяем средним	*
	 ************************************************************************************/

	/* Если mean t_a < 10 то миссинги заменяем на нули иначе просто средним */
	proc fedsql sessref=casauto;
		create table casuser.na_abt15{option replace=true} as
			select
				t1.PBO_LOCATION_ID,
				PROMO_LIFETIME, 
				CHANNEL_CD, 
				NP_GIFT_PRICE_AMT, 
				PROMO_GROUP_ID, 
				sales_dt, 
				PROMO_MECHANICS_NAME, 
				t1.PROMO_ID, 
				WEEK_START, 
				NUMBER_OF_OPTIONS, 
				NUMBER_OF_PRODUCTS, 
				NECESSARY_AMOUNT, 
				TRP, 
				Breakfast, 
				ColdDrinks, 
				Condiments, 
				Desserts, 
				Fries, 
				HotDrinks, 
				McCafe, 
				Nonproduct, 
				Nuggets, 
				SNCORE, 
				SNEDAP, 
				SNPREMIUM, 
				Shakes, 
				StartersSalad, 
				UndefinedProductGroup, 
				ValueMeal, 
				LVL3_ID, 
				LVL2_ID, 
				AGREEMENT_TYPE_ID, 
				BREAKFAST_ID, 
				BUILDING_TYPE_ID, 
				COMPANY_ID, 
				DELIVERY_ID, 
				DRIVE_THRU_ID, 
				MCCAFE_TYPE_ID, 
/* 				PRICE_LEVEL_ID,  */
				WINDOW_TYPE_ID, 
				week, 
				weekday, 
				month, 
				year, 
				regular_weekend_flag, 
				weekend_flag, 
				CHRISTMAS, 
				CHRISTMAS_DAY, 
				DAY_AFTER_NEW_YEAR, 
				DAY_OF_UNITY, 
				DEFENDENCE_OF_THE_FATHERLAND, 
				INTERNATIONAL_WOMENS_DAY, 
				LABOUR_DAY, 
				NATIONAL_DAY, 
				NEW_YEAR_SHIFT, 
				NEW_YEAR, 
				VICTORY_DAY, 
				MEAN_RECEIPT_QTY, 
				STD_RECEIPT_QTY, 
				mean_sales_qty,
				std_sales_qty,
				(case
					when (t_a is missing) and (t2.mean_t_a <= 10) then 0
					when (t_a is missing) and (t2.mean_t_a > 10) then t2.mean_t_a
					else t_a
				end) as t_a,
				(case
					when (t_a is missing) and (t2.mean_t_a <= 10) then 0
					when (t_a is missing) and (t2.mean_t_a > 10) then t2.mean_n_a
					else n_a
				end) as n_a
			from
				casuser.na_abt14 as t1
			inner join
				casuser.na_stat as t2
			on
				t1.promo_id = t2.promo_id and
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;

	/* Удаляем промежуточные таблицы */		
	proc casutil;
		droptable casdata="na_abt14" incaslib="casuser" quiet;
		droptable casdata="na_stat" incaslib="casuser" quiet;

	run;

	/* 	------------ End. Замена средним или нулём ------------- */	


	/************************************************************************************
	 * 16. Дополнительная кодировка промо категорий										*
	 ************************************************************************************/
	/*
			Закодируем промо категории через целевую переменную.
		Для каждой акции усредним n_a (t_a) по всем промо акциям в том
		же ресторане с той же механикой за прошедший период. Если ресторан новый
		и у него недостает акций для усреднения, то усредним по всем ресторанам.
	*/

	/* Удаляем таблицы для скоринга, чтобы заново их запромоутить */		
	proc casutil;
		droptable casdata="score_mean_target_variable" incaslib="casuser" quiet;
		droptable casdata="score_mean_target_variable_pbo" incaslib="casuser" quiet;
	run;


	/* Суммируем целевую переменную и считаем количество дней с промо */
	proc fedsql sessref=casauto;
		create table casuser.mean_target_variable_pbo{option replace=true} as
			select
				t1.promo_id,
				t1.promo_mechanics_name,
				t1.pbo_location_id,
				t2.start_dt,
				t2.end_dt,
				sum(t1.n_a) as sum_n_a,
				sum(t1.t_a) as sum_t_a,
				count(1) as count_promo_days
			from
				casuser.na_abt15 as t1
			inner join (
				select distinct
					PROMO_ID,
					start_dt,
					end_dt,
					pbo_location_id
				from
					casuser.promo_ml
			) as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.promo_id = t2.promo_id
			group by
				t1.promo_id,
				t1.promo_mechanics_name,
				t1.pbo_location_id,
				t2.start_dt,
				t2.end_dt
		;
	quit;
	
	/* Суммируем целевую переменную и считаем количество дней с промо без привязки к pbo */
	proc fedsql sessref=casauto;
		create table casuser.mean_target_variable{option replace=true} as
			select
				t1.promo_id,
				t1.promo_mechanics_name,
				t2.start_dt,
				t2.end_dt,
				sum(t1.n_a) as sum_n_a,
				sum(t1.t_a) as sum_t_a,
				count(1) as count_promo_days
			from
				casuser.na_abt15 as t1
			inner join (
				select distinct
					PROMO_ID,
					start_dt,
					end_dt
				from
					casuser.promo_ml
			) as t2
			on
				t1.promo_id = t2.promo_id
			group by
				t1.promo_id,
				t1.promo_mechanics_name,
				t2.start_dt,
				t2.end_dt
		;
	quit;
	
	/* Суммируем целевую переменную и считаем количество дней с промо для */
	proc fedsql sessref=casauto;
		create table casuser.score_mean_target_variable_pbo{option replace=true} as
			select
				t1.promo_mechanics_name,
				t1.pbo_location_id,
				mean(t1.n_a) as mean_n_a,
				mean(t1.t_a) as mean_t_a
			from
				casuser.na_abt15 as t1
			group by
				t1.promo_mechanics_name,
				t1.pbo_location_id
		;
	quit;
	
	/* Суммируем целевую переменную и считаем количество дней с промо без привязки к pbo */
	proc fedsql sessref=casauto;
		create table casuser.score_mean_target_variable{option replace=true} as
			select
				t1.promo_mechanics_name,
				mean(t1.n_a) as mean_n_a,
				mean(t1.t_a) as mean_t_a
			from
				casuser.na_abt15 as t1
			group by
				t1.promo_mechanics_name
		;
	quit;
	
	/* Джоиним с самой собой, чтобы получить агрегаты за прошедшие акции */
	proc fedsql sessref=casauto;
		create table casuser.target_aggr_pbo{option replace=true} as
			select
				t1.promo_id,
				t1.promo_mechanics_name,
				t1.pbo_location_id,
				divide(sum(t2.sum_n_a), sum(t2.count_promo_days)) as mean_past_n_a,
				divide(sum(t2.sum_t_a), sum(t2.count_promo_days)) as mean_past_t_a
			from
				casuser.mean_target_variable_pbo as t1
			inner join
				casuser.mean_target_variable_pbo as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.promo_mechanics_name = t2.promo_mechanics_name and
				t1.start_dt >= t2.end_dt
			group by
				t1.promo_id,
				t1.promo_mechanics_name,
				t1.pbo_location_id
		;
	quit;
	
	/* Джоиним с самой собой, чтобы получить агрегаты за прошедшие акции без привяки к пбо */
	proc fedsql sessref=casauto;
		create table casuser.target_aggr{option replace=true} as
			select
				t1.promo_id,
				t1.promo_mechanics_name,
				divide(sum(t2.sum_n_a), sum(t2.count_promo_days)) as mean_past_n_a,
				divide(sum(t2.sum_t_a), sum(t2.count_promo_days)) as mean_past_t_a
			from
				casuser.mean_target_variable as t1
			inner join
				casuser.mean_target_variable as t2
			on
				t1.promo_mechanics_name = t2.promo_mechanics_name and
				t1.start_dt >= t2.end_dt
			group by
				t1.promo_id,
				t1.promo_mechanics_name
		;
	quit;
	
	/* Добавляем к витрине */
	proc fedsql sessref=casauto;
		create table casuser.na_abt16{option replace=true} as
			select
				t1.*,
				coalesce(t2.mean_past_n_a, t3.mean_past_n_a) as mean_past_n_a,
				coalesce(t2.mean_past_t_a, t3.mean_past_t_a) as mean_past_t_a
			from
				casuser.na_abt15 as t1
			left join
				casuser.target_aggr_pbo as t2
			on
				t1.promo_id = t2.promo_id and
				t1.pbo_location_id = t2.pbo_location_id
			left join
				casuser.target_aggr as t3
			on
				t1.promo_id = t3.promo_id

		;	
	quit;

	/* Удаляем промежуточные таблицы */		
	proc casutil;
		droptable casdata="na_abt15" incaslib="casuser" quiet;
		droptable casdata="mean_target_variable_pbo" incaslib="casuser" quiet;
		droptable casdata="mean_target_variable" incaslib="casuser" quiet;
		droptable casdata="target_aggr_pbo" incaslib="casuser" quiet;
		droptable casdata="target_aggr" incaslib="casuser" quiet;
	run;

	
	/* Сохраняем таблицы для сборки скоринговой витрины */
	proc casutil;
		promote casdata="score_mean_target_variable_pbo" incaslib="casuser" outcaslib="casuser";
		promote casdata="score_mean_target_variable" incaslib="casuser" outcaslib="casuser";
	run;
	
	/* Дополнительно сохраняем в casuser */
/* 	data casuser.score_mean_target_variable_pbo; */
/* 		set casuser.score_mean_target_variable_pbo; */
/* 	run; */
/* 	 */
/* 	data casuser.score_mean_target_variable; */
/* 		set casuser.score_mean_target_variable; */
/* 	run; */

	/* 	------------ End. Дополнительная кодировка промо категорий ------------- */	


	/************************************************************************************
	 * 17. Добавляем погоду																*
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
		create table casuser.na_abt17{option replace=true} as
			select
				t1.*,
				t2.temperature,
				t2.precipitation
			from
				casuser.na_abt16 as t1
			left join
				casuser.ia_weather as t2
			on
				t1.sales_dt = t2.report_dt and
				t1.pbo_location_id = t2.pbo_location_id
		;	
	quit;

	/* Удаляем промежуточные таблицы */		
	proc casutil;
		droptable casdata="na_abt16" incaslib="casuser" quiet;
		droptable casdata="ia_weather" incaslib="casuser" quiet;
	run;

	/* 	------------ End. Добавляем погоду ------------- */	
	

	/* Дополнительно сохраняем витрину в casuser */
	data casuser.na_abt17;
		set casuser.na_abt17;
	run;

%mend;
