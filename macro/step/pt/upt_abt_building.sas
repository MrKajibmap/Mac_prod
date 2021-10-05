/* 
	Скрипт собирающий обучающую выборку для модели,
	 раскладывающей UPT на эффекты от промо
*/

%macro upt_abt_building(
	promo_lib = public, 
	ia_promo = ia_promo,
	ia_promo_x_pbo = ia_promo_x_pbo,
	ia_promo_x_product = ia_promo_x_product,
	period_start_dt = '1jan2019'd,
	period_end_dt = '12apr2021'd
);
	/*
		Макрос создает витрину для модели линейной регрессии.

		Параметры:
		----------
			promo_lib : Название директории CAS в которой хранятся промо таблицы 
			ia_promo : Название таблицы с информацией о промо
			ia_promo_x_pbo : Название таблицы с информацией о промо ресторанах
			ia_promo_x_product : Название таблицы с информацией о промо товарах
			period_start_dt : Дата начала рассматриваемой истории
			period_end_dt : Дата конца рассматриваемой истории
		Выход:
		------
			* Таблица nac.upt_train с обучающей выборкой для модели	
	*/

	/*** 1. Собираем целевую переменную ***/
	
	/* Агрегируем pmix */
	proc sql;
		create table nac.aggr_pmix as
			select
				t1.product_id,
				t1.sales_dt,
				sum(t1.sum_qty) as sum_qty
			from (
				select
					pbo_location_id,
					product_id,
					sales_dt,
					sum(sales_qty, sales_qty_promo) as sum_qty
				from
					etl_ia.pmix_sales as t1
				where
					channel_cd = 'ALL' and	
					sales_dt <= &period_end_dt. and
					sales_dt >= &period_start_dt. and
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
			) as t1
			group by
				t1.product_id,
				t1.sales_dt
		;
	quit;
	
	/* Формируем список активных регулярных товаров */
	proc sql;
		create table nac.active_regular_product as
			select
				t1.sales_dt,
				t1.product_id
			from
				nac.aggr_pmix as t1
			inner join
				etl_ia.product_attributes as t2
			on
				t1.product_id = t2.product_id
			where
				t2.product_attr_nm = 'REGULAR_ID' and
				&ETL_CURRENT_DTTM. <= t2.valid_to_dttm and
				&ETL_CURRENT_DTTM. >= t2.valid_from_dttm and
				t2.product_id = input(t2.product_attr_value, best32.) and
				t1.sum_qty > 100
		;
	quit;
	
	/* Агрегируем GC */
	proc sql;
		create table work.aggr_gc as
			select
				t1.sales_dt,
				sum(t1.receipt_qty) as receipt_qty
			from (
				select
					pbo_location_id,
					sales_dt,
					receipt_qty
				from
					etl_ia.pbo_sales as t1
				where
					channel_cd = 'ALL' and	
					sales_dt <= &period_end_dt. and
					sales_dt >= &period_start_dt. and
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
			) as t1
			group by
				t1.sales_dt
		;
	quit;
	
	/* Формируем таблицу со средним количеством чеков в ресторане */
	proc sql;
		create table nac.history_mean_gc as
			select
				pbo_location_id,
				mean(receipt_qty) as mean_gc
			from
				etl_ia.pbo_sales as t1
			where
				channel_cd = 'ALL' and	
				sales_dt <= &period_end_dt. and
				sales_dt >= &period_start_dt. and
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm
			group by
				pbo_location_id
		;
	quit;
	
	/* Считаем UPT */
	proc sql;
		create table nac.aggr_upt as
			select
				t1.product_id,
				t1.sales_dt,
				divide(t1.sum_qty, t2.receipt_qty)*1000 as upt
			from 
				nac.aggr_pmix as t1
			left join
				work.aggr_gc as t2
			on
				t1.sales_dt = t2.sales_dt
		;
	quit;
	
	
	/****** 2. Собираем признаковое пространство ******/
	proc casutil;
		load data=etl_ia.pbo_loc_hierarchy(
				where=(
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
				)
			) casout='ia_pbo_loc_hierarchy' outcaslib='public' replace;
	
		load data=etl_ia.product_hierarchy(
				where=(
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
				)
			) casout='ia_product_hierarchy' outcaslib='public' replace;
	run;
	
	/* Создаем таблицу связывающую PBO на листовом уровне и на любом другом */
	proc fedsql sessref=casauto;
		create table public.pbo_hier_flat{options replace=true} as
			select
				t1.pbo_location_id, 
				t2.PBO_LOCATION_ID as LVL3_ID,
				t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				1 as LVL1_ID
			from 
				(select * from public.ia_pbo_loc_hierarchy where pbo_location_lvl=4) as t1
			left join 
				(select * from public.ia_pbo_loc_hierarchy where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
		;
		create table public.lvl4{options replace=true} as 
			select distinct
				pbo_location_id as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				public.pbo_hier_flat
		;
		create table public.lvl3{options replace=true} as 
			select distinct
				LVL3_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				public.pbo_hier_flat
		;
		create table public.lvl2{options replace=true} as 
			select distinct
				LVL2_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				public.pbo_hier_flat
		;
		create table public.lvl1{options replace=true} as 
			select 
				1 as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				public.pbo_hier_flat
		;
	quit;
	
	/* Соединяем в единый справочник ПБО */
	data public.pbo_lvl_all;
		set public.lvl4 public.lvl3 public.lvl2 public.lvl1;
	run;
	
	/* Создаем таблицу связывающую товары на листовом уровне и на любом другом */
	proc fedsql sessref=casauto;
	   create table public.product_hier_flat{options replace=true} as
			select t1.product_id, 
				   t2.product_id  as LVL4_ID,
				   t3.product_id  as LVL3_ID,
				   t3.PARENT_product_id as LVL2_ID, 
				   1 as LVL1_ID
			from 
			(select * from public.ia_product_hierarchy where product_lvl=5) as t1
			left join 
			(select * from public.ia_product_hierarchy where product_lvl=4) as t2
			on t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
			left join 
			(select * from public.ia_product_hierarchy where product_lvl=3) as t3
			on t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
	 	;
		create table public.lvl5{options replace=true} as 
			select distinct
				product_id as product_id,
				product_id as product_leaf_id
			from
				public.product_hier_flat
		;
		create table public.lvl4{options replace=true} as 
			select distinct
				LVL4_ID as product_id,
				product_id as product_leaf_id
			from
				public.product_hier_flat
		;
		create table public.lvl3{options replace=true} as 
			select distinct
				LVL3_ID as product_id,
				product_id as product_leaf_id
			from
				public.product_hier_flat
		;
		create table public.lvl2{options replace=true} as 
			select distinct
				LVL2_ID as product_id,
				product_id as product_leaf_id
			from
				public.product_hier_flat
		;
		create table public.lvl1{options replace=true} as 
			select distinct
				1 as product_id,
				product_id as product_leaf_id
			from
				public.product_hier_flat
		;
	quit;
	
	/* Соединяем в единый справочник ПБО */
	data public.product_lvl_all;
		set public.lvl5 public.lvl4 public.lvl3 public.lvl2 public.lvl1;
	run;
	
	/* Добавляем к таблице промо ПБО и товары */
	proc fedsql sessref = casauto;
		create table public.ia_promo_x_pbo_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t2.PBO_LEAF_ID as pbo_location_id
			from
				&promo_lib..&ia_promo_x_pbo. as t1,
				public.pbo_lvl_all as t2
			where
				t1.pbo_location_id = t2.pbo_location_id
		;
		create table public.ia_promo_x_product_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t1.OPTION_NUMBER,
				t1.PRODUCT_QTY,
				t2.product_LEAF_ID as product_id
			from
				&promo_lib..&ia_promo_x_product. as t1,
				public.product_lvl_all as t2
			where
				t1.product_id = t2.product_id
		;
		create table public.promo_ml{options replace = true} as 
			select
				t1.PROMO_ID,
				datepart(t1.start_dt) as start_dt,
				datepart(t1.end_dt) as end_dt,
				t1.promo_mechanics,
				t3.product_id,
				t3.option_number,
				t2.pbo_location_id
			from
				&promo_lib..&ia_promo. as t1 
			left join
				public.ia_promo_x_pbo_leaf as t2
			on 
				t1.PROMO_ID = t2.PROMO_ID
			left join
				public.ia_promo_x_product_leaf as t3
			on
				t1.PROMO_ID = t3.PROMO_ID
			where
				t1.channel_cd = 'ALL'
		;	
	quit;
	
	/* Выгрузим из cas */
	data work.promo_ml;
		set public.promo_ml;
	run;
	
	/* Добавляем id store */
	proc sql;
		create table work.promo_ml2 as
			select
				t1.promo_id,
				t1.start_dt,
				t1.end_dt,
				t1.promo_mechanics,
				t1.product_id,
				t1.option_number,
				t1.pbo_location_id,
				input(t2.PBO_LOC_ATTR_VALUE, best32.) as store_id 
			from
				work.promo_ml as t1
			inner join (
				select distinct
					PBO_LOCATION_ID,
					PBO_LOC_ATTR_VALUE
				from
					etl_ia.pbo_loc_attributes
				where
					PBO_LOC_ATTR_NM='STORE_ID' and
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
			) as t2
			on
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
		;
	quit;
	
	/* Считаем средние продажи товара в ресторан/день */
	proc sql;
		create table nac.mean_pmix as
			select
				t1.product_id,
				mean(t1.sum_qty) as mean_sum_qty
			from (
				select
					pbo_location_id,
					product_id,
					sales_dt,
					sum(sales_qty, sales_qty_promo) as sum_qty
				from
					etl_ia.pmix_sales as t1
				where
					channel_cd = 'ALL' and
					sales_dt <= &period_end_dt. and
					sales_dt >= &period_start_dt. and
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
			) as t1
			group by
				t1.product_id
		;
	quit;
	
	/* Считаем доли товара в рамках option number */
	proc sql;
		create table work.option_mean as
			select
				t1.promo_id,
				t1.option_number,
				t1.product_id,
				t2.mean_sum_qty
			from (
				select distinct
					promo_id,
					option_number,
					product_id
				from
					work.promo_ml2
			) as t1
			inner join
				nac.mean_pmix as t2
			on
				t1.product_id = t2.product_id
		;
	
		create table work.option_freq as
			select
				t1.promo_id,
				t1.option_number,
				t1.product_id,
				divide(t1.mean_sum_qty, t2.option_sum) as freq
			from
				work.option_mean as t1
			inner join (
				select
					promo_id,
					option_number,
					sum(mean_sum_qty) as option_sum	
				from
					work.option_mean
				group by
					promo_id,
					option_number
			) as t2			
			on
				t1.promo_id = t2.promo_id and
				t1.option_number = t2.option_number
		;
	quit;
	
	/* Добавляем посчитанные N_a */
	proc sql;
		create table work.promo_ml3 as
			select
				t2.promo_id,
				t1.option_number,
				t1.product_id,
				t2.pbo_location_id,
				t2.sales_dt,
				t2.n_a,
				t2.n_a * t3.freq as n_a_modified
			from
				work.promo_ml2 as t1
			inner join
				nac.na_calculation_result as t2
			on
				t1.promo_id = t2.promo_id and
				t1.store_id = t2.pbo_location_id
			inner join 
				work.option_freq as t3
			on
				t1.promo_id = t3.promo_id and
				t1.option_number = t3.option_number and
				t1.product_id = t3.product_id
		;
	quit;
	
	/* Суммируем промо эффект по товарам */
	proc sql;
		create table work.product_sum_promo_effect as
			select
				product_id,
				sales_dt,
				sum(n_a_modified) as sum_n_a_modified
			from
				work.promo_ml3
			group by
				product_id,
				sales_dt
		;
	quit;
	
	/* Добавляем категории товаров */
	proc casutil;
		load data=etl_ia.product(
				where=(
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
				)
			) casout='ia_product' outcaslib='public' replace;
	
		load data=etl_ia.product_hierarchy(
				where=(
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
				)
			) casout='ia_product_hierarchy' outcaslib='public' replace;
	
		load data=etl_ia.product_attributes(
				where=(
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
				)
			) casout='ia_product_attributes' outcaslib='public' replace;
	run;
	  
	proc cas;
	transpose.transpose /
	   table={name="ia_product_attributes", caslib="public", groupby={"product_id"}} 
	   attributes={{name="product_id"}} 
	   transpose={"PRODUCT_ATTR_VALUE"} 
	   prefix="A_" 
	   id={"PRODUCT_ATTR_NM"} 
	   casout={name="attr_transposed", caslib="public", replace=true};
	quit;
	
	proc fedsql sessref=casauto;
		create table public.product_hier_flat{options replace=true} as
			select 
				t1.product_id, 
				t2.product_id  as LVL4_ID,
				t3.product_id  as LVL3_ID,
				t3.PARENT_product_id as LVL2_ID, 
				1 as LVL1_ID
			from 
				(select * from public.ia_product_hierarchy where product_lvl=5) as t1
			left join 
				(select * from public.ia_product_hierarchy where product_lvl=4) as t2
			on 
				t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
			left join 
				(select * from public.ia_product_hierarchy where product_lvl=3) as t3
			on
				t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
	 	;
	quit;
	
	proc fedsql sessref=casauto;
		create table public.product_dictionary_ml{options replace=true} as
			select 
				t1.product_id, 
				coalesce(t1.lvl4_id,-9999) as prod_lvl4_id,
				coalesce(t1.lvl3_id,-999) as prod_lvl3_id,
				coalesce(t1.lvl2_id,-99) as prod_lvl2_id,
				coalesce(t15.product_nm,'NA') as product_nm,
				coalesce(t14.product_nm,'NA') as prod_lvl4_nm,
				coalesce(t13.product_nm,'NA') as prod_lvl3_nm,
				coalesce(t12.product_nm,'NA') as prod_lvl2_nm,
				t3.A_HERO,
				t3.A_ITEM_SIZE,
				t3.A_OFFER_TYPE,
				t3.A_PRICE_TIER
			from
				public.product_hier_flat as t1
			left join
				public.attr_transposed as t3
			on
				t1.product_id = t3.product_id
			left join
				PUBLIC.IA_product as t15
			on
				t1.product_id = t15.product_id
			left join
				PUBLIC.IA_product as t14
			on
				t1.lvl4_id = t14.product_id
			left join
				PUBLIC.IA_product as t13
			on
				t1.lvl3_id = t13.product_id
			left join
				PUBLIC.IA_product as t12
			on
				t1.lvl2_id = t12.product_id
		;
	quit;
	
	/* Выгружаем из cas */
	data nac.product_dictionary_ml;
		set public.product_dictionary_ml;
		category_name = translate(trim(prod_lvl2_nm),'_',' ', '_', '&', '_', '-');
	run;
	
	proc sql;
		create table work.product_sum_promo_effect_cat as
			select
				t1.product_id,
				t1.sales_dt,
				t1.sum_n_a_modified,
				t2.prod_lvl2_id,
				t2.category_name
			from
				work.product_sum_promo_effect as t1
			inner join
				nac.product_dictionary_ml as t2
			on
				t1.product_id = t2.product_id
		;
	quit;
	
	%macro feature_space_creation(product_id);
	/* 
		Процедуру формирования признакового пространства
		Параметры:
		----------
			* product_id -  ID товара для которого строим обучающую выборку
			
	*/
		
		/* Удалим промежуточные таблицы */
		proc datasets library=work nolist;
			delete product_promo;
			delete other_product_promo;
			delete other_product_promo_zero;
			delete other_product_promo_zero_t;
			delete product_promo_together;
			delete product_promo_together2;
			delete mastercode_promo;
			delete same_mastercode_products;
		run;	
	
		proc casutil;
			droptable casdata="other_product_promo_zero" incaslib="public" quiet;
		run;
	
		/* Выделяем промо на товар */
		proc sql;
			create table work.product_promo as
				select
					product_id,
					sales_dt,
					sum_n_a_modified as positive_promo_na
				from
					work.product_sum_promo_effect_cat
				where
					product_id = &product_id.
			;
		quit;
		
		/* Выделяем промо на товары под тем же мастеркодом */
		proc sql;
			create table work.same_mastercode_products as 
				select
					t1.product_id
				from
					nac.product_dictionary_ml as t1
				inner join (
					select distinct
						PROD_LVL4_ID
					from
						nac.product_dictionary_ml
					where
						product_id = &product_id.
				) as t2
				on
					t1.PROD_LVL4_ID = t2.PROD_LVL4_ID
			;
		quit;
		
		/* Посчитаем по ним суммарный N_a */
		proc sql;
			create table work.mastercode_promo as
				select
					t1.sales_dt,
					sum(t1.sum_n_a_modified) as mastercode_promo_na
				from
					work.product_sum_promo_effect_cat as t1
				inner join 
					(select * from work.same_mastercode_products where product_id ^= &product_id.) as t2
				on
					t1.product_id = t2.product_id
				group by
					t1.sales_dt
			;
		quit;
	
		/* Выделяем промо на другие товары */
		proc sql;
			create table work.other_product_promo as
				select
					category_name,
					sales_dt,
					sum(sum_n_a_modified) as promo_na
				from
					work.product_sum_promo_effect_cat as t1
				left join
					work.same_mastercode_products as t2
				on
					t1.product_id = t2.product_id
				where
					t2.product_id is missing
				group by
					category_name,
					sales_dt
			;
		quit;
	
		/* Добавляем нули */
		proc sql;
			create table work.other_product_promo_zero as
				select
					t1.category_name,
					t1.sales_dt,
					coalesce(t2.promo_na, 0) as promo_na
				from (
					select
						t1.sales_dt,
						t2.category_name
					from
						(select distinct sales_dt from work.other_product_promo) as t1,
						(select distinct category_name from nac.product_dictionary_ml) as t2					
				) as t1
				left join
					work.other_product_promo as t2
				on
					t1.category_name = t2.category_name and
					t1.sales_dt = t2.sales_dt
			;
		quit;
		
		/* Транспонируем промо на другие товары */
		data public.other_product_promo_zero;
			set work.other_product_promo_zero;
		run;
	
		proc cas;
		transpose.transpose /
		   table={name="other_product_promo_zero", caslib="public", groupby={"sales_dt"}} 
		   transpose={"promo_na"} 
		   id={"category_name"} 
		   casout={name="other_product_promo_zero_t", caslib="public", replace=true};
		quit;
		
		data work.other_product_promo_zero_t;
			set public.other_product_promo_zero_t;
		run;
		
		/* Соединяем промо на товар с промо на другие категории */
		proc sql;
			create table work.product_promo_together as	
				select
					&product_id. as product_id,
					coalesce(t1.sales_dt, t2.sales_dt) as sales_dt format date9.,
					coalesce(t1.positive_promo_na, 0) as positive_promo_na,
					coalesce(t2.Undefined_Product_Group, 0) as Undefined_Product_Group,
					coalesce(t2.Cold_Drinks, 0) as Cold_Drinks,
					coalesce(t2.Hot_Drinks, 0) as Hot_Drinks,
					coalesce(t2.Breakfast, 0) as Breakfast,
					coalesce(t2.Condiments, 0) as Condiments,
					coalesce(t2.Desserts, 0) as Desserts,
					coalesce(t2.Fries, 0) as Fries,
					coalesce(t2.Starters___Salad, 0) as Starters___Salad,
					coalesce(t2.SN_CORE, 0) as SN_CORE,
					coalesce(t2.McCafe, 0) as McCafe,
					coalesce(t2.Non_product, 0) as Non_product,
					coalesce(t2.SN_EDAP, 0) as SN_EDAP,
					coalesce(t2.SN_PREMIUM, 0) as SN_PREMIUM,
					coalesce(t2.Value_Meal, 0) as Value_Meal,
					coalesce(t2.Nuggets, 0) as Nuggets,
					coalesce(t2.Shakes, 0) as Shakes
				from
					work.product_promo as t1
				full join
					work.other_product_promo_zero_t as t2
				on
					t1.sales_dt = t2.sales_dt
				order by
					sales_dt
			;
		quit;
	
		/* Соеднияем с промо на мастеркод */
		proc sql;
			create table work.product_promo_together2 as	
				select
					&product_id. as product_id,
					coalesce(t1.sales_dt, t2.sales_dt) as sales_dt format date9.,
					coalesce(t1.positive_promo_na, 0) as positive_promo_na,
					coalesce(t2.mastercode_promo_na, 0) as mastercode_promo_na,
					coalesce(t1.Undefined_Product_Group, 0) as Undefined_Product_Group,
					coalesce(t1.Cold_Drinks, 0) as Cold_Drinks,
					coalesce(t1.Hot_Drinks, 0) as Hot_Drinks,
					coalesce(t1.Breakfast, 0) as Breakfast,
					coalesce(t1.Condiments, 0) as Condiments,
					coalesce(t1.Desserts, 0) as Desserts,
					coalesce(t1.Fries, 0) as Fries,
					coalesce(t1.Starters___Salad, 0) as Starters___Salad,
					coalesce(t1.SN_CORE, 0) as SN_CORE,
					coalesce(t1.McCafe, 0) as McCafe,
					coalesce(t1.Non_product, 0) as Non_product,
					coalesce(t1.SN_EDAP, 0) as SN_EDAP,
					coalesce(t1.SN_PREMIUM, 0) as SN_PREMIUM,
					coalesce(t1.Value_Meal, 0) as Value_Meal,
					coalesce(t1.Nuggets, 0) as Nuggets,
					coalesce(t1.Shakes, 0) as Shakes
				from
					work.product_promo_together as t1
				full join
					work.mastercode_promo as t2
				on
					t1.sales_dt = t2.sales_dt
				order by
					sales_dt
			;		
		quit;		
	
		/* Добавим результат к витрине */
		proc append base=work.upt_promo_effect_feature_space
			data = work.product_promo_together2 force;
		run;
	
	%mend;
	
	
	/* Удаляем таблицу с результатом */
	proc datasets library=work nolist;
	   delete upt_promo_effect_feature_space;
	run;
	
	options nomlogic nomprint nosymbolgen nosource nonotes;

	/* Пройдем в цикле по товарам и будем вызывать макрос */
	data _null_;
	   set nac.product_dictionary_ml;
	   call execute('%feature_space_creation('||product_id||')');
	run;
	
	options mlogic mprint symbolgen source notes;
	
	/* Создаем обучающую витрину */
	proc sql;
		create table work.upt_promo_effect_abt as
			select
				t1.product_id,
				t1.sales_dt,
				t1.upt,
				coalesce(t2.positive_promo_na, 0) as positive_promo_na,
				coalesce(t2.mastercode_promo_na, 0) as mastercode_promo_na,
				coalesce(t2.Undefined_Product_Group, 0) as Undefined_Product_Group,
				coalesce(t2.Cold_Drinks, 0) as Cold_Drinks,
				coalesce(t2.Hot_Drinks, 0) as Hot_Drinks,
				coalesce(t2.Breakfast, 0) as Breakfast,
				coalesce(t2.Condiments, 0) as Condiments,
				coalesce(t2.Desserts, 0) as Desserts,
				coalesce(t2.Fries, 0) as Fries,
				coalesce(t2.Starters___Salad, 0) as Starters___Salad,
				coalesce(t2.SN_CORE, 0) as SN_CORE,
				coalesce(t2.McCafe, 0) as McCafe,
				coalesce(t2.Non_product, 0) as Non_product,
				coalesce(t2.SN_EDAP, 0) as SN_EDAP,
				coalesce(t2.SN_PREMIUM, 0) as SN_PREMIUM,
				coalesce(t2.Value_Meal, 0) as Value_Meal,
				coalesce(t2.Nuggets, 0) as Nuggets,
				coalesce(t2.Shakes, 0) as Shakes			
			from
				nac.aggr_upt as t1
			left join
				work.upt_promo_effect_feature_space as t2
			on
				t1.product_id = t2.product_id and
				t1.sales_dt = t2.sales_dt
			inner join
				(select distinct product_id from work.upt_promo_effect_feature_space) as t3
			on
				t1.product_id = t3.product_id
		;
	quit;
	
	/* Считаем дату начала временного ряда для создания тренда */
	proc sql;
		create table work.time_series_start as
			select
				product_id,
				min(sales_dt) as ts_start
			from
				work.upt_promo_effect_abt
			group by
				product_id
		;
	quit;
	
	/* Делим n_a на GC и получаем обучающую выборку */
	proc sql;
		create table nac.upt_train as
			select
				t1.product_id,
				t1.sales_dt,
				(t1.sales_dt - t3.ts_start + 1) as t,
				t1.upt,
				t1.positive_promo_na,
				t1.mastercode_promo_na,
				t1.Undefined_Product_Group,
				t1.Cold_Drinks,
				t1.Hot_Drinks,
				t1.Breakfast,
				t1.Condiments,
				t1.Desserts,
				t1.Fries,
				t1.Starters___Salad,
				t1.SN_CORE,
				t1.McCafe,
				t1.Non_product,
				t1.SN_EDAP,
				t1.SN_PREMIUM,
				t1.Value_Meal,
				t1.Nuggets,
				t1.Shakes
			from
				work.upt_promo_effect_abt as t1
			inner join
				work.time_series_start as t3
			on
				t1.product_id = t3.product_id
		;		
	quit;
	
	/* Удалим ненужные промежуточные таблицы */
	proc datasets library=work nolist;
		delete product_promo;
		delete other_product_promo;
		delete other_product_promo_zero_t;
		delete product_promo_together;
		delete product_promo_together2;
		delete mastercode_promo;
		delete same_mastercode_products;

		delete aggr_gc;
		delete option_freq;
		delete option_mean;
		delete product_sum_promo_effect;
		delete product_sum_promo_effect_cat;
		
		delete promo_ml;
		delete promo_ml2;
		delete promo_ml3;
		delete time_series_start;
		delete upt_promo_effect_abt;
		delete upt_promo_effect_feature_space;
	run;	

	proc datasets library=nac nolist;
		delete aggr_upt;
		delete mean_pmix;
	run;

	proc casutil;
		droptable casdata="other_product_promo_zero" incaslib="public" quiet;
		droptable casdata="pbo_lvl_all" incaslib="public" quiet;
		droptable casdata="product_lvl_all" incaslib="public" quiet;
	run;

%mend;
