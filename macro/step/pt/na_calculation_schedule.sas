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
);


%macro evm_schedule(promo, option_number, promo_start, promo_end, receipt_table);
	/*
		Макрос, который считает N_a, T_a для механики по типу EVM/Set
		Алгоритм:
			1. Фильтруем таблицу с промо
			2. Пересекаем чеки с промо таблицей, считая сумму в рамках каждой позиции
			3. Если число поцизий в чеке = число позиций в промо, то N_a = min(среди всех позиций чека)
	*/
	
	/* Фильтруем чеки */
	proc sql;
		create table work.receipt_filter as
			select
				t1.order_number,
				t1.STORE_ID as pbo_location_id,
				t1.menu_code as product_id,
				datepart(t1.order_date) as sales_dt format date9.,
				t1.qty,
				t1.qty_promo
			from
				&receipt_table as t1
			where
				datepart(t1.order_date) <= &promo_end. and
				datepart(t1.order_date) >= &promo_start.
		;	
	quit;
	
	/* 	Сортируем таблицу */
	proc sort data=work.receipt_filter;
		by order_number pbo_location_id sales_dt;
	run;
	
	/* 	Создаем уникальный ID чека */
	data work.receipt_filter_id;
		set work.receipt_filter;
		by order_number pbo_location_id sales_dt;
		if first.sales_dt then receipt_id+1;
	run;
	
	/* Фильтруем таблицу промо */
	proc sql;
		create table work.one_promo as
			select
				promo_txt_id,
				option_number,
				product_qty,
				product_id,
				pbo_location_id,
				promo_nm,
				start_dt,
				end_dt,
				channel_cd,
				promo_mechanics
			from
				work.promo_ml_filter
			where
				promo_txt_id = "&promo."
		;
	quit;

	/* Пересекаем с чеками */
	proc sql;
		create table work.promo_receipt as
			select
				t1.receipt_id,
				t1.sales_dt,
				t1.pbo_location_id,
				t2.option_number,
				t2.product_qty,
				sum(sum(t1.qty), sum(t1.qty_promo)) as sum_qty
			from
				work.receipt_filter_id as t1
			inner join
				work.one_promo as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.product_id = t2.product_id
			group by
				t1.receipt_id,
				t1.sales_dt,
				t1.pbo_location_id,
				t2.option_number,
				t2.product_qty
		;
	quit;

	/* Считаем число позиций в чеке и минимальное число товара в позиции */
	proc sql;
		create table work.receipt_options as
			select
				t1.receipt_id,
				count(distinct t1.option_number) as number_of_options,
				min(divide(t1.sum_qty, t1.product_qty)) as n_a
			from
				work.promo_receipt as t1
			where
				divide(t1.sum_qty, t1.product_qty) >= 1 /* Убираем позиции, где было куплено недостаточно товара */
			group by
				t1.receipt_id
		;	
	quit;
	
	/* Считаем N_a и T_a */
	proc sql;
		create table work.evm_na as 
			select
				"&promo." as promo_txt_id,
				t1.pbo_location_id,
				t1.sales_dt,
				sum(t2.n_a) as n_a,
				count(distinct t2.receipt_id) as t_a
			from
				work.promo_receipt as t1
			inner join
				(select * from work.receipt_options where number_of_options = &option_number.) as t2
			on
				t1.receipt_id = t2.receipt_id
			group by
				t1.pbo_location_id,
				t1.sales_dt
		;
	quit;

	/* 	Добавляем результат к итоговой таблице */
	proc append base=nac.na_calculation_schedule
		data = work.evm_na force;
	run;

	/* Удаляем промежуточные таблицы */
	proc datasets library=work;
		delete receipt_filter;
		delete receipt_filter_id;
		delete one_promo;
		delete promo_receipt;
		delete receipt_options;
		delete evm_na;
	run;
	
%mend;


%macro na_calculation_schedule();
/*
	Скрипт, который рассчитывает эффективность прошедших промо акций.
	Выход:
	------
		Таблица nac.na_calculation_result с подсчитанными показателями
*/
	
	/****** Загрузим справочные иерархии ******/
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
			select 
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
				t2.PBO_LEAF_ID
			from
				casuser.promo_pbo_enh as t1,
				public.pbo_lvl_all as t2
			where
				t1.pbo_location_id = t2.PBO_LOCATION_ID
		;
		create table public.ia_promo_x_product_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t1.option_number,
				t1.product_qty,
				t2.product_LEAF_ID
			from
				casuser.promo_prod_enh as t1,
				public.product_lvl_all as t2
			where
				t1.product_id = t2.product_id
		;
	quit;
	
	/* Выделяем прошедшие акции (end_dt < today) */
	proc fedsql sessref=casauto;
		create table casuser.past_promo{options replace=True} as
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
			) and channel_cd = 'ALL' and
			FROM_PT = 1 and
			end_dt < &ETL_CURRENT_DT_DB.
		;
	quit;
	
	/* Добавляем товары и рестораны */
	proc fedsql sessref = casauto;	
		create table public.promo_ml{options replace = true} as 
			select
				t1.PROMO_ID,
				t1.PROMO_TXT_ID,
				t3.option_number,
				t3.product_qty,
				t3.product_LEAF_ID as product_id,
				t2.PBO_LEAF_ID as pbo_location_id,
				t1.PROMO_NM,
				datepart(t1.START_DT) as start_dt,
				datepart(t1.END_DT) as end_dt,
				t1.CHANNEL_CD,
				t1.PROMO_MECHANICS
			from
				casuser.past_promo as t1 
			left join
				public.ia_promo_x_pbo_leaf as t2
			on 
				t1.PROMO_ID = t2.PROMO_ID
			left join
				public.ia_promo_x_product_leaf as t3
			on
				t1.PROMO_ID = t3.PROMO_ID 
		;
	quit;

	/* Удаляем промежуточные таблицы */
	proc casutil;
		droptable casdata="pbo_hier_flat" incaslib="public" quiet;
		droptable casdata="product_hier_flat" incaslib="public" quiet;
		droptable casdata="lvl5" incaslib="public" quiet;
		droptable casdata="lvl4" incaslib="public" quiet;
		droptable casdata="lvl3" incaslib="public" quiet;
		droptable casdata="lvl2" incaslib="public" quiet;
		droptable casdata="lvl1" incaslib="public" quiet;
	  	droptable casdata="ia_pbo_loc_hierarchy" incaslib="public" quiet;
	  	droptable casdata="ia_product_hierarchy" incaslib="public" quiet;
	  	droptable casdata="ia_promo_x_product_leaf" incaslib="public" quiet;
	  	droptable casdata="ia_promo_x_pbo_leaf" incaslib="public" quiet;
	  	droptable casdata="pbo_lvl_all" incaslib="public" quiet;
	  	droptable casdata="product_lvl_all" incaslib="public" quiet;	
	run;

	/* Выгружаем из cas  таблицу с промо */
	data work.promo_ml;
		set public.promo_ml;
	run;
	
	/* Меняем ID ресторнов */
	proc sql;
		create table work.promo_ml_filter as 
			select
				t1.PROMO_ID,
				t1.PROMO_TXT_ID,
				t1.option_number,
				t1.product_qty,
				t1.product_ID,
				input(t2.PBO_LOC_ATTR_VALUE, best32.) as pbo_location_id,
				t1.PROMO_NM,
				t1.start_dt,
				t1.end_dt,
				t1.CHANNEL_CD,
				t1.PROMO_MECHANICS
			from
				work.promo_ml as t1
			inner join (
				select distinct
					PBO_LOCATION_ID,
					PBO_LOC_ATTR_VALUE
				from
					etl_ia.pbo_loc_attributes
				where
					PBO_LOC_ATTR_NM = 'STORE_ID' and
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm

			) as t2
			on
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
		;
	quit;

	
	/* Создаем список промо, которые могут быть посчитаны по аналогу механики evm */
	proc sql;
		create table work.unique_evm_like_promo as
			select
				put(PROMO_ID, 12.) as promo_id,
				PROMO_TXT_ID,
				PROMO_NM,
				put(start_dt,8.) as start_dt,
				put(end_dt,8.) as end_dt,
				CHANNEL_CD,
				PROMO_MECHANICS,
				put(max(option_number),8.) as max_option_number
			from
				work.promo_ml_filter
			where
				promo_mechanics ^= 'Other: Discount for volume'
			group by
				PROMO_ID,
				PROMO_TXT_ID,
				PROMO_NM,
				start_dt,
				end_dt,
				CHANNEL_CD,
				PROMO_MECHANICS
		;
	quit;

	/* Если существует таблица с посчитанной эффективностью, то 
		запускаем цикл по всем прошедшим промо, которых нет еще в результате */
	%if %sysfunc(exist(nac.na_calculation_schedule)) %then %do;
		/* Список промо, которых нет в результирующей таблице */
		proc sql;
			create table work.unique_evm_like_promo_new as
				select
					t1.promo_id,
					t1.promo_txt_id,
					t1.PROMO_NM,
					t1.start_dt,
					t1.end_dt,
					t1.CHANNEL_CD,
					t1.PROMO_MECHANICS,
					t1.max_option_number
				from
					work.unique_evm_like_promo as t1
				left join
					(select distinct promo_txt_id from nac.na_calculation_schedule) as t2
				on
					t1.promo_txt_id = t2.promo_txt_id
				where
					t2.promo_txt_id is missing 
			;			
		quit;
		
	%end;
	%else %do; /* Если не существует, то запускаем цикл по всем прошедшим промо*/
		data work.unique_evm_like_promo_new;
			set work.unique_evm_like_promo;
		run;
	%end;

	/* Проверяем, что таблица не пустая (если уже все прошедшие промо просчитали) */
	proc sql noprint;
		select count(*) into :n_obs from work.unique_evm_like_promo_new;
	quit;
	
	%if %eval(&n_obs. > 0) %then %do;
	
		options nomlogic nomprint nosymbolgen nosource nonotes;

		/* Вызываем в цикле макрос */
		data _null_;
			set work.unique_evm_like_promo_new;
			call execute('%evm_schedule('||promo_txt_id||','||max_option_number||','||start_dt||','||end_dt||', nac.tda_pmx_2021)');
		run;
	
		options mlogic mprint symbolgen source notes;

	
	%end;

	/* Удаляем промежуточные таблицы */
	proc datasets library=work;
		delete unique_evm_like_promo;
		delete unique_evm_like_promo_new;
		delete promo_ml_filter;
	run;
	
%mend;

%na_calculation_schedule();