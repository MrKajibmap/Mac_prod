/* Скрипт по рассчету эффективности промо акций на истории */
%macro interval_intersection(year, promo_start, promo_end);
/* 
	Макрос фильтрует интервал промо из таблицы с чеками.
	Глупо будет пересекать промо из 2019 года с чеками 2020 года
	потому что вернется пустая таблица, а фильтрация займет минут 7.
	Поэтому предже чем применять фильтр, нужно убедиться что интервал промо
	пересекается с чеками за рассматриваемый период.
	Параметры:
	----------
		* year - год за который вернутся чеки
		* promo_start - начало промо интервала
		* promo_end - конец промо интервала
	Выход:
	------
		Таблица nac.russca_receipt_filter_&year.
	
*/
	/* Чеки за 2021 год лежат в /data/MN_CALC, остальные чеки в /data/backup */
	%if %eval(&year. = 2021) %then %do;
		%let promo_lib = nac;
	%end;
	%else %do;
		%let promo_lib = cheque;
	%end;
	
	%if %eval(
		(
			(&promo_start. < %sysfunc(PUTN("1jan&year."d, 8.))) and
			(&promo_end. > %sysfunc(PUTN("31dec&year."d, 8.)))
		) or
		(
			(&promo_start. < %sysfunc(PUTN("1jan&year."d, 8.))) and
			(&promo_end. >= %sysfunc(PUTN("1jan&year."d, 8.)))
		) or
		(
			(&promo_start. >= %sysfunc(PUTN("1jan&year."d, 8.))) and
			(&promo_end. <= %sysfunc(PUTN("31dec&year."d, 8.)))
		) or
		(
			(&promo_start. <= %sysfunc(PUTN("31dec&year."d, 8.))) and
			(&promo_end. > %sysfunc(PUTN("31dec&year."d, 8.)))
		) 
	) %then %do;
		proc sql;
			create table nac.russca_receipt_filter_&year. as
				select
					t1.order_number,
					t1.STORE_ID as pbo_location_id,
					t1.menu_code as product_id,
					datepart(t1.order_date) as sales_dt format date9.,
					t1.qty,
					t1.qty_promo
				from
					&promo_lib..tda_pmx_&year. as t1
				where
					datepart(t1.order_date) <= &promo_end. and
					datepart(t1.order_date) >= &promo_start. 
			;
		quit;
	%end;
	%else %do;
		proc sql;
			create table nac.russca_receipt_filter_&year. as
				select
					t1.order_number,
					t1.STORE_ID as pbo_location_id,
					t1.menu_code as product_id,
					datepart(t1.order_date) as sales_dt format date9.,
					t1.qty,
					t1.qty_promo
				from
					&promo_lib..tda_pmx_&year.(obs=0) as t1
			;
		quit;
	%end;

%mend;


%macro prepare_receipt_data(promo, promo_start, promo_end);
/* 
	Макрос подготоваливает таблицы с чеками. На вход поступает ID промо акции.
		1. Фильтруем чековые данные в интервале действия промо.
		2. Создаем нормальный ID чека
		3. Объединяем чеки 2019, 2020, 2021 годов в одну табицу

	Задача макроса оставить только те чеки, которые имют отношение к промо акции.
	Чтобы в дальнейшем не работать со всей таблицей чеков при подсчете Na, а иметь дело
	только с подвыборкой.
	Параметры:
	----------
		* promo - ID промо
		* promo_start - начало промо интервала
		* promo_end - начало промо интервала

*/
	
	/* Фильтруем промо таблицу */
	proc sql;
		create table nac.discount_promo_filter as 
			select
				t1.hybrid_promo_id,
				t1.product_ID,
				t1.pbo_location_id,
				t1.PROMO_NM,
				t1.start_dt,
				t1.end_dt,
				t1.CHANNEL_CD,
				t1.PROMO_MECHANICS
			from
				work.promo_ml2 as t1
			where
				t1.hybrid_promo_id = "&promo."
		;
	quit;
	
	/* Фильтруем чеки по датам промо акции */
	%interval_intersection(2019, &promo_start., &promo_end.);
	%interval_intersection(2020, &promo_start., &promo_end.);
	%interval_intersection(2021, &promo_start., &promo_end.);


	/* 	Объединяем результаты, чтобы не пропустить переходящие промо из года в год */
	data nac.russca_receipt_filter;
		set 
			nac.russca_receipt_filter_2019 
			nac.russca_receipt_filter_2020
			nac.russca_receipt_filter_2021;
	run;
	
	/* 	Сортируем таблицу */
	proc sort data=nac.russca_receipt_filter;
		by order_number pbo_location_id sales_dt;
	run;
	
	/* 	Создаем уникальный ID чека */
	data nac.russca_receipt_filter_id;
		set nac.russca_receipt_filter;
		by order_number pbo_location_id sales_dt;
		if first.sales_dt then receipt_id+1;
	run;

	/* Стираем временные таблицы */
	proc datasets library=nac;
		delete discount_promo_filter;
		delete russca_receipt_filter_2019;
		delete russca_receipt_filter_2020;
		delete russca_receipt_filter_2021;
		delete russca_receipt_filter;
	run;
	
%mend;



%macro evm(promo, option_number);
	/*
		Макрос, который считает N_a, T_a для механики по типу EVM/Set
		Алгоритм:
			1. Фильтруем таблицу с промо
			2. Пересекаем чеки с промо таблицей, считая сумму в рамках каждой позиции
			3. Если число поцизий в чеке = число позиций в промо, то N_a = min(среди всех позиций чека)
	*/
	
	/* Фильтруем чеки */
	proc sql;
		create table work.one_promo as
			select
				hybrid_promo_id,
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
				work.promo_ml2
			where
				hybrid_promo_id = "&promo."
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
				nac.russca_receipt_filter_id as t1
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
				"&promo." as hybrid_promo_id,
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
	proc append base=nac.na_calculation_result_new
		data = work.evm_na force;
	run;

	/* Удаляем промежуточные таблицы */
	proc datasets library=work;
		delete one_promo;
		delete promo_receipt;
		delete receipt_options;
		delete evm_na;
	run;

	proc datasets library=nac;
		delete russca_receipt_filter_id;
	run;
	
%mend;


%macro na_calculation(
	promo_lib = casuser, 
	ia_promo = past_promo,
	ia_promo_x_pbo = promo_pbo_enh,
	ia_promo_x_product = promo_prod_enh
);
/*
	Скрипт, который рассчитывает на истории эффективность промо акций.
	Параметры:
	----------
		* promo_lib: библиотека, где лежат таблицы с промо (предполагается,
			что таблицы лежат в cas)
		* ia_promo: название таблицы с информацией о промо 
		* ia_promo_x_pbo: название таблицы с привязкой промо к ресторнам
		* ia_promo_x_product: название таблицы с привязкой промо к товарам
	Выход:
	------
		Таблица nac.na_calculation_result_new с подсчитанными показателями
*/
	

	/****** Загрузим справочные иерархии ******/
	proc casutil;
		load data=etl_ia.pbo_loc_hierarchy(
			where=(year(valid_to_dttm) = 5999)
		) casout='ia_pbo_loc_hierarchy' outcaslib='casuser' replace;
		
		load data=etl_ia.product_hierarchy(
			where=(year(valid_to_dttm) = 5999)
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
				   1 as LVL1_ID
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
			select 
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
				t2.PBO_LEAF_ID
			from
				&promo_lib..&ia_promo_x_pbo. as t1,
				casuser.pbo_lvl_all as t2
			where
				t1.pbo_location_id = t2.PBO_LOCATION_ID
		;
		create table casuser.ia_promo_x_product_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t1.option_number,
				t1.product_qty,
				t2.product_LEAF_ID
			from
				&promo_lib..&ia_promo_x_product. as t1,
				casuser.product_lvl_all as t2
			where
				t1.product_id = t2.product_id
		;
		create table casuser.promo_ml{options replace = true} as 
			select
				t1.hybrid_promo_id,
				t3.option_number,
				t3.product_qty,
				t3.product_leaf_id as product_id,
				t2.pbo_leaf_id as pbo_location_id,
				t1.promo_nm,
				datepart(t1.start_dt) as start_dt,
				datepart(t1.end_dt) as end_dt,
				t1.channel_cd,
				t1.promo_mechanics
			from
				&promo_lib..&ia_promo. as t1 
			left join
				casuser.ia_promo_x_pbo_leaf as t2
			on 
				t1.PROMO_ID = t2.PROMO_ID
			left join
				casuser.ia_promo_x_product_leaf as t3
			on
				t1.PROMO_ID = t3.PROMO_ID 
		;
	quit;
	

	proc casutil;
		droptable casdata="pbo_hier_flat" incaslib="casuser" quiet;
		droptable casdata="product_hier_flat" incaslib="casuser" quiet;
		droptable casdata="lvl5" incaslib="casuser" quiet;
		droptable casdata="lvl4" incaslib="casuser" quiet;
		droptable casdata="lvl3" incaslib="casuser" quiet;
		droptable casdata="lvl2" incaslib="casuser" quiet;
		droptable casdata="lvl1" incaslib="casuser" quiet;
	  	droptable casdata="ia_pbo_loc_hierarchy" incaslib="casuser" quiet;
	  	droptable casdata="ia_product_hierarchy" incaslib="casuser" quiet;
	  	droptable casdata="ia_promo_x_product_leaf" incaslib="casuser" quiet;
	  	droptable casdata="ia_promo_x_pbo_leaf" incaslib="casuser" quiet;
	  	droptable casdata="pbo_lvl_all" incaslib="casuser" quiet;
	  	droptable casdata="product_lvl_all" incaslib="casuser" quiet;
		
	run;

	/* Выгружаем из cas  таблицу с промо */
	data work.promo_ml;
		set casuser.promo_ml;
	run;
	
	/* Меняем ID ресторнов */
	proc sql;
		create table work.promo_ml2 as 
			select
				t1.hybrid_promo_id,
				t1.option_number,
				t1.product_qty,
				t1.product_ID,
				input(t2.PBO_LOC_ATTR_VALUE, best32.) as pbo_location_id,
				t1.promo_nm,
				t1.start_dt,
				t1.end_dt,
				t1.channel_cd,
				t1.promo_mechanics
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

	/* Стираем итоговую таблицу */
	proc datasets library=nac;
		delete na_calculation_result_new;
	run;

	/* Задаем длинну переменных	*/
	data nac.na_calculation_result_new;
		length n_a 8. pbo_location_id 8. sales_dt 8. t_a 8. hybrid_promo_id varchar(36);
		stop;
	run;
	
	/* Считаем максимальный option number */
	proc sql;
		create table work.unique_evm_like_promo as
			select
				hybrid_promo_id,
				promo_nm,
				start_dt,
				end_dt,
				channel_cd,
				promo_mechanics,
				max(option_number) as max_option_number
			from
				work.promo_ml2
			group by
				hybrid_promo_id,
				promo_nm,
				start_dt,
				end_dt,
				channel_cd,
				promo_mechanics
		;
	quit;

	/* Вызываем в цикле макросы */
	data _null_;
	    set work.unique_evm_like_promo;
	    call execute('%prepare_receipt_data('||hybrid_promo_id||','||start_dt||','||end_dt||')');
	    call execute('%evm('||hybrid_promo_id||','||max_option_number||')');
	run;

	/* Удаляем промежуточные таблицы */
	proc datasets library=work;
		delete unique_evm_like_promo;
		delete promo_ml2;
		delete promo_ml;
	run;
	
	proc casutil;
	  	droptable casdata="promo_ml" incaslib="casuser" quiet;
	run;

%mend;