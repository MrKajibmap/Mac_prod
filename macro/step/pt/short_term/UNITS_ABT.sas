/*
	Схема программы:
	
	0. Объявление макропеременных: первый и последний день в истории, фильтрация по категориям
		товаров и каналам.
	1. Сбор "каркаса" из таблиц ia_pmix и ia_pmix_history с учетом фильтра.
	2. Добавление цен.
	3. Протягиваем временные ряды на 42 день с последнего дня в истории. Целевую переменную
		заполняем пропусками, цены заполняем предыдущим значением.
	4. Фильтрация:
		* Убираем временные закрытия ПБО.
		* Убираем закрытые магазины из справочника ПБО.
		* Убираем из истории пропуски в продажах (появившиеся после протяжки временных рядов).
		* PLM, убираем выведенные товары (пока на паузе).
		* Пересекаем с ассортиментной матрицей скоринговую витрину.
	5. Подсчет лагов.
	6. Добавление промо:
		* Бинарные флаги промо по промо механикам.
		* Флаг side_promo для регулярного товара вне промо.
		* Скидка?
	7. Добавляем мароэкономику (проверить заполненность на будущее).
	8. Добавляем погоду (проверить заполненность на будещее).
	9. Добавляем trp конкурентов.
	10. Добавляем медиаподдержку.
	11. Добавим атрибуты товаров. Дополнительно перекодируем текстовые переменные.
	12. Добавляем атрибуты ПБО. Дополнительно перекодируем текстовые переменные.
	13. Добавляем календарные признаки.
	14. Добавляем события. Можно забить на те события, которые нам передают сейчас, а
		просто взять и использовать, те события, которые Лиза считает существенными.
		*** Взять день города! ***
	15. Добавим ценовые ранги.
	16. Перекодируем channel_cd (текстовое поле на число). Делаем это в последнюю очередб
		 для удобства соединения таблиц.
	17. Разделяем витрину на обучение и скоринг. 
	Что еще хочется видеть в витрине?
	* Закодировать ID ПБО и товаров через агрегаты целевой переменной
	* Придумать признаков по промо. 
*/


/****** 0. Объявление макропеременных ******/
options mprint nomprintnest nomlogic nomlogicnest nosymbolgen mcompilenote=all mreplace;
/*Создать cas-сессию, если её нет*/
%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( casauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

options casdatalimit=600000M;

/* Подключение библиотек */
/* libname ETL_STG "/data2/etl_stg_23_11_2020"; */
/* Объявление макропеременных */
%let inlib=IA;
/* только бургеры в канале ALL */
/* %let filter = t2.prod_lvl3_id in (900116, 2420116, 2430116) and t1.channel_cd = 'ALL';  */
%let filter = t1.channel_cd = 'ALL';

%let hist_start_dt = date '2018-02-08';
%let hist_end_dt =  date '2021-05-16';

%let hist_end_dt_sas = '16may2021'd;

data _null_;
d = '16may2021'd;
d1 = '16may2018'd;
d2 = intnx('day',d1, -91, 'B');
format d2 date9.;
put d2= ;
run;


/****** 1. Сбор "каркаса" из таблиц ia_pmix и ia_pmix_history с учетом фильтра ******/
/* Сначала собираем справочник товаров для того, чтобы создать фильтр */
proc casutil;
  droptable casdata="product_dictionary_ml" incaslib="casuser" quiet;
  load data=&inlib..IA_product casout='ia_product' outcaslib='casuser' replace;
  load data=&inlib..IA_product_HIERARCHY casout='IA_product_HIERARCHY' outcaslib='casuser' replace;
  load data=&inlib..IA_product_ATTRIBUTES casout='IA_product_ATTRIBUTES' outcaslib='casuser' replace;
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
quit;

proc fedsql sessref=casauto;
   create table casuser.product_dictionary_ml{options replace=true} as
   select t1.product_id, 
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
   from casuser.product_hier_flat t1
   left join casuser.attr_transposed t3
   on t1.product_id=t3.product_id
   left join casuser.IA_product t15
   on t1.product_id=t15.product_id
   left join casuser.IA_product t14
   on t1.lvl4_id=t14.product_id
   left join casuser.IA_product t13
   on t1.lvl3_id=t13.product_id
   left join casuser.IA_product t12
   on t1.lvl2_id=t12.product_id;
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

	data work.encoding_&variable.;
		set work.unique;
		&variable._id = _N_;
	run;

	data casuser.encoding_&variable.;
		set work.encoding_&variable.;
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

	proc casutil;
 		promote casdata="encoding_&variable." incaslib="casuser" outcaslib="casuser"; 
	run;
%mend;

%text_encoding(casuser.product_dictionary_ml, a_hero)
%text_encoding(casuser.product_dictionary_ml, a_item_size)
%text_encoding(casuser.product_dictionary_ml, a_offer_type)
%text_encoding(casuser.product_dictionary_ml, a_price_tier)

proc casutil;
/*   promote casdata="product_dictionary_ml" incaslib="casuser" outcaslib="casuser"; */
  droptable casdata='ia_product' incaslib='casuser' quiet;
  droptable casdata='IA_product_HIERARCHY' incaslib='casuser' quiet;
  droptable casdata='IA_product_ATTRIBUTES' incaslib='casuser' quiet;
  droptable casdata='product_hier_flat' incaslib='casuser' quiet;
  droptable casdata='attr_transposed' incaslib='casuser' quiet;
run;

/* Подготовка таблицы с продажами */
/* proc casutil; */
/* 	droptable casdata="abt1_ml" incaslib="casuser" quiet; */
/* 	load data=&inlib..IA_pmix_sales_HISTORY casout='IA_pmix_sales_HISTORY' outcaslib='casuser' replace; */
/* run; */

proc fedsql sessref=casauto; 
	create table casuser.abt1_ml{options replace=true} as
		select 
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			datepart(t1.SALES_DT) as sales_dt,
			coalesce(sum(t1.SALES_QTY,t1.SALES_QTY_PROMO), 0) as sum_qty
		from
			MN_SHORT.PMIX_SALES /*MAX_CASL.IA_pmix_sales_HISTORY*/ as t1 
		where
			&filter. and
			datepart(t1.SALES_DT) >= &hist_start_dt. and
			datepart(t1.SALES_DT) <= &hist_end_dt.
	;
quit;

proc casutil;
/* 	droptable casdata="IA_pmix_sales_HISTORY" incaslib="casuser" quiet; */
	load data=&inlib..ia_product_chain casout="ia_product_chain" outcaslib="casuser" replace;
run;

/* Добавляем к скоринговой витрине записи из product_chain с флагом N */
proc fedsql sessref=casauto;
	create table casuser.future_product_chain{options replace=true} as
		select
			SUCCESSOR_PRODUCT_ID as product_id,
			SUCCESSOR_DIM2_ID as pbo_location_id,
			datepart(SUCCESSOR_START_DT) as period_start_dt
		from
			casuser.ia_product_chain as t1
		where
			LIFECYCLE_CD = 'N' and 
			datepart(SUCCESSOR_START_DT) >= &hist_end_dt.
	;
quit;

/* Продляем эти интервалы */
data casuser.future_product_scoring;
	set casuser.future_product_chain;
	format sales_dt DATE9.;
	channel_cd = 'ALL';
	do sales_dt = period_start_dt to (&hist_end_dt_sas. + 91);
	   output;
	end;
run;

/* Фильтруем временные ряды, оставляя только даты в рамках (hist_end_dt, hist_end_dt+91) */
proc fedsql sessref=casauto;
	create table casuser.future_product_scoring{options replace=true} as
		select
			channel_cd,
			product_id,
			pbo_location_id,
			sales_dt,
			. as sum_qty,
			. as GROSS_PRICE_AMT			
		from
			casuser.future_product_scoring
		where
			sales_dt > &hist_end_dt. and
			sales_dt <= &hist_end_dt. + 91
	;
quit;

/* Добавляем к abt новые товары */
data casuser.abt1_ml_unioun;
	set casuser.abt1_ml casuser.future_product_scoring;
run;

/* Избавляемся от дублей, которые могут возникнуть при объединении */
proc fedsql sessref=casauto;
	create table casuser.abt1_ml{options replace=true} as
		select
			channel_cd,
			product_id,
			pbo_location_id,
			sales_dt,
			max(sum_qty) as sum_qty,
			max(GROSS_PRICE_AMT) as GROSS_PRICE_AMT
		from
			casuser.abt1_ml_unioun
		group by
			channel_cd,
			product_id,
			pbo_location_id,
			sales_dt
	;
quit;

proc casutil;
/* 	  droptable casdata="abt1_ml" incaslib="casuser" quiet; */
	  droptable casdata="abt1_ml_unioun" incaslib="casuser" quiet;
run;

/****** 3. Добавление цен ******/
proc casutil;
  droptable casdata="price_ml" incaslib="casuser" quiet;
  droptable casdata="abt2_ml" incaslib="casuser" quiet;
  load data=&inlib..ia_price_history casout='ia_price_history' outcaslib='casuser' replace;
  load data=&inlib..ia_price casout='ia_price' outcaslib='casuser' replace;
run;

proc fedsql sessref=casauto; 
	/* Объединяем историю с актуальными данными */
	create table casuser.price_ml{options replace=true} as
		select 
			coalesce(t1.PBO_LOCATION_ID, t2.PBO_LOCATION_ID) as PBO_LOCATION_ID,
			coalesce(t1.PRODUCT_ID, t2.PRODUCT_ID) as PRODUCT_ID,
			coalesce(datepart(t1.start_dt), datepart(t2.start_dt)) as start_dt,
			coalesce(datepart(t1.end_dt), datepart(t2.end_dt)) as end_dt,
			coalesce(t1.GROSS_PRICE_AMT, t2.GROSS_PRICE_AMT) as GROSS_PRICE_AMT,
			coalesce(t1.NET_PRICE_AMT, t2.NET_PRICE_AMT) as NET_PRICE_AMT,
			coalesce(t1.PRICE_TYPE, t2.PRICE_TYPE) as PRICE_TYPE
		from 
			casuser.ia_price as t1
		full outer join
			casuser.ia_price_history as t2
		on
			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
			t1.PRODUCT_ID = t2.PRODUCT_ID and 
			t1.start_dt = t2.start_dt and
			t1.end_dt = t2.end_dt and 
			t1.PRICE_TYPE = t2.PRICE_TYPE
	;
quit;

/* Добавляем к продажам цены */
proc fedsql sessref=casauto; 
	create table casuser.abt2_ml{options replace=true} as 
        select
            t1.PBO_LOCATION_ID,
            t1.PRODUCT_ID,
            t1.CHANNEL_CD,
            t1.SALES_DT,
            t1.sum_qty,
            max(t2.GROSS_PRICE_AMT) as GROSS_PRICE_AMT
        from
            casuser.abt1_ml as t1 left join
            casuser.price_ml as t2
        on
            t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
            t1.PRODUCT_ID = t2.PRODUCT_ID and
            t1.SALES_DT <= t2.end_dt and   
            t1.SALES_DT >= t2.start_dt
        group by 
			t1.PBO_LOCATION_ID,
            t1.PRODUCT_ID,
            t1.CHANNEL_CD,
            t1.SALES_DT,
            t1.sum_qty
   	 	;
quit;

/****** 2. Протяжка временных рядов ******/
proc casutil;
  droptable casdata="abt3_ml" incaslib="casuser" quiet;
  droptable casdata="abt1_ml" incaslib="casuser" quiet;
run;

%let fc_end_sas=%sysfunc(inputn(%scan(%bquote(&hist_end_dt),2,%bquote(' )),yymmdd10.));
%let fc_end= %sysfunc(intnx(day,&fc_end_sas, 91),yymmddd10.);
proc cas;
timeData.timeSeries result =r /
	series={
		{name="sum_qty", setmiss="MISSING"},
		{name="GROSS_PRICE_AMT", setmiss="PREV"}
	}
	tEnd= "&fc_end"
	table={
		caslib="casuser",
		name="abt2_ml",
		groupby={"PBO_LOCATION_ID","PRODUCT_ID", "CHANNEL_CD"}
	}
	timeId="SALES_DT"
	trimId="LEFT"
	interval="day"
	casOut={caslib="casuser", name="abt3_ml", replace=True}
	;
run;
quit;
proc casutil;
  droptable casdata="price_ml" incaslib="casuser" quiet;
  droptable casdata="ia_price_history" incaslib="casuser" quiet;
  droptable casdata="ia_price" incaslib="casuser" quiet;
  droptable casdata="abt2_ml" incaslib="casuser" quiet;
run;
/****** 4. Фильтрация ******/
/* Проверки. Аня писала, что могут буть дубли. Вставил дополнительную проверку */
%macro check(table, name);
proc fedsql sessref=casauto;
	select
		count(1) as &name.
	from
		(
		select
			pbo_location_id,
			product_id,
			sales_dt,
			channel_cd,
			count(sales_dt) as cnt
		from
			casuser.&table.
		group by
			pbo_location_id,
			product_id,
			sales_dt,
			channel_cd
		) as t1
	where
		t1.cnt > 1
	;
quit;
%mend;
/* 4.1 Убираем временные закрытия ПБО */
proc casutil;
	droptable casdata="pbo_closed_ml" incaslib="casuser" quiet;
	droptable casdata="abt4_ml" incaslib="casuser" quiet;
	load data=&inlib..ia_pbo_close_period casout='ia_pbo_close_period' outcaslib='casuser' replace;
run;
/* заполняем пропуски в end_dt */
proc fedsql sessref=casauto;
	create table casuser.pbo_closed_ml {options replace=true} as
		select 
			CHANNEL_CD,
			PBO_LOCATION_ID,
			datepart(start_dt) as start_dt,
			coalesce(datepart(end_dt), date '2100-01-01') as end_dt,
			CLOSE_PERIOD_DESC
		from
			casuser.ia_pbo_close_period
	;
quit;
/* Удалаем даты закрытия pbo из abt */
proc fedsql sessref=casauto;
	create table casuser.abt4_ml{options replace=true} as
		select 
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT
		from
			casuser.abt3_ml as t1
		left join
			casuser.pbo_closed_ml as t2
		on
			t1.sales_dt >= t2.start_dt and
			t1.sales_dt <= t2.end_dt and
			t1.pbo_location_id = t2.pbo_location_id and
			t1.channel_cd = t2.channel_cd
		where
			t2.pbo_location_id is missing
	;
quit;
/* 4.2 Убираем закрытые насовсем магазины */
proc casutil;
	load data=&inlib..IA_PBO_LOC_ATTRIBUTES casout='IA_PBO_LOC_ATTRIBUTES' outcaslib='casuser' replace;
	droptable casdata="closed_pbo" incaslib="casuser" quiet;
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
/* Преобразовываем даты открытия и закрытия магазинов */
proc fedsql sessref=casauto;
	create table casuser.closed_pbo{options replace=true} as 
		select distinct
			pbo_location_id,
		    cast(inputn(A_OPEN_DATE,'ddmmyy10.') as date) as OPEN_DATE,
		    coalesce(
				cast(inputn(A_CLOSE_DATE,'ddmmyy10.') as date),
				date '2100-01-01'
			) as CLOSE_DATE
		from casuser.attr_transposed
	;
quit;
/* Удаляем закрытые насовсем магазины  */
proc fedsql sessref=casauto;
	create table casuser.abt4_ml{options replace = true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT
		from
			casuser.abt4_ml as t1
		left join
			casuser.closed_pbo as t2
		on
			t1.pbo_location_id = t2.pbo_location_id and
			t1.sales_dt >= t2.OPEN_DATE and
			t1.sales_dt <= t2.CLOSE_DATE
		where
			t2.pbo_location_id is not missing
	;
quit;

proc casutil;
	promote 
		casdata="ABT4_ML" 
		casout="ABT4_ML"  
		incaslib="casuser" 
		outcaslib="MAX_CASL"
	;
run;
/* <UPDATE> */
/* 4.3 Оставляем нулевые продажи в периодах ввода товаров из product chain */
proc fedsql sessref=casauto;
	create table casuser.abt4_ml_1{options replace=true} as 
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t2.predecessor_product_id,
/* 			coalesce(t1.sum_qty, case when t2.predecessor_product_id ^= . then 0 else . end) as sum_qty, */
			coalesce(t1.sum_qty, case when sales_dt between date '2020-11-01' and date '2021-01-31' then 0 else . end) as sum_qty,
/* 			case when t2.predecessor_product_id ^= . then coalesce(t1.sum_qty, 0) else . end as sum_qty, */
			t1.GROSS_PRICE_AMT
		from 
			casuser.abt4_ml as t1
		left join 
			casuser.ia_product_chain t2
		on 
			t2.predecessor_product_id = t1.product_id 
			and t2.predecessor_dim2_id = t1.pbo_location_id
			and t1.sales_dt between datepart(t2.successor_start_dt) and datepart(t2.predecessor_end_dt)
			and t2.lifecycle_cd = 'N'
			and t1.sales_dt <= &hist_end_dt.
	;
quit;

proc casutil;
	promote 
		casdata="ABT4_ML_1" 
		casout="ABT4_ML_1"  
		incaslib="casuser" 
		outcaslib="MAX_CASL"
	;
run;
/* </UPDATE> */
/* 4.4 Убираем из истории пропуски в продажах */
proc fedsql sessref=casauto;
	create table casuser.abt4_ml{options replace=true} as 
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT
		from 
			casuser.abt4_ml as t1
		where 
			(t1.sum_qty is not missing and t1.sales_dt <= &hist_end_dt.) or
			(t1.sales_dt > &hist_end_dt.)
	;
quit;

/* 4.4 Пересекаем с АМ. Оставляем ВР если он есть в АМ или product_chain с флагом N */
proc casutil;
	load data=&inlib..ia_assort_matrix casout='ia_assort_matrix' outcaslib='casuser' replace;
run;
proc fedsql sessref=casauto;
	create table casuser.abt4_ml {options replace = true} as	
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT					
		from
			casuser.abt4_ml as t1
		left join
			casuser.ia_assort_matrix  t2
		on
			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
			t1.PRODUCT_ID = t2.PRODUCT_ID and
			t1.SALES_DT <= datepart(t2.end_dt) and 
			t1.SALES_DT >= datepart(t2.start_dt)
		left join
			casuser.future_product_scoring as t3
		on
			t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID and
			t1.PRODUCT_ID = t3.PRODUCT_ID and
			t1.sales_dt = t3.sales_dt		
		where	
			(t1.SALES_DT <= &hist_end_dt) or 
			(t2.PBO_LOCATION_ID is not missing) or 
			(t3.PBO_LOCATION_ID is not missing)
	;
quit;
/* Это нужно на ретроспективе, чтобы лаги считались правильно */
/* proc fedsql sessref=casauto; */
/* 	create table casuser.abt4_ml {options replace = true} as	 */
/* 		select */
/* 			t1.PBO_LOCATION_ID, */
/* 			t1.PRODUCT_ID, */
/* 			t1.CHANNEL_CD, */
/* 			t1.SALES_DT, */
/* 			case when t1.sales_dt <= date '2020-10-31' then t1.sum_qty else . end as sum_qty, */
/* 			t1.GROSS_PRICE_AMT					 */
/* 		from */
/* 			casuser.abt4_ml t1 */
/* 	; */
/* quit; */
proc casutil;
	droptable casdata="pbo_closed_ml" incaslib="casuser" quiet;
	droptable casdata="closed_pbo" incaslib="casuser" quiet;
	droptable casdata="ia_assort_matrix" incaslib="casuser" quiet;
	droptable casdata="ia_pbo_close_period" incaslib="casuser" quiet;
	droptable casdata="IA_PBO_LOC_ATTRIBUTES" incaslib="casuser" quiet;
	droptable casdata="attr_transposed" incaslib="casuser" quiet;
    droptable casdata="abt3_ml" incaslib="casuser" quiet;
	promote casdata="abt4_ml" incaslib="casuser" outcaslib="casuser";
run;
/****** 5. Подсчет лагов ******/
proc casutil;
  droptable casdata='lag_abt1' incaslib='casuser' quiet;
  droptable casdata='lag_abt2' incaslib='casuser' quiet;
  droptable casdata='lag_abt3' incaslib='casuser' quiet;
  droptable casdata='abt5_ml' incaslib='casuser' quiet;
run;
/* Макрос разворачивает переменную var в список элементов массива, разделенных запятой */
/* var[t-0],var[t-1],var[t-2],... */
%macro argt(var,index,start,end);
%do ii=&start. %to &end.;
 &var.[&index.-&ii.]
 %if &ii. ne &end. %then %do;
  ,
 %end;
%end;
%mend argt;
/* Перекодирование числа дней в название интервала*/
%macro namet(l_int);
%if &l_int=7 %then week ;
%if &l_int=30 %then month ;
%if &l_int=90 %then qtr ;
%if &l_int=180 %then halfyear ;
%if &l_int=365 %then year ;
%mend namet;
/* считаем медиану и среднее арифметическое */
%macro cmpcode;
proc cas;
timeData.runTimeCode result=r /
	table = {
		name ='abt4_ml',
		caslib = 'casuser', 
        groupBy = {
			{name = 'PRODUCT_ID'},
			{name = 'PBO_LOCATION_ID'},
			{name = 'CHANNEL_CD'}
		}
	},
 	series = {{name='sum_qty'}},
 	interval='DAY',
	timeId = {name='SALES_DT'},
	trimId = "LEFT", 
	code=
		%unquote(%str(%"))
		%let names=; /*будущий список выходных переменных для proc cas */
		%let minlag=42; /*параметр MinLag*/
		/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
		%let window_list = 7 30 90 180 365;
		%let lag=&minlag;
		%let n_win_list=%sysfunc(countw(&window_list.));
		%do ic=1 %to &n_win_list.;
		  %let window=%scan(&window_list,&ic); /*текущее окно*/
		  %let intnm=%namet(&window);        /*название интервала окна; 7->week итд */
		  %let intnm=%sysfunc(strip(&intnm.));
		  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
		    lag_&intnm._avg[t]=mean(%argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
		    lag_&intnm._med[t]=median(%argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
		  end;
		 %let names={name=%tslit(lag_&intnm._avg)}, &names;
		 %let names={name=%tslit(lag_&intnm._med)}, &names; 
		%end; /* ic over window_list*/
		/*remove last comma from names*/
		%let len=%length(&names);
		%let names=%substr(%quote(&names),1,%eval(&len-1));
		/*-=-=-завершающий код proc cas=-=-=*/
		%unquote(%str(%"))
	,
	arrayOut={
		table={name='lag_abt1', replace=true, caslib='casuser'},
	    arrays={&names}
	}
;
run;
quit;
%mend cmpcode;
%cmpcode
/* Считаем стандартное отклонение */
%macro cmpcode2;
proc cas;
timeData.runTimeCode result=r /
	table = {
		name ='abt4_ml',
		caslib = 'casuser', 
        groupBy = {
			{name = 'PRODUCT_ID'},
			{name = 'PBO_LOCATION_ID'},
			{name = 'CHANNEL_CD'}
		}
	},
 	series = {{name='sum_qty'}},
 	interval='DAY',
	timeId = {name='SALES_DT'},
	trimId = "LEFT",
	code=
		%unquote(%str(%"))
		%let names=; /*будущий список выходных переменных для proc cas */
		%let minlag=42; /*параметр MinLag*/
		/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
		%let window_list = 7 30 90 180 365;
		%let lag=&minlag;
		%let n_win_list=%sysfunc(countw(&window_list.));
		%do ic=1 %to &n_win_list.;
		  %let window=%scan(&window_list,&ic); /*текущее окно*/
		  %let intnm=%namet(&window);        /*название интервала окна; 7->week итд */
		  %let intnm=%sysfunc(strip(&intnm.));
		  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
		    lag_&intnm._std[t]=std(%argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
		  end;
		 %let names={name=%tslit(lag_&intnm._std)}, &names;
		%end; /* ic over window_list*/
		/*remove last comma from names*/
		%let len=%length(&names);
		%let names=%substr(%quote(&names),1,%eval(&len-1));
		/*-=-=-завершающий код proc cas=-=-=*/
		%unquote(%str(%"))
	,
	arrayOut={
		table={name='lag_abt2', replace=true, caslib='casuser'},
	    arrays={&names}
	}
;
run;
quit;
%mend cmpcode2;
%cmpcode2
/* Считаем процентили */
%macro cmpcode3;
proc cas;
timeData.runTimeCode result=r /
	table = {
		name ='abt4_ml',
		caslib = 'casuser', 
        groupBy = {
			{name = 'PRODUCT_ID'},
			{name = 'PBO_LOCATION_ID'},
			{name = 'CHANNEL_CD'}
		}
	},
 	series = {{name='sum_qty'}},
 	interval='DAY',
	timeId = {name='SALES_DT'},
	trimId = "LEFT",
	code=
		%unquote(%str(%"))
		%let names=; /*будущий список выходных переменных для proc cas */
		%let minlag=42; /*параметр MinLag*/
		/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
		%let window_list = 7 30 90 180 365;
		%let lag=&minlag;
		%let n_win_list=%sysfunc(countw(&window_list.));
		%do ic=1 %to &n_win_list.;
		  %let window=%scan(&window_list,&ic); /*текущее окно*/
		  %let intnm=%namet(&window);        /*название интервала окна; 7->week итд */
		  %let intnm=%sysfunc(strip(&intnm.));
		  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
			lag_&intnm._pct10[t]=pctl(10,%argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1))) ;
   			lag_&intnm._pct90[t]=pctl(90,%argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1))) ;
		  end;
		 %let names={name=%tslit(lag_&intnm._pct10)}, &names;
		 %let names={name=%tslit(lag_&intnm._pct90)}, &names;
		%end; /* ic over window_list*/
		/*remove last comma from names*/
		%let len=%length(&names);
		%let names=%substr(%quote(&names),1,%eval(&len-1));
		/*-=-=-завершающий код proc cas=-=-=*/
		%unquote(%str(%"))
	,
	arrayOut={
		table={name='lag_abt3', replace=true, caslib='casuser'},
	    arrays={&names}
	}
;
run;
quit;
%mend cmpcode3;
%cmpcode3
/* соеденим среднее, медиану, стд, процентили вместе, убирая пропуску вр ВР */
proc fedsql sessref=casauto;
	create table casuser.abt5_ml{options replace=true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT,
			t2.lag_halfyear_avg,
			t2.lag_halfyear_med,
			t2.lag_month_avg,
			t2.lag_month_med,
			t2.lag_qtr_avg,
			t2.lag_qtr_med,
			t2.lag_week_avg,
			t2.lag_week_med,
			t2.lag_year_avg,
			t2.lag_year_med
		from
			casuser.abt4_ml as t1,
			casuser.lag_abt1 as t2
		where
			t1.CHANNEL_CD = t2.CHANNEL_CD and
			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
			t1.PRODUCT_ID = t2.PRODUCT_ID and
			t1.SALES_DT = t2.SALES_DT
	;
quit;
proc fedsql sessref=casauto;
	create table casuser.abt5_ml{options replace=true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT,
			t1.lag_halfyear_avg,
			t1.lag_halfyear_med,
			t1.lag_month_avg,
			t1.lag_month_med,
			t1.lag_qtr_avg,
			t1.lag_qtr_med,
			t1.lag_week_avg,
			t1.lag_week_med,
			t1.lag_year_avg,
			t1.lag_year_med,
			t2.lag_halfyear_std,
			t2.lag_month_std,
			t2.lag_qtr_std,
			t2.lag_week_std,
			t2.lag_year_std
		from
			casuser.abt5_ml as t1,
			casuser.lag_abt2 as t2
		where
			t1.CHANNEL_CD = t2.CHANNEL_CD and
			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
			t1.PRODUCT_ID = t2.PRODUCT_ID and
			t1.SALES_DT = t2.SALES_DT
	;
quit;
proc fedsql sessref=casauto;
	create table casuser.abt5_ml{options replace=true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT,
			t1.lag_halfyear_avg,
			t1.lag_halfyear_med,
			t1.lag_month_avg,
			t1.lag_month_med,
			t1.lag_qtr_avg,
			t1.lag_qtr_med,
			t1.lag_week_avg,
			t1.lag_week_med,
			t1.lag_year_avg,
			t1.lag_year_med,
			t1.lag_halfyear_std,
			t1.lag_month_std,
			t1.lag_qtr_std,
			t1.lag_week_std,
			t1.lag_year_std,
			t2.lag_halfyear_pct10,		 
			t2.lag_halfyear_pct90,		 
			t2.lag_month_pct10	,
			t2.lag_month_pct90	,
			t2.lag_qtr_pct10,	
			t2.lag_qtr_pct90,	
			t2.lag_week_pct10,	
			t2.lag_week_pct90,	
			t2.lag_year_pct10,	
			t2.lag_year_pct90
		from
			casuser.abt5_ml as t1,
			casuser.lag_abt3 as t2
		where
			t1.CHANNEL_CD = t2.CHANNEL_CD and
			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
			t1.PRODUCT_ID = t2.PRODUCT_ID and
			t1.SALES_DT = t2.SALES_DT
	;
quit;
proc casutil;
/*   promote casdata="abt5_ml" incaslib="casuser" outcaslib="casuser"; */
  droptable casdata='lag_abt1' incaslib='casuser' quiet;
  droptable casdata='lag_abt2' incaslib='casuser' quiet;
  droptable casdata='lag_abt3' incaslib='casuser' quiet;
  droptable casdata="abt4_ml" incaslib="casuser" quiet;
run;
/****** 6. Добавление промо ******/
	%add_promotool_marks2(mpOutCaslib=casuser,
							mpPtCaslib=pt);
proc casutil;
  promote casdata="abt5_ml" incaslib="casuser" outcaslib="casuser";
run;

proc casutil;
	load data=&inlib..ia_pbo_loc_hierarchy casout='ia_pbo_loc_hierarchy' outcaslib='casuser' replace;
	load data=&inlib..ia_product_hierarchy casout='ia_product_hierarchy' outcaslib='casuser' replace;
/* 	load data=&inlib..promo_enh casout='ia_promo' outcaslib='casuser' replace; */
/* 	load data=&inlib..promo_pbo_enh casout='ia_promo_x_pbo' outcaslib='casuser' replace;	 */
/* 	load data=&inlib..promo_product_enh casout='ia_promo_x_product' outcaslib='casuser' replace; */
	droptable casdata="pbo_hier_flat" incaslib="casuser" quiet;
	droptable casdata="product_hier_flat" incaslib="casuser" quiet;
	droptable casdata="lvl5" incaslib="casuser" quiet;
	droptable casdata="lvl4" incaslib="casuser" quiet;
	droptable casdata="lvl3" incaslib="casuser" quiet;
	droptable casdata="lvl2" incaslib="casuser" quiet;
	droptable casdata="lvl1" incaslib="casuser" quiet;
	droptable casdata="pbo_lvl_all" incaslib="casuser" quiet;
	droptable casdata="product_lvl_all" incaslib="casuser" quiet;
  	droptable casdata="promo_ml" incaslib="casuser" quiet;
  	droptable casdata="promo_transposed" incaslib="casuser" quiet;
  	droptable casdata="abt6_ml" incaslib="casuser" quiet;
  	droptable casdata="ia_promo_x_product_leaf" incaslib="casuser" quiet;
  	droptable casdata="ia_promo_x_pbo_leaf" incaslib="casuser" quiet;
  	droptable casdata="promo_ml_main_code" incaslib="casuser" quiet;
  	droptable casdata="abt_promo" incaslib="casuser" quiet;
run;
/* Создаем таблицу соответствия наименований промо-механик и реальных механик */
/* data casuser.promo_mech_transformation; */
/* 	infile datalines delimiter='|'; */
/* 	length old_mechanic new_mechanic $50;  */
/*     input old_mechanic $ new_mechanic $; */
/*     datalines; */
/* BOGO / 1+1|bogo */
/* Discount|discount */
/* EVM/Set|evm_set */
/* Non-Product Gift|non_product_gift */
/* Pairs|pairs */
/* Product Gift|product_gift */
/* Other: Discount for volume|other_discount */
/* Other: Digital (app)|other_digital */
/* NP Promo Support|support */
/* ; */
/* run; */
%let RTP_PROMO_MECH_TRANSF_FILE = /home/ru-mpovod/my_data/PROMO_MECH_TRANSFORMATION.csv;
data casuser.promo_mech_transformation;
	length old_mechanic new_mechanic $50;
	infile "&RTP_PROMO_MECH_TRANSF_FILE." dsd firstobs=2;                 
	input old_mechanic $ new_mechanic $;                            
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
			casuser.promo_pbo_enh /*ia_promo_x_pbo*/ as t1,
			casuser.pbo_lvl_all as t2
		where t1.pbo_location_id = t2.PBO_LOCATION_ID
	;
	create table casuser.ia_promo_x_product_leaf{options replace = true} as 
		select distinct
			t1.promo_id,
			t2.product_LEAF_ID
		from
			casuser.promo_prod_enh /*ia_promo_x_product*/ as t1,
			casuser.product_lvl_all as t2
		where t1.product_id = t2.product_id
	;
	create table casuser.promo_ml{options replace = true} as 
		select
			t1.PROMO_ID,
			t3.product_LEAF_ID,
			t2.PBO_LEAF_ID,
			t1.PROMO_NM,
			t1.PROMO_PRICE_AMT,
			datepart(t1.START_DT) as start_dt,
			datepart(t1.END_DT) as end_dt,
			t1.CHANNEL_CD,
			t1.NP_GIFT_PRICE_AMT,
			t1.PROMO_MECHANICS,
			t4.new_mechanic as promo_mechanics_name,
			1 as promo_flag		
		from
			casuser.promo_enh /*ia_promo*/ as t1 
		left join
			casuser.ia_promo_x_pbo_leaf as t2
		on 
			t1.PROMO_ID = t2.PROMO_ID
		left join
			casuser.ia_promo_x_product_leaf as t3
		on
			t1.PROMO_ID = t3.PROMO_ID 
		inner join 
			casuser.promo_mech_transformation as t4
		on t1.promo_mechanics = t4.old_mechanic 
	;
quit;
/* Генерим макропеременные для вставки в код */
data _null_;
	set casuser.promo_ml end=end;
	length sql_list sql_max_list model_list $1000;
	retain sql_list model_list sql_max_list;
	by promo_mechanics_name;
	if _n_ = 1 then do;
		sql_list = cats('t1.', promo_mechanics_name);
		model_list = promo_mechanics_name;
		sql_max_list = cat('max(coalesce(t2.', strip(promo_mechanics_name), ', 0)) as ', strip(promo_mechanics_name));
	end;
	else if first.promo_mechanics_name then do;
		sql_list = cats(sql_list, ', t1.', promo_mechanics_name);
		model_list = catx('', model_list, promo_mechanics_name);
		sql_max_list = cat(strip(sql_max_list), ', max(coalesce(t2.', strip(promo_mechanics_name), ', 0)) as ', strip(promo_mechanics_name));
	end;
	if end then do;
/* 		put model_list; */
		call symputx('promo_list_sql', sql_list, 'G');
		call symputx('promo_list_model', model_list, 'G');
		call symputx('promo_list_sql_max', sql_max_list, 'G');
	end;
run;
%put &promo_list_sql.;
%put &promo_list_model.;
%put &promo_list_sql_max.;
	
proc fedsql sessref=casauto;
	create table casuser.promo_ml_main_code{options replace = true} as 
		select distinct
			(MOD(t2.LVL4_ID, 10000)) AS product_MAIN_CODE,
			t1.PBO_LEAF_ID,
			datepart(t1.START_DT) as start_dt,
			datepart(t1.END_DT) as end_dt,
			t1.CHANNEL_CD,
			case
				when product_LEAF_ID = MOD(t2.LVL4_ID, 10000) then 0
				else 1
			end as side_promo_flag
				
		from
			casuser.promo_ml as t1 
		left join
			casuser.product_hier_flat as t2
		on 
			t1.product_LEAF_ID = t2.product_id
	;
quit;
/* транспонируем таблицу с промо по типам промо механк */
proc cas;
transpose.transpose /
	table = {
		name="promo_ml",
		caslib="casuser",
		groupby={"promo_id", "product_LEAF_ID", "PBO_LEAF_ID", "CHANNEL_CD", "START_DT", "END_DT", "NP_GIFT_PRICE_AMT"}}
	transpose={"promo_flag"} 
	id={"promo_mechanics_name"} 
	casout={name="promo_transposed", caslib="casuser", replace=true};
quit;
%let promo_list_sql_t2 = %sysfunc(tranwrd(%quote(&promo_list_sql.),%str(t1),%str(t2)));
%put &promo_list_sql_t2.;
/* Соединяем с витриной */
proc fedsql sessref = casauto;
	/* Подготавливаем таблицу для джойна с витриной */
	create table casuser.abt_promo{options replace = true} as 
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			&promo_list_sql_max.,
			max(coalesce(t2.NP_GIFT_PRICE_AMT, .)) as NP_GIFT_PRICE_AMT,
			max(coalesce(t3.side_promo_flag, 0)) as side_promo_flag
		from
			casuser.abt5_ml as t1
		left join
			casuser.promo_transposed as t2
		on
			t1.product_id = t2.product_LEAF_ID 
			and t1.pbo_location_id = t2.PBO_LEAF_ID 
			and t1.SALES_DT <= t2.END_DT 
			and t1.SALES_DT >= t2.START_DT
/* 			and t1.CHANNEL_CD = t2.CHANNEL_CD */
		left join
			casuser.promo_ml_main_code as t3
		on
			t1.product_id = t3.product_MAIN_CODE and
			t1.pbo_location_id = t3.PBO_LEAF_ID and
			t1.SALES_DT <= t3.END_DT and
			t1.SALES_DT >= t3.START_DT
		group by
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT
	;
	/* Добавляем промо к витрине */
	create table casuser.abt6_ml{options replace = true} as 
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT,
			t1.lag_halfyear_avg,
			t1.lag_halfyear_med,
			t1.lag_month_avg,
			t1.lag_month_med,
			t1.lag_qtr_avg,
			t1.lag_qtr_med,
			t1.lag_week_avg,
			t1.lag_week_med,
			t1.lag_year_avg,
			t1.lag_year_med,
			t1.lag_halfyear_std,
			t1.lag_month_std,
			t1.lag_qtr_std,
			t1.lag_week_std,
			t1.lag_year_std,
			t1.lag_halfyear_pct10,		 
			t1.lag_halfyear_pct90,		 
			t1.lag_month_pct10	,
			t1.lag_month_pct90	,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			&promo_list_sql_t2.,
			t2.NP_GIFT_PRICE_AMT,
 			t2.side_promo_flag 
		from
			casuser.abt5_ml as t1
		left join
			casuser.abt_promo as t2
		on
			t1.product_id = t2.product_id and
			t1.pbo_location_id = t2.pbo_location_id and
			t1.SALES_DT = t2.SALES_DT and
			t1.CHANNEL_CD = t2.CHANNEL_CD
	;
quit;

proc casutil;
	promote casdata="abt6_ml" casout="abt6_ml" incaslib="casuser" outcaslib="MAX_CASL";
	promote casdata="abt6_ml" casout="abt6_ml" incaslib="casuser" outcaslib="casuser";
run;

proc casutil;
/* 	promote casdata="abt6_ml" incaslib="casuser" outcaslib="casuser"; */
/* 	promote casdata="pbo_lvl_all" incaslib="casuser" outcaslib="casuser"; */
/* 	promote casdata="product_lvl_all" incaslib="casuser" outcaslib="casuser"; */
  	promote casdata="promo_ml" incaslib="casuser" outcaslib="casuser";
	droptable casdata="pbo_hier_flat" incaslib="casuser" quiet;
	droptable casdata="product_hier_flat" incaslib="casuser" quiet;
	droptable casdata="lvl5" incaslib="casuser" quiet;
	droptable casdata="lvl4" incaslib="casuser" quiet;
	droptable casdata="lvl3" incaslib="casuser" quiet;
	droptable casdata="lvl2" incaslib="casuser" quiet;
	droptable casdata="lvl1" incaslib="casuser" quiet;
  	droptable casdata="ia_pbo_loc_hierarchy" incaslib="casuser" quiet;
  	droptable casdata="ia_product_hierarchy" incaslib="casuser" quiet;
  	droptable casdata="ia_promo_x_pbo" incaslib="casuser" quiet;
  	droptable casdata="ia_promo_x_product" incaslib="casuser" quiet;
  	droptable casdata="promo_transposed" incaslib="casuser" quiet;
  	droptable casdata="ia_promo_x_product_leaf" incaslib="casuser" quiet;
  	droptable casdata="ia_promo_x_pbo_leaf" incaslib="casuser" quiet;
  	droptable casdata="promo_ml_main_code" incaslib="casuser" quiet;
  	droptable casdata="abt_promo" incaslib="casuser" quiet;
    droptable casdata="abt5_ml" incaslib="casuser" quiet;
run;
/****** 7. Добавляем макроэкономику ******/
proc casutil;
  droptable casdata="macro_ml" incaslib="casuser" quiet;
  droptable casdata="macro2_ml" incaslib="casuser" quiet;
  droptable casdata="macro_transposed_ml" incaslib="casuser" quiet;
  droptable casdata="abt7_ml" incaslib="casuser" quiet;
  load data=&inlib..IA_macro_factor casout='ia_macro' outcaslib='casuser' replace;
run;
proc fedsql sessref=casauto;
	create table casuser.macro_ml{options replace=true} as 
		select 
			factor_cd,
			datepart(cast(REPORT_DT as timestamp)) as period_dt,
			FACTOR_CHNG_PCT
		from casuser.ia_macro;
quit;
data casuser.macro2_ml;
  format period_dt date9.;
  drop pdt;
  set casuser.macro_ml(rename=(period_dt=pdt));
  by factor_cd pdt;
  factor_cd=substr(factor_cd,1,3);
  period_dt=pdt;
  do until (period_dt>=intnx('day',intnx('month',pdt,3,'b'),0,'b'));
    output;
    period_dt=intnx('day',period_dt,1,'b');
  end;
run;
proc cas;
transpose.transpose /
   table={name="macro2_ml", caslib="casuser", groupby={"period_dt"}} 
   attributes={{name="period_dt"}} 
   transpose={"FACTOR_CHNG_PCT"} 
   prefix="A_" 
   id={"factor_cd"} 
   casout={name="macro_transposed_ml", caslib="casuser", replace=true};
quit;
/* Соединяем с ABT */
proc fedsql sessref = casauto;
	create table casuser.abt7_ml{options replace = true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT,
			t1.lag_halfyear_avg,
			t1.lag_halfyear_med,
			t1.lag_month_avg,
			t1.lag_month_med,
			t1.lag_qtr_avg,
			t1.lag_qtr_med,
			t1.lag_week_avg,
			t1.lag_week_med,
			t1.lag_year_avg,
			t1.lag_year_med,
			t1.lag_halfyear_std,
			t1.lag_month_std,
			t1.lag_qtr_std,
			t1.lag_week_std,
			t1.lag_year_std,
			t1.lag_halfyear_pct10,		 
			t1.lag_halfyear_pct90,		 
			t1.lag_month_pct10,
			t1.lag_month_pct90,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			&promo_list_sql.,
 			t1.NP_GIFT_PRICE_AMT,
			t1.side_promo_flag,
			t2.A_CPI,
			t2.A_GPD,
			t2.A_RDI
		from
			max_casl.abt6_ml as t1 left join 
			casuser.macro_transposed_ml as t2
		on
			t1.sales_dt = t2.period_dt
	;
quit;
proc casutil;
  droptable casdata="macro_transposed_ml" incaslib="casuser" quiet;
  droptable casdata="macro2_ml" incaslib="casuser" quiet;
  droptable casdata="ia_macro" incaslib="casuser" quiet;
  droptable casdata="macro_ml" incaslib="casuser" quiet;
  droptable casdata="abt6_ml" incaslib="casuser" quiet;
/*   promote casdata="abt7_ml" incaslib="casuser" outcaslib="casuser"; */
run;
/***** 8. Добавляем погоду. *****/
proc casutil;
  load data=&inlib..ia_weather casout = 'ia_weather' outcaslib = 'casuser' replace;
  droptable casdata = "abt8_ml" incaslib = "casuser" quiet;
run;
proc fedsql sessref =casauto;
	create table casuser.abt8_ml{options replace = true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT,
			t1.lag_halfyear_avg,
			t1.lag_halfyear_med,
			t1.lag_month_avg,
			t1.lag_month_med,
			t1.lag_qtr_avg,
			t1.lag_qtr_med,
			t1.lag_week_avg,
			t1.lag_week_med,
			t1.lag_year_avg,
			t1.lag_year_med,
			t1.lag_halfyear_std,
			t1.lag_month_std,
			t1.lag_qtr_std,
			t1.lag_week_std,
			t1.lag_year_std,
			t1.lag_halfyear_pct10,		 
			t1.lag_halfyear_pct90,		 
			t1.lag_month_pct10,
			t1.lag_month_pct90,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			&promo_list_sql.,
 			t1.NP_GIFT_PRICE_AMT,
			t1.side_promo_flag,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t2.TEMPERATURE,
			t2.PRECIPITATION
		from
			casuser.abt7_ml as t1
		left join
			casuser.ia_weather as t2
		on 
			t1.pbo_location_id = t2.pbo_location_id and
			t1.sales_dt = datepart(t2.REPORT_DT)
	;
quit;
proc casutil;
  droptable casdata="ia_weather" incaslib="casuser" quiet;
  droptable casdata="abt7_ml" incaslib="casuser" quiet;
/*   promote casdata="abt8_ml" incaslib="casuser" outcaslib="casuser"; */
run;
/***** 9. Добавляем trp конкурентов *****/
proc casutil;
	droptable casdata="comp_media_ml" incaslib="casuser" quiet;
	droptable casdata="abt9_ml" incaslib="casuser" quiet;
	load data=&inlib..IA_comp_media casout='ia_comp_media' outcaslib='casuser' replace;
run;
proc fedsql sessref=casauto;
	create table casuser.comp_media_ml{options replace=true} as 
		select
			COMPETITOR_CD,
			TRP,
			datepart(cast(report_dt as timestamp)) as report_dt
		from 
			casuser.IA_COMP_MEDIA
	;
quit;
/* Транспонируем таблицу */
proc cas;
transpose.transpose /
   table={name="comp_media_ml", caslib="casuser", groupby={"REPORT_DT"}} 
   transpose={"TRP"} 
   prefix="comp_trp_" 
   id={"COMPETITOR_CD"} 
   casout={name="comp_transposed_ml", caslib="casuser", replace=true};
quit;
/* Протягиваем trp на всю неделю вперед */
data casuser.comp_transposed_ml_expand;
	set casuser.comp_transposed_ml;
	by REPORT_DT;
	do i = 1 to 7;
	   output;
	   REPORT_DT + 1;
	end;
run;
/*
	Пока в данных есть ошибка, все интевалы report_dt указаны
	с интервалом в неделю, но есть одно наблюдение
	в котором этот порядок рушится 16dec2019 и 22dec2019 (6 Дней)
	Поэтому, пока в таблице есть дубль, который мы убираем путем усреднения
*/
proc fedsql sessref=casauto;
	create table casuser.comp_transposed_ml_expand{options replace=true} as
		select
			REPORT_DT,
			mean(comp_trp_BK) as comp_trp_BK,
			mean(comp_trp_KFC) as comp_trp_KFC
		from
			casuser.comp_transposed_ml_expand
		group by report_dt
	;
quit;
/* Соединяем с ABT */
proc fedsql sessref = casauto;
	create table casuser.abt9_ml{options replace=true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT,
			t1.lag_halfyear_avg,
			t1.lag_halfyear_med,
			t1.lag_month_avg,
			t1.lag_month_med,
			t1.lag_qtr_avg,
			t1.lag_qtr_med,
			t1.lag_week_avg,
			t1.lag_week_med,
			t1.lag_year_avg,
			t1.lag_year_med,
			t1.lag_halfyear_std,
			t1.lag_month_std,
			t1.lag_qtr_std,
			t1.lag_week_std,
			t1.lag_year_std,
			t1.lag_halfyear_pct10,		 
			t1.lag_halfyear_pct90,		 
			t1.lag_month_pct10,
			t1.lag_month_pct90,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			&promo_list_sql.,
 			t1.NP_GIFT_PRICE_AMT,
			t1.side_promo_flag,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t2.comp_trp_BK,
			t2.comp_trp_KFC
		from
			casuser.abt8_ml as t1
		left join
			casuser.comp_transposed_ml_expand as t2
		on
			t1.sales_dt = t2.REPORT_DT
	;
quit;
proc casutil;
	droptable casdata='ia_comp_media' incaslib='casuser' quiet;
	droptable casdata='comp_media_ml' incaslib='casuser' quiet;
	droptable casdata='comp_transposed_ml' incaslib='casuser' quiet;
	droptable casdata='comp_transposed_ml_expand' incaslib='casuser' quiet;
  droptable casdata="abt8_ml" incaslib="casuser" quiet;
/* 	promote casdata="abt9_ml" incaslib="casuser" outcaslib="casuser"; */
run;
/***** 10. Добавляем медиаподдержку *****/
proc casutil;
  droptable casdata="media_ml" incaslib="casuser" quiet;
  droptable casdata="abt10_ml" incaslib="casuser" quiet;
/*   load data=&inlib..media_enh casout='ia_media' outcaslib='casuser' replace; */
/*   load data=&inlib..promo_enh casout='ia_promo' outcaslib='casuser' replace; */
/*   load data=&inlib..promo_product_enh casout='ia_promo_x_product' outcaslib='casuser' replace; */
/*   load data=&inlib..promo_pbo_enh casout='ia_promo_x_pbo' outcaslib='casuser' replace; */
run;
proc fedsql sessref=casauto;
	create table casuser.ia_promo_x_pbo_leaf{options replace = true} as 
		select
			t1.promo_id,
			t2.PBO_LEAF_ID
		from
			casuser.promo_pbo_enh /*ia_promo_x_pbo*/ as t1,
			casuser.pbo_lvl_all as t2
		where t1.pbo_location_id = t2.PBO_LOCATION_ID
	;
	create table casuser.ia_promo_x_product_leaf{options replace = true} as 
		select
			t1.promo_id,
			t2.product_LEAF_ID
		from
			casuser.promo_prod_enh /*ia_promo_x_product*/ as t1,
			casuser.product_lvl_all as t2
		where t1.product_id = t2.product_id
	;
	create table casuser.promo_ml_trp{options replace = true} as 
		select distinct
			t3.product_LEAF_ID,
			t2.PBO_LEAF_ID,
			datepart(t1.START_DT) as start_dt,
			datepart(t1.END_DT) as end_dt,
			coalesce(t4.TRP, 0) as trp
		from
			casuser.promo_enh /*ia_promo*/ as t1 
		left join
			casuser.ia_promo_x_pbo_leaf as t2
		on 
			t1.PROMO_ID = t2.PROMO_ID
		left join
			casuser.ia_promo_x_product_leaf as t3
		on
			t1.PROMO_ID = t3.PROMO_ID
		left join
			casuser.media_enh /*ia_media*/ as t4
		on
			t1.PROMO_GROUP_ID = t4.PROMO_GROUP_ID and
			datepart(t4.report_dt) <= datepart(t1.end_dt) and
			datepart(t4.report_dt) >= datepart(t1.start_dt)
	;
quit;
/* Усредняем TRP в рамках одного итервала промо */
proc fedsql sessref=casauto;
	create table casuser.promo_ml_trp2{options replace=true} as
		select
			product_LEAF_ID,
			PBO_LEAF_ID,
			start_dt,
			end_dt,
			mean(TRP) as mean_trp
		from
			casuser.promo_ml_trp as t1
		group by
			product_LEAF_ID,
			PBO_LEAF_ID,
			start_dt,
			end_dt			
	;
quit;
data casuser.promo_ml_trp_expand;
	set casuser.promo_ml_trp2;
	do sales_dt=start_dt to end_dt;
		output;
	end;
run;
proc fedsql sessref=casauto;
	create table casuser.sum_trp{options replace=true} as 
		select
			t1.PRODUCT_LEAF_ID,
			t1.PBO_LEAF_ID,
			t1.sales_dt,
			sum(t1.mean_trp) as sum_trp
		from
			casuser.promo_ml_trp_expand as t1
		group by
			t1.PRODUCT_LEAF_ID,
			t1.PBO_LEAF_ID,
			t1.sales_dt
	;
	create table casuser.abt10_ml{options replace=true} as 
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT,
			t1.lag_halfyear_avg,
			t1.lag_halfyear_med,
			t1.lag_month_avg,
			t1.lag_month_med,
			t1.lag_qtr_avg,
			t1.lag_qtr_med,
			t1.lag_week_avg,
			t1.lag_week_med,
			t1.lag_year_avg,
			t1.lag_year_med,
			t1.lag_halfyear_std,
			t1.lag_month_std,
			t1.lag_qtr_std,
			t1.lag_week_std,
			t1.lag_year_std,
			t1.lag_halfyear_pct10,		 
			t1.lag_halfyear_pct90,		 
			t1.lag_month_pct10,
			t1.lag_month_pct90,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			&promo_list_sql.,
 			t1.NP_GIFT_PRICE_AMT,
			t1.side_promo_flag,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			coalesce(t2.sum_trp, 0) as sum_trp
		from
			casuser.abt9_ml as t1
		left join
			casuser.sum_trp as t2
		on 
			t1.product_id = t2.PRODUCT_LEAF_ID and
			t1.pbo_location_id = t2.PBO_LEAF_ID and
			t1.sales_dt = t2.sales_dt
	;
quit;
proc casutil;
  droptable casdata="IA_media" incaslib="casuser" quiet;
  droptable casdata="IA_promo" incaslib="casuser" quiet;
  droptable casdata="ia_promo_x_product" incaslib="casuser" quiet;
  droptable casdata="ia_promo_x_pbo" incaslib="casuser" quiet;
  droptable casdata="ia_promo_x_pbo_leaf" incaslib="casuser" quiet;
  droptable casdata="ia_promo_x_product_leaf" incaslib="casuser" quiet;
  droptable casdata="promo_ml_trp" incaslib="casuser" quiet;
  droptable casdata="promo_ml_trp_expand" incaslib="casuser" quiet;
  droptable casdata="sum_trp" incaslib="casuser" quiet;
  droptable casdata="abt9_ml" incaslib="casuser" quiet;
/*   promote casdata="abt10_ml" incaslib="casuser" outcaslib="casuser"; */
run;
/****** 11. Добавляем атрибуты товаров ******/
proc casutil;
	promote casdata="abt10_ml" casout="abt10_ml" incaslib="casuser" outcaslib="MAX_CASL";
run;

proc casutil;
  droptable casdata="abt11_ml" incaslib="casuser" quiet;
run;
proc fedsql sessref=casauto;
	create table casuser.abt11_ml{options replace=true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT,
			t1.lag_halfyear_avg,
			t1.lag_halfyear_med,
			t1.lag_month_avg,
			t1.lag_month_med,
			t1.lag_qtr_avg,
			t1.lag_qtr_med,
			t1.lag_week_avg,
			t1.lag_week_med,
			t1.lag_year_avg,
			t1.lag_year_med,
			t1.lag_halfyear_std,
			t1.lag_month_std,
			t1.lag_qtr_std,
			t1.lag_week_std,
			t1.lag_year_std,
			t1.lag_halfyear_pct10,		 
			t1.lag_halfyear_pct90,		 
			t1.lag_month_pct10,
			t1.lag_month_pct90,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			&promo_list_sql.,
	 		t1.NP_GIFT_PRICE_AMT,
			t1.side_promo_flag,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t1.sum_trp,
			t2.prod_lvl4_id, 
			t2.prod_lvl3_id,
			t2.prod_lvl2_id,
			t2.a_hero_id as hero,
			t2.a_item_size_id as item_size,
			t2.a_offer_type_id as offer_type,
			t2.a_price_tier_id as price_tier
	from
		casuser.abt10_ml as t1
	left join
		casuser.product_dictionary_ml as t2
	on
		t1.product_id = t2.product_id
	;
quit;
 
proc casutil;
/*   promote casdata="abt11_ml" incaslib="casuser" outcaslib="casuser"; */
  droptable casdata="abt10_ml" incaslib="casuser" quiet;
run;
/******	12. Добавим атрибуты ПБО ******/
proc casutil;
  droptable casdata="abt12_ml" incaslib="casuser" quiet;
  load data=&inlib..IA_pbo_location casout='ia_pbo_location' outcaslib='casuser' replace;
  load data=&inlib..IA_PBO_LOC_HIERARCHY casout='IA_PBO_LOC_HIERARCHY' outcaslib='casuser' replace;
  load data=&inlib..IA_PBO_LOC_ATTRIBUTES casout='IA_PBO_LOC_ATTRIBUTES' outcaslib='casuser' replace;
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
			t3.A_PRICE_LEVEL,
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
%text_encoding(casuser.pbo_dictionary_ml, A_AGREEMENT_TYPE)
%text_encoding(casuser.pbo_dictionary_ml, A_BREAKFAST)
%text_encoding(casuser.pbo_dictionary_ml, A_BUILDING_TYPE)
%text_encoding(casuser.pbo_dictionary_ml, A_COMPANY)
%text_encoding(casuser.pbo_dictionary_ml, A_DELIVERY)
%text_encoding(casuser.pbo_dictionary_ml, A_MCCAFE_TYPE)
%text_encoding(casuser.pbo_dictionary_ml, A_PRICE_LEVEL)
%text_encoding(casuser.pbo_dictionary_ml, A_DRIVE_THRU)
%text_encoding(casuser.pbo_dictionary_ml, A_WINDOW_TYPE)
proc fedsql sessref=casauto;
	create table casuser.abt12_ml{options replace=true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT,
			t1.lag_halfyear_avg,
			t1.lag_halfyear_med,
			t1.lag_month_avg,
			t1.lag_month_med,
			t1.lag_qtr_avg,
			t1.lag_qtr_med,
			t1.lag_week_avg,
			t1.lag_week_med,
			t1.lag_year_avg,
			t1.lag_year_med,
			t1.lag_halfyear_std,
			t1.lag_month_std,
			t1.lag_qtr_std,
			t1.lag_week_std,
			t1.lag_year_std,
			t1.lag_halfyear_pct10,		 
			t1.lag_halfyear_pct90,		 
			t1.lag_month_pct10,
			t1.lag_month_pct90,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			&promo_list_sql.,
	 		t1.NP_GIFT_PRICE_AMT,
t1.side_promo_flag,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t1.sum_trp,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t2.lvl3_id,
			t2.lvl2_id,
			t2.A_AGREEMENT_TYPE_id as agreement_type,
			t2.A_BREAKFAST_id as breakfast,
			t2.A_BUILDING_TYPE_id as building_type,
			t2.A_COMPANY_id as company,
			t2.A_DELIVERY_id as delivery,
			t2.A_DRIVE_THRU_id as drive_thru,
			t2.A_MCCAFE_TYPE_id as mccafe_type,
			t2.A_PRICE_LEVEL_id as price_level,
			t2.A_WINDOW_TYPE_id as window_type
		from
			casuser.abt11_ml as t1
		left join
			casuser.pbo_dictionary_ml as t2
		on
			t1.pbo_location_id = t2.pbo_location_id
	;
quit;
proc casutil;
  droptable casdata='ia_pbo_location' incaslib='casuser' quiet;
  droptable casdata='IA_PBO_LOC_HIERARCHY' incaslib='casuser' quiet;
  droptable casdata='IA_PBO_LOC_ATTRIBUTES' incaslib='casuser' quiet;
  droptable casdata='pbo_hier_flat' incaslib='casuser' quiet;
  droptable casdata='attr_transposed' incaslib='casuser' quiet;
  droptable casdata='pbo_dictionary_ml' incaslib='casuser' quiet;
/*   promote casdata="abt12_ml" incaslib="casuser" outcaslib="casuser"; */
  droptable casdata="abt11_ml" incaslib="casuser" quiet;
run;
/****** 13. Добавляем календарные признаки *******/
proc casutil;
  droptable casdata="abt13_ml" incaslib="casuser" quiet;
run;
%let first_date = '01jan2017'd;
%let last_date = '31dec2021'd;
data work.cldr_prep;
	retain date &first_date;
	do while(date <= &last_date);
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
			(case
				when weekday(date) in (1, 7) then 1
				else 0
			end) as weekend_flag
		from
			work.cldr_prep
	;
quit;
/* Список выходных дней в РФ с 2017 по 2021 */
data work.russia_weekend;
input date :yymmdd10.;
format date yymmddd10.;
datalines;
2017-01-02
2017-01-03
2017-01-04
2017-01-05
2017-01-06
2017-02-23
2017-02-24
2017-03-08
2017-05-01
2017-05-08
2017-05-09
2017-06-12
2017-11-06
2018-01-01
2018-01-02
2018-01-03
2018-01-04
2018-01-05
2018-01-08
2018-02-23
2018-03-08
2018-03-09
2018-04-30
2018-05-01
2018-05-02
2018-05-09
2018-06-11
2018-06-12
2018-11-05
2018-12-31
2019-01-01
2019-01-02
2019-01-03
2019-01-04
2019-01-07
2019-01-08
2019-03-08
2019-05-01
2019-05-02
2019-05-03
2019-05-09
2019-05-10
2019-06-12
2019-11-04
2020-01-01
2020-01-02
2020-01-03
2020-01-06
2020-01-07
2020-01-08
2020-02-24
2020-03-09
2020-05-01
2020-05-04
2020-05-05
2020-05-11
2020-06-12
2020-06-24
2020-07-01
2020-11-04
2021-01-01
2021-01-04
2021-01-05
2021-01-06
2021-01-07
2021-01-08
2021-02-23
2021-03-08
2021-05-03
2021-05-10
2021-06-14
2021-11-04
;
run;
/* Объединяем государственные выходные с субботой и воскресеньем */
proc sql;
	create table work.cldr_prep_features2 as 
		select
			t1.date,
			t1.week,
			t1.weekday,
			t1.month,
			case
				when t2.date is not missing then 1
				else t1.weekend_flag
			end as weekend_flag
		from
			work.cldr_prep_features as t1
		left join
			work.russia_weekend as t2
		on
			t1.date = t2.date
	;
quit;
proc casutil;
  load data=work.cldr_prep_features2 casout='cldr_prep_features' outcaslib='casuser' replace;
run;
/* Добавляем к витрине */
proc fedsql sessref = casauto;
	create table casuser.abt13_ml{options replace = true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT,
			t1.lag_halfyear_avg,
			t1.lag_halfyear_med,
			t1.lag_month_avg,
			t1.lag_month_med,
			t1.lag_qtr_avg,
			t1.lag_qtr_med,
			t1.lag_week_avg,
			t1.lag_week_med,
			t1.lag_year_avg,
			t1.lag_year_med,
			t1.lag_halfyear_std,
			t1.lag_month_std,
			t1.lag_qtr_std,
			t1.lag_week_std,
			t1.lag_year_std,
			t1.lag_halfyear_pct10,		 
			t1.lag_halfyear_pct90,		 
			t1.lag_month_pct10,
			t1.lag_month_pct90,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			&promo_list_sql.,
	 		t1.NP_GIFT_PRICE_AMT,
t1.side_promo_flag,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t1.sum_trp,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.lvl3_id,
			t1.lvl2_id,
			t1.agreement_type,
			t1.breakfast,
			t1.building_type,
			t1.company,
			t1.delivery,
			t1.drive_thru,
			t1.mccafe_type,
			t1.price_level,
			t1.window_type,
			t2.week,
			t2.weekday,
			t2.month,
			t2.weekend_flag
		from
			casuser.abt12_ml as t1
		left join
			casuser.cldr_prep_features as t2
		on
			t1.sales_dt = t2.date
	;
quit;

proc casutil;
	promote casdata="abt13_ml" casout="abt13_ml" incaslib="casuser" outcaslib="MAX_CASL";
run;
/******  14. Добавим события ******/
proc casutil;
	droptable casdata="russia_event" incaslib="casuser" quiet;
	droptable casdata="russia_event2" incaslib="casuser" quiet;
	droptable casdata="russia_event_t" incaslib="casuser" quiet;
	droptable casdata="abt14_ml" incaslib="casuser" quiet;
/* 	promote casdata="abt13_ml" incaslib="casuser" outcaslib="casuser"; */
    droptable casdata="abt12_ml" incaslib="casuser" quiet;
run;
data work.russia_event;
input date :yymmdd10. event_nm $32.;
format date yymmddd10.;
datalines;
2017-01-01 new_year
2017-01-02 new_year
2017-01-03 new_year
2017-01-04 new_year
2017-01-05 new_year
2017-01-06 new_year
2017-01-07 new_year
2017-01-08 new_year
2017-01-25 student_day
2017-02-14 valentine_day
2017-02-23 defender_day
2017-02-24 defender_day
2017-03-08 female_day
2017-04-29 may_holiday
2017-04-30 may_holiday
2017-05-01 may_holiday
2017-05-02 may_holiday
2017-05-03 may_holiday
2017-05-04 may_holiday
2017-05-05 may_holiday
2017-05-06 may_holiday
2017-05-07 may_holiday
2017-05-08 may_holiday
2017-05-09 may_holiday
2017-06-01 summer_start
2017-06-12 russia_day
2017-09-01 school_start
2017-12-31 new_year
2018-01-01 new_year
2018-01-02 new_year
2018-01-03 new_year
2018-01-04 new_year
2018-01-05 new_year
2018-01-08 new_year
2018-01-25 student_day
2018-02-14 valentine_day
2018-02-23 defender_day
2018-03-08 female_day
2018-03-09 female_day
2018-04-30 may_holiday
2018-05-01 may_holiday
2018-05-02 may_holiday
2018-05-03 may_holiday
2018-05-04 may_holiday
2018-05-05 may_holiday
2018-05-06 may_holiday
2018-05-07 may_holiday
2018-05-08 may_holiday
2018-05-09 may_holiday
2018-06-01 summer_start
2018-06-11 russia_day
2018-06-12 russia_day
2018-09-01 school_start
2018-12-31 new_year
2019-01-01 new_year
2019-01-02 new_year
2019-01-03 new_year
2019-01-04 new_year
2019-01-07 new_year
2019-01-08 new_year
2019-01-25 student_day
2019-02-14 valentine_day
2019-02-23 defender_day
2019-03-08 female_day
2019-04-29 may_holiday
2019-04-30 may_holiday
2019-05-01 may_holiday
2019-05-02 may_holiday
2019-05-03 may_holiday
2019-05-04 may_holiday
2019-05-05 may_holiday
2019-05-06 may_holiday
2019-05-07 may_holiday
2019-05-08 may_holiday
2019-05-09 may_holiday
2019-05-10 may_holiday
2019-06-01 summer_start
2019-06-12 russia_day
2019-09-01 school_start
2019-12-31 new_year
2020-01-01 new_year
2020-01-02 new_year
2020-01-03 new_year
2020-01-06 new_year
2020-01-07 new_year
2020-01-08 new_year
2020-01-25 student_day
2020-02-14 valentine_day
2020-02-23 defender_day
2020-02-24 defender_day
2020-03-08 female_day
2020-03-09 female_day
2020-05-01 may_holiday
2020-05-02 may_holiday
2020-05-03 may_holiday
2020-05-04 may_holiday
2020-05-05 may_holiday
2020-05-06 may_holiday
2020-05-07 may_holiday
2020-05-08 may_holiday
2020-05-09 may_holiday 
2020-05-10 may_holiday
2020-05-11 may_holiday
2020-06-01 summer_start
2020-06-12 russia_day
2020-09-01 school_start
2020-12-31 new_year
2021-01-01 new_year
2021-01-02 new_year
2021-01-03 new_year
2021-01-04 new_year
2021-01-05 new_year
2021-01-06 new_year
2021-01-07 new_year
2021-01-08 new_year
2021-01-25 student_day
2021-02-14 valentine_day
2021-02-23 defender_day
2021-03-08 female_day
2021-05-01 may_holiday
2021-05-02 may_holiday
2021-05-03 may_holiday
2021-05-04 may_holiday
2021-05-05 may_holiday
2021-05-06 may_holiday
2021-05-07 may_holiday
2021-05-08 may_holiday
2021-05-09 may_holiday
2021-05-10 may_holiday
2021-06-01 summer_start
2021-06-14 russia_day
2021-09-01 school_start
;
run;
/* загружаем таблицу в cas */
proc casutil;
  load data=work.russia_event casout='russia_event' outcaslib='casuser' replace;
run;
/* добваляем константный флаг */
proc fedsql sessref = casauto;
	create table casuser.russia_event2{options replace=true} as
		select *, 1 as event_flag from casuser.russia_event;
quit;
/* транспонируем таблицу */
proc cas;
transpose.transpose /
   table={name="russia_event2", caslib="casuser", groupby={"date"}} 
   attributes={{name="date"}} 
   transpose={"event_flag"} 
   id={"event_nm"} 
   casout={name="russia_event_t", caslib="casuser", replace=true};
quit;
/* добавляем к ваитрине */
proc fedsql sessref=casauto;
	create table casuser.abt14_ml{options replace=true} as 
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT,
			t1.lag_halfyear_avg,
			t1.lag_halfyear_med,
			t1.lag_month_avg,
			t1.lag_month_med,
			t1.lag_qtr_avg,
			t1.lag_qtr_med,
			t1.lag_week_avg,
			t1.lag_week_med,
			t1.lag_year_avg,
			t1.lag_year_med,
			t1.lag_halfyear_std,
			t1.lag_month_std,
			t1.lag_qtr_std,
			t1.lag_week_std,
			t1.lag_year_std,
			t1.lag_halfyear_pct10,		 
			t1.lag_halfyear_pct90,		 
			t1.lag_month_pct10,
			t1.lag_month_pct90,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			&promo_list_sql.,
	 		t1.NP_GIFT_PRICE_AMT,
t1.side_promo_flag,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t1.sum_trp,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.lvl3_id,
			t1.lvl2_id,
			t1.agreement_type,
			t1.breakfast,
			t1.building_type,
			t1.company,
			t1.delivery,
			t1.drive_thru,
			t1.mccafe_type,
			t1.price_level,
			t1.window_type,
			t1.week,
			t1.weekday,
			t1.month,
			t1.weekend_flag,
			coalesce(t2.defender_day, 0) as defender_day,
			coalesce(t2.female_day, 0) as female_day,
			coalesce(t2.may_holiday, 0) as may_holiday,
			coalesce(t2.new_year , 0) as new_year,
			coalesce(t2.russia_day, 0) as russia_day,
			coalesce(t2.school_start, 0) as school_start,
			coalesce(t2.student_day, 0) as student_day,
			coalesce(t2.summer_start, 0) as summer_start,
			coalesce(t2.valentine_day, 0) as valentine_day
		from
			max_casl.abt13_ml as t1
		left join
			casuser.russia_event_t as t2
		on
			t1.sales_dt = t2.date
	;	
quit;

proc casutil;
	promote casdata="abt14_ml" casout="abt14_ml" incaslib="casuser" outcaslib="MAX_CASL";
run;

proc casutil;
	droptable casdata="russia_event" incaslib="casuser" quiet;
	droptable casdata="russia_event2" incaslib="casuser" quiet;
	droptable casdata="russia_event_t" incaslib="casuser" quiet;
/* 	promote casdata="abt14_ml" incaslib="casuser" outcaslib="casuser"; */
    droptable casdata="abt13_ml" incaslib="casuser" quiet;
run;
/******	15. Добавим ценовые ранги ******/
proc casutil;
	droptable casdata="abt15_ml" incaslib="casuser" quiet;
	droptable casdata="unique_day_price" incaslib="casuser" quiet;
	droptable casdata="sum_count_price" incaslib="casuser" quiet;
	droptable casdata="price_rank" incaslib="casuser" quiet;
	droptable casdata="price_rank2" incaslib="casuser" quiet;
	droptable casdata="price_rank3" incaslib="casuser" quiet;
	droptable casdata="price_feature" incaslib="casuser" quiet;
	
run;
/* уникальные ПБО/день/категория товаров/товар/цена */
proc fedsql sessref = casauto;
	create table casuser.unique_day_price as 
		select distinct
			t1.pbo_location_id,
			t1.PROD_LVL3_ID,
			t1.sales_dt,
			t1.product_id,
			t1.GROSS_PRICE_AMT
		from
			MAX_CASL.abt14_ml as t1
	;
quit;
/* Считаем суммарную цену в групе и количество товаров */
proc fedsql sessref = casauto;
	create table casuser.sum_count_price{options replace = true} as
		select
			t1.pbo_location_id,
			t1.PROD_LVL3_ID,
			t1.sales_dt,
			count(t1.product_id) as count_product,
			sum(t1.GROSS_PRICE_AMT) as sum_gross_price_amt
		from casuser.unique_day_price as t1
		group by
			t1.pbo_location_id,
			t1.PROD_LVL3_ID,
			t1.sales_dt
	;
quit;
/* считаем позицию товара в отсортированном списке цен */
data casuser.price_rank;
set casuser.unique_day_price;
by pbo_location_id sales_dt PROD_LVL3_ID GROSS_PRICE_AMT ;
if first.PROD_LVL3_ID then i = 0;
if GROSS_PRICE_AMT ^= lag(GROSS_PRICE_AMT) then i+1;
run;
proc fedsql sessref = casauto;
	create table casuser.price_rank2{options replace=true} as
		select
			t1.pbo_location_id,
			t1.sales_dt,
			t1.PROD_LVL3_ID,
			max(t1.i) as max_i
		from
			casuser.price_rank as t1
		group by
			t1.pbo_location_id,
			t1.sales_dt,
			t1.PROD_LVL3_ID
	; 
quit;
/* Соединяем таблицы price_rank, price_rank2 */
proc fedsql sessref=casauto;
	create table casuser.price_rank3{options replace=true} as
		select
			t1.product_id,
			t1.pbo_location_id,
			t1.PROD_LVL3_ID,
			t1.sales_dt,
			t1.GROSS_PRICE_AMT,
			t1.i,
			t2.max_i
		from
			casuser.price_rank as t1
		left join
			casuser.price_rank2 as t2
		on
			t1.pbo_location_id = t2.pbo_location_id and
			t1.PROD_LVL3_ID = t2.PROD_LVL3_ID and
			t1.sales_dt = t2.sales_dt
	;
quit;
/* Соединяем таблицы price_rank3 и sum_count_price */
proc fedsql sessref=casauto;
	create table casuser.price_feature{options replace=true} as
		select
			t1.product_id,
			t1.pbo_location_id,
			t1.PROD_LVL3_ID,
			t1.sales_dt,
			t1.GROSS_PRICE_AMT,
			t1.i,
			t1.max_i,
			t2.count_product,
			t2.sum_gross_price_amt,
			divide(t1.i,t1.max_i) as price_rank,
			(
				case
					when t2.sum_gross_price_amt = t1.GROSS_PRICE_AMT then 1
					else divide(t1.GROSS_PRICE_AMT,divide((t2.sum_gross_price_amt - t1.GROSS_PRICE_AMT),(t2.count_product - 1)))
				end
			) as price_index
		from
			casuser.price_rank3 as t1
		left join
			casuser.sum_count_price as t2
		on
			t1.pbo_location_id = t2.pbo_location_id and
			t1.PROD_LVL3_ID = t2.PROD_LVL3_ID and
			t1.sales_dt = t2.sales_dt
	;
quit;
/* Добавляем в витрину */
proc fedsql sessref = casauto;
	create table casuser.abt15_ml{options replace=true} as 
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT,
			t1.lag_halfyear_avg,
			t1.lag_halfyear_med,
			t1.lag_month_avg,
			t1.lag_month_med,
			t1.lag_qtr_avg,
			t1.lag_qtr_med,
			t1.lag_week_avg,
			t1.lag_week_med,
			t1.lag_year_avg,
			t1.lag_year_med,
			t1.lag_halfyear_std,
			t1.lag_month_std,
			t1.lag_qtr_std,
			t1.lag_week_std,
			t1.lag_year_std,
			t1.lag_halfyear_pct10,		 
			t1.lag_halfyear_pct90,		 
			t1.lag_month_pct10,
			t1.lag_month_pct90,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			&promo_list_sql.,
	 		t1.NP_GIFT_PRICE_AMT,
t1.side_promo_flag,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t1.sum_trp,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.lvl3_id,
			t1.lvl2_id,
			t1.agreement_type,
			t1.breakfast,
			t1.building_type,
			t1.company,
			t1.delivery,
			t1.drive_thru,
			t1.mccafe_type,
			t1.price_level,
			t1.window_type,
			t1.week,
			t1.weekday,
			t1.month,
			t1.weekend_flag,
			t1.defender_day,
			t1.female_day,
			t1.may_holiday,
			t1.new_year,
			t1.russia_day,
			t1.school_start,
			t1.student_day,
			t1.summer_start,
			t1.valentine_day, 
			t2.price_rank,
			t2.price_index
		from
			MAX_CASL.abt14_ml as t1
		left join
			casuser.price_feature as t2
		on
			t1.pbo_location_id = t2.pbo_location_id and
			t1.product_id = t2.product_id and
			t1.sales_dt = t2.sales_dt
	;
quit;

proc casutil;
	promote casdata="abt15_ml" casout="abt15_ml" incaslib="casuser" outcaslib="MAX_CASL";
run;

proc casutil;
	droptable casdata="unique_day_price" incaslib="casuser" quiet;
	droptable casdata="sum_count_price" incaslib="casuser" quiet;
	droptable casdata="price_rank" incaslib="casuser" quiet;
	droptable casdata="price_rank2" incaslib="casuser" quiet;
	droptable casdata="price_rank3" incaslib="casuser" quiet;
	droptable casdata="price_feature" incaslib="casuser" quiet;
/* 	promote casdata="abt15_ml" incaslib="casuser" outcaslib="casuser"; */
    droptable casdata="abt14_ml" incaslib="casuser" quiet;
run;
/******	16. Перекодируем channel_cd  ******/
proc casutil;
	droptable casdata="units_abt_NOV_DEC_JAN" incaslib="casuser" quiet;
run;
%text_encoding(casuser.abt15_ml, channel_cd)
/* Заменяем текстовое поле на числовое */
proc fedsql sessref = casauto;
	create table casuser.units_abt_NOV_DEC_JAN{options replace=true} as 
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD as channel_cd,
			t1.SALES_DT,
			t1.sum_qty,
			t1.GROSS_PRICE_AMT,
			t1.lag_halfyear_avg,
			t1.lag_halfyear_med,
			t1.lag_month_avg,
			t1.lag_month_med,
			t1.lag_qtr_avg,
			t1.lag_qtr_med,
			t1.lag_week_avg,
			t1.lag_week_med,
			t1.lag_year_avg,
			t1.lag_year_med,
			t1.lag_halfyear_std,
			t1.lag_month_std,
			t1.lag_qtr_std,
			t1.lag_week_std,
			t1.lag_year_std,
			t1.lag_halfyear_pct10,		 
			t1.lag_halfyear_pct90,		 
			t1.lag_month_pct10,
			t1.lag_month_pct90,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			&promo_list_sql.,
	 		t1.NP_GIFT_PRICE_AMT,
			t1.side_promo_flag,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t1.sum_trp,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.lvl3_id,
			t1.lvl2_id,
			t1.agreement_type,
			t1.breakfast,
			t1.building_type,
			t1.company,
			t1.delivery,
			t1.drive_thru,
			t1.mccafe_type,
			t1.price_level,
			t1.window_type,
			t1.week,
			t1.weekday,
			t1.month,
			t1.weekend_flag,
			t1.defender_day,
			t1.female_day,
			t1.may_holiday,
			t1.new_year,
			t1.russia_day,
			t1.school_start,
			t1.student_day,
			t1.summer_start,
			t1.valentine_day, 
			t1.price_rank,
			t1.price_index
		from
			MAX_CASL.abt15_ml as t1
	;
quit;



proc casutil;
/* 	promote casdata="units_abt_NOV_DEC_JAN" casout="units_abt_may_test" incaslib="casuser" outcaslib="casuser"; */
	promote casdata="units_abt_NOV_DEC_JAN" casout="UNITS_ABT_MAY_TEST" incaslib="casuser" outcaslib="MAX_CASL";
run;

proc casutil;
    save casdata="UNITS_ABT_MAY_TEST" incaslib=MAX_CASL outcaslib=MAX_CASL casout="UNITS_ABT_MAY_TEST.sashdat" replace;
quit;

