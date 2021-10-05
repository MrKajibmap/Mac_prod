/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных в CAS в рамках сквозного процесса для оперпрогноза
*
*  ПАРАМЕТРЫ:
*     mpWorkCaslib 		- CAS-либа источник для расчета сквозного процесса
*	
******************************************************************
*  Использует: 
*	  нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*    %rtp_load_data_to_caslib(mpWorkCaslib=casshort)
*
****************************************************************************
*  24-07-2020  Борзунов     Начальное кодирование
*  27-08-2020  Борзунов		Заменен источник данных на ETL_IA. Добавлена выгрузка на диск целевых таблиц
*  24-09-2020  Борзунов		Добавлена промо-разметка из ПТ
****************************************************************************/
%macro rtp_load_data_to_caslib(mpWorkCaslib=mn_short);

	*options mprint nomprintnest nomlogic nomlogicnest nosymbolgen mcompilenote=all mreplace;
	
	%local 	lmvInLib
			lmvReportDttm 
			lmvStartDate 
			lmvEndDate 
			lmvWorkCaslib
			lmvScoreEndDate
			;

	%let lmvInLib=ETL_IA;
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	%let lmvWorkCaslib = &mpWorkCaslib.;
	%let lmvStartDate = &RTP_START_DATE.;
	%let lmvEndDate = &VF_HIST_END_DT_SAS.;
	%let lmvScoreEndDate = %sysfunc(intnx(day,&VF_HIST_END_DT_SAS.,91,s));
	
	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto;
		caslib _all_ assign;
	%end;
	
	%tech_clean_lib(mpCaslibNm=&lmvWorkCaslib.);
	%tech_clean_lib(mpCaslibNm=mn_long);
							
	%add_promotool_marks2(mpOutCaslib=&lmvWorkCaslib.,
							mpPtCaslib=pt);
	
	/****** 1. Сбор "каркаса" из pmix ******/
	/* Сначала собираем справочник товаров для того, чтобы создать фильтр */
	proc casutil;
	  droptable casdata="product_dictionary_ml" incaslib="casuser" quiet;
	run;
	
	data CASUSER.product (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..product(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.product_HIERARCHY (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..product_HIERARCHY(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.product_ATTRIBUTES (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..product_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
  
	proc cas;
	transpose.transpose /
	   table={name="product_attributes", caslib="casuser", groupby={"product_id"}} 
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
			(select * from casuser.product_hierarchy where product_lvl=5) as t1
			left join 
			(select * from casuser.product_hierarchy where product_lvl=4) as t2
			on t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
			left join 
			(select * from casuser.product_hierarchy where product_lvl=3) as t3
			on t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
			;
			create table casuser.lvl5{options replace=true} as 
			select 
				product_id as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
		create table casuser.lvl4{options replace=true} as 
			select 
				LVL4_ID as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
		create table casuser.lvl3{options replace=true} as 
			select 
				LVL3_ID as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
		create table casuser.lvl2{options replace=true} as 
			select 
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
	   left join casuser.product t15
	   on t1.product_id=t15.product_id
	   left join casuser.product t14
	   on t1.lvl4_id=t14.product_id
	   left join casuser.product t13
	   on t1.lvl3_id=t13.product_id
	   left join casuser.product t12
	   on t1.lvl2_id=t12.product_id;
	quit;

	%text_encoding(mpTable=casuser.product_dictionary_ml, mpVariable=a_hero);
	%text_encoding(mpTable=casuser.product_dictionary_ml, mpVariable=a_item_size);
	%text_encoding(mpTable=casuser.product_dictionary_ml, mpVariable=a_offer_type);
	%text_encoding(mpTable=casuser.product_dictionary_ml, mpVariable=a_price_tier);

	/* Соединяем в единый справочник ПБО */
	data casuser.product_lvl_all;
		set casuser.lvl5 casuser.lvl4 casuser.lvl3 casuser.lvl2 casuser.lvl1;
	run;
	
	proc casutil;
		promote casdata="product_dictionary_ml" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		droptable casdata='product' incaslib='casuser' quiet;
		droptable casdata='product_ATTRIBUTES' incaslib='casuser' quiet;
	run;

	/* Подготовка таблицы с продажами */
	data CASUSER.pmix_sales(replace=yes  drop=valid_from_dttm valid_to_dttm SALES_QTY_DISCOUNT GROSS_SALES_AMT_DISCOUNT NET_SALES_AMT_DISCOUNT);
			set &lmvInLib..pmix_sales(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
			and sales_dt<=&lmvEndDate. and sales_dt>=&lmvStartDate.));
	run; 

	proc casutil; 
		droptable casdata="pmix_sales" incaslib="&lmvWorkCaslib." quiet;
		promote casdata="pmix_sales" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
	run;

	/****** 2. Добавление цен ******/
	proc casutil;
	  droptable casdata="price_ml" incaslib="casuser" quiet;
	run;

	 data CASUSER.price_ml (replace=yes  drop=valid_from_dttm valid_to_dttm);
			set &lmvInLib..price(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	
	proc casutil;
		promote casdata="price_ml" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
	run;

	/****** 4. Фильтрация ******/
	/* 4.1 Убираем временные закрытия ПБО */
	proc casutil;
		droptable casdata="pbo_closed_ml" incaslib="casuser" quiet;
	run;

	data CASUSER.pbo_close_period (replace=yes  drop=valid_from_dttm valid_to_dttm);
			set &lmvInLib..pbo_close_period(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
				and end_dt<=&lmvEndDate. and start_dt>=&lmvStartDate.));
	run;

	/* заполняем пропуски в end_dt */
	proc fedsql sessref=casauto;
		create table casuser.pbo_closed_ml {options replace=true} as
			select 
				CHANNEL_CD,
				PBO_LOCATION_ID,
				start_dt as start_dt,
				coalesce(end_dt, date '2100-01-01') as end_dt,
				CLOSE_PERIOD_DESC
			from
				casuser.pbo_close_period
		;
	quit;

	proc casutil;
		promote casdata="pbo_closed_ml" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		droptable casdata="closed_pbo" incaslib="casuser" quiet;
	run;

	data CASUSER.PBO_LOC_ATTRIBUTES (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PBO_LOC_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	proc cas;
	transpose.transpose /
	   table={name="pbo_loc_attributes", caslib="casuser", groupby={"pbo_location_id"}} 
	   attributes={{name="pbo_location_id"}} 
	   transpose={"PBO_LOC_ATTR_VALUE"} 
	   prefix="A_" 
	   id={"PBO_LOC_ATTR_NM"} 
	   casout={name="attr_transposed_pbo", caslib="casuser", replace=true};
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
			from casuser.attr_transposed_pbo
		;
	quit;

	/* 4.4 Пересекаем с ассортиментной матрицей скоринговую витрину */
	proc casutil;
		promote casdata="closed_pbo" incaslib="casuser" outcaslib="&lmvWorkCaslib."; 
	run;
	
	data CASUSER.assort_matrix (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..assort_matrix(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	proc casutil;
		promote casdata="assort_matrix" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		promote casdata="pbo_close_period" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		droptable casdata="pbo_closed_ml" incaslib="casuser" quiet;
	run;

	/****** 6. Добавление промо ******/
	proc casutil;
		droptable casdata="pbo_hier_flat" incaslib="casuser" quiet;
		droptable casdata="lvl5" incaslib="casuser" quiet;
		droptable casdata="lvl4" incaslib="casuser" quiet;
		droptable casdata="lvl3" incaslib="casuser" quiet;
		droptable casdata="lvl2" incaslib="casuser" quiet;
		droptable casdata="lvl1" incaslib="casuser" quiet;
		droptable casdata="pbo_lvl_all" incaslib="casuser" quiet;
		droptable casdata="promo_ml" incaslib="casuser" quiet;
		droptable casdata="promo_transposed" incaslib="casuser" quiet;
		droptable casdata="promo_x_product_leaf" incaslib="casuser" quiet;
		droptable casdata="promo_x_pbo_leaf" incaslib="casuser" quiet;
		droptable casdata="promo_ml_main_code" incaslib="casuser" quiet;
		droptable casdata="abt_promo" incaslib="casuser" quiet;
		droptable casdata="promo" incaslib="casuser" quiet;
	run;
	data CASUSER.pbo_loc_hierarchy (replace=yes  drop=valid_from_dttm valid_to_dttm);
			set &lmvInLib..pbo_loc_hierarchy(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.promo (replace=yes);
		set &lmvWorkCaslib..promo_enh;
	run;
	
	data CASUSER.promo_x_pbo (replace=yes);
		set &lmvWorkCaslib..promo_pbo_enh;
	run;
	
	data CASUSER.promo_x_product (replace=yes);
		set &lmvWorkCaslib..promo_prod_enh;
	run;
	
	proc fedsql sessref=casauto noprint;
			create table casuser.promo_pbo {options replace=true} as 
			select PBO_LOCATION_ID,PROMO_ID
			from casuser.promo_X_PBO
			;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.promo_prod {options replace=true} as 
		select GIFT_FLAG,OPTION_NUMBER,PRODUCT_ID,PRODUCT_QTY,PROMO_ID
		from casuser.promo_X_PRODUCT
		;
	quit;
	
	/* Создаем таблицу связывающую PBO на листовом уровне и на любом другом */
	proc fedsql sessref=casauto;
		create table casuser.pbo_hier_flat{options replace=true} as
			select
				t1.pbo_location_id, 
				t2.PBO_LOCATION_ID as LVL3_ID,
				t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				1 as LVL1_ID
			from 
				(select * from casuser.pbo_loc_hierarchy where pbo_location_lvl=4) as t1
			left join 
				(select * from casuser.pbo_loc_hierarchy where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
		;
		create table casuser.lvl4{options replace=true} as 
			select 
				pbo_location_id as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_hier_flat
		;
		create table casuser.lvl3{options replace=true} as 
			select 
				LVL3_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_hier_flat
		;
		create table casuser.lvl2{options replace=true} as 
			select 
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
		create table casuser.lvl5{options replace=true} as 
			select 
				product_id as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
		create table casuser.lvl4{options replace=true} as 
			select 
				LVL4_ID as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
		create table casuser.lvl3{options replace=true} as 
			select 
				LVL3_ID as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
		create table casuser.lvl2{options replace=true} as 
			select 
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

	data casuser.promo_mech_transformation;
		length old_mechanic new_mechanic $50;
		infile "&RTP_PROMO_MECH_TRANSF_FILE." dsd firstobs=2 delimiter=';';                 
		input old_mechanic $ new_mechanic $;                            
	run;
	
	/* Добавляем к таблице промо ПБО и товары */
	proc fedsql sessref = casauto;
		create table casuser.promo_x_pbo_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t2.PBO_LEAF_ID
			from
				casuser.promo_x_pbo as t1,
				casuser.pbo_lvl_all as t2
			where t1.pbo_location_id = t2.PBO_LOCATION_ID
		;
		create table casuser.promo_x_product_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t2.product_LEAF_ID
			from
				casuser.promo_x_product as t1,
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
				t1.START_DT as start_dt,
				t1.END_DT as end_dt,
				t1.CHANNEL_CD,
	 			t1.NP_GIFT_PRICE_AMT, 
				t1.PROMO_MECHANICS,
				/*
				(case
					when t1.PROMO_MECHANICS = 'BOGO / 1+1' then 'bogo'
					when t1.PROMO_MECHANICS = 'Discount' then 'discount'
					when t1.PROMO_MECHANICS = 'EVM/Set' then 'evm_set'
					when t1.PROMO_MECHANICS = 'Non-Product Gift' then 'non_product_gift'
					when t1.PROMO_MECHANICS = 'Pairs' then 'pairs'
					when t1.PROMO_MECHANICS = 'Product Gift' then 'product_gift'
					when t1.PROMO_MECHANICS = 'Other: Discount for volume' then 'other_promo'
					when t1.PROMO_MECHANICS = 'NP Promo Support' then 'support'
				end) as promo_mechanics_name,
				*/
				t4.new_mechanic as promo_mechanics_name,
				1 as promo_flag		
			from
				casuser.promo as t1 
			left join
				casuser.promo_x_pbo_leaf as t2
			on 
				t1.PROMO_ID = t2.PROMO_ID
			left join
				casuser.promo_x_product_leaf as t3
			on
				t1.PROMO_ID = t3.PROMO_ID 
			inner join 
				casuser.promo_mech_transformation as t4
			on t1.promo_mechanics = t4.old_mechanic 
		;
	quit;
		
	proc casutil;
		droptable casdata="promo_x_product_leaf" incaslib="&lmvWorkCaslib." quiet;
		promote casdata="promo_mech_transformation" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		promote casdata="product_lvl_all" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		promote casdata="pbo_lvl_all" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		promote casdata="promo_prod" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		promote casdata="promo_pbo" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
	run;
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
			groupby={"promo_id", "product_LEAF_ID", "PBO_LEAF_ID", "CHANNEL_CD", "START_DT", "END_DT"}}
		transpose={"promo_flag"} 
		id={"promo_mechanics_name"} 
		casout={name="promo_transposed", caslib="casuser", replace=true};
	quit;

	proc casutil;
		promote casdata="promo_ml" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		promote casdata="promo_transposed" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		promote casdata="promo_ml_main_code" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		droptable casdata="product_hier_flat" incaslib="casuser" quiet;
		droptable casdata="lvl5" incaslib="casuser" quiet;
		droptable casdata="lvl4" incaslib="casuser" quiet;
		droptable casdata="lvl3" incaslib="casuser" quiet;
		droptable casdata="lvl2" incaslib="casuser" quiet;
		droptable casdata="lvl1" incaslib="casuser" quiet;
		droptable casdata="pbo_loc_hierarchy" incaslib="casuser" quiet;
		droptable casdata="product_hierarchy" incaslib="casuser" quiet;
		droptable casdata="abt_promo" incaslib="casuser" quiet;
	run;

	/****** 7. Добавляем мароэкономику ******/
	proc casutil;
	  droptable casdata="macro_ml" incaslib="casuser" quiet;
	  droptable casdata="macro2_ml" incaslib="casuser" quiet;
	  droptable casdata="macro_transposed_ml" incaslib="casuser" quiet;
	  droptable casdata="abt7_ml" incaslib="casuser" quiet;
	  run;

	data CASUSER.macro (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..macro_factor(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc fedsql sessref=casauto;
		create table casuser.macro_ml{options replace=true} as 
			select 
				factor_cd,
				datepart(cast(REPORT_DT as timestamp)) as period_dt,
				FACTOR_CHNG_PCT
			from casuser.macro;
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

	proc casutil;
		promote casdata="macro_transposed_ml" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		droptable casdata="macro2_ml" incaslib="casuser" quiet;
		droptable casdata="macro" incaslib="casuser" quiet;
		droptable casdata="macro_ml" incaslib="casuser" quiet;
		droptable casdata="abt6_ml" incaslib="casuser" quiet;
	run;


	/***** 8. Добавляем погоду. *****/
	proc casutil;
	  droptable casdata = "weather" incaslib = "casuser" quiet;
	run;

	data CASUSER.weather (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..weather(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc casutil;
		promote casdata="weather" incaslib="casuser" outcaslib="&lmvWorkCaslib."; 
	run;


	/***** 9. Добавляем trp конкурентов *****/
	proc casutil;
		droptable casdata="comp_transposed_ml_expand" incaslib="casuser" quiet;
		droptable casdata="comp_media_ml" incaslib="casuser" quiet;
	run;

	data CASUSER.comp_media (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..comp_media(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc fedsql sessref=casauto;
		create table casuser.comp_media_ml{options replace=true} as 
			select
				COMPETITOR_CD,
				TRP,
				datepart(cast(report_dt as timestamp)) as report_dt
			from 
				casuser.comp_media
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

	proc casutil;
		promote casdata='comp_transposed_ml_expand' incaslib='casuser' outcaslib="&lmvWorkCaslib.";
		promote casdata='comp_media' incaslib='casuser' outcaslib="&lmvWorkCaslib.";
		droptable casdata='comp_media_ml' incaslib='casuser' quiet;
		droptable casdata='comp_transposed_ml' incaslib='casuser' quiet;
	run;

	/***** 10. Добавляем медиаподдержку *****/
	proc casutil;
	  droptable casdata="media_ml" incaslib="casuser" quiet;
	  droptable casdata="promo_ml_trp_expand" incaslib="casuser" quiet;
	run;

	data CASUSER.media (replace=yes);
		set &lmvWorkCaslib..media_enh;
	run;

	/* Changes begin: Maxim Povod, 20.05.2021 */
	/*
	proc fedsql sessref=casauto;
		create table casuser.promo_ml_trp{options replace = true} as 
			select
				t1.PROMO_ID,
				t3.product_LEAF_ID,
				t2.PBO_LEAF_ID,
				t1.PROMO_NM,
				t1.START_DT,
				t1.END_DT,
				t4.report_dt,
				t4.TRP
			from
				casuser.promo as t1 
			left join
				casuser.promo_x_pbo_leaf as t2
			on 
				t1.PROMO_ID = t2.PROMO_ID
			left join
				casuser.promo_x_product_leaf as t3
			on
				t1.PROMO_ID = t3.PROMO_ID
			left join
				casuser.media as t4
			on
				t1.PROMO_GROUP_ID = t4.PROMO_GROUP_ID
				and t4.report_dt <= t1.end_dt
				and t4.report_dt >= t1.start_dt
		;
		
		create table casuser.promo_ml{options replace = true} as 
			select
				t1.PROMO_ID,
				t3.product_LEAF_ID,
				t2.PBO_LEAF_ID,
				t1.PROMO_NM,
				t1.PROMO_PRICE_AMT,
				t1.CHANNEL_CD,
				t1.NP_GIFT_PRICE_AMT,
				t1.PROMO_MECHANICS,
				start_dt,
				 end_dt
			from
				casuser.promo as t1
			left join
				casuser.promo_x_pbo_leaf as t2 
			on 
				t1.PROMO_ID = t2.PROMO_ID
			left join
				casuser.promo_x_product_leaf as t3 
			on
				t1.PROMO_ID = t3.PROMO_ID 
		;
	quit;
	data casuser.promo_ml2;
		set casuser.promo_ml;
		format period_dt date9.;
		do period_dt=start_dt to end_dt;
			output;
		end;
	run;
	proc fedsql sessref=casauto;
		create table casuser.num_of_promo_prod{options replace=true} as 
			select
				t1.CHANNEL_CD,
				t1.PBO_LEAF_ID,
				t1.period_dt,
				count(distinct t1.product_LEAF_ID) as count_promo_product,
				count(distinct t1.PROMO_ID) as nunique_promo
			from
				casuser.promo_ml2 as t1
			group by
				t1.CHANNEL_CD,
				t1.PBO_LEAF_ID,
				t1.period_dt
		;
	quit;	
	data casuser.promo_ml_trp_expand;
		set casuser.promo_ml_trp;
		do i = 1 to 7;
			output;
			REPORT_DT + 1;
		end;
	run;
	*/
	
	/* Разворачиваем медиаподдержку до уровня Товар-ПБО-интервал действия */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml_trp{options replace = true} as 
		select distinct
			t3.product_LEAF_ID,
			t2.PBO_LEAF_ID,
			datepart(t1.START_DT) as start_dt,
			datepart(t1.END_DT) as end_dt,
			coalesce(t4.TRP, 0) as trp
		from
			casuser.promo /* promo_enh */ as t1   
		left join
			casuser.promo_x_pbo_leaf as t2
		on 
			t1.PROMO_ID = t2.PROMO_ID
		left join
			casuser.promo_x_product_leaf as t3
		on
			t1.PROMO_ID = t3.PROMO_ID
		left join
			casuser.media /* media_enh */ as t4
		on
			t1.PROMO_GROUP_ID = t4.PROMO_GROUP_ID and
			datepart(t4.report_dt) <= datepart(t1.end_dt) and
			datepart(t4.report_dt) >= datepart(t1.start_dt)
		;
	quit;
	
	/* Усредняем TRP в рамках одного интервала промо */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml_trp2{options replace=true} as
			select
				product_LEAF_ID,
				PBO_LEAF_ID,
				start_dt,
				end_dt,
				/*mean(TRP) as mean_trp*/
				mean(TRP) as trp
			from
				casuser.promo_ml_trp as t1
			
			where product_LEAF_ID is not null 
				and PBO_LEAF_ID is not null 

			group by
				product_LEAF_ID,
				PBO_LEAF_ID,
				start_dt,
				end_dt			
		;
	quit;
	
	/* Раскладываем до уровня Товар-ПБО-день */
	data casuser.promo_ml_trp_expand;
		set casuser.promo_ml_trp2;
		do sales_dt = start_dt to end_dt;
			output;
		end;
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.sum_trp{options replace=true} as 
			select
				t1.PRODUCT_LEAF_ID,
				t1.PBO_LEAF_ID,
				cast(t1.sales_dt as date) as REPORT_DT,
				sum(t1.trp) as sum_trp
			from
				casuser.promo_ml_trp_expand as t1
			group by
				t1.PRODUCT_LEAF_ID,
				t1.PBO_LEAF_ID,
				t1.sales_dt
		;
	quit;
	
	/* Changes end: Maxim Povod, 20.05.2021 */

	proc casutil;
		*promote casdata="num_of_promo_prod" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		promote casdata="promo" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		promote casdata="promo_x_pbo_leaf" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		promote casdata="promo_x_product_leaf" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		promote casdata="promo_ml_trp_expand" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		promote casdata="media" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		promote casdata="sum_trp" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		droptable casdata="promo_x_product" incaslib="casuser" quiet;
		droptable casdata="promo_x_pbo" incaslib="casuser" quiet;
		droptable casdata="promo_ml_trp" incaslib="casuser" quiet;
	run;

	/******	12. Добавим атрибуты ПБО ******/
	proc casutil;
	  droptable casdata="pbo_dictionary_ml" incaslib="casuser" quiet;
	run;
	
	data CASUSER.pbo_location (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..pbo_location(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

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
				casuser.attr_transposed_pbo t3
			on
				t2.pbo_location_id=t3.pbo_location_id
			left join
				casuser.pbo_location t14
			on 
				t2.pbo_location_id=t14.pbo_location_id
			left join
				casuser.pbo_location t13
			on 
				t2.lvl3_id=t13.pbo_location_id
			left join
				casuser.pbo_location t12
			on
				t2.lvl2_id=t12.pbo_location_id;
	quit;
	
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_AGREEMENT_TYPE)
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_BREAKFAST)
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_BUILDING_TYPE)
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_COMPANY)
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_DELIVERY)
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_MCCAFE_TYPE)
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_PRICE_LEVEL)
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_DRIVE_THRU)
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_WINDOW_TYPE)

	proc casutil;
		promote casdata="pbo_dictionary_ml" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		droptable casdata='pbo_location' incaslib='casuser' quiet;
		droptable casdata='pbo_loc_attributes' incaslib='casuser' quiet;
		droptable casdata='pbo_hier_flat' incaslib='casuser' quiet;
		droptable casdata='attr_transposed' incaslib='casuser' quiet;
	run;

	/****** 13. Добавляем календарные признаки *******/
	data work.cldr_prep;
		retain date &lmvStartDate;
		do while(date <= &lmvScoreEndDate);
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
	PROC IMPORT DATAFILE='/data/files/input/russia_weekend.csv'
		DBMS=CSV
		OUT=WORK.russia_weekend
		replace 
		;
		GETNAMES=YES 
		;
	RUN;

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
		promote casdata="cldr_prep_features" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		droptable casdata="russia_event" incaslib="casuser" quiet;
		droptable casdata="russia_event2" incaslib="casuser" quiet;
		droptable casdata="russia_event_t" incaslib="casuser" quiet;
	run;

	PROC IMPORT DATAFILE='/data/files/input/russia_event.csv'
		DBMS=dlm
		OUT=WORK.russia_event
		replace 
		;
		delimiter=";"
		;
		GETNAMES=YES
		;
	RUN;

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

	proc casutil;
		promote casdata="russia_event_t" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
		droptable casdata="russia_event" incaslib="casuser" quiet;
		droptable casdata="russia_event2" incaslib="casuser" quiet;
	run;

	/* part for va_fpply_w_prof */
	proc casutil;
	  droptable casdata="product_dictionary" incaslib="&lmvWorkCaslib." quiet;
	run;
	
	data CASUSER.product (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..product(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.product_HIERARCHY (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..product_HIERARCHY(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.product_ATTRIBUTES (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..product_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc cas;
	transpose.transpose /
	   table={name="product_ATTRIBUTES", caslib="casuser", groupby={"product_id"}} 
	   attributes={{name="product_id"}} 
	   transpose={"PRODUCT_ATTR_VALUE"} 
	   prefix="A_" 
	   id={"PRODUCT_ATTR_NM"} 
	   casout={name="attr_transposed", caslib="casuser", replace=true};
	quit;

	proc fedsql sessref=casauto noprint;
	   create table casuser.product_hier_flat{options replace=true} as
			select t1.product_id, 
				   t2.product_id  as LVL4_ID,
				   t3.product_id  as LVL3_ID,
				   t3.PARENT_product_id as LVL2_ID, 
				   1 as LVL1_ID
			from 
			(select * from casuser.product_HIERARCHY where product_lvl=5) as t1
			left join 
			(select * from casuser.product_HIERARCHY where product_lvl=4) as t2
			on t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
			left join 
			(select * from casuser.product_HIERARCHY where product_lvl=3) as t3
			on t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
			;
	quit;

	proc fedsql sessref=casauto noprint;
	   create table casuser.product_dictionary{options replace=true} as
	   select t1.product_id, 
		   coalesce(t1.lvl4_id,-9999) as prod_lvl4_id,
		   coalesce(t1.lvl3_id,-999) as prod_lvl3_id,
		   coalesce(t1.lvl2_id,-99) as prod_lvl2_id,
		   cast(1 as double) as prod_lvl1_id,
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
	   left join casuser.product t15
	   on t1.product_id=t15.product_id
	   left join casuser.product t14
	   on t1.lvl4_id=t14.product_id
	   left join casuser.product t13
	   on t1.lvl3_id=t13.product_id
	   left join casuser.product t12
	   on t1.lvl2_id=t12.product_id
	   ;
	quit;

	proc casutil;
	  promote casdata="product_dictionary" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
	  droptable casdata='product' incaslib='casuser' quiet;
	  droptable casdata='product_HIERARCHY' incaslib='casuser' quiet;
	  droptable casdata='product_ATTRIBUTES' incaslib='casuser' quiet;
	  droptable casdata='product_hier_flat' incaslib='casuser' quiet;
	  droptable casdata='attr_transposed' incaslib='casuser' quiet;
	run;
	
	data CASUSER.PBO_LOCATION (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..pbo_location(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.PBO_LOC_HIERARCHY (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PBO_LOC_HIERARCHY(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	data CASUSER.PBO_LOC_ATTRIBUTES (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PBO_LOC_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	
	proc fedsql sessref=casauto noprint;
	   create table casuser.pbo_loc_attr{options replace=true} as
			select distinct *
			from casuser.PBO_LOC_ATTRIBUTES
			;
	quit;

	proc cas;
	transpose.transpose /
	   table={name="pbo_loc_attr", caslib="casuser", groupby={"pbo_location_id"}} 
	   attributes={{name="pbo_location_id"}} 
	   transpose={"PBO_LOC_ATTR_VALUE"} 
	   prefix="A_" 
	   id={"PBO_LOC_ATTR_NM"} 
	   casout={name="attr_transposed", caslib="casuser", replace=true};
	quit;

	proc fedsql sessref=casauto noprint;
	   create table casuser.pbo_hier_flat{options replace=true} as
			select t1.pbo_location_id, 
				   t2.PBO_LOCATION_ID as LVL3_ID,
				   t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				   1 as LVL1_ID
			from 
			(select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=4) as t1
			left join 
			(select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
			;
	quit;

	proc fedsql sessref=casauto noprint;
	   create table casuser.pbo_dictionary{options replace=true} as
	   select t2.pbo_location_id, 
		   coalesce(t2.lvl3_id,-999) as lvl3_id,
		   coalesce(t2.lvl2_id,-99) as lvl2_id,
		   cast(1 as double) as lvl1_id,
		   coalesce(t14.pbo_location_nm,'NA') as pbo_location_nm,
		   coalesce(t13.pbo_location_nm,'NA') as lvl3_nm,
		   coalesce(t12.pbo_location_nm,'NA') as lvl2_nm,
		   cast(inputn(t3.A_OPEN_DATE,'ddmmyy10.') as date) as A_OPEN_DATE,
		   cast(inputn(t3.A_CLOSE_DATE,'ddmmyy10.') as date) as A_CLOSE_DATE,
		   t3.A_PRICE_LEVEL,
		   t3.A_DELIVERY,
		   t3.A_AGREEMENT_TYPE,
		   t3.A_BREAKFAST,
		   t3.A_BUILDING_TYPE,
		   t3.A_COMPANY,
		   t3.A_DRIVE_THRU,
		   t3.A_MCCAFE_TYPE,
		   t3.A_WINDOW_TYPE
	   from casuser.pbo_hier_flat t2
	   left join casuser.attr_transposed t3
	   on t2.pbo_location_id=t3.pbo_location_id
	   left join casuser.pbo_location t14
	   on t2.pbo_location_id=t14.pbo_location_id
	   left join casuser.pbo_location t13
	   on t2.lvl3_id=t13.pbo_location_id
	   left join casuser.pbo_location t12
	   on t2.lvl2_id=t12.pbo_location_id
	   ;
	quit;

	proc casutil;
	  promote casdata="pbo_dictionary" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
	  droptable casdata='pbo_loc_attr' incaslib='casuser' quiet;
	  droptable casdata='pbo_location' incaslib='casuser' quiet;
	  droptable casdata='PBO_LOC_HIERARCHY' incaslib='casuser' quiet;
	  droptable casdata='PBO_LOC_ATTRIBUTES' incaslib='casuser' quiet;
	  droptable casdata='pbo_hier_flat' incaslib='casuser' quiet;
	  droptable casdata='attr_transposed' incaslib='casuser' quiet;
	run;
	
	/* part for vf_new_product*/
	/*
	data CASUSER.product_chain (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..product_chain(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	*/
	proc fedsql sessref=casauto noprint;
		create table casuser.product_chain{options replace=true} as
		  select 
			LIFECYCLE_CD
			,PREDECESSOR_DIM2_ID
			,PREDECESSOR_PRODUCT_ID
			,SCALE_FACTOR_PCT
			,SUCCESSOR_DIM2_ID
			,SUCCESSOR_PRODUCT_ID
			,PREDECESSOR_END_DT
			,SUCCESSOR_START_DT
		  from &lmvWorkCaslib..product_chain_enh
		;
	quit;

	proc casutil;
	  promote casdata="product_chain" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
	run;
	
	data CASUSER.ingridients (replace=yes);
        set ETL_TMP.ia_ingridients;
    run;
	
	/*
	data CASUSER.ingridients (replace=yes);
        set &lmvInLib..ingridients(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
    run;
	*/
	proc casutil;
	  promote casdata="ingridients" incaslib="casuser" outcaslib="&lmvWorkCaslib.";
	run;
	
%mend rtp_load_data_to_caslib;
