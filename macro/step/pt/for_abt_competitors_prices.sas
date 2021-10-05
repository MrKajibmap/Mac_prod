%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( cmasauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

options casdatalimit=600000M;

/* Инициализация */
%let lmvInLib 		 = ETL_IA;
%let lmvReportDttm	 = &ETL_CURRENT_DTTM.;

/* Через какие поля должны связываться товары McDonald's с товарами конкурентов */
/* Замечание: вообще на уровне 4, то есть связка по мастер-коду, но временно связываем по 3 */
%let lmvMcdProdLevel = 	PROD_LVL3_ID; 
%let lmvCompProdLevel = product_lvl_id3;


/* Справочник SKU */
%let lmvMcdProdDict  = MAX_CASL.PRODUCT_DICTIONARY;
%let lmvHorizonEndDt = 22615; /* 01dec2021 */


/***********************************************************************************************************/
/* Шаг 0. Подготовка всех необходимых данных для алгоритма */ 

/* Загрузка цен конкурентов */
data CASUSER.COMP_PRICE (replace=yes  drop=valid_from_dttm valid_to_dttm);
	set &lmvInLib..COMP_PRICE (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
run;

/* Проверка цен */
PROC MEANS 
	data=CASUSER.COMP_PRICE 
	N Mean Median Min Max Q1 Q3 
	MaxDec = 2 
	;
	CLASS product_comp_id;
	VAR price;
	output  out = CASUSER.TEST_PRICE_3 (drop= _TYPE_ _FREQ_)
		n 		=
		Min 	=
		Q1 		=
		Median 	=
		Mean 	=
		Q3 		=
		Max 	= 
		/ autoname;
RUN; 

data CASUSER.TEST_PRICE_4;
set CASUSER.TEST_PRICE_3;
k = price_max / price_median;
where 
 	price_min < price_median * 0.1 
	or price_max > price_median *2
;
run;



/* Загрузка справочника товаров конкурентов (связка с товарами McDonald's) */
data CASUSER.COMP_PROD (replace=yes  drop=valid_from_dttm valid_to_dttm);
	set &lmvInLib..COMP_PROD (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
run;

/* Загрузка справочника ресторанов конкурентов */
data CASUSER.COMP_PBO (replace=yes  drop=valid_from_dttm valid_to_dttm);
	set &lmvInLib..COMP_PBO (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
run;

/* Загрузка промо-таблиц */
%add_promotool_marks2(
		  mpOutCaslib	= casuser
		, mpPtCaslib	= pt
		, PromoCalculationRk =
	);

/* Связка PROMO_ID с группой товаров */
proc fedsql sessref=casauto;
	create table CASUSER.PROMO_VS_PROD_PRE{options replace=true} as 
		select distinct
			  prm.PROMO_ID
			, prm.PRODUCT_ID
			, sku.PRODUCT_NM
			, sku.PROD_LVL1_ID 
			, sku.PROD_LVL1_NM 
			, sku.PROD_LVL2_ID 
			, sku.PROD_LVL2_NM 
			, sku.PROD_LVL3_ID 
			, sku.PROD_LVL3_NM 
			, sku.PROD_LVL4_ID 
			, sku.PROD_LVL4_NM
		from CASUSER.PROMO_PROD_ENH as prm
		inner join &lmvMcdProdDict. as sku
			on prm.PRODUCT_ID = sku.PRODUCT_ID
	;
	/* 1379 = 258 x 228 */
	create table CASUSER.PROMO_VS_PROD{options replace=true} as 
		select distinct
			  PROMO_ID
			, &lmvMcdProdLevel. 
			, PROD_LVL2_ID
		from CASUSER.PROMO_VS_PROD_PRE
	;
quit;



/***********************************************************************************************************/

/* Шаг 1. Преобразование цен 
			из разреза:	ПБО конкурента - Товар конкурента - Дата
			в разрез:	ПБО ближайшего ПБО McDonald's - PROMO_ID - Месяц 
Цена расчитывается как средняя в разрезе категории
*/

/* Связать товары конкурентов с PROMO_ID и товарами McDonald's*/		
proc fedsql sessref=casauto;
	create table CASUSER.PROMO_VS_PROD_COMP{options replace=true} as 
		select
			  cmp.*
			, mcd.*
		from  CASUSER.COMP_PROD as cmp
		inner join CASUSER.PROMO_VS_PROD as mcd
			on cmp.&lmvCompProdLevel. = 	
				/* !!! Временное решение !!! Должно просто по уровню 4 джойниться !!! */
				mod(mcd.&lmvMcdProdLevel., 10000)		
	;
quit;


/* Связать ПБО McDonald's с ближайшими ресторанами конкурентов */
/* Связывается в конце скрипта:
	/opt/sas/mcd_config/macro/step/pt/for_abt_competitors_distance.sas
	таблица CASUSER.PBOS_MCD_NEAREST_COMP
*/

/* Связать SKU и ПБО */
proc fedsql sessref=casauto;
	create table CASUSER.PROMO_VS_PROD_VS_PBO{options replace=true} as 
		select
			  prod.PROMO_ID 
			, prod.&lmvMcdProdLevel. 	
			, prod.competitor_id 	
			, prod.product_comp_id	
			, prod.PROD_LVL2_ID
			, pbo.pbo_location_id 
			, pbo.pbo_loc_comp_id 
		from  CASUSER.PROMO_VS_PROD_COMP as prod
		inner join CASUSER.PBOS_MCD_NEAREST_COMP as pbo
			on prod.competitor_id = pbo.competitor_id
		
	;
quit;


proc fedsql sessref=casauto;
	select distinct competitor_id from CASUSER.PROMO_VS_PROD_COMP;
	select distinct competitor_id from CASUSER.PBOS_MCD_NEAREST_COMP;
quit;

/* Сагрегировать до месяца цены конкурентов и протянуть вперед предыдущими значениями */
proc cas;
	timeData.timeSeries result =r /
		series={
			  {name="price", setmiss="PREV"}
		}
		tEnd= "&lmvHorizonEndDt"
		table={
			caslib="casuser",
			name="COMP_PRICE",
			groupby={"pbo_loc_comp_id","product_comp_id"}
		}
		timeId="start_dt"
		trimId="LEFT"
		interval="month"
		casOut={
			caslib="casuser", 
			name="COMP_PRICE_TS", 
			replace=True
		}
		;
	run;
quit;



/* Огромная развернутая таблица для  вычисления статистик  */
proc fedsql sessref=casauto;
	create table CASUSER.COMP_PRICE_EXT{options replace=true} as 
		select
			  lvl.PROMO_ID 
			, lvl.pbo_location_id
			, price.start_dt as month_dt
			
			, lvl.PROD_LVL2_ID
			, lvl.competitor_id 	
			
			, price.price 
			
			, lvl.&lmvMcdProdLevel. 	
			, price.pbo_loc_comp_id
			, price.product_comp_id
			
		from CASUSER.COMP_PRICE_TS as price
		inner join CASUSER.PROMO_VS_PROD_VS_PBO as lvl
			on  price.pbo_loc_comp_id 	= lvl.pbo_loc_comp_id
			and price.product_comp_id 	= lvl.product_comp_id	
	;
quit;

/* Расчет минимальной и средней цен внутри категории */
proc fedsql sessref=casauto;
	create table CASUSER.NEAREST_COMP_PRICES{options replace=true} as 
		select
			  PROMO_ID 
			, pbo_location_id
			, month_dt
			
			, PROD_LVL2_ID
			, competitor_id 
			, catx('_', competitor_id, PROD_LVL2_ID) as id_group
			
			, avg(price) as avg_price
			, min(price) as min_price
			
		from CASUSER.COMP_PRICE_EXT
		group by 1,2,3,4,5,6
	;
quit;

/* PROC MEANS  */
/* 	data=CASUSER.COMP_PRICE_EXT  */
/* 	Mean Median Min */
/* 	MaxDec = 2  */
/* 	noprint */
/* 	; */
/* 	CLASS PROMO_ID pbo_location_id month_dt */
/* 		PROD_LVL2_ID competitor_id */
/* 		; */
/* 	VAR price; */
/* 	output  out = CASUSER.NEAREST_COMP_PRICES (drop= _TYPE_ _FREQ_) */
/* 		Min		=  */
/* 		Median 	= */
/* 		Mean 	= */
/* 		/ autoname; */
/* RUN;  */

proc cas;
	transpose.transpose /
	   table = {
				  name="NEAREST_COMP_PRICES"
				, caslib="casuser"
				, groupby={"PROMO_ID", "pbo_location_id", "month_dt"}
			} 
	   attributes = {
			{name="id_group"}
			}
	   transpose={"avg_price", "min_price"} 
	   prefix="A_" 
	   id={"id_group"} 
	   casout={name="NEAREST_COMP_PRICES_T", caslib="casuser", replace=true}
	;
quit;



