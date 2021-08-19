/********************************************************************************************************/
/* Инициализация */

%include "/opt/sas/mcd_config/config/initialize_global.sas"; 
cas casauto;
caslib _all_ assign;

/* Подключение макросов */

%let common_path = /opt/sas/mcd_config/macro/step/pt/alerts;
%include "&common_path./data_prep_product.sas"; 
%include "&common_path./data_prep_pbo.sas"; 
%include "&common_path./avgrcp/avgrcp_alert_year_trend.sas"; 
%include "&common_path./avgrcp/avgrcp_alert_strange_seasonality.sas"; 


/********************************************************************************************************/
/* Подготовка справочников */

/* Справочник SKU */
%data_prep_product(
	  mpInLib 		= ETL_IA
	, mpReportDttm 	= &ETL_CURRENT_DTTM.
	, mpOutCasTable = CASUSER.PRODUCT_DICTIONARY
);

/* Справочник ПБО */
%data_prep_pbo(
	  mpInLib 		= ETL_IA
	, mpReportDttm 	= &ETL_CURRENT_DTTM.
	, mpOutCasTable = CASUSER.PBO_DICTIONARY
);


/********************************************************************************************************/
/* Подготовка прогнозов */

proc fedsql sessref=casauto;
	create table CASUSER.AVGRCP_FCST_MONTH {options replace=true} as
	select distinct
		  cast(pmix.prod as integer) as PROD /*– ИД продукта*/
		, cast(pmix.location as integer) as LOCATION /*– ИД ресторана*/
		, pmix.data as DATA /*– Дата прогноза или факта (месяц)*/
		
		, pmix.OVERRIDED_FCST_UNITS
		, pmix.OVERRIDED_FCST_SALE
		, gc.OVERRIDED_FCST_GC
		
		, case 
			when abs(gc.OVERRIDED_FCST_GC) > 1e-5 
				and abs(pmix.OVERRIDED_FCST_UNITS) > 1e-5 
			then pmix.OVERRIDED_FCST_UNITS / gc.OVERRIDED_FCST_GC * 1000 
			else 0
			end
		  as UPT 
		
	from MN_SHORT.PLAN_PMIX_MONTH as pmix 
	left join MN_SHORT.PLAN_GC_MONTH as gc
		on  pmix.location = gc.location 
			and pmix.data = gc.data
	;
quit;




/********************************************************************************************************/
/* Подготовка фактов */

%let lmvReportDttm = &ETL_CURRENT_DTTM.;

data CASUSER.PMIX_SALES (replace=yes drop=valid_from_dttm valid_to_dttm);
    set ETL_IA.PMIX_SALES (where=(valid_to_dttm>=&lmvReportDttm.));
run;

data CASUSER.PBO_SALES (replace=yes drop=valid_from_dttm valid_to_dttm);
   set ETL_IA.PBO_SALES (where=(valid_to_dttm>=&lmvReportDttm.));
run;

/* В том числе Убираем канал и суммируем до месяцев ! */
proc fedsql sessref=casauto;
	create table CASUSER.AVGRCP_FACT_MONTH {options replace=true} as
	select distinct
/* 		  pmix.channel_cd */
		  pmix.pbo_location_id
		, pmix.product_id
		, cast(intnx('month', pmix.sales_dt, 0, 'B') as date) as sales_dt
		, sum(pmix.sales_qty) as sales_qty
		, sum(pmix.net_sales_amt) as sales_rur
		, sum(gc.receipt_qty) as receipt_qty
/* 		, case  */
/* 			when abs(gc.receipt_qty)> 1e-5 */
/* 				and abs(pmix.sales_qty)> 1e-5 */
/* 			then pmix.sales_qty / gc.receipt_qty * 1000  */
/* 			else 0 */
/* 			end */
/* 		  as AVGRCP */

	from CASUSER.PMIX_SALES as pmix 

	left join CASUSER.PBO_SALES as gc
		on 		pmix.channel_cd 	 = gc.channel_cd 
			and pmix.pbo_location_id = gc.pbo_location_id
			and pmix.sales_dt 		 = gc.sales_dt
	
	group by 1,2,3
	;
quit;



/********************************************************************************************************/
/* Запуск алертов */


/* Допустимые параметры:
	Агрегация ПБО:
		A_AGREEMENT_TYPE
		LVL1_NM
		LVL2_NM
		LVL3_NM
		pbo_location_nm
	Граничные даты:
		22333 - 22feb2021
		22646 - 01jan2022
	Сезоны:
		month
		qtr
*/


/* Изменения годовых трендов */
/* На уровне Регион - Категория - Текущая дата */
%avgrcp_alert_year_trend(
      mpProdLvl = PROD_LVL2_NM
	, mpLocLvl = LVL2_NM
	, mpBorderDt = today()
	, mpAlertCriterionDamp = 0.05
	, mpAlertCriterionGrowth = 0.1
	, mpOutTableNm = AVGRCP_YEAR_TREND_CAT_REG_CURDT
);

/* Изменения годовых трендов */
/* На уровне Регион - Все товары - Текущая дата */
%avgrcp_alert_year_trend(
      mpProdLvl = PROD_LVL1_NM
	, mpLocLvl = LVL2_NM
	, mpBorderDt = today()
	, mpAlertCriterionDamp = 0.05
	, mpAlertCriterionGrowth = 0.1
	, mpOutTableNm = AVGRCP_YEAR_TREND_ALL_REG_CURDT
);

/* Изменения годовых трендов */
/* На уровне Регион - Категория - Следующий календарный год */
%avgrcp_alert_year_trend(
      mpProdLvl = PROD_LVL2_NM
	, mpLocLvl = LVL2_NM
	, mpBorderDt = intnx('year', today(), 1, 'B')
	, mpAlertCriterionDamp = 0.05
	, mpAlertCriterionGrowth = 0.1
	, mpOutTableNm = AVGRCP_YEAR_TREND_CAT_REG_CALYR
);

/* Изменения годовых трендов */
/* На уровне Регион - Все товары - Следующий календарный год */
%avgrcp_alert_year_trend(
      mpProdLvl = PROD_LVL1_NM
	, mpLocLvl = LVL2_NM
	, mpBorderDt = intnx('year', today(), 1, 'B')
	, mpAlertCriterionDamp = 0.05
	, mpAlertCriterionGrowth = 0.1
	, mpOutTableNm = AVGRCP_YEAR_TREND_ALL_REG_CALYR
);


/*************************************************************************************/
/* Изменения сезонных трендов */
/* На уровне Категория - Регион - Полугодие */
%avgrcp_alert_strange_seasonality(
	  mpProdLvl = PROD_LVL2_NM
	, mpLocLvl = LVL2_NM
	, mpBorderDt = today()
	, mpSeason = semiyear
	, mpAlertCriterionRelChange = 0.3
	, mpOutTableNm = AVGRCP_SMY_CHANGE_CAT_REG_CURDT
);

/* Изменения сезонных трендов */
/* На уровне Категория - Регион - Квартал */
%avgrcp_alert_strange_seasonality(
	  mpProdLvl = PROD_LVL2_NM
	, mpLocLvl = LVL2_NM
	, mpBorderDt = today()
	, mpSeason = qtr
	, mpAlertCriterionRelChange = 0.3
	, mpOutTableNm = AVGRCP_QTR_CHANGE_CAT_REG_CURDT
);

/* Изменения сезонных трендов */
/* На уровне Категория - Все товары - Полугодие */
%avgrcp_alert_strange_seasonality(
	  mpProdLvl = PROD_LVL1_NM
	, mpLocLvl = LVL2_NM
	, mpBorderDt = today()
	, mpSeason = semiyear
	, mpAlertCriterionRelChange = 0.3
	, mpOutTableNm = AVGRCP_SMY_CHANGE_ALL_REG_CURDT
);

/* Изменения сезонных трендов */
/* На уровне Категория - Все товары - Квартал */
%avgrcp_alert_strange_seasonality(
	  mpProdLvl = PROD_LVL1_NM
	, mpLocLvl = LVL2_NM
	, mpBorderDt = today()
	, mpSeason = qtr
	, mpAlertCriterionRelChange = 0.3
	, mpOutTableNm = AVGRCP_QTR_CHANGE_ALL_REG_CURDT
);


/* Изменения сезонных трендов */
/* На уровне Категория - Вся сеть - Месяц */
%avgrcp_alert_strange_seasonality(
	  mpProdLvl = PROD_LVL2_NM
	, mpLocLvl = LVL1_NM
	, mpBorderDt = today()
	, mpSeason = month
	, mpAlertCriterionRelChange = 0.3
	, mpOutTableNm = AVGRCP_MTH_CHANGE_CAT_ALL_CURDT
);


/*************************************************************************************/
/* Сохранение алертов в excel-файл.
	Необходимо следить за:
		- указанный путь 
		- наименование excel-страницы 
		- соответствие названию таблицы
*/
ods excel file="&common_path./AVGRCP_ALERTS.xlsx"  style=statistical;

ods excel options(sheet_interval = 'none' sheet_name = "Год-год, Кат.Рег., Тек.дата"	);
proc print data = DM_ALERT.AVGRCP_YEAR_TREND_CAT_REG_CURDT 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Год-год, Кат.Рег., След.кал.год"	);
proc print data = DM_ALERT.AVGRCP_YEAR_TREND_CAT_REG_CALYR	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Год-год, ALL.Рег., Тек.дата"	);
proc print data = DM_ALERT.AVGRCP_YEAR_TREND_ALL_REG_CURDT 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Год-год, ALL.Рег., След.кал.год"	);
proc print data = DM_ALERT.AVGRCP_YEAR_TREND_ALL_REG_CALYR	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Полугодие-Категория-Регион"	);
proc print data = DM_ALERT.AVGRCP_SMY_CHANGE_CAT_REG_CURDT	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Полугодие-Все товары-Регион");
proc print data = DM_ALERT.AVGRCP_SMY_CHANGE_ALL_REG_CURDT	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Квартал-Категория-Регион"	);
proc print data = DM_ALERT.AVGRCP_QTR_CHANGE_CAT_REG_CURDT 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Квартал-Все товары-Регион");
proc print data = DM_ALERT.AVGRCP_QTR_CHANGE_ALL_REG_CURDT	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "Месяц-Категория-Вся сеть"	);
proc print data = DM_ALERT.AVGRCP_MTH_CHANGE_CAT_ALL_CURDT 	label; run;

ods excel close;







