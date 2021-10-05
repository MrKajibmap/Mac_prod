%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( cmasauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

options casdatalimit=600000M;

/* Инициализация входных параметроа */
%let lmvInLib 			= ETL_IA; 
%let lmvReportDttm		= &ETL_CURRENT_DTTM.; 	
%let lmvEarthRadius 	= %sysevalf((6356.752 + 6378.137) / 2);
%let lmvPiValueRad 		= %sysevalf(3.14159 / 180);
%let lmvHistStartDate 	= '01jan2018'd;
%let lmvHorizonEndDate 	= '01jan2022'd;


/***********************************************************************************************************/
/* Шаг 0. Подготовка всех необходимых данных для алгоритма */ 

/* Справочник ПБО McDonald's */
%let common_path = /opt/sas/mcd_config/macro/step/pt/alerts;
%include "&common_path./data_prep_pbo.sas"; 
%data_prep_pbo(
	  mpInLib 		= ETL_IA
	, mpReportDttm 	= &ETL_CURRENT_DTTM.
	, mpOutCasTable = CASUSER.PBO_DICTIONARY_EXT
);

/* Справочник ПБО конкурентов */
data WORK.COMP_PBO (drop=valid_from_dttm valid_to_dttm);
	set &lmvInLib..COMP_PBO(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
run;


/* Развернуть справочник ПБО McDonald's по месяцам на историю и будущее */
data WORK.PBO_DICT_MCD;
	set CASUSER.PBO_DICTIONARY_EXT;
	where A_LATITUDE is not missing 
		and A_LONGITUDE is not missing;
	
	latitude_mcd = input(tranwrd(A_LATITUDE, ",", "."), best32.);
	longitude_mcd = input(tranwrd(A_LONGITUDE, ",", "."), best32.);
	
	format month_dt date9.;
	month_dt = intnx('month', max(&lmvHistStartDate., A_OPEN_DATE), 0, 'B');
	do until(month_dt > intnx('month', min(&lmvHorizonEndDate., max(A_CLOSE_DATE, &lmvHorizonEndDate.)), 0, 'B'));
	    month_dt = intnx("month", month_dt, 1, "same");
		output;
	end;

	keep 
		pbo_location_id 
		latitude_mcd 
		longitude_mcd
		A_OPEN_DATE
		A_CLOSE_DATE
		month_dt
	;
run;


/***********************************************************************************************************/
/* Шаг 1. Обработка данных для KFC */
/* Развернуть справочник ПБО для KFC по месяцам на историю и будущее */
data WORK.COMPETITORS_KFC;
	set WORK.COMP_PBO;
	where competitor_id = 8;
	
	latitude_kfc = input(tranwrd(latitude, ",", "."), best32.);
	longitude_kfc = input(tranwrd(longitude, ",", "."), best32.);
	
	format month_dt date9.;
	month_dt = intnx('month', max(&lmvHistStartDate., open_dt), 0, 'B');
	do until(month_dt > intnx('month', min(&lmvHorizonEndDate., max(close_dt, &lmvHorizonEndDate.)), 0, 'B'));
	    month_dt = intnx("month", month_dt, 1, "same");
		output;
	end;
	
	keep 
		pbo_loc_comp_id 
		latitude_kfc 
		longitude_kfc
		open_dt
		close_dt
		month_dt
	;
run;

/* Расчет расстояний между каждой парой ресторанов McDonald's и KFC
		для каждого месяца на истории и горизонте прогнозирования */
proc sql;
	create table WORK.PBOS_MCD_VS_KFC as 
	select 
		  mcd.pbo_location_id 
		, mcd.latitude_mcd 
		, mcd.longitude_mcd
		, mcd.A_OPEN_DATE
		, mcd.A_CLOSE_DATE
		, mcd.month_dt
		
		, kfc.pbo_loc_comp_id 
		, kfc.latitude_kfc 
		, kfc.longitude_kfc
		, kfc.open_dt
		, kfc.close_dt

		, 2 * &lmvEarthRadius. * ARSIN( SQRT( 0.5 - 
			cos( ( latitude_kfc - latitude_mcd ) * &lmvPiValueRad. ) / 2 + 
				cos( latitude_mcd * &lmvPiValueRad. ) * 
					cos( latitude_kfc * &lmvPiValueRad. ) * 
						(1 - cos( ( longitude_kfc - longitude_mcd ) * &lmvPiValueRad. ) ) / 2 
			)) as distance_kfc

	from WORK.PBO_DICT_MCD as mcd
	inner join WORK.COMPETITORS_KFC as kfc
		on mcd.month_dt = kfc.month_dt
	;
quit;

/* Формирование фичей:
	Кол-во ресторанов KFC внутри 1км радиуса вокруг ресторана McDonald's
	Кол-во ресторанов KFC внутри 2км радиуса вокруг ресторана McDonald's
*/
proc sql;
	create table WORK.PBOS_MCD_VS_KFC_L1 as 
	select 
		  pbo_location_id
		, month_dt
		, count(pbo_location_id) as n_comp_kfc_within_1km
	from WORK.PBOS_MCD_VS_KFC
	where distance_kfc <= 1
	group by 1,2
	;
	create table WORK.PBOS_MCD_VS_KFC_L2 as 
	select 
		  pbo_location_id
		, month_dt
		, count(pbo_location_id) as n_comp_kfc_within_2km
	from WORK.PBOS_MCD_VS_KFC
	where distance_kfc <= 2
	group by 1,2
	;
quit;


/***********************************************************************************************************/
/* Шаг 2. Обработка данных для Burger King */
/* Развернуть справочник ПБО для BK по месяцам на историю и будущее */
data WORK.COMPETITORS_BK;
	set WORK.COMP_PBO;
	where competitor_id = 9;

	latitude_bk = input(tranwrd(latitude, ",", "."), best32.);
	longitude_bk = input(tranwrd(longitude, ",", "."), best32.);

	format month_dt date9.;
	month_dt = intnx('month', max(&lmvHistStartDate., open_dt), 0, 'B');
	do until(month_dt > intnx('month', min(&lmvHorizonEndDate., max(close_dt, &lmvHorizonEndDate.)), 0, 'B'));
	    month_dt = intnx("month", month_dt, 1, "same");
		output;
	end;

	keep 
		pbo_loc_comp_id 
		latitude_bk 
		longitude_bk
		open_dt
		close_dt
		month_dt
	;
run;

/* Расчет расстояний между каждой парой ресторанов McDonald's и BK
		для каждого месяца на истории и горизонте прогнозирования */
proc sql;
	create table WORK.PBOS_MCD_VS_BK as 
	select 
		  mcd.pbo_location_id 
		, mcd.latitude_mcd 
		, mcd.longitude_mcd
		, mcd.A_OPEN_DATE
		, mcd.A_CLOSE_DATE
		, mcd.month_dt
		
		, bk.pbo_loc_comp_id 
		, bk.latitude_bk
		, bk.longitude_bk
		, bk.open_dt
		, bk.close_dt

		, 2 * &lmvEarthRadius. * ARSIN( SQRT( 0.5 - 
			cos( ( latitude_bk - latitude_mcd ) * &lmvPiValueRad. ) / 2 + 
				cos( latitude_mcd * &lmvPiValueRad. ) * 
					cos( latitude_bk * &lmvPiValueRad. ) * 
						(1 - cos( ( longitude_bk - longitude_mcd ) * &lmvPiValueRad. ) ) / 2 
			)) as distance_bk

	from WORK.PBO_DICT_MCD as mcd
	inner join WORK.COMPETITORS_BK as bk
		on mcd.month_dt = bk.month_dt
	;
quit;

/* Формирование фичей:
	Кол-во ресторанов BK внутри 1км радиуса вокруг ресторана McDonald's
	Кол-во ресторанов BK внутри 2км радиуса вокруг ресторана McDonald's
*/
proc sql;
	create table WORK.PBOS_MCD_VS_BK_L1 as 
	select 
		  pbo_location_id
		, month_dt
		, count(pbo_location_id) as n_comp_bk_within_1km
	from WORK.PBOS_MCD_VS_BK 
	where distance_bk <= 1
	group by 1,2
	;
	create table WORK.PBOS_MCD_VS_BK_L2 as 
	select 
		  pbo_location_id
		, month_dt
		, count(pbo_location_id) as n_comp_bk_within_2km
	from WORK.PBOS_MCD_VS_BK
	where distance_bk <= 2
	group by 1,2
	;
quit;



/***********************************************************************************************************/
/* Шаг 3. ОБъединение фичей в одну таблицу для дальнейшего присоединения к ABT */
/* Финальную таблицу можно попробовать добавить двумя способами:
	- coalesce(<поле>, 0)
	- coalesce(<поле>, .)
	Вообще надо продумать получше логику выше, 
		потому что может быть и 0 и missing в разных случаях
*/
proc sql;
select min(n_comp_bk_within_1km) from WORK.PBOS_MCD_VS_BK_L1;
select min(n_comp_bk_within_2km) from WORK.PBOS_MCD_VS_BK_L2;
select min(n_comp_kfc_within_1km) from WORK.PBOS_MCD_VS_KFC_L1;
select min(n_comp_kfc_within_2km) from WORK.PBOS_MCD_VS_KFC_L2;
;
quit;

proc sql;
	create table WORK.N_COMPETITORS_FOR_PBOS as
	select 
		  mcd.*
		, coalesce(bk1.n_comp_bk_within_1km   , 0) as n_comp_bk_within_1km   
		, coalesce(bk2.n_comp_bk_within_2km   , 0) as n_comp_bk_within_2km   
		, coalesce(kfc1.n_comp_kfc_within_1km , 0) as n_comp_kfc_within_1km 
		, coalesce(kfc2.n_comp_kfc_within_2km , 0) as n_comp_kfc_within_2km 

	from WORK.PBO_DICT_MCD as mcd
	left join WORK.PBOS_MCD_VS_BK_L1 as bk1
		on  mcd.pbo_location_id = bk1.pbo_location_id
		and mcd.month_dt 		= bk1.month_dt
	left join WORK.PBOS_MCD_VS_BK_L2 as bk2
		on  mcd.pbo_location_id = bk2.pbo_location_id
		and mcd.month_dt 		= bk2.month_dt
	left join WORK.PBOS_MCD_VS_KFC_L1 as kfc1
		on  mcd.pbo_location_id = kfc1.pbo_location_id
		and mcd.month_dt 		= kfc1.month_dt
	left join WORK.PBOS_MCD_VS_KFC_L2 as kfc2
		on  mcd.pbo_location_id = kfc2.pbo_location_id
		and mcd.month_dt 		= kfc2.month_dt
	;
quit;

data CASUSER.COMPETITORS_DISTANCE;
	set WORK.N_COMPETITORS_FOR_PBOS;
run;


/***********************************************************************************************************/
/* Дополнительная таблица с ближайшими ресторанами конкурентов
	для применения в ценах конкурентов */

proc sort data = WORK.PBOS_MCD_VS_KFC;
	by pbo_location_id distance_kfc;
run;

data CASUSER.PBOS_MCD_NEAREST_KFC;
	set WORK.PBOS_MCD_VS_KFC;
	by pbo_location_id;
	competitor_id = 8;
	if first.pbo_location_id then output;
	rename distance_kfc = distance;
run;


proc sort data = WORK.PBOS_MCD_VS_BK;
	by pbo_location_id distance_bk;
run;

data CASUSER.PBOS_MCD_NEAREST_BK;
	set WORK.PBOS_MCD_VS_BK;
	by pbo_location_id;
	competitor_id = 9;
	if first.pbo_location_id then output;
	rename distance_bk = distance;
run;

data CASUSER.PBOS_MCD_NEAREST_COMP;
	set 
		CASUSER.PBOS_MCD_NEAREST_KFC
		CASUSER.PBOS_MCD_NEAREST_BK 
	;
	keep pbo_location_id pbo_loc_comp_id competitor_id distance;
run;

/* Очистка WORK */
/*
proc datasets library=WORK kill nolist;
quit;