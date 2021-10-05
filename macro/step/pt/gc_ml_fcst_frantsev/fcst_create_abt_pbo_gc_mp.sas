/* ****************** */
/* Макрос для построения витрин PBO GC и PBO UNITS в рамках сквозного процесса прогнозирования */
/* Параметры 1. mpMode  		- GC | PBO */
/*  		 2. mpOutTableDmVf 	- имя выходной таблицы в двухуровневом формате */
/*  		 3. mpOutTableDmABT - имя выходной расширенной таблицы в двухуровневом формате */
/* 			 4. mpSeasonMode    - 1 | 2 (одно-этапный или двух-этапный способ очистки от сезонности)
*/
/* ****************** */
/* 
	Пример использования: 
		%fcst_create_abt_pbo_gc(
			  mpMode		  = gc
			, mpOutTableDmVf  = MN_DICT.DM_TRAIN_TRP_GC				* для теста запускать в CASUSER либу
			, mpOutTableDmABT = MN_DICT.TRAIN_ABT_TRP_GC			* для теста запускать в CASUSER либу
		);
	или
		%fcst_create_abt_pbo_gc(
			  mpMode		  = pbo
			, mpOutTableDmVf  = MN_DICT.DM_TRAIN_TRP_PBO
			, mpOutTableDmABT = MN_DICT.TRAIN_ABT_TRP_PBO
		);
*/
%macro fcst_create_abt_pbo_gc_mp(
			  mpMode		  = gc
			, mpSeasonMode	  = 2
			, mpOutTableDmVf  = MN_DICT.DM_TRAIN_TRP_GC
			, mpOutTableDmABT = MN_DICT.TRAIN_ABT_TRP_GC
		);
							
	%local 	lmvMode					/* Режим прогнозирования PBO GC или PBO UNITS */
			lmvReportDttm			/* Текущее дата-время для ETL-процессов */
			
			/* CAS-библиотека и название соответственно для финальной выходной ABT. 
				Служит в качестве ABT для загрузки и построения short-term прогноза в SAS VF.
				Оба локальных параметра формируются на базе макро-параметра mpOutTableDmVf */
			lmvLibrefOut			
			lmvTabNmOut				
			
			/* CAS-библиотека и название соответственно для расширенной выходной ABT. 
				Требуется для восстановления сезонности в построенном прогнозе VF. 
				Оба локальных параметра формируются на базе макро-параметра mpOutTableDmABT */
			lmvLibrefOutABT			
			lmvTabNmOutABT			
			
			lmvInLib				/* Библиотека с входными данными */
	;
	
	%let lmvInLib = ETL_IA;
	
	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						);
	
	/* Подтягиваем данные из PROMOTOOL и обогащаем product_chain:
		- promo_enh
		- promo_prod_enh
		- promo_pbo_enh
		- media_enh
		- product_chain_enh 
	*/
	%add_promotool_marks2(mpOutCaslib=casuser,
							mpPtCaslib=pt);
							
	
	%member_names (mpTable=&mpOutTableDmVf, mpLibrefNameKey=lmvLibrefOut, mpMemberNameKey=lmvTabNmOut);
	%member_names (mpTable=&mpOutTableDmABT, mpLibrefNameKey=lmvLibrefOutABT, mpMemberNameKey=lmvTabNmOutABT);
	
	%let lmvMode = %upcase(&mpMode.);
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;

	%let lmvABTDepthYear = 3;	/* Глубина ABT - кол-во лет в обучающей выборке для VF от начала горизонта прогнозирования */
	%let lmvStartABTDt	 = '01jan2018'd;	

	/* Временной отрезок [lmvSeasonCalcStartDt ; lmvSeasonCalcEndDt] для расчета сезонностей DOW и WOY */
	%let lmvSeasonCalcStartDt = %str(date%')%sysfunc(putn(%sysfunc(intnx(day,'01JAN2018'd,0)), yymmdd10.))%str(%');
	%let lmvSeasonCalcEndDt   = %str(date%')%sysfunc(putn(%sysfunc(intnx(day,'15SEP2019'd,0)), yymmdd10.))%str(%');

	/* Временной отрезок [lmvCalcSeasDayOfYearStartDt ; lmvCalcSeasDayOfYearEndDt] для расчета сезонности DOY */
	/* Замечание: 
		Для сезонности дата-к-дате не рекомендуется брать менее 3-4 лет!
		Так как даты приходятся на разные дни недели в соседних годах!
		При этом в данных очень выраженная внутринедельная сезонность!
	*/
	%let lmvCalcSeasDayOfYearStartDt = '01JAN2014'd;
	%let lmvCalcSeasDayOfYearEndDt   = '07JAN2020'd;
	/* Количество дней в центральном окне для расчета сезонности DOY */
	%let lmvCalcSeasDayOfYearWindow  = 14; 

	/* Ограничение снизу на анализируемую историю */
/* 	%let lmvGCHistStartDt  = '01JAN2014'd; */
/* 	%let lmvPBOHistStartDt = '01JAN2014'd; */

	
	/* Включать ли некомповые рестораны в расчет сезонности {inner - исключать, left - включать } */
	%let lmvCompExcludeMode = inner;

	/* Вычисление вспомоготельных локальных параметров-дат на базе глобального макро-параметра ETL_CURRENT_DT.
		Параметр должен быть определен до вызова данного макроса. По умолчанию задан в конфигурационном скрипте initialize_global.sas  
	*/
	
	%let fcst_start_dt	= &ETL_CURRENT_DT.;								/* Дата начала горизонта прогнозирования */
	%let fcst_end_dt	= %sysfunc(intnx(day,&ETL_CURRENT_DT.,92));		/* Дата окончания горизонта прогнозирования */
	%let hist_end_dt	= %sysfunc(intnx(day,&ETL_CURRENT_DT.,-1));		/* Дата окончания истории фактических значений */

	%let fcst_start_dt_formatted = %str(date%')%sysfunc(putn(&ETL_CURRENT_DT., yymmdd10.))%str(%');								/* fcst_start_dt в формате date 'YYYY-MM-DD' */
	%let fcst_end_dt_formatted	 = %str(date%')%sysfunc(putn(%sysfunc(intnx(day,&ETL_CURRENT_DT.,92)), yymmdd10.))%str(%');		/* fcst_end_dt в формате date 'YYYY-MM-DD' */
	%let hist_end_dt_formatted	 = %str(date%')%sysfunc(putn(%sysfunc(intnx(day,&ETL_CURRENT_DT.,-1)), yymmdd10.))%str(%');		/* hist_end_dt в формате date 'YYYY-MM-DD' */

/************************************************************************************
 *	1.	Timeseries MA[7] 															*
 ************************************************************************************/
/*		Сглаживаем по 7 дням, чтобы убрать недельную сезонность.
 *		Это позволяет на уровне недели не ошибаться с плавающими праздниками.
 *		При тестировании использование сгалженного ряда давало наибольшую точность при восстановлении продаж
 */


/*	------------ Start GC mode. Сбор GC на уровне ресторан-день --------------------*/
	%if &lmvMode. = GC %then %do;
		PROC SQL noprint;
		   CREATE TABLE work.PBO_SALES AS 
		   SELECT t1.PBO_LOCATION_ID, 
				  t1.CHANNEL_CD, 
				  t1.RECEIPT_QTY, 
				  t1.SALES_DT
			  FROM ETL_IA.pbo_sales t1
			  where valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.		/* Срез данных из ETL_IA, актуальный на момент lmvReportDttm */
/* 				and t1.SALES_DT >= &lmvGCHistStartDt. */
		;
		QUIT;
		
		PROC SORT
			DATA=WORK.pbo_sales
			OUT=WORK.fact_hist_sorted														
			;
			BY PBO_LOCATION_ID CHANNEL_CD SALES_DT;
		RUN;
		/* KEEP=SALES_DT RECEIPT_QTY PBO_LOCATION_ID CHANNEL_CD */
	%end;
/* 	------------ End GC mode -------------------------------------------------------*/


/*	------------ Start PBO mode. Сбор суммарных UNITS на уровне ресторан-день ----- */
	%else %if &lmvMode. = PBO %then %do;
		PROC SQL noprint;
		   CREATE TABLE work.PMIX_PBO_AGGR AS 
		   SELECT t1.PBO_LOCATION_ID, 
				  t1.CHANNEL_CD, 
				  sum(sum(t1.SALES_QTY, t1.SALES_QTY_PROMO)) as RECEIPT_QTY, 
				  t1.SALES_DT
			  FROM ETL_IA.pmix_sales t1
			  where valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.		/* Срез данных из ETL_IA, актуальный на момент lmvReportDttm */
/* 				and t1.SALES_DT >= &lmvPBOHistStartDt. */
			  GROUP BY 
				  t1.CHANNEL_CD,
				  t1.PBO_LOCATION_ID,
				  t1.SALES_DT
		;
		QUIT;
		
		PROC SORT
			DATA=work.PMIX_PBO_AGGR(KEEP=SALES_DT RECEIPT_QTY PBO_LOCATION_ID CHANNEL_CD)
			OUT=WORK.FACT_HIST_SORTED
			;
			BY PBO_LOCATION_ID CHANNEL_CD SALES_DT;
		RUN;
	%end;
/* 	------------ End PBO mode -------------------------------------------------------*/


/*	------------ Start Режим учета сезонности 1-этапный -----------------------------*/
	
	%if &mpSeasonMode. = 1 %then %do;

		/* Добавляем те же поля в финальную таблицу шага, что и в 2-х этапной очистке */
		data WORK.FACT_HIST_SORTED_REBUILT;
			set WORK.FACT_HIST_SORTED;
			RECEIPT_QTY_SRC = RECEIPT_QTY;
			DOY = 1000 * month(sales_dt) + day(sales_dt);
			avg_koef = 1;
		run;

		/* Создаем болванку для сезонности дата-к-дате */
		/* Добавляется в расширенную ABT на последнем шаге */
		data CASUSER.TGT_AVG_TOTAL_MA7_KOEF_BY_DOY;
			attrib 
				DOY			length=8 	format=best12. 
				CHANNEL_CD 	length=$12 	format=$12.
				avg_koef 	length=8 	format=best12. 
				;
			stop;
		run;
	%end;
/* 	------------ End Режим учета сезонности 1-этапный --------------------------------*/

/*	------------ Start Режим учета сезонности 2-этапный -----------------------------*/
	%else %if &mpSeasonMode. = 2 %then %do;

	/*
	Пристрелочный алгоритм (изменения возможны на любом шаге 
	1. Берем GC с 1-го января 2014 по 7-е января 2020 и усредняем по всем магазинам 
		в разрезе до канала-день
	2. Сглаживаем ряд с центральным окном 7
	3. Делим средний трафик на сглаженный трафик. Получаем коэффициенты вокруг 1-цы.
	4. Усредняем коэффициенты по дню в году по всем годам отрезанной истории.
		DOY = sales_dt - intnx('year', sales_dt, 0, 'B') + 1; 
	5. Присоединяем коэффициенты по дню в году к исходной таблице и делим GC на эти коэф.
	*/

		/*	Номер дня в году */
		data CASUSER.FACT_HIST;
			set WORK.FACT_HIST_SORTED;
			DOY = 1000 * month(sales_dt) + day(sales_dt);	
		run;

		/* 1 */
		PROC SQL noprint;
			CREATE TABLE WORK.TGT_AVG_TOTAL AS 
			SELECT 
				  CHANNEL_CD
				/* , PBO_LOCATION_ID */
				, SALES_DT
				, avg(RECEIPT_QTY) as avg_tgt
			FROM WORK.FACT_HIST_SORTED
			/* Исследуемая глубина истории для сезонности "дата-к-дате" */
			where   SALES_DT >= &lmvCalcSeasDayOfYearStartDt.
				and SALES_DT <= &lmvCalcSeasDayOfYearEndDt.
			group by 1,2
			order by 1,2
		;
		QUIT;
		
		/* 2 */
		PROC EXPAND 
			DATA = WORK.TGT_AVG_TOTAL
			OUT	 = CASUSER.TGT_AVG_TOTAL_MA7
			ALIGN = BEGINNING
			METHOD = SPLINE(NOTAKNOT, NOTAKNOT) 
			OBSERVED = (BEGINNING, BEGINNING) 
			;
			BY /*PBO_LOCATION_ID*/ CHANNEL_CD;
			ID SALES_DT;
			CONVERT avg_tgt = new_avg_tgt / 
				TRANSFORMIN	= (CMOVAVE  &lmvCalcSeasDayOfYearWindow.)
				;
		RUN;
	
		/* 3 */
		data CASUSER.TGT_AVG_TOTAL_MA7_KOEF;
			set CASUSER.TGT_AVG_TOTAL_MA7;
			koef = avg_tgt / new_avg_tgt;
			WD = ifn(weekday(SALES_DT) = 1, 7, weekday(SALES_DT) - 1);
			DOY = 1000 * month(sales_dt) + day(sales_dt); 
		run;
		
		/* 4 */
		PROC FEDSQL sessref=casauto;
			CREATE TABLE CASUSER.TGT_AVG_TOTAL_MA7_KOEF_BY_DOY{options replace=true} AS 
			SELECT 
				  CHANNEL_CD
				, DOY
				, avg(koef) as avg_koef
			FROM CASUSER.TGT_AVG_TOTAL_MA7_KOEF 
			group by 1,2								
			;
		QUIT;
	
		/* 5 */
		PROC FEDSQL sessref=casauto;
			CREATE TABLE CASUSER.FACT_HIST_REBUILT{options replace=true} AS 
			SELECT src.CHANNEL_CD
				, src.PBO_LOCATION_ID
				, src.SALES_DT
				, src.DOY
				, koef.avg_koef
				, src.RECEIPT_QTY as RECEIPT_QTY_SRC
				, src.RECEIPT_QTY / koef.avg_koef as RECEIPT_QTY
			FROM CASUSER.FACT_HIST as src
			inner join CASUSER.TGT_AVG_TOTAL_MA7_KOEF_BY_DOY as koef
				on src.CHANNEL_CD = koef.CHANNEL_CD
				and src.DOY = koef.DOY	
			;
		QUIT;

		data WORK.FACT_HIST_SORTED_REBUILT;
			set CASUSER.FACT_HIST_REBUILT;
		run;
		proc sort data=WORK.FACT_HIST_SORTED_REBUILT;
			by PBO_LOCATION_ID CHANNEL_CD SALES_DT;
		run;

	%end;
/* 	------------ End Режим учета сезонности 2-этапный --------------------------------*/

	
/*	------------ Start MA[7]. Сглаживаем GC/UNITS в рамках недели ------------------ */
	PROC EXPAND DATA=WORK.FACT_HIST_SORTED_REBUILT
		OUT=casuser.MA7_TIMESERIES
		ALIGN = BEGINNING
		METHOD = SPLINE(NOTAKNOT, NOTAKNOT) 
		OBSERVED = (BEGINNING, BEGINNING) 
	;

		BY PBO_LOCATION_ID CHANNEL_CD;
		ID SALES_DT;
		CONVERT RECEIPT_QTY = new_RECEIPT_QTY / 
			TRANSFORMIN	= (CMOVAVE  7)
				
			;
	RUN;
/*	------------ End MA[7]. Сглаживаем GC/UNITS в рамках недели -------------------- */


/************************************************************************************
 *	2.	Генерируем табличку Channel-PBO-Date для прогноза							*
 ************************************************************************************/
/*		Для прогнозирования на будущее необходимо создать таблицу,
 *		в которой будут периоды истории и будущие значения:
 *			-	периоды истории в колонке прогнозируемой величины содержат факт прогнозируемой величины, 
 *			-	периоды прогноза в колонке прогнозируемой величины содержат пустые значения
 */
 

/*	------------ Start. Фильтруем продажи по концу истории и преобразуем дату ----- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.MA7_TIMESERIES_CMP{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.CHANNEL_CD, 
			  t1.new_RECEIPT_QTY, 
			  t1.RECEIPT_QTY, 
			  /* SALES_DT */
				(DATEPART(t1.SALES_DT)) AS SALES_DT
		  FROM casuser.MA7_TIMESERIES t1
		  WHERE DATEPART(t1.SALES_DT) <= &hist_end_dt_formatted.									
	;
	QUIT;
/*	------------ End. Фильтруем продажи по концу истории и преобразуем дату ----- */


/*	------------ Start. Генерируем даты на будущее -------------------------------- */
	data casuser.dates;
		do SALES_DT=&fcst_start_dt. to &fcst_end_dt.;
		new_RECEIPT_QTY = .;
		RECEIPT_QTY = .;
		output;
		end;
		format SALES_DT DDMMYYP.;
	run;
/*	------------ End. Генерируем даты на будущее ---------------------------------- */


/*	------------ Start. Генерируем уникальные пары CHANNEL-PBO -------------------- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.MA7_CMP_DISTINCT{options replace=true} AS 
	   SELECT DISTINCT t1.CHANNEL_CD, 
			  t1.PBO_LOCATION_ID
		  FROM casuser.MA7_TIMESERIES_CMP t1
	;
	QUIT;
/*	------------ End. Генерируем уникальные пары CHANNEL-PBO ---------------------- */


/*	------------ Start. Генерация таблицы, все пары CHANNEL-PBO на все даты прогноза*/
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE CASUSER.FUTURE_SKELETON{options replace=true} AS 
	   SELECT t1.CHANNEL_CD, 
			  t1.PBO_LOCATION_ID, 
			  t2.SALES_DT, 
			  t2.new_RECEIPT_QTY, 
			  t2.RECEIPT_QTY
		  FROM casuser.MA7_CMP_DISTINCT t1
			   CROSS JOIN casuser.DATES t2;
	QUIT;
/*	------------ End. Генерация таблицы - все пары CHANNEL-PBO на все даты прогноза */


/* 	------------ Start. Соединяем даты прогноза с историей ------------------------ */
	data casuser.SALES_FULL;
		set casuser.MA7_TIMESERIES_CMP casuser.FUTURE_SKELETON;
	run;
/* 	------------ End. Соединяем даты прогноза с историей -------------------------- */


/* 	------------ Start. Соединяем даты прогноза с историей ------------------------ */
	data casuser.SALES_FULL;
		set casuser.MA7_TIMESERIES_CMP casuser.FUTURE_SKELETON;
	run;
/* 	------------ End. Соединяем даты прогноза с историей -------------------------- */


/* ------------ Start. Подтягиваем справочник ПБО (с датами открытия и закрытия)  - */
	data CASUSER.PBO_LOCATION (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..pbo_location(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.PBO_LOC_HIERARCHY (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PBO_LOC_HIERARCHY(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	data CASUSER.PBO_LOC_ATTRIBUTES (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PBO_LOC_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc cas;
	transpose.transpose /
	   table={name="PBO_LOC_ATTRIBUTES", caslib="casuser", groupby={"pbo_location_id"}} 
	   attributes={{name="pbo_location_id"}} 
	   transpose={"PBO_LOC_ATTR_VALUE"} 
	   prefix=""
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
			(select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=4) as t1
			left join 
			(select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
			;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.pbo_dictionary{options replace=true} as
		select 
			t2.pbo_location_id,
			coalesce(t2.lvl3_id,-999) as lvl3_id,
			coalesce(t2.lvl2_id,-99) as lvl2_id,
			cast(1 as double) as lvl1_id,
			coalesce(t14.pbo_location_nm,'NA') as pbo_location_nm,
			coalesce(t13.pbo_location_nm,'NA') as lvl3_nm,
			coalesce(t12.pbo_location_nm,'NA') as lvl2_nm,
			cast(inputn(t3.OPEN_DATE,'ddmmyy10.') as date) as OPEN_DATE,
			cast(inputn(t3.CLOSE_DATE,'ddmmyy10.') as date) as CLOSE_DATE,
			t3.PRICE_LEVEL,
			t3.DELIVERY,
			t3.AGREEMENT_TYPE,
			t3.BREAKFAST,
			t3.BUILDING_TYPE,
			t3.COMPANY,
			t3.DRIVE_THRU,
			t3.MCCAFE_TYPE,
			t3.WINDOW_TYPE
		from 
			casuser.pbo_hier_flat t2
		left join 
			casuser.attr_transposed t3
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
			t2.lvl2_id=t12.pbo_location_id
		;
	quit;

	/* Создаем таблицу связывающую PBO на листовом уровне и на любом другом */
	proc fedsql sessref=casauto;
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

	/* Соединяем в единый справочник ПБО. Необходимо для связки с промо-таблицами далее */
	data casuser.pbo_lvl_all;
		set casuser.lvl4 casuser.lvl3 casuser.lvl2 casuser.lvl1;
	run;

/* ------------ End. Подтягиваем справочник ПБО (с датами открытия и закрытия) ---- */


/* 	------------ Start. Формируем список комповых ресторанов по отношению к последней дате горизонта --- */
	
	/* Оставляем для расчета сезонности только те ПБО, которые являются сопоставимыми для последней даты горизонта прогнозирования.
		Особенности расчета COMP в данному случае:
		- Проверяется COMP только по дате открытия (дата закрытия учитывается на этапе post-processing полученных прогнозов)
		- COMP рассчитывается по дням, глубина для наличия истории - 365 дней
		- Данное условие - слабое, то есть, если ПБО является COMP для последней даты горизонта прогнозирования, 
				то данный ПБО будет COMP и для любой даты внутри горизонта прогнозирования.
	*/
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_COMP_LIST_FOR_TGT_PERIOD{options replace=true} as
		select 
			pbo_location_id
		from 
			casuser.pbo_dictionary
		where 
			intnx('day', &fcst_end_dt., -365, 'b') >= OPEN_DATE
		;
	quit;

/* 	------------ End. Формируем список комповых ресторанов по отношению к последней дате горизонта --- */


/************************************************************************************
 *	3.	Рассчитываем тренды и сезонности											*
 ************************************************************************************/
/*			В используемом подходе прогнозирования GC PBO/ UNITS PBO прогнозируется
 *		обессезоненная величина. Для восстановления итоговой величины сезонность 
 *		накладывается назад после прогноза.
 *			Для того, чтобы рассчитать сезонность необходимо определить тренд, вычесть 
 *		его и по соответствующим дням за несколько лет рассчитать сезонность.
 *			В случае, если истории продаж конкретного ресторана недостаточно для 
 *		расчета сезонности, она наследуются с уровней выше.
 *			Сезонность декабря рассчитывается с конца года. Это связано с разным количеством
 *		недель в году в разные года. 
 */

/* 	------------ Start. Добавляем характеристики даты (WOY, DOY, ...) ------------- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE CASUSER.SALES_WITH_WOY_DOW{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.CHANNEL_CD, 
			  t1.new_RECEIPT_QTY, 
			  t1.RECEIPT_QTY, 
			  t1.SALES_DT, 
			  /* WOY */
				(week(t1.SALES_DT, 'w')) AS WOY, 
			  /* DOW */
				(case when weekday(t1.SALES_DT) = 1 then 7 else weekday(t1.SALES_DT) - 1 end) AS DOW, 
			  /* WBY_TEMP */
				(week(MDY(12, 31, YEAR(t1.SALES_DT)), 'w') - (week(t1.SALES_DT, 'w')) + 1) AS WBY_TEMP, 
			  /* EOY */
				(intnx('year', t1.SALES_DT, 0, 'e')) AS EOY, 
			  /* WBY */
				(intck('week.2', t1.SALES_DT, Intnx('year', t1.SALES_DT, 0, 'e'), 'continuous') + 1) AS WBY, 
			  /* LWY */
				(week(MDY(12, 31, YEAR(t1.SALES_DT)), 'w')) AS LWY
		  FROM casuser.SALES_FULL t1
	;
	QUIT;
/* 	------------ End. Добавляем характеристики даты (WOY, DOY, ...) --------------- */


/* 	------------ Start. Фильтруем период до COVID-19 в России со стаб. продажами -- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE CASUSER.PRE_COVID_WOY_DOY{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.CHANNEL_CD, 
			  t1.new_RECEIPT_QTY AS new_RECEIPT_QTY_weekly, 
			  t1.RECEIPT_QTY, 
			  t1.SALES_DT, 
			  t1.WOY, 
			  t1.DOW, 
			  t1.WBY
		  FROM casuser.SALES_WITH_WOY_DOW t1
		  WHERE t1.SALES_DT < date '2020-03-01';
	QUIT;
/* 	------------ End. Фильтруем период до COVID-19 в России со стаб. продажами ---- */


/* 	------------ Start. Считаем тренд на уровне CHANNEL-PBO как MA[364] ----------- */
	PROC SORT
		DATA=casuser.PRE_COVID_WOY_DOY(KEEP=SALES_DT RECEIPT_QTY CHANNEL_CD PBO_LOCATION_ID)
		OUT=WORK.PRE_COVID_WOY_DOY_SORTED
		;
		BY CHANNEL_CD PBO_LOCATION_ID SALES_DT;
	RUN;

	PROC EXPAND DATA=WORK.PRE_COVID_WOY_DOY_SORTED
		OUT=CASUSER.MA364_TIMESERIES(LABEL="Modified Time Series data for casuser.PRE_COVID_WOY_DOY")
		ALIGN = BEGINNING
		METHOD = SPLINE(NOTAKNOT, NOTAKNOT) 
		OBSERVED = (BEGINNING, BEGINNING) 
	;
		BY CHANNEL_CD PBO_LOCATION_ID;
		ID SALES_DT;
		CONVERT RECEIPT_QTY = new_RECEIPT_QTY / 
			TRANSFORMIN	= (CMOVAVE  364)
			;
	RUN;
/* 	------------ End. Считаем тренд на уровне CHANNEL-PBO как MA[364] ------------- */


/* 	------------ Start. Добавляем тренд к таблице для прогноза с фактом ----------- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.MA364_WITH_WOY_DOY{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.CHANNEL_CD, 
			  t1.new_RECEIPT_QTY_weekly, 
			  t1.RECEIPT_QTY, 
			  t1.SALES_DT, 
			  t1.WOY, 
			  t1.DOW, 
			  t1.WBY, 
			  t2.new_RECEIPT_QTY AS new_RECEIPT_QTY_yearly
		  FROM casuser.PRE_COVID_WOY_DOY t1, casuser.MA364_TIMESERIES t2
		  WHERE (t1.CHANNEL_CD = t2.CHANNEL_CD AND t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID AND t1.SALES_DT = t2.SALES_DT);
	QUIT;
/* 	------------ End. Добавляем тренд к таблице для прогноза с фактом ------------- */


/* 	------------ Start. Вычитаем тренд из [сглаженного MA[7]] факта --------------- */

	/*Также фильтруем ПБО и даты, которые участвуют в расчете сезонности */

	proc fedsql sessref=casauto;
		create table CASUSER.MA364_DETREND_DESEASON{options replace=true} as 
		select 
			  main.pbo_location_id
			, main.channel_cd
			, main.new_receipt_qty_weekly
			, main.receipt_qty
			, main.sales_dt
			, main.woy
			, main.dow
			, main.wby
			, main.new_receipt_qty_yearly
		      /* detrend_multi */
			, (main.receipt_qty / main.new_receipt_qty_yearly) as detrend_multi 
			  /* detrend_sm_multi */
			, (main.new_receipt_qty_weekly / main.new_receipt_qty_yearly) as detrend_sm_multi 
		from CASUSER.MA364_WITH_WOY_DOY as main
	
		&lmvCompExcludeMode. join CASUSER.PBO_COMP_LIST_FOR_TGT_PERIOD as comp
			on main.pbo_location_id = comp.pbo_location_id
		
		/* интервал данных, используемых для рассчета сезонности - задается в начале макроса*/
		where main.sales_dt between &lmvSeasonCalcStartDt. and &lmvSeasonCalcEndDt. 
		;				
	quit;
/* 	------------ End. Вычитаем тренд из [сглаженного MA[7]] факта ----------------- */


/* ------------ Start. Считаем сезонность WOY, DOW как среднее за несколько лет --- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.MA364_DETREND_DESEASON_AVG_WOY{options replace=true} AS 
	   SELECT t1.CHANNEL_CD, 
			  t1.PBO_LOCATION_ID, 
			  t1.WOY, 
			  t1.DOW, 
			  /* AVG_of_Detrend_sm_multi */
				(AVG(t1.Detrend_sm_multi)) AS AVG_of_Detrend_sm_multi, 
			  /* AVG_of_Detrend_multi */
				(AVG(t1.Detrend_multi)) AS AVG_of_Detrend_multi
		  FROM casuser.MA364_DETREND_DESEASON t1
		  GROUP BY t1.CHANNEL_CD,
				   t1.PBO_LOCATION_ID,
				   t1.WOY,
				   t1.DOW;
	QUIT;
/* ------------ End. Считаем сезонность WOY, DOW как среднее за несколько лет ----- */


/* ------------ Start. Считаем сезонность WBY, DOW как среднее за несколько лет --- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.MA364_DETREND_DESEASON_AVG_WBY{options replace=true} AS 
	   SELECT t1.CHANNEL_CD, 
			  t1.PBO_LOCATION_ID, 
			  t1.WBY, 
			  t1.DOW, 
			  /* AVG_of_Detrend_sm_multi */
				(AVG(t1.Detrend_sm_multi)) AS AVG_of_Detrend_sm_multi, 
			  /* AVG_of_Detrend_multi */
				(AVG(t1.Detrend_multi)) AS AVG_of_Detrend_multi 
		  FROM casuser.MA364_DETREND_DESEASON t1
		  GROUP BY t1.CHANNEL_CD,
				   t1.PBO_LOCATION_ID,
				   t1.WBY,
				   t1.DOW;
	QUIT;
/* ------------ End. Считаем сезонность WBY, DOW как среднее за несколько лет ----- */


/* ------------ Start. Подтягиваем сезонности к актуальным данным ----------------- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.MA364_DETREND_DESEASON_JOINT{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.CHANNEL_CD, 
			  t1.new_RECEIPT_QTY, 
			  t1.RECEIPT_QTY, 
			  t1.SALES_DT, 
			  (INTNX('week.2', t1.SALES_DT, 0, 'b')) AS SALES_WK,
			  cast( (1000 * month(t1.sales_dt) + day(t1.sales_dt)) as integer) as DOY,
			  t1.WOY, 
			  t1.DOW, 
			  t1.WBY, 
			  t2.AVG_of_Detrend_sm_multi, 
			  t2.AVG_of_Detrend_multi, 
			  t3.AVG_of_Detrend_sm_multi AS AVG_of_Detrend_sm_multi_WBY, 
			  t3.AVG_of_Detrend_multi AS AVG_of_Detrend_multi_WBY
		  FROM casuser.SALES_WITH_WOY_DOW t1
			   LEFT JOIN casuser.MA364_DETREND_DESEASON_AVG_WOY t2 ON (t1.CHANNEL_CD = t2.CHANNEL_CD) AND 
			  (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID) AND (t1.WOY = t2.WOY) AND (t1.DOW = t2.DOW)
			   LEFT JOIN casuser.MA364_DETREND_DESEASON_AVG_WBY t3 ON (t1.CHANNEL_CD = t3.CHANNEL_CD) AND 
			  (t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID) AND (t1.DOW = t3.DOW) AND (t1.WBY = t3.WBY)
	;
	QUIT;
/* ------------ Start. Подтягиваем сезонности к актуальным данным ----------------- */


/************************************************************************************
 *	4.	Наследуем сезонность, туда, где она не рассчиталась							*
 ************************************************************************************/
/*			У некоторых ресторанов, дамже если они комповые, может не хватать исторических
 *		данных для расчета сезонности. В этом случае сезонность считается на уровнях выше и
 *		наследуется на данные рестораны.
 */


/* ------------ Start. Смотрим CHANNEL-PBO-DATE, где не рассчиталась сезонность --- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.MA364_DETREND_DESEASON_JOINTCLR{options replace=true} AS 
	   SELECT t1.CHANNEL_CD, 
			  t1.SALES_DT, 
			  t1.PBO_LOCATION_ID, 
			  t1.WOY, 
			  t1.DOW, 
			  t1.WBY, 
			  t1.AVG_of_Detrend_sm_multi, 
			  t1.AVG_of_Detrend_multi
		  FROM casuser.MA364_DETREND_DESEASON_JOINT t1
		  WHERE t1.AVG_of_Detrend_sm_multi ^= .;
	QUIT;
/* ------------ End. Смотрим CHANNEL-PBO-DATE, где не рассчиталась сезонность ----- */

	
/* ------------ Start. Считаем сезонность WBY, DOW на уровне Region-Building Type - */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.REGION_BT_SEASONALITY_WBY{options replace=true} AS 
	   SELECT t2.CHANNEL_CD, 
			  t1.LVL2_ID AS Region, 
			  t1.BUILDING_TYPE, 
			  t2.WBY, 
			  t2.DOW, 
			  /* AVG_of_AVG_of_Detrend_sm_multi */
				(AVG(t2.AVG_of_Detrend_sm_multi)) AS AVG_of_AVG_of_Detrend_sm_multi, 
			  /* AVG_of_AVG_of_Detrend_multi */
				(AVG(t2.AVG_of_Detrend_multi)) AS AVG_of_AVG_of_Detrend_multi
		  FROM casuser.pbo_dictionary t1
			   INNER JOIN casuser.MA364_DETREND_DESEASON_JOINTCLR t2 ON (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID)
		  GROUP BY t2.CHANNEL_CD,
				   t1.LVL2_ID,
				   t1.BUILDING_TYPE,
				   t2.WBY,
				   t2.DOW;
	QUIT;
/* ------------ End. Считаем сезонность на уровне Region-Building Type ------------ */


/* ------------ Start. Считаем сезонность WOY, DOW на уровне Region-Building Type - */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.REGION_BT_SEASONALITY_WOY{options replace=true} AS 
	   SELECT t2.CHANNEL_CD, 
			  t1.LVL2_ID AS Region, 
			  t1.BUILDING_TYPE, 
			  t2.WOY, 
			  t2.DOW, 
			  /* AVG_of_AVG_of_Detrend_sm_multi */
				(AVG(t2.AVG_of_Detrend_sm_multi)) AS AVG_of_AVG_of_Detrend_sm_multi, 
			  /* AVG_of_AVG_of_Detrend_multi */
				(AVG(t2.AVG_of_Detrend_multi)) AS AVG_of_AVG_of_Detrend_multi
		  FROM casuser.pbo_dictionary t1
			   INNER JOIN casuser.MA364_DETREND_DESEASON_JOINTCLR t2 ON (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID)
		  GROUP BY t2.CHANNEL_CD,
				   t1.LVL2_ID,
				   t1.BUILDING_TYPE,
				   t2.WOY,
				   t2.DOW;
	QUIT;
/* ------------ End. Считаем сезонность WOY, DOW на уровне Region-Building Type --- */


/* ------------ Start. Считаем сезонность WOY, DOW на уровне Building Type -------- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.BT_SEASONALITY_WOY{options replace=true} AS 
	   SELECT t2.CHANNEL_CD, 
			  t1.BUILDING_TYPE, 
			  t2.WOY, 
			  t2.DOW, 
			  /* AVG_of_AVG_of_Detrend_sm_multi */
				(AVG(t2.AVG_of_Detrend_sm_multi)) AS AVG_of_AVG_of_Detrend_sm_multi, 
			  /* AVG_of_AVG_of_Detrend_multi */
				(AVG(t2.AVG_of_Detrend_multi)) AS AVG_of_AVG_of_Detrend_multi
		  FROM casuser.pbo_dictionary t1
			   INNER JOIN casuser.MA364_DETREND_DESEASON_JOINTCLR t2 ON (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID)
		  GROUP BY t2.CHANNEL_CD,
				   t1.BUILDING_TYPE,
				   t2.WOY,
				   t2.DOW;
	QUIT;
/* ------------ End. Считаем сезонность WOY, DOW на уровне Building Type ---------- */


/* ------------ Start. Считаем сезонность WBY, DOW на уровне Building Type -------- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.BT_SEASONALITY_WBY{options replace=true} AS 
	   SELECT t2.CHANNEL_CD, 
			  t1.BUILDING_TYPE, 
			  t2.WBY, 
			  t2.DOW, 
			  /* AVG_of_AVG_of_Detrend_sm_multi */
				(AVG(t2.AVG_of_Detrend_sm_multi)) AS AVG_of_AVG_of_Detrend_sm_multi, 
			  /* AVG_of_AVG_of_Detrend_multi */
				(AVG(t2.AVG_of_Detrend_multi)) AS AVG_of_AVG_of_Detrend_multi
		  FROM casuser.pbo_dictionary t1
			   INNER JOIN casuser.MA364_DETREND_DESEASON_JOINTCLR t2 ON (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID)
		  GROUP BY t2.CHANNEL_CD,
				   t1.BUILDING_TYPE,
				   t2.WBY,
				   t2.DOW;
	QUIT;
/* ------------ End. Считаем сезонность WBY, DOW на уровне Building Type ---------- */


/* ------------ Start. Распространяем сезонность WBY, DOW, Region-Building Type --- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.REST_SEAS_REGION_BT_WBY{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t2.CHANNEL_CD, 
			  t2.Region, 
			  t2.BUILDING_TYPE, 
			  t2.WBY, 
			  t2.DOW, 
			  t2.AVG_of_AVG_of_Detrend_sm_multi, 
			  t2.AVG_of_AVG_of_Detrend_multi
		  FROM casuser.pbo_dictionary t1, casuser.REGION_BT_SEASONALITY_WBY t2
		  WHERE (t1.BUILDING_TYPE = t2.BUILDING_TYPE AND t1.LVL2_ID = t2.Region);
	QUIT;
/* ------------ End. Распространяем сезонность WBY, DOW, Region-Building Type ----- */


/* ------------ Start. Распространяем сезонность WOY, DOW, Region-Building Type --- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.REST_SEAS_REGION_BT_WOY{options replace=true} AS 
	   SELECT t2.CHANNEL_CD, 
	          t2.Region, 
			  t2.BUILDING_TYPE, 
			  t1.PBO_LOCATION_ID, 
			  t2.WOY, 
			  t2.DOW, 
			  t2.AVG_of_AVG_of_Detrend_sm_multi, 
			  t2.AVG_of_AVG_of_Detrend_multi
		  FROM casuser.pbo_dictionary t1, casuser.REGION_BT_SEASONALITY_WOY t2
		  WHERE (t1.BUILDING_TYPE = t2.BUILDING_TYPE AND t1.LVL2_ID = t2.Region)
	;
	QUIT;
/* ------------ End. Распространяем сезонность WOY, DOW, Region-Building Type ----- */


/* ------------ Start. Распространяем сезонность WBY, DOW, Building Type ---------- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.REST_SEAS_BT_WBY{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t2.CHANNEL_CD, 
	/*           t2.Region,  */
			  t2.BUILDING_TYPE, 
			  t2.WBY, 
			  t2.DOW, 
			  t2.AVG_of_AVG_of_Detrend_sm_multi, 
			  t2.AVG_of_AVG_of_Detrend_multi
		  FROM casuser.pbo_dictionary t1, casuser.BT_SEASONALITY_WBY t2
		  WHERE (t1.BUILDING_TYPE = t2.BUILDING_TYPE);
	QUIT;
/* ------------ End. Распространяем сезонность WBY, DOW, Building Type ------------ */


/* ------------ Start. Распространяем сезонность WOY, DOW, Building Type ---------- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.REST_SEAS_BT_WOY{options replace=true} AS 
	   SELECT t2.CHANNEL_CD, 
/* 			  t2.Region,  */
			  t2.BUILDING_TYPE,
			  t1.PBO_LOCATION_ID, 
			  t2.WOY, 
			  t2.DOW, 
			  t2.AVG_of_AVG_of_Detrend_sm_multi, 
			  t2.AVG_of_AVG_of_Detrend_multi 
		  FROM casuser.pbo_dictionary t1, casuser.BT_SEASONALITY_WOY t2
		  WHERE (t1.BUILDING_TYPE = t2.BUILDING_TYPE)
	;
	QUIT;
/* ------------ End. Распространяем сезонность WOY, DOW, Building Type ------------ */


/************************************************************************************
 *	5.	Загружаем дополнительные фичи							*
 ************************************************************************************/
/*			Для прогноза обессезонного спроса (GC PBO, UNITS PBO) используются 
 *		дополнительные переменные. Например:
 *			-	COVID-19 Pattern (рассчитанный вручную)
 *			-	суммарный TRP
 *			-	...
 */


/* ------------ Start. Загружаем паттерн COVID-19 (падения продаж из-за Lockdowns)  */
	FILENAME REFFILE DISK '/data/files/input/mcd_covid_pattern_day.csv';

	PROC IMPORT DATAFILE=REFFILE
		DBMS=CSV
		OUT=WORK.MCD_COVID_PATTERN_DAY;
		GETNAMES=YES;
	RUN;
	
	proc casutil;
	  load data=WORK.MCD_COVID_PATTERN_DAY casout='MCD_COVID_PATTERN_DAY' outcaslib='casuser' replace;
	run;
/* ------------ End. Загружаем паттерн COVID-19 (падения продаж из-за Lockdowns) -- */


/* ------------ Start. TRP. Соединяем промо с листами ----------------------------- */
/* Описать словами логику сборки TRP в комментарии*/
	proc fedsql sessref = casauto;
		create table casuser.promo_x_pbo_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t2.PBO_LEAF_ID
			from
				casuser.promo_pbo_enh as t1,
				casuser.pbo_lvl_all as t2
			where t1.pbo_location_id = t2.PBO_LOCATION_ID
		;
	quit;
/* ------------ End. TRP. Соединяем промо с ресторанами --------------------------- */


/* ------------ Start. TRP. Соединяем промо с листам. Шаг 2 ----------------------- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.PROMO_GROUP_PBO{options replace=true} AS 
	   SELECT DISTINCT
	/* 		  t1.CHANNEL_CD,  */
			  t2.PBO_LEAF_ID as PBO_LOCATION_ID, 
			  t1.PROMO_GROUP_ID,
			  datepart(t1.START_DT) as START_DT,
			  datepart(t1.END_DT) as END_DT,
			  weekday(datepart(t1.start_dt))
		  FROM casuser.promo_enh t1
		  INNER JOIN casuser.promo_x_pbo_leaf t2
		  ON t1.PROMO_ID = t2.PROMO_ID
	;
	QUIT;
/* ------------ End. TRP. Соединяем промо с листам. Шаг 2 ------------------------- */


/* ------------ Start. TRP. Подготавливаем таблицу MEDIA -------------------------- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.TRP{options replace=true} AS 
	   SELECT t1.PROMO_GROUP_ID, 
			  t1.REPORT_DT AS REPORT_DT, 
			  t1.TRP, 
			  DATEPART(t1.REPORT_DT) AS REPORT_WK
		  FROM casuser.MEDIA_ENH t1
	;
	QUIT;
/* ------------ End. TRP. Подготавливаем таблицу MEDIA ---------------------------- */


/* ------------ Start. TRP. Соединяем TRP c промо --------------------------------- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.TRP_PBO{options replace=true} AS 
	   SELECT t1.PROMO_GROUP_ID, 
			  t1.REPORT_DT, 
			  t1.TRP, 
			  t1.REPORT_WK, 
			  t2.PROMO_GROUP_ID AS PROMO_GROUP_ID1, 
			  t2.PBO_LOCATION_ID, 
	/*           t2.CHANNEL_CD,  */
			  t2.START_DT, 
			  t2.END_DT
		  FROM casuser.TRP t1
		  LEFT JOIN casuser.PROMO_GROUP_PBO t2 
		  ON (t1.PROMO_GROUP_ID = t2.PROMO_GROUP_ID) 
			AND (t1.REPORT_DT >= t2.START_DT) 
			AND (t1.REPORT_DT <= t2.END_DT)
	;
	QUIT;
/* ------------ End. TRP. Соединяем TRP c промо ----------------------------------- */


/* ------------ Start. TRP. Суммируем TRP по всем промо --------------------------- */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.TRP_PBO_SUM{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.REPORT_WK, 
			  t1.REPORT_DT, 
			  SUM(t1.TRP) AS SUM_TRP
		  FROM casuser.TRP_PBO t1
		  GROUP BY t1.PBO_LOCATION_ID,
				   t1.REPORT_WK,
				   t1.REPORT_DT
	;
	QUIT;
/* ------------ End. TRP. Суммируем TRP по всем промо ----------------------------- */


/************************************************************************************
 *	6.	Собираем финальную витрину							*
 ************************************************************************************/
/*			Из рассчитанных сезонностей, внешних факторов (TRP, COVID-19 и пр.)
 *		собираем финальную витрину для прогноза:
 *			-	Обессезониваем продажи с учетом всех видов сезонности
 *			- 	Добавляем внешние фичи
 *			-	...
 */

/* ------------ Start. Фичи + актуальные продажи + вычистка сезонности -------------*/
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.ABT_EXTENDED {options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.CHANNEL_CD, 
			  t1.new_RECEIPT_QTY, 
			  t1.RECEIPT_QTY, 
			  t1.SALES_DT, 
			  t1.WOY, 
			  t1.WBY, 
			  t1.DOW,
			  
				/* start new fields */
				t1.DOY, 
				coalesce(koef.avg_koef, 1) as avg_koef_DOY,
				/* end new fields */

			  (LOG(t7.SUM_TRP)) AS SUM_TRP_LOG,
			  /* COVID_pattern */
				(COALESCE(t2.COVID_pattern, 0)) AS COVID_pattern, 
			  /* COVID_lockdown */
				(CASE  
				   WHEN t2.COVID_pattern ^= .
				   THEN 1
				   ELSE 0
				END) AS COVID_lockdown, 
			  /* COVID_level */
				(CASE  
				   WHEN t1.SALES_DT >= date '2020-03-16'
				   THEN 1
				   ELSE 0
				END) AS COVID_level, 

			  /* AVG_of_Detrend_sm_multi */
				(COALESCE(t1.AVG_of_Detrend_sm_multi, t3.AVG_of_AVG_of_Detrend_sm_multi, t5.AVG_of_AVG_of_Detrend_sm_multi)) AS AVG_of_Detrend_sm_multi, 
			  /* AVG_of_Detrend_multi */
				(COALESCE(t1.AVG_of_Detrend_multi, t3.AVG_of_AVG_of_Detrend_multi, t5.AVG_of_AVG_of_Detrend_multi)) AS AVG_of_Detrend_multi, 
			  /* AVG_of_Detrend_sm_multi_WBY */
				(COALESCE(t1.AVG_of_Detrend_sm_multi_WBY, t4.AVG_of_AVG_of_Detrend_sm_multi, t6.AVG_of_AVG_of_Detrend_sm_multi)) AS AVG_of_Detrend_sm_multi_WBY, 
			  /* AVG_of_Detrend_multi_WBY */
				(COALESCE(t1.AVG_of_Detrend_multi_WBY, t4.AVG_of_AVG_of_Detrend_multi, t6.AVG_of_AVG_of_Detrend_multi)) AS AVG_of_Detrend_multi_WBY, 

			  /* Detrend_sm_multi */
				(CASE  
				   WHEN MONTH(t1.SALES_DT) = 12
				   THEN (COALESCE(t1.AVG_of_Detrend_sm_multi_WBY, t4.AVG_of_AVG_of_Detrend_sm_multi, t6.AVG_of_AVG_of_Detrend_sm_multi))
				   ELSE (COALESCE(t1.AVG_of_Detrend_sm_multi, t3.AVG_of_AVG_of_Detrend_sm_multi, t5.AVG_of_AVG_of_Detrend_sm_multi))
				END) AS Detrend_sm_multi, 
			  /* Detrend_multi */
				(CASE  
				   WHEN MONTH(t1.SALES_DT) = 12
				   THEN (COALESCE(t1.AVG_of_Detrend_multi_WBY, t4.AVG_of_AVG_of_Detrend_multi, t6.AVG_of_AVG_of_Detrend_multi))
				   ELSE (COALESCE(t1.AVG_of_Detrend_multi, t3.AVG_of_AVG_of_Detrend_multi, t5.AVG_of_AVG_of_Detrend_multi))
				END) AS Detrend_multi, 
			  /* Deseason_multi */
				(t1.RECEIPT_QTY / (CASE  
				   WHEN MONTH(t1.SALES_DT) = 12
				   THEN (COALESCE(t1.AVG_of_Detrend_multi_WBY, t4.AVG_of_AVG_of_Detrend_multi, t6.AVG_of_AVG_of_Detrend_multi))
				   ELSE (COALESCE(t1.AVG_of_Detrend_multi, t3.AVG_of_AVG_of_Detrend_multi, t5.AVG_of_AVG_of_Detrend_multi))
				END)) AS Deseason_multi, 
			  /* Deseason_sm_multi */
				(t1.new_RECEIPT_QTY / (CASE  
				   WHEN MONTH(t1.SALES_DT) = 12
				   THEN (COALESCE(t1.AVG_of_Detrend_sm_multi_WBY, t4.AVG_of_AVG_of_Detrend_sm_multi, t6.AVG_of_AVG_of_Detrend_sm_multi))
				   ELSE (COALESCE(t1.AVG_of_Detrend_sm_multi, t3.AVG_of_AVG_of_Detrend_sm_multi, t5.AVG_of_AVG_of_Detrend_sm_multi))
				END)) AS Deseason_sm_multi
		FROM 
			casuser.MA364_DETREND_DESEASON_JOINT t1
		/* Добавляем COVID-pattern и TRP */
		LEFT JOIN 
			casuser.MCD_COVID_PATTERN_DAY t2 
			ON  (t1.CHANNEL_CD = t2.CHANNEL_CD) 
			AND (t1.SALES_DT = t2.SALES_DT)
		LEFT JOIN 
			casuser.TRP_PBO_SUM t7 
			ON  (t1.PBO_LOCATION_ID = t7.PBO_LOCATION_ID) 
			AND (t1.SALES_WK = t7.REPORT_WK)
		/* Добавляем коэффициенты сезонности в разных разрезах */
		LEFT JOIN 
			casuser.REST_SEAS_BT_WOY t3 
			ON  (t1.CHANNEL_CD = t3.CHANNEL_CD) 
			AND (t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID) 
			AND (t1.WOY = t3.WOY) 
			AND (t1.DOW = t3.DOW)
		LEFT JOIN 
			casuser.REST_SEAS_BT_WBY t4 
			ON  (t1.PBO_LOCATION_ID = t4.PBO_LOCATION_ID) 
			AND (t1.CHANNEL_CD = t4.CHANNEL_CD) 
			AND (t1.WBY = t4.WBY) 
			AND (t1.DOW = t4.DOW)
		LEFT JOIN 
			casuser.REST_SEAS_REGION_BT_WOY t5 
			ON  (t1.PBO_LOCATION_ID = t5.PBO_LOCATION_ID) 
			AND (t1.CHANNEL_CD = t5.CHANNEL_CD) 
			AND (t1.WOY = t5.WOY) 
			AND (t1.DOW = t5.DOW)
		LEFT JOIN 
			casuser.REST_SEAS_REGION_BT_WBY t6 
			ON  (t1.PBO_LOCATION_ID = t6.PBO_LOCATION_ID) 
			AND (t1.CHANNEL_CD = t6.CHANNEL_CD) 
			AND (t1.WBY = t6.WBY) 
			AND (t1.DOW = t6.DOW)

		LEFT JOIN 
			CASUSER.TGT_AVG_TOTAL_MA7_KOEF_BY_DOY as koef
			on t1.CHANNEL_CD = koef.CHANNEL_CD
			and t1.DOY = koef.DOY	

		;
	QUIT;

/* ------------ End. Фичи + актуальные продажи + вычистка сезонности ---------------*/


	proc casutil;
		droptable incaslib='casuser' casdata='ABT_FOR_VF' quiet;
	run;


/* ------------ Start. Сборка витрины для VF, фильтрация ALL CHANNEL ---------------*/
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.ABT_FOR_VF{options replace=true} AS 
	   SELECT t1.CHANNEL_CD, 
			  t1.PBO_LOCATION_ID, 
			  t1.SALES_DT, 
			  t1.COVID_pattern, 
			  t1.COVID_lockdown, 
			  t1.COVID_level, 
			  t1.SUM_TRP_LOG,
			  /* Target */
				(CASE  
				   WHEN t1.SALES_DT >= &fcst_start_dt_formatted.
				   THEN .
				   ELSE t1.Deseason_sm_multi
				END) AS Target
		  FROM casuser.ABT_EXTENDED t1
		  WHERE t1.CHANNEL_CD = 'ALL';
	QUIT;
/* ------------ End. Сборка витрины для VF, фильтрация ALL CHANNEL -------------------------------*/


/* ------------ Start. Фильтрация финальной ABT для VF -------------------------------------------*/
/* 	DATA casuser.ABT_FOR_VF(replace=yes); */
/* 		set casuser.ABT_FOR_VF(where=(sales_dt>=intnx('year', &fcst_start_dt., - &lmvABTDepthYear., 's'))); */
/* 		format sales_dt date9.; */
/* 	RUN; */

	DATA casuser.ABT_FOR_VF(replace=yes);
		set casuser.ABT_FOR_VF(where=(sales_dt>=intnx('day', &lmvStartABTDt., 0, 'b')));
		format sales_dt date9.;
	RUN;

/* ------------ End. Фильтрация финальной ABT для VF ---------------------------------------------*/
	

/* ------------ Start. Фильтрация расширенной ABT  -----------------------------------------------*/
/* 	DATA casuser.ABT_EXTENDED(replace=yes); */
/* 		set casuser.ABT_EXTENDED(where=(sales_dt>=intnx('year', &fcst_start_dt., - &lmvABTDepthYear., 's'))); */
/* 		format sales_dt date9.; */
/* 	RUN; */

	DATA casuser.ABT_EXTENDED(replace=yes);
		set casuser.ABT_EXTENDED(where=(sales_dt>=intnx('day', &lmvStartABTDt., 0, 'b')));
		format sales_dt date9.;
	RUN;
/* ------------ End. Фильтрация расширенной ABT ---------------------------------------------------*/	


/* ------------ Start. Сохранение ABTs в целевые таблицы, заданные входными параметрами макроса ---*/
	proc casutil;
		droptable incaslib="&lmvLibrefOut." casdata="&lmvTabNmOut." quiet;
		droptable incaslib="&lmvLibrefOutABT." casdata="&lmvTabNmOutABT." quiet;
		promote incaslib='casuser' casdata='ABT_FOR_VF' outcaslib="&lmvLibrefOut." casout="&lmvTabNmOut.";
		promote incaslib='casuser' casdata='ABT_EXTENDED' outcaslib="&lmvLibrefOutABT." casout="&lmvTabNmOutABT.";
		save incaslib="&lmvLibrefOut." outcaslib="&lmvLibrefOut." casdata="&lmvTabNmOut." casout="&lmvTabNmOut..sashdat" replace; 
		save incaslib="&lmvLibrefOutABT." outcaslib="&lmvLibrefOutABT." casdata="&lmvTabNmOutABT." casout="&lmvTabNmOutABT..sashdat" replace; 
	run;
/* ------------ End. Сохранение ABTs в целевые таблицы, заданные входными параметрами макроса ------*/


%mend fcst_create_abt_pbo_gc_mp;

/* %fcst_create_abt_pbo_gc_mp( */
/* 	  mpMode		  = gc */
/* 	, mpSeasonMode	  = 2 */
/* 	, mpOutTableDmVf  = casuser.DM_TRAIN_TRP_GC_MP	 */
/* 	, mpOutTableDmABT = casuser.TRAIN_ABT_TRP_GC_MP */
/* ); */