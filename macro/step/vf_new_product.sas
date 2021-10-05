%macro vf_new_product(mpInCaslib=mn_short);

	*option dsoptions=nonote2err;
	/***** 1. �������� ���������� ������ *****/
	%local lmvInCaslib 
		lmvReportDttm
		lmvInLib
		lmvFilter
		;
	%let lmvInCaslib = &mpInCaslib.;
	%let lmvReportDttm = %sysfunc(datetime());
	%let lmvInLib=ETL_IA;
	%let lmvFilter = t1.channel_cd = 'ALL';

	%if %sysfunc(exist(mn_dict.product_chain))=0 %then %do;
		/* %load_plm(mpOutput = mn_dict.product_chain); */
		proc cas;
		 loadtable / caslib 'mn_dict',
			path='product_chain.sashdat',
			casout={caslib='mn_dict' name='product_chain', replace=true};
		run;
		quit;
	
		proc casutil;
			promote casdata='product_chain' incaslib='mn_dict' outcaslib='mn_dict';
		run;
	%end;

	/* Фильтруем Lifecycle = N и меняем типы дат */
	proc fedsql sessref=casauto;
		create table casuser.n_product_chain{options replace=true} as
			select
				LIFECYCLE_CD,
				coalesce(PREDECESSOR_PRODUCT_ID, SUCCESSOR_PRODUCT_ID) as PREDECESSOR_PRODUCT_ID,
				PREDECESSOR_DIM2_ID,
				SUCCESSOR_PRODUCT_ID,
				SUCCESSOR_DIM2_ID,
				datepart(SUCCESSOR_START_DT) as SUCCESSOR_START_DT,
				coalesce(datepart(PREDECESSOR_END_DT), date %tslit(&VF_FC_AGG_END_DT)) as PREDECESSOR_END_DT,
				SCALE_FACTOR_PCT
			from
				mn_dict.product_chain
			where
				LIFECYCLE_CD = 'N'
		;
	quit;
	
	/* Cначала избавляемся от дублей по ПБО */
	proc fedsql sessref=casauto;
		create table casuser.without_pbo{option replace=true} as
			select distinct
				PREDECESSOR_PRODUCT_ID,
				SUCCESSOR_START_DT,
				PREDECESSOR_END_DT
			from
				casuser.n_product_chain
		;
	quit;
	
	/* Объединяем интервалы продаж */
	data casuser.n_product_chain_expand;
		set casuser.without_pbo;
		keep PREDECESSOR_PRODUCT_ID period_dt;
		format period_dt DATE9.;
		period_dt=SUCCESSOR_START_DT;
		do until (period_dt>=PREDECESSOR_END_DT);
			output;
			period_dt=intnx('day',period_dt,1,'b');
		end;
	run;
	
	/* Оставляем уникальные пары товар-дата */
	proc fedsql sessref=casauto;
		create table casuser.n_product_chain_union{options replace=true} as
			select distinct
				PREDECESSOR_PRODUCT_ID,
				period_dt
			from
				casuser.n_product_chain_expand
		;
	quit;
	
	/* Подготоваливаем таблицу для рассчета интервалов продаж */
	data casuser.n_product_chain_union_interval;
		set casuser.n_product_chain_union;
		format period_dt intnx_lag DATE9.;
		drop intnx_lag perioddt_lag;
		by PREDECESSOR_PRODUCT_ID period_dt;
		perioddt_lag=lag(period_dt);
		if perioddt_lag ne . then 
			intnx_lag = intnx('day',perioddt_lag,1,'b');
		if first.PREDECESSOR_PRODUCT_ID then 
			do;
				i=0;
				output;
			end;
		else
			do;
				if period_dt ^= intnx_lag then
					do;
						i + 1;
						output;
					end;
				else	
					output;
			end;
	run;
	
	/* Считаем интервалы продаж */
	proc fedsql sessref=casauto;
		create table casuser.n_product_chain_union_interval2{options replace=true} as
			select
				PREDECESSOR_PRODUCT_ID,
				i,
				min(period_dt) as period_start_dt,
				max(period_dt) as period_end_dt
			from
				casuser.n_product_chain_union_interval as t1
			group by
				PREDECESSOR_PRODUCT_ID,
				i
		;
	quit;
	
	/* Соединяем историю продаж с актуальными значениями */
	proc fedsql sessref=casauto; 
		create table casuser.pmix_sales{options replace=true} as
			select 
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				datepart(t1.SALES_DT) as sales_dt,
				coalesce(sum(t1.SALES_QTY,t1.SALES_QTY_PROMO), 0) as sum_qty,
				divide(coalesce(t1.GROSS_SALES_AMT, 0), coalesce(sum(t1.SALES_QTY,t1.SALES_QTY_PROMO), 0)) as price
			from 
				&lmvInCaslib..pmix_sales as t1 
			where
				&lmvFilter and
				t1.SALES_DT >= &vf_hist_start_dt and
				t1.SALES_DT <= date %tslit(&vf_hist_end_dt)
	;
	quit;
	
	/* Агрегируем таблицу с продажами */
	proc fedsql sessref=casauto; 
		create table casuser.pmix_sales_aggr{options replace=true} as
			select
				channel_cd,
				product_id,
				sales_dt,
				sum(sum_qty) as sum_qty
			from
				casuser.pmix_sales
			group by
				channel_cd,
				product_id,
				sales_dt			
		;
	quit;
	
	/* Соединяем объединенный PLM с агрегированными продажами */
	proc fedsql sessref=casauto; 
		create table casuser.pmix_plm{options replace=true} as
			select
				t1.channel_cd,
				t1.product_id,
				t1.sales_dt,
				t1.sum_qty,
				t2.period_start_dt,
				t2.period_end_dt
			from
				casuser.pmix_sales_aggr as t1
			inner join
				casuser.n_product_chain_union_interval2 as t2
			on
				t1.product_id = t2.PREDECESSOR_PRODUCT_ID and
				t1.sales_dt <= t2.period_end_dt and
				t1.sales_dt >= t2.period_start_dt
		;
	quit;
	
	/* Считаем настощую дату старта (когда продажы ненулевые) */
	proc fedsql sessref=casauto; 
		create table casuser.pmix_plm2{options replace=true} as
			select
				channel_cd,
				product_id,
				period_start_dt,
				period_end_dt,
				min(sales_dt) as real_start_dt
			from
				casuser.pmix_plm as t1
			where
				sum_qty > 0 
			group by
				channel_cd,
				product_id,
				period_start_dt,
				period_end_dt		
		;
	quit;
	
	/* Оставляем только новые товары (начало не 2 января 2017) */
	proc fedsql sessref=casauto; 
		create table casuser.pmix_plm3{options replace=true} as
			select
				'N' as lifecycle_cd,
				channel_cd,
				product_id,
				real_start_dt as period_start_dt,
				period_end_dt
			from
				casuser.pmix_plm2
			where
				real_start_dt ^= &VF_HIST_START_DT.
		;
	quit;
	
	/* Убираем короткие интервалы */
	proc fedsql sessref=casauto; 
		create table casuser.new_product_interval{options replace=true} as
			select
				lifecycle_cd,
				channel_cd,
				product_id,
				period_start_dt,
				period_end_dt
			from
				casuser.pmix_plm3
			where
				(period_end_dt - period_start_dt) > 1
		;
	quit;
	
	proc casutil;
	  droptable casdata="without_pbo" incaslib="casuser" quiet;
	  droptable casdata="n_product_chain" incaslib="casuser" quiet;
	  droptable casdata="n_product_chain_expand" incaslib="casuser" quiet;
	  droptable casdata="n_product_chain_union" incaslib="casuser" quiet;
	  droptable casdata="n_product_chain_union_interval" incaslib="casuser" quiet;
	  droptable casdata="n_product_chain_union_interval2" incaslib="casuser" quiet;
	  droptable casdata="pmix_sales_aggr" incaslib="casuser" quiet;
	  droptable casdata="pmix_plm" incaslib="casuser" quiet;
	  droptable casdata="pmix_plm2" incaslib="casuser" quiet;
	  droptable casdata="pmix_plm3" incaslib="casuser" quiet;
		
	 * promote casdata="new_product_interval" incaslib="casuser" outcaslib="casuser";
	
	run;
	
	
	/****** 2. Собираем обучающую выборку ******/
	proc casutil;
		droptable casdata="real_end_interval" incaslib="casuser" quiet;
		droptable casdata="new_product_train" incaslib="casuser" quiet;
	run;
	
	/* Отрезаем интервал продаж последней датой истории или годом */
	proc fedsql sessref=casauto;
		create table casuser.real_end_interval{options replace=true} as
			select
				lifecycle_cd,
				channel_cd,
				product_id,
				period_start_dt,
				period_end_dt,
				cast((case
					when (period_start_dt <=  date %tslit(&vf_hist_end_dt)) and (date %tslit(&vf_hist_end_dt) - period_start_dt) <= 365 then min(date %tslit(&vf_hist_end_dt), period_end_dt)
					when (date %tslit(&vf_hist_end_dt) - period_start_dt) > 365 then min(period_start_dt + 365, period_end_dt)
				end) as date) as real_end_dt 
			from
				casuser.new_product_interval	
		;
	quit;
	
	/* Добавляем продажи */
	proc fedsql sessref=casauto;
		create table casuser.new_product_train{options replace=true} as
			select
				t1.CHANNEL_CD,
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.SALES_DT,
				t1.sum_qty,
				t1.price,
				t2.lifecycle_cd,
				t2.period_start_dt,
				t2.real_end_dt
			from
				casuser.pmix_sales as t1
			inner join
				casuser.real_end_interval as t2
			on
				t1.channel_cd = t2.channel_cd and
				t1.product_id = t2.product_id and
				t1.sales_dt <= t2.real_end_dt and
				t1.sales_dt >= t2.period_start_dt
		;
	quit;
	
	proc casutil;
		droptable casdata="real_end_interval" incaslib="casuser" quiet;
	run;
	
	
	/****** 3. Добаляем коэффициенты недельного профиля к обучающей выборке *******/
	
	/* 3.2 Собираем коэффициенты для недельного профиля */
	proc casutil;
		droptable casdata="npf_frame" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_mean" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_mean2" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile2" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile3" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile4" incaslib="casuser" quiet;
	run;
	
	/* Создаем каркас */
	proc fedsql sessref=casauto;
		create table casuser.npf_frame{options replace=true} as
			select
				t2.PROD_LVL2_ID,
				t1.month,
				t1.weekday
			from
				(select distinct month(sales_dt), weekday(sales_dt) from casuser.pmix_sales) as t1,
				(select distinct PROD_LVL2_ID from &lmvInCaslib..product_dictionary) as t2	
		;
	quit;
	
	/* Добавляем категории товаров и считаем недельный профиль */
	proc fedsql sessref=casauto;
		create table casuser.pmix_sales2{options replace=true} as
			select
				*, month(sales_dt) as month, weekday(sales_dt) as weekday
			from
				casuser.pmix_sales
		;
		create table casuser.npf_weekday_mean{options replace=true} as
			select
				t2.PROD_LVL2_ID,
				t1.month,
				t1.weekday,
				mean(t1.sum_qty) as weekday_mean_sum_qty
			from 
				casuser.pmix_sales2 as t1
			inner join
				&lmvInCaslib..product_dictionary as t2
			on
				t1.product_id = t2.product_id
			group by
				t2.PROD_LVL2_ID,
				t1.month,
				t1.weekday
		;
		create table casuser.npf_weekday_profile{options replace=true} as
			select
				t2.PROD_LVL2_ID,
				t2.month,
				t2.weekday,
				divide(t2.weekday_mean_sum_qty, t1.weekday_profile) as weekday_profile
			from (
				select
					t1.PROD_LVL2_ID,
					t1.month,
					sum(t1.weekday_mean_sum_qty) as weekday_profile			
				from
					casuser.npf_weekday_mean as t1
				group by
					t1.PROD_LVL2_ID,
					t1.month
			) as t1
			inner join
				casuser.npf_weekday_mean as t2
			on
				t1.PROD_LVL2_ID = t2.PROD_LVL2_ID and
				t1.month = t2.month
		;
	quit;
	
	/* Не для всех категорий достаточно статистики, чтобы определить недельный профиль */
	/* Для таких категорий поставим недельный профиль, независящий от категории продаж */
	proc fedsql sessref=casauto;
		create table casuser.npf_weekday_mean2{options replace=true} as
			select
				t1.month,
				t1.weekday,
				mean(t1.sum_qty) as weekday_mean_sum_qty
			from 
				casuser.pmix_sales2 as t1
			inner join
				&lmvInCaslib..product_dictionary as t2
			on
				t1.product_id = t2.product_id
			group by
				t1.month,
				t1.weekday
		;
		create table casuser.npf_weekday_profile2{options replace=true} as
			select
				t2.month,
				t2.weekday,
				divide(t2.weekday_mean_sum_qty, t1.weekday_profile) as weekday_profile
			from (
				select
					t1.month,
					sum(t1.weekday_mean_sum_qty) as weekday_profile			
				from
					casuser.npf_weekday_mean2 as t1
				group by
					t1.month
			) as t1
			inner join
				casuser.npf_weekday_mean2 as t2
			on
				t1.month = t2.month
		;
	quit;
	
	/* Ставим флаг категориям, имеющим недостаточно статистики */
	proc fedsql sessref=casauto;
		create table casuser.npf_weekday_profile3{options replace=true} as
			select
				t2.PROD_LVL2_ID,
				t2.month,
				t2.weekday,
				t2.weekday_profile,
				t1.low_stat_category
			from (
				select
					PROD_LVL2_ID,
					(case when count(1) < 7*12 then 1 else 0 end) as low_stat_category
				from
					casuser.npf_weekday_profile
				group by
					PROD_LVL2_ID
				) as t1
			inner join
				casuser.npf_weekday_profile as t2
			on
				t1.PROD_LVL2_ID = t2.PROD_LVL2_ID
		;
	quit;
	
	/* Объединяем таблицы */
	proc fedsql sessref=casauto;
		create table casuser.npf_weekday_profile4{options replace=true} as 
			select distinct
				t1.PROD_LVL2_ID,
				t1.month,
				t1.weekday,
				(case
					when coalesce(t3.low_stat_category, 1) = 1 then t2.weekday_profile
					else t3.weekday_profile
				end) as weekday_profile
			from
				casuser.npf_frame as t1
			left join
				casuser.npf_weekday_profile2 as t2
			on
				t1.month = t2.month and
				t1.weekday = t2.weekday
			left join
				casuser.npf_weekday_profile3 as t3
			on
				t1.month = t3.month and
				t1.weekday = t3.weekday and
				t1.PROD_LVL2_ID = t3.PROD_LVL2_ID	
		;
	quit;
	
	proc casutil;
		droptable casdata="npf_frame" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_mean" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_mean2" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile2" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile3" incaslib="casuser" quiet;
	run;
	
	
	/* 3.3 Добавляем посчитанные коэффициенты к обучающей выборке */
	/* Добавляем категории товаров, день недели и месяц */
	proc casutil;
		droptable casdata="new_product_train2" incaslib="casuser" quiet;
		droptable casdata="new_product_train3" incaslib="casuser" quiet;
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.new_product_train2{options replace=true} as
			select
				t1.CHANNEL_CD,
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.SALES_DT,
				weekday(t1.sales_dt) as weekday,
				month(t1.sales_dt) as month,
				t1.sum_qty,
				t1.price,
				t1.lifecycle_cd,
				t1.period_start_dt,
				t1.real_end_dt,
				t2.PROD_LVL2_ID
			from
				casuser.new_product_train as t1
			left join
				&lmvInCaslib..product_dictionary as t2
			on
				t1.product_id = t2.product_id
		;
	quit;
	
	/* Добаляем коэффициент недельного профиля */
	proc fedsql sessref=casauto;
		create table casuser.new_product_train3{options replace=true} as
			select
				t1.CHANNEL_CD,
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.SALES_DT,
				t1.weekday,
				t1.month,
				t1.sum_qty,
				t1.price,
				t1.lifecycle_cd,
				t1.period_start_dt,
				t1.real_end_dt,
				t1.PROD_LVL2_ID,
				t2.weekday_profile
			from
				casuser.new_product_train2 as t1
			left join
				casuser.npf_weekday_profile4 as t2
			on
				t1.PROD_LVL2_ID = t2.PROD_LVL2_ID and
				t1.month = t2.month and
				t1.weekday = t2.weekday
		;
	quit;
	
	proc casutil;
		droptable casdata="new_product_train" incaslib="casuser" quiet;
		droptable casdata="new_product_train2" incaslib="casuser" quiet;
	run;
	
	/****** 4. Соберем скоринговую витрину ******/
	proc casutil;
		droptable casdata="last_day_ts" incaslib="casuser" quiet;
		droptable casdata="future_assort_matrix" incaslib="casuser" quiet;
		droptable casdata="new_product_scoring" incaslib="casuser" quiet;
		droptable casdata="existing_product_scoring" incaslib="casuser" quiet;
		droptable casdata="existing_product_scoring2" incaslib="casuser" quiet;
		droptable casdata="future_product_scoring" incaslib="casuser" quiet;
		droptable casdata="new_product_abt" incaslib="casuser" quiet;
	run;
	
	/* Определяем временные ряды, закончившиеся в последний день истории */
	proc fedsql sessref=casauto;
		create table casuser.last_day_ts{options replace=true} as
			select
				t1.CHANNEL_CD,
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				. as sum_qty,
				date %tslit(&vf_hist_end_dt) + 1 as sales_dt,
				t1.period_start_dt
			from (
				select distinct
					CHANNEL_CD,
					PBO_LOCATION_ID,
					PRODUCT_ID,
					period_start_dt
				from
					casuser.new_product_train3
				where
					real_end_dt = date %tslit(&vf_hist_end_dt)
			) as t1
		;
	quit;
	
	%let fc_end= %sysfunc(intnx(day,&vf_fc_start_dt_sas, 365),yymmddd10.); /*�������� ��������������� ������ ����� �������*/
	
	/* ���������� ���������� ����� � ������� last_day_ts � ����������� TS, ���� ������ ����*/
	%tech_count_rows_cas(mpInLibref=casuser
							,mpInTableNm=last_day_ts
							,mpVar=mvCntLastDayTs);
							
	%if &mvCntLastDayTs. gt 0 %then %do;
		/* Протягиваем временные ряды на год */
		proc cas;
		timeData.timeSeries result =r /
			series={
				{name="sum_qty", setmiss="MISSING"},
				{name="period_start_dt", setmiss="PREV"}
			}
			tEnd= "&fc_end"
			table={
				caslib="casuser",
				name="last_day_ts",
				groupby={"PBO_LOCATION_ID","PRODUCT_ID", "CHANNEL_CD"}
			}
			timeId="SALES_DT"
			trimId="LEFT"
			interval="day"
			casOut={caslib="casuser", name="existing_product_scoring", replace=True}
			;
		run;
		quit;
	%end;
	
	/* Берем из product_chain новые товары, начавшие продаваться в будующем */
	proc fedsql sessref=casauto;
		create table casuser.future_product_chain{options replace=true} as
			select
				SUCCESSOR_PRODUCT_ID as product_id,
				SUCCESSOR_DIM2_ID as pbo_location_id,
				datepart(SUCCESSOR_START_DT) as period_start_dt,
				datepart(PREDECESSOR_END_DT) as period_end_dt
			from
				&lmvInCaslib..product_chain as t1
			where
				LIFECYCLE_CD = 'N' and
				datepart(SUCCESSOR_START_DT) > date %tslit(&vf_hist_end_dt)
		;
	quit;
	
	/* Продляем эти интервалы */
	data casuser.future_product_scoring;
		set casuser.future_product_chain;
		drop period_end_dt;
		format sales_dt DATE9.;
		channel_cd = 'ALL';
		if period_end_dt ne . then do;
			do sales_dt = period_start_dt to period_end_dt;
			   output;
			end;
		end;
	run;
	
	/* ���������� ���������� ����� � ������� last_day_ts � ����������� TS, ���� ������ ����*/
	%tech_count_rows_cas(mpInLibref=casuser
							,mpInTableNm=future_product_scoring
							,mpVar=mvCntFtrProdScor);
							
	%if &mvCntFtrProdScor. eq 0 %then %do;
		%return;
	%end;
	
	/* Объединяем скоринговую витрину для новых товаров без истории и с историей */
	data casuser.new_product_scoring;	
		set casuser.future_product_scoring 
			%if &mvCntLastDayTs. gt 0 %then %do;
			casuser.existing_product_scoring
			%end;
		;
	run;
	
	/* Убираем дубликаты в скоринговой витрине */
	proc fedsql sessref=casauto;
		create table casuser.new_product_scoring{options replace=true} as
			select
				channel_cd,
				pbo_location_id,
				product_id,
				sales_dt,
				mean(sum_qty) as sum_qty,
				max(PERIOD_START_DT) as PERIOD_START_DT /* Дату старта берем последнюю */
			from
				casuser.new_product_scoring
			group by
				channel_cd,
				pbo_location_id,
				product_id,
				sales_dt
		;
	quit;
	
	/* Объединяем скоринговую витрину с обучающей */
	data casuser.new_product_abt;
		set casuser.new_product_scoring casuser.new_product_train3;
	run;
	
	proc casutil;
		droptable casdata="last_day_ts" incaslib="casuser" quiet;
		droptable casdata="existing_product_scoring" incaslib="casuser" quiet;
		*droptable casdata="new_product_scoring" incaslib="casuser" quiet;
	run;
	
	/****** 5. Агрегация до недель ******/
	proc casutil;
		droptable casdata="week_start" incaslib="casuser" quiet;
		droptable casdata="week_aggr" incaslib="casuser" quiet;
	run;
	
	/* Преобразуем даты в недели */
	proc fedsql sessref=casauto;
		create table casuser.week_start{options replace=true} as
			select
				channel_cd,
				pbo_location_id,
				product_id,
				PERIOD_START_DT,
				cast(intnx('week.2',PERIOD_START_DT,0,'b') as date)  as start_week,
				cast(intnx('week.2',sales_dt,0,'b') as date) as week,
				sum_qty,
				sales_dt,
				weekday_profile
			from
				casuser.new_product_abt
		;
	quit;
	
	/* Считем количество дней продаж товара в неделю и суммарные продажи за неделю */
	proc fedsql sessref=casauto;
		create table casuser.week_aggr{options replace=true} as
			select
				channel_cd,
				pbo_location_id,
				product_id,
				week,
				min(start_week) as start_week, /* если товар прервал свои продажи посреди недели, то считаем начало минимальным*/
				count(sales_dt) as count_sales_dt,
				sum(sum_qty) as sum_qty,
				sum(weekday_profile) as sum_weekday_profile		
			from
				casuser.week_start
			group by
				channel_cd,
				pbo_location_id,
				product_id,
				week
		;
	quit;
	
	
	/****** 6. Исправляем продажи в первую неделю ******/
	proc casutil;
		droptable casdata="new_product_abt2" incaslib="casuser" quiet;
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.new_product_abt2{options replace=true} as
			select
				channel_cd,
				pbo_location_id,
				product_id,
				start_week,
				week,
				count_sales_dt,
				(case
					when (start_week = week) and (count_sales_dt<7) then divide(1,sum_weekday_profile)*sum_qty
					else sum_qty
				end) as sum_qty_corrected,
				sum_weekday_profile,
				sum_qty
			from
				casuser.week_aggr
		;
	quit;
	
	
	/****** 7. Считаем количество недель с начала продаж ******/
	proc casutil;
		droptable casdata="new_product_abt3" incaslib="casuser" quiet;
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.new_product_abt3{options replace=true} as
			select
				channel_cd,
				pbo_location_id,
				product_id,
				start_week,
				week,
				divide((week - start_week), 7) as weeks_from_start,
				count_sales_dt,
				sum_qty,
				sum_qty_corrected,
				sum_weekday_profile		
			from
				casuser.new_product_abt2
		;
	quit;
	
	proc casutil;
		droptable casdata="new_product_abt2" incaslib="casuser" quiet;
	run;
	
	/****** 8. Добавляем атрибуты ресторана ******/
	proc casutil;
	  droptable casdata="new_product_abt4" incaslib="casuser" quiet;
	run;
	
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
	
		%local	lmvOutLibref
			lmvOutTabName
			;
	
		%member_names (mpTable=&table., mpLibrefNameKey=lmvOutLibref, mpMemberNameKey=lmvOutTabName);
		
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
			create table casuser.&lmvOutTabName.{options replace=true} as 
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
	
	%text_encoding(&lmvInCaslib..pbo_dictionary, A_AGREEMENT_TYPE);
	%text_encoding(casuser.pbo_dictionary, A_BREAKFAST);
	%text_encoding(casuser.pbo_dictionary, A_BUILDING_TYPE);
	%text_encoding(casuser.pbo_dictionary, A_COMPANY);
	%text_encoding(casuser.pbo_dictionary, A_DELIVERY);
	%text_encoding(casuser.pbo_dictionary, A_MCCAFE_TYPE);
	%text_encoding(casuser.pbo_dictionary, A_PRICE_LEVEL);
	%text_encoding(casuser.pbo_dictionary, A_DRIVE_THRU);
	%text_encoding(casuser.pbo_dictionary, A_WINDOW_TYPE);
	
	proc fedsql sessref=casauto;
		create table casuser.new_product_abt4{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.product_id,
				t1.start_week,
				t1.week,
				t1.weeks_from_start,
				t1.count_sales_dt,
				t1.sum_qty,	
				t1.sum_qty_corrected,
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
				casuser.new_product_abt3 as t1
			left join
				casuser.pbo_dictionary as t2
			on
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;
	
	proc casutil;
	  droptable casdata="new_product_abt3" incaslib="casuser" quiet;
	run;
	
	
	/****** 9. Добавляем атрибуты товара ******/
	proc casutil;
	  droptable casdata="new_product_abt5" incaslib="casuser" quiet;
	run;
	  
	%text_encoding(&lmvInCaslib..product_dictionary, a_hero);
	%text_encoding(casuser.product_dictionary, a_item_size);
	%text_encoding(casuser.product_dictionary, a_offer_type);
	%text_encoding(casuser.product_dictionary, a_price_tier);
	
	proc fedsql sessref=casauto;
		create table casuser.new_product_abt5{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.product_id,
				t1.start_week,
				t1.week,
				t1.weeks_from_start,
				t1.count_sales_dt,
				t1.sum_qty,	
				t1.sum_qty_corrected,
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
				t2.prod_lvl4_id, 
				t2.prod_lvl3_id,
				t2.prod_lvl2_id,
				t2.a_hero_id as hero,
				t2.a_item_size_id as item_size,
				t2.a_offer_type_id as offer_type,
				t2.a_price_tier_id as price_tier
		from
			casuser.new_product_abt4 as t1
		left join
			casuser.product_dictionary as t2
		on
			t1.product_id = t2.product_id
		;
	quit;
		
	proc casutil;
	  droptable casdata='new_product_abt4' incaslib='casuser' quiet;
	run;
	
	
	/****** 10. Добавляем макроэкономику ******/
	proc casutil;
	  droptable casdata="macro_ml" incaslib="casuser" quiet;
	  droptable casdata="macro2_ml" incaslib="casuser" quiet;
	  droptable casdata="macro_transposed_ml" incaslib="casuser" quiet;
	  droptable casdata="macro_transposed_ml2" incaslib="casuser" quiet;
	  droptable casdata="new_product_abt6" incaslib="casuser" quiet;
	run;
	
	/* update macro in caslib */
	data casuser.macro (replace=yes  drop=valid_from_dttm valid_to_dttm);
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
	
	/* Усредняем макроэкономику по неделям */
	proc fedsql sessref=casauto;
		create table casuser.macro_transposed_ml2{options replace=true} as
			select
				t1.week,
				mean(A_CPI) as CPI,
				mean(A_GPD) as GPD,
				mean(A_RDI) as RDI			
			from (
				select
					cast(intnx('week.2',period_dt,0,'b') as date)  as week,
					A_CPI,
					A_GPD,
					A_RDI
				from
					casuser.macro_transposed_ml
			) as t1
			group by
				t1.week
		;
	quit;
	
	/* Добавляем макроэкономику */
	proc fedsql sessref=casauto;
		create table casuser.new_product_abt6{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.product_id,
				t1.start_week,
				t1.week,
				t1.weeks_from_start,
				t1.count_sales_dt,
				t1.sum_qty,
				t1.sum_qty_corrected,	
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
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				coalesce(t2.CPI, 0) as CPI,
				coalesce(t2.GPD, 0) as GPD,
				coalesce(t2.RDI, 0) as RDI
		from
			casuser.new_product_abt5 as t1
		left join
			casuser.macro_transposed_ml2 as t2
		on
			t1.week = t2.week
		;
	quit;
	
	proc casutil;
	  droptable casdata="macro2_ml" incaslib="casuser" quiet;
	  droptable casdata="macro_transposed_ml" incaslib="casuser" quiet;
	  droptable casdata="macro_transposed_ml2" incaslib="casuser" quiet;
	  droptable casdata="new_product_abt5" incaslib="casuser" quiet;
	run;
	
	
	/****** 10. Промо ******/
	proc casutil;
		droptable casdata="lvl5" incaslib="casuser" quiet;
		droptable casdata="lvl4" incaslib="casuser" quiet;
		droptable casdata="lvl3" incaslib="casuser" quiet;
		droptable casdata="lvl2" incaslib="casuser" quiet;
		droptable casdata="lvl1" incaslib="casuser" quiet;
		droptable casdata="pbo_lvl_all" incaslib="casuser" quiet;
		droptable casdata="product_lvl_all" incaslib="casuser" quiet;
		droptable casdata="promo_ml" incaslib="casuser" quiet;
		droptable casdata="promo_ml_daily" incaslib="casuser" quiet;
		droptable casdata="promo_mechanics" incaslib="casuser" quiet;
		droptable casdata="promo_mechanics_one_hot" incaslib="casuser" quiet;
		droptable casdata="promo_mechanics_one_hot_zero" incaslib="casuser" quiet;
		droptable casdata="promo_ml_daily_one_hot" incaslib="casuser" quiet;
		droptable casdata="promo_aggr" incaslib="casuser" quiet;
		droptable casdata="new_product_abt7" incaslib="casuser" quiet;
		droptable casdata="ia_promo_x_product_leaf" incaslib="casuser" quiet;
		droptable casdata="ia_promo_x_pbo_leaf" incaslib="casuser" quiet;
	run;
	
	/* Создаем таблицу связывающую PBO на листовом уровне и на любом другом */
	proc fedsql sessref=casauto;
		create table casuser.lvl4{options replace=true} as 
			select distinct
				pbo_location_id as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_dictionary
		;
		create table casuser.lvl3{options replace=true} as 
			select distinct
				LVL3_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_dictionary
		;
		create table casuser.lvl2{options replace=true} as 
			select distinct
				LVL2_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_dictionary
		;
		create table casuser.lvl1{options replace=true} as 
			select 
				1 as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_dictionary
		;
	quit;
	
	/* Соединяем в единый справочник ПБО */
	data casuser.pbo_lvl_all;
		set casuser.lvl4 casuser.lvl3 casuser.lvl2 casuser.lvl1;
	run;
	
	/* Создаем таблицу связывающую товары на листовом уровне и на любом другом */
	proc fedsql sessref=casauto;
		create table casuser.lvl5{options replace=true} as 
			select distinct
				product_id as product_id,
				product_id as product_leaf_id
			from
				casuser.product_dictionary
		;
		create table casuser.lvl4{options replace=true} as 
			select distinct
				prod_LVL4_ID as product_id,
				product_id as product_leaf_id
			from
				casuser.product_dictionary
		;
		create table casuser.lvl3{options replace=true} as 
			select distinct
				prod_LVL3_ID as product_id,
				product_id as product_leaf_id
			from
				casuser.product_dictionary
		;
		create table casuser.lvl2{options replace=true} as 
			select distinct
				prod_LVL2_ID as product_id,
				product_id as product_leaf_id
			from
				casuser.product_dictionary
		;
		create table casuser.lvl1{options replace=true} as 
			select distinct
				1 as product_id,
				product_id as product_leaf_id
			from
				casuser.product_dictionary
		;
	quit;
	
	/* Соединяем в единый справочник ПБО */
	data casuser.product_lvl_all;
		set casuser.lvl5 casuser.lvl4 casuser.lvl3 casuser.lvl2 casuser.lvl1;
	run;
	
	/* Привидение всех промо-механик к одному виду в соотв. с таблицей  &RTP_PROMO_MECH_TRANSF_FILE.*/
	
	/* Добавляем к таблице промо ПБО и товары */
	proc fedsql sessref = casauto;
		create table casuser.ia_promo_x_pbo_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t2.PBO_LEAF_ID
			from
				/*&lmvInCaslib..promo_pbo_enh as t1,*/
				&lmvInCaslib..promo_pbo as t1,
				casuser.pbo_lvl_all as t2
			where t1.pbo_location_id = t2.PBO_LOCATION_ID
		;
		create table casuser.ia_promo_x_product_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t2.product_LEAF_ID
			from
				/*&lmvInCaslib..promo_prod_enh as t1,*/
				&lmvInCaslib..promo_prod as t1,
				casuser.product_lvl_all as t2
			where t1.product_id = t2.product_id
		;
		create table casuser.promo_ml{options replace = true} as 
			select
				t1.PROMO_ID,
				t3.product_LEAF_ID as product_id,
				t2.PBO_LEAF_ID as pbo_location_id,
				datepart(t1.START_DT) as start_dt,
				datepart(t1.END_DT) as end_dt,
				t1.CHANNEL_CD,
				t1.promo_group_id,
				/* Привидение всех промо-механик к одному виду в соотв. с таблицей  &RTP_PROMO_MECH_TRANSF_FILE.*/
				/* compress(promo_mechanics,'', 'ak') as promo_mechanics_name,*/
				t4.new_mechanic as promo_mechanics_name,
				1 as promo_flag		
			from
				/*&lmvInCaslib..promo_enh as t1*/
				&lmvInCaslib..promo as t1				
			left join
				casuser.ia_promo_x_pbo_leaf as t2
			on 
				t1.PROMO_ID = t2.PROMO_ID
			left join
				casuser.ia_promo_x_product_leaf as t3
			on
				t1.PROMO_ID = t3.PROMO_ID 
			/* Привидение всех промо-механик к одному виду в соотв. с таблицей  &RTP_PROMO_MECH_TRANSF_FILE.*/
			inner join 
				mn_short.promo_mech_transformation as t4
			on t1.promo_mechanics = t4.old_mechanic 
		;
	quit;

	/* Расшиваем интервалы по дням */
	data casuser.promo_ml_daily;
		set casuser.promo_ml;
		format sales_dt DATE9.;
		do sales_dt=start_dt to end_dt;
			output;
		end;
	run;
	
	/* Определяем механики промо акций */
	proc fedsql sessref=casauto;
		create table casuser.promo_mechanics{options replace=true} as
			select distinct
				promo_id,
				promo_mechanics_name,
				promo_flag
			from
				casuser.promo_ml
		;
	quit;
	
	/* Транспонируем механику промо в вектор */
	proc cas;
	transpose.transpose /
		table = {
			name="promo_mechanics",
			caslib="casuser",
			groupby={"promo_id"}}
		transpose={"promo_flag"} 
		id={"promo_mechanics_name"} 
		casout={name="promo_mechanics_one_hot", caslib="casuser", replace=true};
	quit;
	
	/* Заменяем пропуски на нули */
	data casuser.promo_mechanics_one_hot_zero;
		set casuser.promo_mechanics_one_hot;
		drop _name_;
		array change _numeric_;
	    	do over change;
	            if change=. then change=0;
	        end;
	run ;
	
	/* Добавляем переменные к витрине */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml_daily_one_hot{options replace=true} as
			select
				t1.pbo_location_id,
				t1.product_id,
				t1.sales_dt,
				intnx('week.2',t1.sales_dt,0,'b') as week,
				t2.*
			from
				casuser.promo_ml_daily as t1
			left join
				casuser.promo_mechanics_one_hot_zero as t2
			on
				t1.promo_id = t2.promo_id
		;
	quit;
	
	data _null_;
		set mn_short.promo_mech_transformation end=end;
		length sql_list sql_sum_list sql_coalesce_list sql_list_def $1000;
		retain sql_list sql_sum_list sql_coalesce_list sql_list_def;
		by new_mechanic;

		if _n_ = 1 then do;
			sql_list = cats('t1.', new_mechanic);
			sql_sum_list = cat('sum(coalesce(t2.', strip(new_mechanic), ',0 )) as ', strip(new_mechanic));
			sql_coalesce_list = cat('coalesce(t2.', strip(new_mechanic), ',0 ) as ', strip(new_mechanic));
			sql_list_def = cat(strip(new_mechanic), ' ' );
		end;
		else if first.new_mechanic then do;
			sql_list = cats(sql_list, ', t1.', new_mechanic);
			sql_sum_list = cat(strip(sql_sum_list), ', sum(coalesce(t2.', strip(new_mechanic), ', 0)) as ', strip(new_mechanic));
			sql_coalesce_list = cat(strip(sql_coalesce_list), ', coalesce(t2.', strip(new_mechanic), ', 0) as ', strip(new_mechanic));
			sql_list_def = cat(strip(sql_list_def),' ', strip(new_mechanic));
		end;

		if end then do;
			call symputx('promo_list_sql', sql_list, 'G');
			call symputx('promo_list_sql_sum', sql_sum_list, 'G');
			call symputx('promo_list_coalesce', sql_coalesce_list, 'G');
			call symputx('promo_list', sql_list_def, 'G');
		end;
	run;
%put &=promo_list_sql_sum;
%put &=promo_list_sql;
%put &=promo_list_coalesce;
%put &=promo_list;


	/* Считаем кол-во сработавших промо в рамках всех каналов */
	proc fedsql sessref=casauto;
		create table casuser.promo_aggr{options replace=true} as
			select
				t2.week,
				t2.pbo_location_id,
				t2.product_id,
				/*
				sum(t1.BOGO) as BOGO,
				sum(t1.Bundle) as Bundle,
				sum(t1.Discount) as Discount,
				sum(t1.EVMSet) as EVMSet,
				sum(t1.GiftforpurchaseforordersaboveXru) as GiftforpurchaseforordersaboveXru,
				sum(t1.Giftforpurchaseforproduct) as Giftforpurchaseforproduct,
				sum(t1.GiftforpurchaseNonProduct) as GiftforpurchaseNonProduct,
				sum(t1.GiftforpurchaseSampling) as GiftforpurchaseSampling,
				sum(t1.NPPromoSupport) as NPPromoSupport,
				sum(t1.OtherDiscountforvolume) as OtherDiscountforvolume,
				sum(t1.Pairs) as Pairs,
				sum(t1.Pairsdifferentcategories) as Pairsdifferentcategories,
				sum(t1.Productlineextension) as Productlineextension,
				sum(t1.ProductnewlaunchLTO) as ProductnewlaunchLTO,
				sum(t1.ProductnewlaunchPermanentinclite) as ProductnewlaunchPermanentinclite,
				sum(t1.Productrehitsameproductnolineext) as Productrehitsameproductnolineext,
				sum(t1.Temppricereductiondiscount) as Temppricereductiondiscount,
				sum(t1.Undefined) as Undefined
				*/
				&promo_list_sql_sum.
			from
				casuser.promo_ml_daily_one_hot as t2
			group by
				t2.week,
				t2.pbo_location_id,
				t2.product_id
		;
	quit;
	
	/* Соединяем с витриной */
	proc fedsql sessref=casauto;
		create table casuser.new_product_abt7{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.product_id,
				t1.start_week,
				t1.week,
				t1.weeks_from_start,
				t1.count_sales_dt,
				t1.sum_qty,
				t1.sum_qty_corrected,	
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
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t1.CPI,
				t1.GPD,
				t1.RDI,
				/*
				coalesce(t2.BOGO, 0) as BOGO,
				coalesce(t2.Bundle, 0) as Bundle,
				coalesce(t2.Discount, 0) as Discount, 
				coalesce(t2.EVMSet, 0) as EVMSet,
				coalesce(t2.GiftforpurchaseforordersaboveXru, 0) as GiftforpurchaseforordersaboveXru,
				coalesce(t2.Giftforpurchaseforproduct, 0) as Giftforpurchaseforproduct,
				coalesce(t2.GiftforpurchaseNonProduct, 0) as GiftforpurchaseNonProduct,
				coalesce(t2.GiftforpurchaseSampling, 0) as GiftforpurchaseSampling,
				coalesce(t2.NPPromoSupport, 0) as NPPromoSupport,
				coalesce(t2.OtherDiscountforvolume, 0) as OtherDiscountforvolume,
				coalesce(t2.Pairs, 0) as Pairs,
				coalesce(t2.Pairsdifferentcategories, 0) as Pairsdifferentcategories,
				coalesce(t2.Productlineextension, 0) as Productlineextension,
				coalesce(t2.ProductnewlaunchLTO, 0) as ProductnewlaunchLTO,
				coalesce(t2.ProductnewlaunchPermanentinclite, 0) as ProductnewlaunchPermanentinclite,
				coalesce(t2.Productrehitsameproductnolineext, 0) as Productrehitsameproductnolineext,
				coalesce(t2.Temppricereductiondiscount, 0) as Temppricereductiondiscount,
				coalesce(t2.Undefined, 0) as Undefined
				*/
				&promo_list_coalesce.
		from
			casuser.new_product_abt6 as t1
		left join
			casuser.promo_aggr as t2
		on
			t1.product_id = t2.product_id and
			t1.pbo_location_id = t2.pbo_location_id and
			t1.week = t2.week 
		;
	quit;
	
	proc casutil;
		droptable casdata="lvl5" incaslib="casuser" quiet;
		droptable casdata="lvl4" incaslib="casuser" quiet;
		droptable casdata="lvl3" incaslib="casuser" quiet;
		droptable casdata="lvl2" incaslib="casuser" quiet;
		droptable casdata="lvl1" incaslib="casuser" quiet;
		droptable casdata="pbo_lvl_all" incaslib="casuser" quiet;
		droptable casdata="product_lvl_all" incaslib="casuser" quiet;
		droptable casdata="promo_ml_daily" incaslib="casuser" quiet;
		droptable casdata="promo_mechanics" incaslib="casuser" quiet;
		droptable casdata="promo_mechanics_one_hot" incaslib="casuser" quiet;
		droptable casdata="promo_mechanics_one_hot_zero" incaslib="casuser" quiet;
		droptable casdata="promo_ml_daily_one_hot" incaslib="casuser" quiet;
		droptable casdata="promo_aggr" incaslib="casuser" quiet;
		droptable casdata="ia_promo_x_product_leaf" incaslib="casuser" quiet;
		droptable casdata="ia_promo_x_pbo_leaf" incaslib="casuser" quiet;

		droptable casdata="new_product_abt6" incaslib="casuser" quiet;
	run;
	
	
	/****** 11. Погода ******/
	proc casutil;
		droptable casdata="ia_weather2" incaslib="casuser" quiet;
		droptable casdata="new_product_abt8" incaslib="casuser" quiet;
	run;
	
	/* Считаем среднюю температуру в неделю */
	proc fedsql sessref=casauto;
		create table casuser.ia_weather2{options replace=true} as
			select
				t1.pbo_location_id,
				t1.week,
				mean(t1.TEMPERATURE) as TEMPERATURE,
				mean(t1.PRECIPITATION) as PRECIPITATION
			from (
				select
					pbo_location_id,
					intnx('week.2',report_dt,0,'b') as week, 
					TEMPERATURE,
					PRECIPITATION
				from
					&lmvInCaslib..weather
			) as t1
			group by
				t1.pbo_location_id,
				t1.week
		;
	quit;
	
	/* Соединяем с витриной */
	proc fedsql sessref=casauto;
		create table casuser.new_product_abt8{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.product_id,
				t1.start_week,
				t1.week,
				t1.weeks_from_start,
				t1.count_sales_dt,
				t1.sum_qty,	
				t1.sum_qty_corrected,
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
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t1.CPI,
				t1.GPD,
				t1.RDI,
				/*
				t1.BOGO,
				t1.Bundle,
				t1.Discount,
				t1.EVMSet,
				t1.GiftforpurchaseforordersaboveXru,
				t1.Giftforpurchaseforproduct,
				t1.GiftforpurchaseNonProduct,
				t1.GiftforpurchaseSampling,
				t1.NPPromoSupport,
				t1.OtherDiscountforvolume,
				t1.Pairs,
				t1.Pairsdifferentcategories,
				t1.Productlineextension,
				t1.ProductnewlaunchLTO,
				t1.ProductnewlaunchPermanentinclite,
				t1.Productrehitsameproductnolineext,
				t1.Temppricereductiondiscount,
				t1.Undefined,
				*/
				&promo_list_sql.
				,t2.PRECIPITATION,
				t2.TEMPERATURE
		from
			casuser.new_product_abt7 as t1
		left join
			casuser.ia_weather2 as t2
		on
			t1.pbo_location_id = t2.pbo_location_id and
			t1.week = t2.week
		;
	quit;
	
	proc casutil;
		droptable casdata="new_product_abt7" incaslib="casuser" quiet;
		droptable casdata="ia_weather2" incaslib="casuser" quiet;
	run;
	
	
	/****** 12. TRP конкурентов ******/
	proc casutil;
		droptable casdata="comp_media_ml" incaslib="casuser" quiet;
		droptable casdata="comp_transposed_ml" incaslib="casuser" quiet;
		droptable casdata="comp_transposed_ml_expand" incaslib="casuser" quiet;
		droptable casdata="comp_transposed_ml_expand2" incaslib="casuser" quiet;
		droptable casdata="new_product_abt9" incaslib="casuser" quiet;
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.comp_media_ml{options replace=true} as 
			select
				COMPETITOR_CD,
				TRP,
				report_dt as report_dt
			from 
				&lmvInCaslib..COMP_MEDIA
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
	
	proc fedsql sessref=casauto;
		create table casuser.new_product_abt9{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.product_id,
				t1.start_week,
				t1.week,
				t1.weeks_from_start,
				t1.count_sales_dt,
				t1.sum_qty,	
				t1.sum_qty_corrected,
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
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t1.CPI,
				t1.GPD,
				t1.RDI,
				/*
				t1.BOGO,
				t1.Bundle,
				t1.Discount,
				t1.EVMSet,
				t1.GiftforpurchaseforordersaboveXru,
				t1.Giftforpurchaseforproduct,
				t1.GiftforpurchaseNonProduct,
				t1.GiftforpurchaseSampling,
				t1.NPPromoSupport,
				t1.OtherDiscountforvolume,
				t1.Pairs,
				t1.Pairsdifferentcategories,
				t1.Productlineextension,
				t1.ProductnewlaunchLTO,
				t1.ProductnewlaunchPermanentinclite,
				t1.Productrehitsameproductnolineext,
				t1.Temppricereductiondiscount,
				t1.Undefined,
				*/
				&promo_list_sql.
				,t1.PRECIPITATION,
				t1.TEMPERATURE,
				coalesce(t2.comp_trp_BK, 0) as comp_trp_BK,
				coalesce(t2.comp_trp_KFC, 0) as comp_trp_KFC
		from
			casuser.new_product_abt8 as t1
		left join
			casuser.comp_transposed_ml as t2
		on
			t1.week = t2.REPORT_DT
		;
	quit;
	
	proc casutil;
		droptable casdata="comp_media_ml" incaslib="casuser" quiet;
		droptable casdata="comp_transposed_ml" incaslib="casuser" quiet;
		droptable casdata="comp_transposed_ml_expand" incaslib="casuser" quiet;
		droptable casdata="comp_transposed_ml_expand2" incaslib="casuser" quiet;
		droptable casdata="new_product_abt8" incaslib="casuser" quiet;
	run;
	
	
	/****** 13. Медиа поддержка ******/
	proc casutil;
	  droptable casdata="ia_promo_x_pbo_leaf" incaslib="casuser" quiet;
	  droptable casdata="ia_promo_x_product_leaf" incaslib="casuser" quiet;
	  droptable casdata="promo_ml_trp" incaslib="casuser" quiet;
	  droptable casdata="promo_ml_trp_expand" incaslib="casuser" quiet;
	  droptable casdata="sum_trp" incaslib="casuser" quiet;
	  droptable casdata="sum_trp2" incaslib="casuser" quiet;
	  droptable casdata="new_product_abt10" incaslib="casuser" quiet;
	run;
	
	/* Добавляем trp к таблице промо */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml_trp{options replace = true} as 
			select 
				t1.PROMO_ID,
				t1.product_ID,
				t1.pbo_location_id,
				t1.start_dt,
				t1.end_dt,
				mean(t2.TRP) as mean_trp
			from
				casuser.promo_ml as t1 
			left join
				&lmvInCaslib..media as t2
			on
				t1.PROMO_GROUP_ID = t2.PROMO_GROUP_ID and
				datepart(t2.report_dt) <= t1.end_dt and
				datepart(t2.report_dt) >= t1.start_dt
			group by
				t1.PROMO_ID,
				t1.product_ID,
				t1.PBO_Location_ID,
				t1.start_dt,
				t1.end_dt
		;
	quit;
	
	/* Расшиваем по дням */
	data casuser.promo_ml_trp_expand;
		set casuser.promo_ml_trp;
		do sales_dt=start_dt to end_dt;
			output;
		end;
	run;
	
	/* Суммируем TRP от всех промо */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml_trp2{options replace=true} as
			select
				product_id,
				pbo_Location_id,
				sales_dt,
				sum(mean_trp) as sum_trp
			from
				casuser.promo_ml_trp_expand as t1
			group by
				product_id,
				pbo_location_id,
				sales_dt			
		;
	quit;
	
	/* Добавляем неделю и усредняем по неделям */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml_trp3{options replace=true} as 
			select
				t1.PRODUCT_ID,
				t1.PBO_location_id,
				t1.week,
				mean(t1.sum_trp) as sum_trp
			from (
				select
					t1.PRODUCT_ID,
					t1.PBO_location_ID,
					cast(intnx('week.2',t1.sales_dt,0,'b') as date)  as week,
					t1.sum_trp
				from
					casuser.promo_ml_trp2 as t1
			) as t1
			group by
				t1.PRODUCT_ID,
				t1.PBO_Location_ID,
				t1.week			
		;
	quit;
	
	proc fedsql sessref=casauto;
		create table casuser.new_product_abt10{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.product_id,
				t1.start_week,
				t1.week,
				t1.weeks_from_start,
				t1.count_sales_dt,
				t1.sum_qty,	
				t1.sum_qty_corrected,
				month(t1.week) as month,
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
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t1.CPI,
				t1.GPD,
				t1.RDI,
				/*
				t1.BOGO,
				t1.Bundle,
				t1.Discount,
				t1.EVMSet,
				t1.GiftforpurchaseforordersaboveXru,
				t1.Giftforpurchaseforproduct,
				t1.GiftforpurchaseNonProduct,
				t1.GiftforpurchaseSampling,
				t1.NPPromoSupport,
				t1.OtherDiscountforvolume,
				t1.Pairs,
				t1.Pairsdifferentcategories,
				t1.Productlineextension,
				t1.ProductnewlaunchLTO,
				t1.ProductnewlaunchPermanentinclite,
				t1.Productrehitsameproductnolineext,
				t1.Temppricereductiondiscount,
				t1.Undefined,
				*/
				&promo_list_sql.,
				t1.PRECIPITATION,
				t1.TEMPERATURE,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				coalesce(t2.sum_trp, 0) as sum_trp
		from
			casuser.new_product_abt9 as t1
		left join
			casuser.promo_ml_trp3 as t2
		on
			t1.product_id = t2.product_id and	
			t1.pbo_location_id = t2.pbo_location_id and
			t1.week = t2.week
		;
	quit;
	
	proc casutil;
		droptable casdata="ia_promo_x_pbo_leaf" incaslib="casuser" quiet;
		droptable casdata="ia_promo_x_product_leaf" incaslib="casuser" quiet;
		droptable casdata="promo_ml_trp" incaslib="casuser" quiet;
		droptable casdata="promo_ml_trp_expand" incaslib="casuser" quiet;
		droptable casdata="sum_trp2" incaslib="casuser" quiet;
		droptable casdata="sum_trp3" incaslib="casuser" quiet;
		droptable casdata="IA_media" incaslib="casuser" quiet;
		droptable casdata="IA_promo" incaslib="casuser" quiet;
		droptable casdata="ia_promo_x_product" incaslib="casuser" quiet;
		droptable casdata="ia_promo_x_pbo" incaslib="casuser" quiet;
		droptable casdata="new_product_abt9" incaslib="casuser" quiet;
	run;
	
	/****** 14. Добавляем информацию о доступной истории продаж товара ******/
	proc casutil;
		droptable casdata="new_product_abt11" incaslib="casuser" quiet;
	run;
	
	options nomlogic nomprint nosymbolgen nosource nonotes;
	
	data casuser.new_product_abt11;
		set casuser.new_product_abt10;
		by PRODUCT_ID PBO_LOCATION_ID CHANNEL_CD week;
		array all_row_values{512} _temporary_;
		retain i;
		drop i l_sum_qty;
		if first.CHANNEL_CD then do; /* первое наблюдение, сбрасываем массив и счетчик массива */
			call missing(of all_row_values{*});
			i=1;
		end;
		l_sum_qty=lag(sum_qty); /* мы считаем агрегат на шаге i по предыдущим известным значениям без текущего */
		if i=1 then l_sum_qty=.;
		all_row_values{i}=l_sum_qty;
		avg=mean(of all_row_values{*});
		std=std(of all_row_values{*});
		med=median(of all_row_values{*});
		pcnt10=pctl(10,of all_row_values{*});
		pcnt25=pctl(25,of all_row_values{*});
		pcnt75=pctl(75,of all_row_values{*});
		pcnt90=pctl(90,of all_row_values{*});
		min=min(of all_row_values{*});
		max=max(of all_row_values{*});
		range=max-min;
		cv=divide(std,avg);
		i+1;
	run;
	
	options mlogic mprint symbolgen source notes;
	
	proc casutil;
		droptable casdata="new_product_abt10" incaslib="casuser" quiet;
	run;
	
	/****** 15. Добавление цены ******/
	
	proc casutil;
	  droptable casdata="price_ml2" incaslib="casuser" quiet;
	  droptable casdata="new_product_abt12" incaslib="casuser" quiet;
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.price_ml2{option replace=true} as
			select
				t1.pbo_location_id,
				t1.product_id,
				t1.week,
				mean(t2.gross_price_amt) as mean_price
			from
				casuser.new_product_abt11 as t1
			left join
				&lmvInCaslib..price_ml as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.product_id = t2.product_id and
				t1.week <= t2.end_dt and
				t1.week >= t2.start_dt
			group by
				t1.pbo_location_id,
				t1.product_id,
				t1.week
		;
	quit;
	
	/* Добавляем к продажам цены */
	proc fedsql sessref=casauto; 
		create table casuser.new_product_abt12{options replace=true} as 
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.product_id,
				t1.start_week,
				t1.week,
				t1.weeks_from_start,
				t1.count_sales_dt,
				t1.sum_qty,	
				t1.sum_qty_corrected,
				t1.month,
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
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t1.CPI,
				t1.GPD,
				t1.RDI,
				/*
				t1.BOGO,
				t1.Bundle,
				t1.Discount,
				t1.EVMSet,
				t1.GiftforpurchaseforordersaboveXru,
				t1.Giftforpurchaseforproduct,
				t1.GiftforpurchaseNonProduct,
				t1.GiftforpurchaseSampling,
				t1.NPPromoSupport,
				t1.OtherDiscountforvolume,
				t1.Pairs,
				t1.Pairsdifferentcategories,
				t1.Productlineextension,
				t1.ProductnewlaunchLTO,
				t1.ProductnewlaunchPermanentinclite,
				t1.Productrehitsameproductnolineext,
				t1.Temppricereductiondiscount,
				t1.Undefined,
				*/
				&promo_list_sql.,
				t1.PRECIPITATION,
				t1.TEMPERATURE,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.avg,
				t1.std,
				t1.med,
				t1.pcnt10,
				t1.pcnt25,
				t1.pcnt75,
				t1.pcnt90,
				t1.min,
				t1.max,
				t1.range,
				t1.cv,
				t2.mean_price
			from
				casuser.new_product_abt11 as t1
			left join
				casuser.price_ml2 as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.product_id = t2.product_id and
				t1.week = t2.week
		;
	quit;
	
	proc casutil;
	  droptable casdata="price_ml2" incaslib="casuser" quiet;
	  droptable casdata="new_product_abt11" incaslib="casuser" quiet;
	run;
	
	
	/****** 16. Добавляем рецептуру ******/
	proc casutil;
		droptable casdata="ingridients_transposed" incaslib="casuser" quiet;
		droptable casdata="ingridients_transposed2" incaslib="casuser" quiet;
		droptable casdata="ingridients_pca" incaslib="casuser" quiet;
		droptable casdata="ingridients_pca2" incaslib="casuser" quiet;
		droptable casdata="ingridients_pca3" incaslib="casuser" quiet;
		droptable casdata="new_product_abt13" incaslib="casuser" quiet;
	run;
	
	/* Транспонируем рецептуру */
	proc cas;
	transpose.transpose /
		table = {
			name="ingridients",
			caslib="&lmvInCaslib.",
			groupby={"MONTH_DT", "PRODUCT_ID"}
		}
		prefix="item_"
		transpose={"FACTOR"} 
		id={"ASMB_ITEM_ID"} 
		casout={name="ingridients_transposed", caslib="casuser", replace=true};
	quit;
	
	/* Заменяем пропуски нулями */
	data casuser.ingridients_transposed2;
		set casuser. ingridients_transposed;
		array _xxx_ _numeric_;
		do i=1 to dim( _xxx_);
			if missing(_xxx_[i]) then _xxx_[i]=0;
		end;
		drop i _name_ _label_;
	run;
	
	/* Создаем список переменных (за исключением ID) */
	proc contents data=casuser.ingridients_transposed2 noprint out=_contents_;
	run;
	
	/* Сохраняем список переменных в макропременные */
	proc sql noprint;
		select name into :names separated by ' ' from _contents_ where upcase(name) not in ('PRODUCT_ID', 'MONTH_DT');
		select name into :sql_names separated by ',' from _contents_ where upcase(name) not in ('PRODUCT_ID', 'MONTH_DT');
	quit;
	
	ods _all_ close;
	/* Выполняем проекцию на меньшее подпространство */
	proc pca data=casuser.ingridients_transposed2 n=30 plots=none;
		var &names.;
		output out=casuser.ingridients_pca copyvars=(month_dt product_id);
	run;
	
	/* Меняем тип datetime на date */
	proc fedsql	sessref=casauto;
		create table casuser.ingridients_pca2{options replace=true} as
			select
				datepart(t2.month_dt) as month_dt,
				t2.product_id,
				t2.Score1,
				t2.Score2,
				t2.Score3,
				t2.Score4,
				t2.Score5,
				t2.Score6,
				t2.Score7,
				t2.Score8,
				t2.Score9,
				t2.Score10,
				t2.Score11,
				t2.Score12,
				t2.Score13,
				t2.Score14,
				t2.Score15,
				t2.Score16,
				t2.Score17,
				t2.Score18,
				t2.Score19,
				t2.Score20,
				t2.Score21,
				t2.Score22,
				t2.Score23,
				t2.Score24,
				t2.Score25,
				t2.Score26,
				t2.Score27,
				t2.Score28,
				t2.Score29,
				t2.Score30
			from
				casuser.ingridients_pca as t2
		;
	quit;
	
	/* Протягиваем рецептру на два года вперед */
	proc cas;
	timeData.timeSeries result =r /
		series={
			{name="Score1", setmiss="PREV"},
			{name="Score2", setmiss="PREV"},
			{name="Score3", setmiss="PREV"},
			{name="Score4", setmiss="PREV"},
			{name="Score5", setmiss="PREV"},
			{name="Score6", setmiss="PREV"},
			{name="Score7", setmiss="PREV"},
			{name="Score8", setmiss="PREV"},
			{name="Score9", setmiss="PREV"},
			{name="Score10", setmiss="PREV"},
			{name="Score11", setmiss="PREV"},
			{name="Score12", setmiss="PREV"},
			{name="Score13", setmiss="PREV"},
			{name="Score14", setmiss="PREV"},
			{name="Score15", setmiss="PREV"},
			{name="Score16", setmiss="PREV"},
			{name="Score17", setmiss="PREV"},
			{name="Score18", setmiss="PREV"},
			{name="Score19", setmiss="PREV"},
			{name="Score20", setmiss="PREV"},
			{name="Score21", setmiss="PREV"},
			{name="Score22", setmiss="PREV"},
			{name="Score23", setmiss="PREV"},
			{name="Score24", setmiss="PREV"},
			{name="Score25", setmiss="PREV"},
			{name="Score26", setmiss="PREV"},
			{name="Score27", setmiss="PREV"},
			{name="Score28", setmiss="PREV"},
			{name="Score29", setmiss="PREV"},
			{name="Score30", setmiss="PREV"}
		}
		tEnd= "&FC_END"
		table={
			caslib="casuser",
			name="ingridients_pca2",
			groupby={"PRODUCT_ID"}
		}
		timeId="month_dt"
		trimId="LEFT"
		interval="month"
		casOut={caslib="casuser", name="ingridients_pca3", replace=True}
		;
	run;
	quit;
	
	/* Добавляем рецептуру к витрине */
	proc fedsql sessref=casauto;
		create table casuser.new_product_abt13{options replace = true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.product_id,
				t1.start_week,
				t1.week,
				t1.weeks_from_start,
				t1.count_sales_dt,
				t1.sum_qty,	
				t1.sum_qty_corrected,
				t1.month,
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
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t1.CPI,
				t1.GPD,
				t1.RDI,
				/*
				t1.BOGO,
				t1.Bundle,
				t1.Discount,
				t1.EVMSet,
				t1.GiftforpurchaseforordersaboveXru,
				t1.Giftforpurchaseforproduct,
				t1.GiftforpurchaseNonProduct,
				t1.GiftforpurchaseSampling,
				t1.NPPromoSupport,
				t1.OtherDiscountforvolume,
				t1.Pairs,
				t1.Pairsdifferentcategories,
				t1.Productlineextension,
				t1.ProductnewlaunchLTO,
				t1.ProductnewlaunchPermanentinclite,
				t1.Productrehitsameproductnolineext,
				t1.Temppricereductiondiscount,
				t1.Undefined,
				*/
				&promo_list_sql.,
				t1.PRECIPITATION,
				t1.TEMPERATURE,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.avg,
				t1.std,
				t1.med,
				t1.pcnt10,
				t1.pcnt25,
				t1.pcnt75,
				t1.pcnt90,
				t1.min,
				t1.max,
				t1.range,
				t1.cv,
				t1.mean_price,
				t2.Score1,
				t2.Score2,
				t2.Score3,
				t2.Score4,
				t2.Score5,
				t2.Score6,
				t2.Score7,
				t2.Score8,
				t2.Score9,
				t2.Score10
			from
				casuser.new_product_abt12 as t1
			left join
				casuser.ingridients_pca3 as t2
			on
				t1.product_id = t2.product_id and
				intnx('month',t1.week,0,'BEGINNING') = t2.month_dt
		;
	quit;
	
	
	proc casutil;
		droptable casdata="ia_ingridients" incaslib="casuser" quiet;
		droptable casdata="ingridients_transposed" incaslib="casuser" quiet;
		droptable casdata="ingridients_transposed2" incaslib="casuser" quiet;
		droptable casdata="ingridients_pca" incaslib="casuser" quiet;
		droptable casdata="ingridients_pca2" incaslib="casuser" quiet;
		droptable casdata="ingridients_pca3" incaslib="casuser" quiet;
		droptable casdata="new_product_abt12" incaslib="casuser" quiet;
		promote casdata="new_product_abt13" incaslib="casuser" outcaslib="casuser";
	run;
	
	
	/****** 17. Разделение на обучение и скоринг ******/
	proc fedsql sessref=casauto;
		create table casuser.npf_scoring{options replace=true} as
			select
				t1.*
			from
				casuser.new_product_abt13 as t1
			where
				t1.week >= &VF_FC_START_DT.
		;
		create table casuser.npf_train{options replace=true} as
			select
				t1.*
			from
				casuser.new_product_abt13 as t1
			where
				t1.week < &VF_FC_START_DT.
		;
	quit;
	
	
	/****** 18. Обучение модели ******/
	proc casutil;
		droptable casdata="models_npf" incaslib="casuser" quiet;
	run;
	
	%let pmix_default_params = seed=12345 loh=0 binmethod=QUANTILE 
		 maxbranch=2 
		 assignmissing=useinsearch 
		 minuseinsearch=5
		 ntrees=50
		 maxdepth=10
		 inbagfraction=0.6
		 minleafsize=5
		 numbin=50
		 printtarget
	;
	
	proc forest data=casuser.npf_train
		&pmix_default_params.;
		input 
			weeks_from_start
			mean_price
			CPI
			GPD
			RDI
			/*
			BOGO
			Bundle
			Discount
			EVMSet
			GiftforpurchaseforordersaboveXru
			Giftforpurchaseforproduct
			GiftforpurchaseNonProduct
			GiftforpurchaseSampling
			NPPromoSupport
			OtherDiscountforvolume
			Pairs
			Pairsdifferentcategories
			Productlineextension
			ProductnewlaunchLTO
			ProductnewlaunchPermanentinclite
			Productrehitsameproductnolineext
			Temppricereductiondiscount
			Undefined
			*/
			&promo_list.
			PRECIPITATION
			TEMPERATURE
			comp_trp_BK
			comp_trp_KFC
			sum_trp
			avg
			std
			med
			pcnt10
			pcnt25
			pcnt75
			pcnt90
			min
			max
			range
			cv
			Score1
			Score2
			Score3
			Score4
			Score5
			Score6
			Score7
			Score8
			Score9
			Score10 / level = interval;
		input
			month
			lvl3_id
			lvl2_id
			agreement_type
			breakfast
			building_type
			company
			delivery
			drive_thru
			mccafe_type
			price_level
			window_type
			prod_lvl4_id
			prod_lvl3_id
			prod_lvl2_id
			hero
			item_size
			offer_type
			price_tier / level = nominal;
		id channel_cd week;
		target sum_qty_corrected / level = interval;
		grow VARIANCE;
		savestate rstore=casuser.models_npf;
	run;
	
	proc casutil;
		promote casdata="models_npf" incaslib="casuser" outcaslib="casuser";
	run;
	
	
	/****** 19. Скоринг ******/
	proc casutil;
		droptable casdata="npf_scoring_pred" incaslib="casuser" quiet;
	run;
	proc astore;
	  score data=casuser.npf_scoring
	  copyvars=(pbo_location_id  product_id sum_qty_corrected PROD_LVL2_ID month)
	  rstore=casuser.models_npf
	  out=casuser.npf_scoring_pred;
	quit;
	
	
	/****** 20. Деление прогноза по дням ******/
	
	proc casutil;
		droptable casdata="npf_frame" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_mean" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_mean2" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile2" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile3" incaslib="casuser" quiet;
		droptable casdata="npf_scoring_pred_day" incaslib="casuser" quiet;
	run;
	
	/* Делим прогноз по дням */
	proc fedsql sessref=casauto;
		create table casuser.npf_scoring_pred_day{options replace=true} as
			select
				t1.channel_cd,
				t1.PRODUCT_ID,
				t1.PBO_LOCATION_ID,
				t1.PROD_LVL2_ID,
				t1.WEEK,
				t2.weekday,
				(case
					when t2.weekday = 1 then cast((t1.week + 6) as date)
					else cast((t1.week + t2.weekday - 2) as date)
				end) as sales_dt,
				t1.MONTH,
				t1.sum_qty_corrected as week_sum_qty,
				t1.P_sum_qty_corrected as week_p_sum_qty,
				t1.p_sum_qty_corrected * t2.weekday_profile as p_sum_qty
			from
				casuser.npf_scoring_pred as t1
			left join
				casuser.npf_weekday_profile4 as t2
			on
				t1.month = t2.month and
				t1.PROD_LVL2_ID = t2.PROD_LVL2_ID 
		;
	quit;

	
	/****** 21. Пересекаем с ассортиментной матрицей и product chain ******/
	proc casutil;
		droptable casdata="npf_prediction" incaslib="casuser" quiet;
	run;

	/* Удаляем дубли из product_chain на будущее */
	proc fedsql sessref=casauto;
		create table casuser.future_product_scoring_distinct{options replace=true} as
			select distinct
				channel_cd,
				product_id,
				pbo_location_id,
				sales_dt
			from
				casuser.future_product_scoring as t1			
		;
	quit;

	/* Расшиваем интервалы в ассортиментой матрице */
	data casuser.assort_matrix_daily;
		set mn_short.assort_matrix;
		format sales_dt DATE9.;
		do sales_dt=start_dt to end_dt;
			output;
		end;
	run;
	
	/* Удаляем дубли */
	proc fedsql sessref=casauto;
		create table casuser.assort_matrix_daily_unique{option replace=true} as
			select distinct
				pbo_location_id,
				product_id,
				sales_dt
			from
				casuser.assort_matrix_daily
		;
	quit;
	
	/* Оставляем прогноз на товар,
	 если есть прогноз он есть в АМ или есть запись в product chain с lifecycle=N */
	proc fedsql sessref=casauto;
		create table casuser.npf_prediction{options replace=true} as
			select
				t1.channel_cd,
				t1.PRODUCT_ID,
				t1.PBO_LOCATION_ID,
				t1.PROD_LVL2_ID,
				t1.WEEK,
				t1.weekday,
				t1.sales_dt,
				t1.MONTH,
				t1.week_sum_qty,
				t1.week_p_sum_qty,
				t1.p_sum_qty
			from
				casuser.npf_scoring_pred_day as t1
			left join
				casuser.assort_matrix_daily_unique as t2
			on
				t1.product_id = t2.product_id and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt
			left join
				casuser.future_product_scoring_distinct as t3
			on
				t1.product_id = t3.product_id and
				t1.pbo_location_id = t3.pbo_location_id and
				t1.sales_dt = t3.sales_dt
			where
				t2.product_id is not missing or
				t3.product_id is not missing
		;
	quit;	
	
	proc casutil;
		droptable casdata="assort_matrix_daily_unique" incaslib="casuser" quiet;
		droptable casdata="assort_matrix_daily" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile4" incaslib="casuser" quiet;
		promote casdata="npf_prediction" incaslib="casuser" outcaslib="casuser";
	run;


%mend vf_new_product;