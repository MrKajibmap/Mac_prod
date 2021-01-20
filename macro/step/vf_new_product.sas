%macro vf_new_product(mpInCaslib=casshort);
	option dsoptions=nonote2err;
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
	/* ��������� Lifecycle = N � ������ ���� ��� */
	proc fedsql sessref=casauto;
		create table casuser.n_product_chain{options replace=true} as
			select
				LIFECYCLE_CD,
				PREDECESSOR_PRODUCT_ID,
				PREDECESSOR_DIM2_ID,
				SUCCESSOR_PRODUCT_ID,
				SUCCESSOR_DIM2_ID,
				datepart(SUCCESSOR_START_DT) as SUCCESSOR_START_DT,
				datepart(PREDECESSOR_END_DT) as PREDECESSOR_END_DT,
				SCALE_FACTOR_PCT
			from
				&lmvInCaslib..product_chain
			where
				LIFECYCLE_CD = 'N'
		;
	quit;

	/* ���������� ��������� ������ */
	data casuser.n_product_chain_expand;
		set casuser.n_product_chain;
		keep PREDECESSOR_PRODUCT_ID period_dt;
		format period_dt DATE9.;
		period_dt=SUCCESSOR_START_DT;
		do until (period_dt>=PREDECESSOR_END_DT);
			output;
			period_dt=intnx('day',period_dt,1,'b');
		end;
	run;

	/* ��������� ���������� ���� �����-���� */
	proc fedsql sessref=casauto;
		create table casuser.n_product_chain_union{options replace=true} as
			select distinct
				PREDECESSOR_PRODUCT_ID,
				period_dt
			from
				casuser.n_product_chain_expand
		;
	quit;

	/* ��������������� ������� ��� �������� ���������� ������ */
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

	/* ������� ��������� ������ */
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

	/* ��������� ������� ������ � ����������� ���������� */
	proc fedsql sessref=casauto; 
		create table casuser.pmix_sales{options replace=true} as
			select 
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				datepart(t1.SALES_DT) as sales_dt,
				coalesce(sum(t1.SALES_QTY,t1.SALES_QTY_PROMO), 0) as sum_qty,
				divide(coalesce(t1.GROSS_SALES_AMT, 0), coalesce(sum(t1.SALES_QTY,t1.SALES_QTY_PROMO), 0)) as price
	 from &lmvInCaslib..pmix_sales as t1 
			where
				&lmvFilter and
				t1.SALES_DT >= &vf_hist_start_dt and
				t1.SALES_DT <= date %tslit(&vf_hist_end_dt) 
	;
	quit;

	/* ���������� ������� � ��������� */
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

	/* ��������� ������������ PLM � ��������������� ��������� */
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

	/* ������� �������� ���� ������ (����� ������� ���������) */
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

	/* ��������� ������ ����� ������ (������ �� 2 ������ 2017) */
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

	/* ������� �������� ��������� */
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


	/****** 2. �������� ��������� ������� ******/
	proc casutil;
		droptable casdata="real_end_interval" incaslib="casuser" quiet;
		droptable casdata="new_product_train" incaslib="casuser" quiet;
	run;

	/* �������� �������� ������ ��������� ����� ������� ��� ����� */
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

	/* ��������� ������� */
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


	/****** 3. �������� ������������ ���������� ������� � ��������� ������� *******/

	/* 3.2 �������� ������������ ��� ���������� ������� */
	proc casutil;
		droptable casdata="npf_frame" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_mean" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_mean2" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile2" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile3" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile4" incaslib="casuser" quiet;
	run;

	/* ������� ������ */
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

	/* ��������� ��������� ������� � ������� ��������� ������� */
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

	/* �� ��� ���� ��������� ���������� ����������, ����� ���������� ��������� ������� */
	/* ��� ����� ��������� �������� ��������� �������, ����������� �� ��������� ������ */
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

	/* ������ ���� ����������, ������� ������������ ���������� */
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

	/* ���������� ������� */
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


	/* 3.3 ��������� ����������� ������������ � ��������� ������� */
	/* ��������� ��������� �������, ���� ������ � ����� */
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

	/* �������� ����������� ���������� ������� */
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

	/****** 3. ������� ����������� ������� ******/
	proc casutil;
		droptable casdata="last_day_ts" incaslib="casuser" quiet;
		droptable casdata="future_assort_matrix" incaslib="casuser" quiet;
		droptable casdata="new_product_scoring" incaslib="casuser" quiet;
		droptable casdata="existing_product_scoring" incaslib="casuser" quiet;
		droptable casdata="existing_product_scoring2" incaslib="casuser" quiet;
		droptable casdata="future_product_scoring" incaslib="casuser" quiet;
		droptable casdata="new_product_abt" incaslib="casuser" quiet;
	run;
	/* ���������� ��������� ����, ������������� � ��������� ���� ������� */
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

	/* ����������� ��������� ���� �� ��� */
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

	/* ����� �� product_chain ����� ������, �������� ����������� � �������� */
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

	/* �������� ��� ��������� */
	data casuser.future_product_scoring;
		set casuser.future_product_chain;
		drop period_end_dt;
		format sales_dt DATE9.;
		channel_cd = 'ALL';
		do sales_dt = period_start_dt to period_end_dt;
		   output;
		end;
	run;

	/* ���������� ����������� ������� ��� ����� ������� ��� ������� � � �������� */
	data casuser.new_product_scoring;
		set casuser.future_product_scoring casuser.existing_product_scoring;
	run;

	/* ������� ��������� � ����������� ������� */
	proc fedsql sessref=casauto;
		create table casuser.new_product_scoring{options replace=true} as
			select
				channel_cd,
				pbo_location_id,
				product_id,
				sales_dt,
				mean(sum_qty) as sum_qty,
				max(PERIOD_START_DT) as PERIOD_START_DT /* ���� ������ ����� ��������� */
			from
				casuser.new_product_scoring
			group by
				channel_cd,
				pbo_location_id,
				product_id,
				sales_dt
		;
	quit;

	/* ���������� ����������� ������� � ��������� */
	data casuser.new_product_abt;
	set casuser.new_product_scoring casuser.new_product_train3;
	run;

	proc casutil;
		droptable casdata="last_day_ts" incaslib="casuser" quiet;
		droptable casdata="existing_product_scoring" incaslib="casuser" quiet;
		droptable casdata="new_product_scoring" incaslib="casuser" quiet;
	run;

	/****** 4. ��������� �� ������ ******/
	proc casutil;
		droptable casdata="week_start" incaslib="casuser" quiet;
		droptable casdata="week_aggr" incaslib="casuser" quiet;
	run;

	/* ����������� ���� � ������ */
	proc fedsql sessref=casauto;
		create table casuser.week_start{options replace=true} as
			select
				channel_cd,
				pbo_location_id,
				product_id,
				PERIOD_START_DT,
				cast(intnx('week.2',PERIOD_START_DT,0,'b') as date)  as start_week,
				cast(intnx('week.2',sales_dt,0,'b') as date)  as week,
				sum_qty,
				sales_dt,
				weekday_profile
			from
				casuser.new_product_abt
		;
	quit;

	/* ������ ���������� ���� ������ ������ � ������ � ��������� ������� �� ������ */
	proc fedsql sessref=casauto;
		create table casuser.week_aggr{options replace=true} as
			select
				channel_cd,
				pbo_location_id,
				product_id,
				PERIOD_START_DT,
				start_week,
				week,
				count(sales_dt) as count_sales_dt,
				sum(sum_qty) as sum_qty,
				sum(weekday_profile) as sum_weekday_profile		
			from
				casuser.week_start
			group by
				channel_cd,
				pbo_location_id,
				product_id,
				PERIOD_START_DT,
				start_week,
				week
		;
	quit;


	/****** 6. ���������� ������� � ������ ������ ******/
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


	/****** 7. ������� ���������� ������ � ������ ������ ******/
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

	/****** 7. ��������� �������� ��������� ******/
	proc casutil;
	  droptable casdata="new_product_abt4" incaslib="casuser" quiet;
	run;

	/* ������������� ��������� ����������. */
	%macro text_encoding(table, variable);
		/*
		���������:
			table : ������� � ������� ����� �������� ��������� ����������
			variable : �������� ��������� ����������
		�����:
			* ������� table � �������������� �������� variable_id
			* ������� encoding_variable � ��������� id � ������ ���������
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

	%text_encoding(&lmvInCaslib..pbo_dictionary, A_AGREEMENT_TYPE);
	%text_encoding(&lmvInCaslib..pbo_dictionary, A_BREAKFAST);
	%text_encoding(&lmvInCaslib..pbo_dictionary, A_BUILDING_TYPE);
	%text_encoding(&lmvInCaslib..pbo_dictionary, A_COMPANY);
	%text_encoding(&lmvInCaslib..pbo_dictionary, A_DELIVERY);
	%text_encoding(&lmvInCaslib..pbo_dictionary, A_MCCAFE_TYPE);
	%text_encoding(&lmvInCaslib..pbo_dictionary, A_PRICE_LEVEL);
	%text_encoding(&lmvInCaslib..pbo_dictionary, A_DRIVE_THRU);
	%text_encoding(&lmvInCaslib..pbo_dictionary, A_WINDOW_TYPE);

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
				&lmvInCaslib..pbo_dictionary as t2
			on
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;

	proc casutil;
	  droptable casdata="new_product_abt3" incaslib="casuser" quiet;
	run;


	/****** 8. ��������� �������� ������ ******/
	proc casutil;
	  droptable casdata="new_product_abt5" incaslib="casuser" quiet;
	run;
	  
	%text_encoding(&lmvInCaslib..product_dictionary, a_hero);
	%text_encoding(&lmvInCaslib..product_dictionary, a_item_size);
	%text_encoding(&lmvInCaslib..product_dictionary, a_offer_type);
	%text_encoding(&lmvInCaslib..product_dictionary, a_price_tier);

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
			&lmvInCaslib..product_dictionary as t2
		on
			t1.product_id = t2.product_id
		;
	quit;

	proc casutil;
	  droptable casdata='new_product_abt4' incaslib='casuser' quiet;
	run;


	/****** 9. ��������� �������������� ******/
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

	/* ��������� �������������� �� ������� */
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

	/* ��������� �������������� */
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


	/****** 10. ����� ******/
	proc casutil;
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
		droptable casdata="promo_transposed2" incaslib="casuser" quiet;
		droptable casdata="promo_transposed3" incaslib="casuser" quiet;
		droptable casdata="promo_transposed4" incaslib="casuser" quiet;
		droptable casdata="promo_transposed5" incaslib="casuser" quiet;
		droptable casdata="promo_transposed6" incaslib="casuser" quiet;
		droptable casdata="new_product_abt7" incaslib="casuser" quiet;
		droptable casdata="ia_promo_x_product_leaf" incaslib="casuser" quiet;
		droptable casdata="ia_promo_x_pbo_leaf" incaslib="casuser" quiet;
		droptable casdata="promo_ml_main_code" incaslib="casuser" quiet;
		droptable casdata="abt_promo" incaslib="casuser" quiet;
	run;

	/* ������� ������� ����������� PBO �� �������� ������ � �� ����� ������ */
	proc fedsql sessref=casauto;
		create table casuser.lvl4{options replace=true} as 
			select distinct
				pbo_location_id as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				&lmvInCaslib..pbo_dictionary
		;
		create table casuser.lvl3{options replace=true} as 
			select distinct
				LVL3_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				&lmvInCaslib..pbo_dictionary
		;
		create table casuser.lvl2{options replace=true} as 
			select distinct
				LVL2_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				&lmvInCaslib..pbo_dictionary
		;
		create table casuser.lvl1{options replace=true} as 
			select 
				1 as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				&lmvInCaslib..pbo_dictionary
		;
	quit;

	/* ��������� � ������ ���������� ��� */
	data casuser.pbo_lvl_all;
		set casuser.lvl4 casuser.lvl3 casuser.lvl2 casuser.lvl1;
	run;

	/* ������� ������� ����������� ������ �� �������� ������ � �� ����� ������ */
	proc fedsql sessref=casauto;
		create table casuser.lvl5{options replace=true} as 
			select distinct
				product_id as product_id,
				product_id as product_leaf_id
			from
				&lmvInCaslib..product_dictionary
		;
		create table casuser.lvl4{options replace=true} as 
			select distinct
				prod_LVL4_ID as product_id,
				product_id as product_leaf_id
			from
				&lmvInCaslib..product_dictionary
		;
		create table casuser.lvl3{options replace=true} as 
			select distinct
				prod_LVL3_ID as product_id,
				product_id as product_leaf_id
			from
				&lmvInCaslib..product_dictionary
		;
		create table casuser.lvl2{options replace=true} as 
			select distinct
				prod_LVL2_ID as product_id,
				product_id as product_leaf_id
			from
				&lmvInCaslib..product_dictionary
		;
		create table casuser.lvl1{options replace=true} as 
			select distinct
				1 as product_id,
				product_id as product_leaf_id
			from
				&lmvInCaslib..product_dictionary
		;
	quit;

	/* ��������� � ������ ���������� ��� */
	data casuser.product_lvl_all;
		set casuser.lvl5 casuser.lvl4 casuser.lvl3 casuser.lvl2 casuser.lvl1;
	run;

	/* ��������� � ������� ����� ��� � ������ */
	proc fedsql sessref = casauto;
		create table casuser.ia_promo_x_pbo_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t2.PBO_LEAF_ID
			from
				&lmvInCaslib..promo_pbo as t1,
				casuser.pbo_lvl_all as t2
			where t1.pbo_location_id = t2.PBO_LOCATION_ID
		;
		create table casuser.ia_promo_x_product_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t2.product_LEAF_ID
			from
				&lmvInCaslib..promo_prod as t1,
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
				t1.PROMO_GROUP_ID,
				(case
					when t1.PROMO_MECHANICS = 'BOGO / 1+1' then 'bogo'
					when t1.PROMO_MECHANICS = 'Discount' then 'discount'
					when t1.PROMO_MECHANICS = 'EVM/Set' then 'evm_set'
					when t1.PROMO_MECHANICS = 'Non-Product Gift' then 'non_product_gift'
					when t1.PROMO_MECHANICS = 'Pairs' then 'pairs'
					when t1.PROMO_MECHANICS = 'Product Gift' then 'product_gift'
					when t1.PROMO_MECHANICS = 'Other: Discount for volume' then 'other_discount'
					when t1.PROMO_MECHANICS = 'Other: Digital (app)' then 'other_digital'
					when t1.PROMO_MECHANICS = 'NP Promo Support' then 'support'
				end) as promo_mechanics_name,
				1 as promo_flag		
			from
				&lmvInCaslib..promo as t1 
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

	/* ������������� ������� � ����� �� ����� ����� ������ */
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

	proc fedsql sessref=casauto;
		create table casuser.promo_transposed2{options replace=true} as
			select 
				product_leaf_id,
				pbo_leaf_id,
				channel_cd,
				start_dt,
				end_dt,
				coalesce(bogo, 0) as bogo,
				coalesce(discount, 0) as discount,
				coalesce(evm_set, 0) as evm_set,
				coalesce(non_product_gift, 0) as non_product_gift,
				coalesce(pairs, 0) as pairs,
				coalesce(product_gift, 0) as product_gift,
				coalesce(other_discount, 0) as other_discount,
				coalesce(other_digital, 0) as other_digital,
				coalesce(support, 0) as support
			from
				casuser.promo_transposed
		;
	quit;

	data casuser.promo_transposed3;
		set casuser.promo_transposed2;
		format sales_dt DATE9.;
		do sales_dt=start_dt to end_dt;
			output;
		end;
	run;

	/* ������� ��������� ���� ����� */
	proc fedsql sessref=casauto;
		create table casuser.promo_transposed4{options replace=true} as
			select
				t1.*,
				cast(intnx('week.2',t1.sales_dt,0,'b') as date)  as week
			from
				casuser.promo_transposed3 as t1
		;
	quit;

	/* ����������� �� ������ */
	proc fedsql sessref=casauto;
		create table casuser.promo_transposed5{options replace=true} as
			select
				t1.week,
				t1.pbo_leaf_id,
				t1.product_leaf_id,
				sum(t1.bogo) as bogo,
				sum(t1.discount) as discount,
				sum(t1.evm_set) as evm_set,
				sum(t1.non_product_gift) as non_product_gift,
				sum(t1.other_discount) as other_discount,
				sum(t1.other_digital) as other_digital,
				sum(t1.pairs) as pairs,
				sum(t1.product_gift) as product_gift,
				sum(t1.support) as support
			from
				casuser.promo_transposed4 as t1
			group by
				t1.week,
				t1.pbo_leaf_id,
				t1.product_leaf_id
		;
	quit;

	/* ��������� �� ������� */
	proc fedsql sessref=casauto;
		create table casuser.promo_transposed6{options replace=true} as
			select	
				t1.pbo_leaf_id,
				t1.product_leaf_id,
				divide(sum(t1.bogo), 7) as bogo,
				divide(sum(t1.discount), 7) as discount,
				divide(sum(t1.evm_set), 7) as evm_set,
				divide(sum(t1.non_product_gift), 7) as non_product_gift,
				divide(sum(t1.other_discount), 7) as other_discount,
				divide(sum(t1.other_digital), 7) as other_digital,
				divide(sum(t1.pairs), 7) as pairs,
				divide(sum(t1.product_gift), 7) as product_gift,
				divide(sum(t1.support), 7) as support
			from casuser.promo_transposed4 as t1
			group by
				t1.pbo_leaf_id,
				t1.product_leaf_id,
				t1.week
		;
	quit;

	/* ��������� � �������� */
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
				coalesce(t2.bogo, 0) as bogo,
				coalesce(t2.discount, 0) as discount,
				coalesce(t2.evm_set, 0) as evm_set,
				coalesce(t2.non_product_gift, 0) as non_product_gift,
				coalesce(t2.other_discount, 0) as other_discount,
				coalesce(t2.other_digital, 0) as other_digital,
				coalesce(t2.pairs, 0) as pairs,
				coalesce(t2.product_gift, 0) as product_gift,
				coalesce(t2.support, 0) as support
		from
			casuser.new_product_abt6 as t1
		left join
			casuser.promo_transposed5 as t2
		on
			t1.product_id = t2.product_leaf_id and
			t1.pbo_location_id = t2.pbo_leaf_id and
			t1.week = t2.week 
		;
	quit;

	proc casutil;
		droptable casdata="ia_pbo_loc_hierarchy" incaslib="casuser" quiet;
		droptable casdata="ia_product_hierarchy" incaslib="casuser" quiet;
		droptable casdata="ia_promo" incaslib="casuser" quiet;
		droptable casdata="ia_promo_x_pbo" incaslib="casuser" quiet;
		droptable casdata="ia_promo_x_product" incaslib="casuser" quiet;	
		droptable casdata="pbo_hier_flat" incaslib="casuser" quiet;
		droptable casdata="product_hier_flat" incaslib="casuser" quiet;
		droptable casdata="lvl5" incaslib="casuser" quiet;
		droptable casdata="lvl4" incaslib="casuser" quiet;
		droptable casdata="lvl3" incaslib="casuser" quiet;
		droptable casdata="lvl2" incaslib="casuser" quiet;
		droptable casdata="lvl1" incaslib="casuser" quiet;
		droptable casdata="promo_transposed" incaslib="casuser" quiet;
		droptable casdata="promo_transposed2" incaslib="casuser" quiet;
		droptable casdata="promo_transposed3" incaslib="casuser" quiet;
		droptable casdata="promo_transposed4" incaslib="casuser" quiet;
		droptable casdata="promo_transposed5" incaslib="casuser" quiet;
		droptable casdata="promo_transposed6" incaslib="casuser" quiet;
		droptable casdata="new_product_abt6" incaslib="casuser" quiet;
		droptable casdata="promo_ml_main_code" incaslib="casuser" quiet;
		droptable casdata="abt_promo" incaslib="casuser" quiet;
	run;


	/****** 11. ������ ******/
	proc casutil;
		droptable casdata="ia_weather2" incaslib="casuser" quiet;
		droptable casdata="new_product_abt8" incaslib="casuser" quiet;
	run;

	/* ������� ������� ����������� � ������ */
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

	/* ��������� � �������� */
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
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.other_discount,
				t1.other_digital,
				t1.pairs,
				t1.product_gift,
				t1.support,
				t2.PRECIPITATION,
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


	/****** 12. TRP ����������� ******/
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

	/* ������������� ������� */
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
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.other_discount,
				t1.other_digital,
				t1.pairs,
				t1.product_gift,
				t1.support,
				t1.PRECIPITATION,
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


	/****** 13. ����� ��������� ******/
	proc casutil;
	  droptable casdata="ia_promo_x_pbo_leaf" incaslib="casuser" quiet;
	  droptable casdata="ia_promo_x_product_leaf" incaslib="casuser" quiet;
	  droptable casdata="promo_ml_trp" incaslib="casuser" quiet;
	  droptable casdata="promo_ml_trp_expand" incaslib="casuser" quiet;
	  droptable casdata="sum_trp" incaslib="casuser" quiet;
	  droptable casdata="sum_trp2" incaslib="casuser" quiet;
	  droptable casdata="new_product_abt10" incaslib="casuser" quiet;
	run;

	/* ��������� trp � ������� ����� */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml_trp{options replace = true} as 
			select 
				t1.PROMO_ID,
				t1.product_LEAF_ID,
				t1.PBO_LEAF_ID,
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
				t1.product_LEAF_ID,
				t1.PBO_LEAF_ID,
				t1.start_dt,
				t1.end_dt
		;
	quit;

	/* ��������� �� ���� */
	data casuser.promo_ml_trp_expand;
		set casuser.promo_ml_trp;
		do sales_dt=start_dt to end_dt;
			output;
		end;
	run;

	/* ��������� TRP �� ���� ����� */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml_trp2{options replace=true} as
			select
				product_LEAF_ID,
				PBO_LEAF_ID,
				sales_dt,
				sum(mean_trp) as sum_trp
			from
				casuser.promo_ml_trp_expand as t1
			group by
				product_LEAF_ID,
				PBO_LEAF_ID,
				sales_dt			
		;
	quit;

	/* ��������� ������ � ��������� �� ������� */
	proc fedsql sessref=casauto;
		create table casuser.promo_ml_trp3{options replace=true} as 
			select
				t1.PRODUCT_LEAF_ID,
				t1.PBO_LEAF_ID,
				t1.week,
				mean(t1.sum_trp) as sum_trp
			from (
				select
					t1.PRODUCT_LEAF_ID,
					t1.PBO_LEAF_ID,
					cast(intnx('week.2',t1.sales_dt,0,'b') as date)  as week,
					t1.sum_trp
				from
					casuser.promo_ml_trp2 as t1
			) as t1
			group by
				t1.PRODUCT_LEAF_ID,
				t1.PBO_LEAF_ID,
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
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.other_discount,
				t1.other_digital,
				t1.pairs,
				t1.product_gift,
				t1.support,
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
			t1.product_id = t2.product_leaf_id and	
			t1.pbo_location_id = t2.pbo_leaf_id and
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

	/****** 14. ��������� ���������� � ��������� ������� ������ ������ ******/
	proc casutil;
		droptable casdata="new_product_abt11" incaslib="casuser" quiet;
	run;

	data casuser.new_product_abt11;
		set casuser.new_product_abt10;
		by PRODUCT_ID PBO_LOCATION_ID CHANNEL_CD week;
		array all_row_values{512} _temporary_;
		retain i;
		drop i l_sum_qty;
		if first.CHANNEL_CD then do; /*������ ����������, ���������� ������ � ������� �������*/
			call missing(of all_row_values{*});
			i=1;
		end;
		l_sum_qty=lag(sum_qty); /*�� ������� ������� �� ���� i �� ���������� ��������� ��������� ��� ��������*/
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
		cv=std/avg;
		i+1;
	run;

	proc casutil;
		droptable casdata="new_product_abt10" incaslib="casuser" quiet;
	run;

	/****** 15. ���������� ���� ******/
	proc casutil;
	  droptable casdata="price_ml" incaslib="casuser" quiet;
	  droptable casdata="price_ml2" incaslib="casuser" quiet;
	  droptable casdata="price_ml3" incaslib="casuser" quiet;
	  droptable casdata="new_product_abt12" incaslib="casuser" quiet;
	run;

	/* ��������� �� ���� */
	data casuser.price_ml2;
		set &lmvInCaslib..price_ml;
		drop start_dt end_dt;
		do sales_dt=start_dt to end_dt;
			output;
		end;
	run;

	/* ���������� �� ������� */
	proc fedsql sessref=casauto;
		create table casuser.price_ml3{options replace=true} as
			select
				t1.pbo_location_id,
				t1.product_id,
				t1.week,
				mean(t1.gross_price_amt) as mean_price
			from (
				select
					*,
					cast(intnx('week.2',sales_dt,0,'b') as date)  as week
				from
					casuser.price_ml2
			) as t1
			group by
				t1.pbo_location_id,
				t1.product_id,
				t1.week
		;
	quit;

	/* ��������� � �������� ���� */
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
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.other_discount,
				t1.other_digital,
				t1.pairs,
				t1.product_gift,
				t1.support,
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
				casuser.price_ml3 as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.product_id = t2.product_id and
				t1.week = t2.week
		;
	quit;

	proc casutil;
	  droptable casdata="price_ml" incaslib="casuser" quiet;
	  droptable casdata="price_ml2" incaslib="casuser" quiet;
	  droptable casdata="price_ml3" incaslib="casuser" quiet;
	  droptable casdata="ia_price_history" incaslib="casuser" quiet;
	  droptable casdata="ia_price" incaslib="casuser" quiet;
	  droptable casdata="new_product_abt11" incaslib="casuser" quiet;

	run;


	/****** 16. ��������� ��������� ******/
	proc casutil;
		droptable casdata="ingridients_transposed" incaslib="casuser" quiet;
		droptable casdata="ingridients_transposed2" incaslib="casuser" quiet;
		droptable casdata="ingridients_pca" incaslib="casuser" quiet;
		droptable casdata="ingridients_pca2" incaslib="casuser" quiet;
		droptable casdata="ingridients_pca3" incaslib="casuser" quiet;
		droptable casdata="new_product_abt13" incaslib="casuser" quiet;
	run;

	/* ������������� ��������� */
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

	/* �������� �������� ������ */
	data casuser.ingridients_transposed2;
		set casuser. ingridients_transposed;
		array _xxx_ _numeric_;
		do i=1 to dim( _xxx_);
			if missing(_xxx_[i]) then _xxx_[i]=0;
		end;
		drop i _name_ _label_;
	run;

	/* ������� ������ ���������� (�� ����������� ID) */
	proc contents data=casuser.ingridients_transposed2 noprint out=_contents_;
	run;

	/* ��������� ������ ���������� � �������������� */
	proc sql noprint;
		select name into :names separated by ' ' from _contents_ where upcase(name) not in ('PRODUCT_ID', 'MONTH_DT');
		select name into :sql_names separated by ',' from _contents_ where upcase(name) not in ('PRODUCT_ID', 'MONTH_DT');
	quit;

	ods _all_ close;
	/* ��������� �������� �� ������� ��������������� */
	proc pca data=casuser.ingridients_transposed2 n=30 plots=none;
		var &names.;
		output out=casuser.ingridients_pca copyvars=(month_dt product_id);
	run;

	/* ������ ��� datetime �� date */
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

	/* ����������� �������� �� ��� ���� ������ */
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

	/* ��������� ��������� � ������� */
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
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.other_discount,
				t1.other_digital,
				t1.pairs,
				t1.product_gift,
				t1.support,
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


	/****** 17. ���������� �� �������� � ������� ******/
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

	/****** 18. �������� ������ ******/
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
			bogo
			discount
			evm_set
			non_product_gift
			other_discount
			other_digital
			pairs
			product_gift
			support
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


	/****** 19. ������� ******/
	proc casutil;
		droptable casdata="npf_scoring_pred" incaslib="casuser" quiet;
	run;
	proc astore;
	  score data=casuser.npf_scoring
	  copyvars=(pbo_location_id  product_id sum_qty_corrected PROD_LVL2_ID month)
	  rstore=casuser.models_npf
	  out=casuser.npf_scoring_pred;
	quit;


	/****** 20. ������� �������� �� ���� ******/
	/* 
		������� ���� ��������� ��� ������ ����
			��������������� ��������� | ����� 
		������������ ������ � ������ ������� ������� �������
		���� ����� �� �������� �� ��� ����� ��������� �������, �� 
		����� ��� ������� ��������������� ������ ������ ������ mcd
	*/
	proc casutil;
		droptable casdata="npf_frame" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_mean" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_mean2" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile2" incaslib="casuser" quiet;
		droptable casdata="npf_weekday_profile3" incaslib="casuser" quiet;
		droptable casdata="npf_scoring_pred_day" incaslib="casuser" quiet;
	run;

	/* ����� ������� �� ���� */
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

	/****** 21. ���������� � �������������� �������� � product chain ******/
	proc casutil;
	*	load data=&inlib..ia_assort_matrix casout='ia_assort_matrix' outcaslib='casuser' replace;
		droptable casdata="npf_prediction" incaslib="casuser" quiet;
	run;

	/* ��������� ������� �� �����,
	 ���� ���� ������� �� ���� � �� ��� ���� ������ � product chain � lifecycle=N */
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
				&lmvInCaslib..assort_matrix as t2
			on
				t1.product_id = t2.product_id and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt <= datepart(end_dt) and
				t1.sales_dt >= datepart(start_dt)
			left join
				casuser.future_product_scoring as t3
			on
				t1.product_id = t3.product_id and
				t1.pbo_location_id = t3.pbo_location_id and
				t1.sales_dt = t3.sales_dt
			where
				t2.product_id is not missing or t3.product_id is not missing
		;
	quit;


	proc casutil;
		droptable casdata="npf_weekday_profile4" incaslib="casuser" quiet;
		promote casdata="npf_prediction" incaslib="casuser" outcaslib="casuser";
	run;

%mend vf_new_product;

