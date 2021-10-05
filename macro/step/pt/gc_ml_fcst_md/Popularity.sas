%macro Popularity;
	%let lmvSalesTb = MN_SHORT.PMIX_SALES;
	%let sales_tgt = sum(sales_qty,sales_qty_promo);
	%let ta = week.2;
	%let d1 = date '2021-01-04';
	%let d2 = date '2021-08-29';
	/*1 схлопываем до недель и срезаем по датам*/
	proc fedsql sessref=casauto;
		create table casuser.Sales_weekly{options replace=true} as
		select 
			channel_cd
			, intnx(%tslit(&ta.), sales_dt,0,'b')::DATE as sales_week_dt
			, pbo_location_id
			, product_id
			, sum(&sales_tgt) as q  
		from &lmvSalesTb
		where sales_dt between &d1 and &d2
		group by	channel_cd, sales_week_dt, pbo_location_id, product_id
	;quit;
	/*2 усредняем*/
	proc fedsql sessref=casauto;
		create table casuser.Sales_weekly_avg{options replace=true} as
		select 
			channel_cd
			, pbo_location_id
			, product_id
			, avg(q) as q  
		from casuser.Sales_weekly
		group by	channel_cd, pbo_location_id, product_id
	;quit;
	/*3 тоталы  по ПБО*/
	proc fedsql sessref=casauto;
		create table casuser.Sales_weekly_total{options replace=true} as
		select 
			channel_cd
			, pbo_location_id
			, sum(q) as q_total
		from casuser.Sales_weekly
		group by	channel_cd, pbo_location_id
	;quit;
	/*4 пересчет в доли*/	
	proc fedsql sessref=casauto;
		create table casuser.Sales_weekly_share {options replace=true} as
		select 
			t1.channel_cd
			, t1.pbo_location_id
			, t1.product_id
			, t1.q as q_amt
			, t1.q/t2.q_total as q_pct
		from casuser.Sales_weekly_avg as t1
		inner join casuser.Sales_weekly_total as t2
			on t1.channel_cd = t2.channel_cd
			and t1.pbo_location_id = t2.pbo_location_id
	;quit;
	proc casutil;
	    droptable casdata="Sales_weekly" incaslib="casuser" quiet;
		droptable casdata="Sales_weekly_avg" incaslib="casuser" quiet;
		droptable casdata="Sales_weekly_total" incaslib="casuser" quiet;
		promote incaslib='casuser' casdata='Sales_weekly_share' 
					outcaslib="casuser" casout="Popularity";
		run;
		droptable casdata="Sales_weekly_share" incaslib="casuser" quiet;
	quit;
%mend;