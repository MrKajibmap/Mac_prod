%macro load_plm(mpOutput = mn_dict.product_chain);
	%tech_cas_session(mpMode = start
				,mpCasSessNm = casauto
				,mpAssignFlg= y
				,mpAuthinfoUsr=
				);
	
	%local
		lmvOutputLib
		lmvOutputTable
	;
	
	%let lmvOutputLib = %scan(&mpOutput., 1, %str(.));
	%let lmvOutputTable = %scan(&mpOutput., 2, %str(.));

	data casuser.GP_PMIX_SALES_HISTORY(replace=yes drop=SALES_QTY_DISCOUNT GROSS_SALES_AMT_DISCOUNT NET_SALES_AMT_DISCOUNT);
		set  etl_ia.pmix_sales(where=(channel_cd='ALL'));
	run;
  

	proc fedsql sessref=casauto;
		create table casuser.gp_tmp_pmix_all_products_p1{options replace=true} as 
		select 
			a.product_id, a.sales_dt,
			sum(a.sales_qty) as sales_qty,
			/* sum(a.sales_qty_discount) as sales_qty_discount, */
			sum(a.sales_qty_promo) as sales_promo,
			/* sum(a.sales_qty) + sum(a.sales_qty_discount) + sum(a.sales_qty_promo) as all_sales_qty, */
			sum(a.sales_qty) + sum(a.sales_qty_promo) as all_sales_qty,
			count(distinct a.PBO_LOCATION_ID) as cnt_dist_pbo,
			count(a.PBO_LOCATION_ID) as cnt_pbo
		from casuser.GP_PMIX_SALES_HISTORY a
		where a.channel_cd = 'ALL'
		group by 
			a.product_id,
			a.sales_dt
		;
	quit;


	proc fedsql sessref=casauto;
		create table casuser.gp_tmp_all_sales{options replace=true} as 
		select 
			a.sales_dt, sum(a.sales_qty) as all_sum_qty
		from casuser.gp_tmp_pmix_all_products_p1 a
		group by a.sales_dt
		;
	quit;

	proc fedsql sessref=casauto;
	create table casuser.gp_tmp_pmix_all_products_p2{options replace=true} as 
	select
		a.product_id, a.sales_dt,
		a.sales_qty, a.cnt_pbo,
		b.all_sum_qty, 
		a.sales_qty / b.all_sum_qty as part_sales_qty
	from casuser.gp_tmp_pmix_all_products_p1 a
		left join casuser.gp_tmp_all_sales b
			on a.sales_dt = b.sales_dt
	;
	quit;


	proc univariate data=casuser.gp_tmp_pmix_all_products_p2;
		by product_id;
		var part_sales_qty ;
		output out=casuser.pctls pctlpts=5 pctlpre=pre_ pctlname=p5;
	run;

	/* %let hard_border = divide(3, 4039971); */
	%let hard_border =  divide(1, 10000000000);
	%put &=hard_border;


	proc fedsql sessref=casauto;
		create table casuser.gp_tmp_pmix_all_products_p3{options replace=true} as 
		select
			a.*,
			b.pre_p5,
			&hard_border. as hard_border,
			case when b.pre_p5 >= &hard_border. then b.pre_p5 else &hard_border. end as fin_border,
			case 
				when a.part_sales_qty >= b.pre_p5 and a.part_sales_qty >= &hard_border. then 1
				when a.part_sales_qty < &hard_border. then 0
				when a.part_sales_qty >= &hard_border. and  a.part_sales_qty < b.pre_p5 then 0
				else -1
			end as flag_in 
		from casuser.gp_tmp_pmix_all_products_p2 a
			left join casuser.pctls  b
			on a.product_id = b.product_id
		;
	quit;


	proc sort data=casuser.gp_tmp_pmix_all_products_p3 
		out= work.gp_tmp_pmix_all_products_p4;
		by product_id sales_dt;
	run;

	proc sql ;
		select max(a.sales_dt) format=date9. into: max_history_date 
		from work.gp_tmp_pmix_all_products_p4 a
		;
	quit;

	%put &=max_history_date;

	data work.gp_tmp_pmix_all_products_p5;
		set work.gp_tmp_pmix_all_products_p4(where=(flag_in=1));
		keep product_id sales_dt flag_in;
		format sales_dt date9.;
	run;




	data work.gp_tmp_pmix_all_products_p6;
		set work.gp_tmp_pmix_all_products_p5;
		by product_id;
		length buff_prev_date group_flag buff_per 8;
		format buff_prev_date buff_per date9.;
		retain buff_prev_date buff_per group_flag;
		keep product_id sales_dt flag_in group_flag;

		if first.product_id then do;
			group_flag = 1;
			buff_per = .;
		end;

		if buff_per ne . then do;
		if (sales_dt - buff_per <= 14) and (sales_dt - buff_per >= 0) 
		  then do;
			group_flag = group_flag;
		  end;
		  else do;
			group_flag = group_flag + 1;
		  end;
		end;

		buff_prev_date = buff_per;
		buff_per = sales_dt;

		/*  if last.product_id and ("&max_history_date."d - sales_dt <= 2) then do;
		sales_dt = '01JAN2023'd;
		end;
		*/
	run;




	proc sql;
		create table work.gp_tmp_pmix_all_products_p7 as
		select
			a.product_id, a.group_flag,
			min(a.sales_dt) as start_dt format=date9.,
			max(a.sales_dt) as end_dt format=date9.,
			max(a.sales_dt) - min(a.sales_dt) as delta
		from work.gp_tmp_pmix_all_products_p6 a
		group by a.product_id, a.group_flag
		;
	quit;

	data casuser.gp_tmp_pmix_all_products_p8;
		set work.gp_tmp_pmix_all_products_p7(where=(delta>=0));
		by product_id;
		if last.product_id and ("&max_history_date."d - end_dt <= 2) then do;
			end_dt = '01JAN2023'd;
		  end;

		  if first.product_id then do;
			 first_row_flag = 1;
			end;
			else do;
			  first_row_flag = 0;
			end;
	run;


	proc fedsql sessref=casauto;
		create table casuser.gp_tmp_product_chain_p1{options replace=true} as 
		select
			a.pbo_location_id,
			a.product_id,
			min(a.sales_dt) as first_date
		from casuser.GP_PMIX_SALES_HISTORY a
		where a.channel_cd='ALL'
		group by a.pbo_location_id, a.product_id
		;
	quit;



	proc fedsql sessref=casauto;
		create table casuser.gp_tmp_product_chain_p2{options replace=true} as 
		select
			b.pbo_location_id,
			a.product_id,
			a.start_dt,
			a.end_dt,
			a.first_row_flag,
			a.group_flag,
			a.delta,
			sum(case when b.sales_qty > 0 then 1 else 0 end) as cnt_rows
		from casuser.gp_tmp_pmix_all_products_p8 a
		left join casuser.GP_PMIX_SALES_HISTORY b
			on b.channel_cd='ALL' and b.sales_qty > 0
			and a.product_id = b.product_id
			and b.sales_dt between a.start_dt and a.end_dt
		group by 
			b.pbo_location_id,
			a.product_id,
			a.start_dt,
			a.end_dt,
			a.first_row_flag,
			a.group_flag,
			a.delta
			;
	quit;


	proc fedsql sessref=casauto;
		create table casuser.gp_tmp_product_chain_p3{options replace=true} as 
		select
			a.*
		from casuser.gp_tmp_product_chain_p2 a
		where a.cnt_rows >=1
		;
	quit;


	proc fedsql sessref=casauto;
		create table casuser.gp_tmp_product_chain_p4{options replace=true} as 
		select
			a.*,
			b.first_date,
			/* case when a.group_flag = 1 and b.first_date between a.start_dt - 7 and a.start_dt + 7 
				then b.first_date else a.start_dt end as true_start_dt,*/

			case when a.first_row_flag = 1 then b.first_date else a.start_dt end as true_start_dt,
			a.end_dt as true_end_dt
		from casuser.gp_tmp_product_chain_p3 a
			left join casuser.gp_tmp_product_chain_p1 b
			on a.pbo_location_id = b.pbo_location_id and a.product_id = b.product_id
		;
	quit;


	proc fedsql sessref=casauto;
		create table casuser.gp_tmp_product_chain_fin{options replace=true} as 
		select
			'N' as lifecycle_cd,
			a.PRODUCT_ID as predecessor_product_id, 
			a.PBO_LOCATION_ID as successor_dim2_id, 
			a.PRODUCT_ID as successor_product_id,
			a.PBO_LOCATION_ID as predecessor_dim2_id, 
			a.true_start_dt as successor_start_dt,
			a.true_end_dt as predecessor_end_dt,
			100 as scale_factor_pct
		from casuser.gp_tmp_product_chain_p4 a
		;
	quit;


	proc fedsql sessref=casauto;
	create table casuser.gp_tmp_product_chain_fin{options replace=true} as 
	select
		/* 'N' as lifecycle_cd, */
		a.PRODUCT_ID as predecessor_product_id, 
		a.PBO_LOCATION_ID as successor_dim2_id, 
		a.PRODUCT_ID as successor_product_id,
		a.PBO_LOCATION_ID as predecessor_dim2_id, 
		a.true_start_dt as successor_start_dt,
		a.true_end_dt as predecessor_end_dt,
		100 as scale_factor_pct
	from casuser.gp_tmp_product_chain_p4 a
	;
	quit;


	data casuser.gp_tmp_product_chain_fin;
		set casuser.gp_tmp_product_chain_fin;
		lifecycle_cd = "N";
		/* format lifecycle_cd CHAR1. ; */
		/* length lifecycle_cd 1;  */
	run;


	data casuser.gp_tmp_product_chain_p4;
		set casuser.gp_tmp_product_chain_p4;
		lifecycle_cd = "N";
		format lifecycle_cd CHAR1. ;
	run;

	proc fedsql sessref=casauto;
		create table casuser.gp_tmp_product_chain_fin{options replace=true} as 
		select
			a.lifecycle_cd,
			a.PRODUCT_ID as predecessor_product_id, 
			a.PBO_LOCATION_ID as successor_dim2_id, 
			a.PRODUCT_ID as successor_product_id,
			a.PBO_LOCATION_ID as predecessor_dim2_id, 
			a.true_start_dt as successor_start_dt,
			a.true_end_dt as predecessor_end_dt,
			100 as scale_factor_pct
		from casuser.gp_tmp_product_chain_p4 a
		;
	quit;

	proc casutil;
		droptable casdata='product_chain' incaslib="mn_dict" quiet;
		save casdata='gp_tmp_product_chain_fin' incaslib="casuser" casout="&lmvOutputTable..sashdat" outcaslib="&lmvOutputLib." replace;
		promote casdata="gp_tmp_product_chain_fin" casout ="&lmvOutputTable." incaslib="casuser" outcaslib="&lmvOutputLib."  ;
	run;

%mend load_plm;