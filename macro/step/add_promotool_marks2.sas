%macro add_promotool_marks2(mpOutCaslib=casuser,
							mpPtCaslib=pt,
							PromoCalculationRk=);

	%if %sysfunc(sessfound(casauto))=0 %then %do;
			cas casauto;
			caslib _all_ assign;
	%end;
	
	%local lmvPtCaslib
			lmvOutCaslib
			lmvReportDttm 
			lmvInLib
			;

	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	%let lmvPtCaslib=&mpPtCaslib.;
	%let lmvOutCaslib=%sysfunc(upcase(&mpOutCaslib.));
	%let lmvInLib=ETL_IA;

	proc casutil;
		droptable casdata='media_enh' incaslib="&lmvOutCaslib." quiet;
		droptable casdata='promo_prod_enh' incaslib="&lmvOutCaslib." quiet;
		droptable casdata='promo_pbo_enh' incaslib="&lmvOutCaslib." quiet;
		droptable casdata='promo_enh' incaslib="&lmvOutCaslib." quiet;
		droptable casdata='product_chain_enh' incaslib="&lmvOutCaslib." quiet;
		droptable casdata="pt_promo_x_dim_point" incaslib="&lmvOutCaslib" quiet;
		droptable casdata="pt_promo_detail" incaslib="&lmvOutCaslib" quiet;
		droptable casdata="pt_promo_calendar" incaslib="&lmvOutCaslib" quiet;
		droptable casdata="pt_promo" incaslib="&lmvOutCaslib" quiet;
		droptable casdata="pt_dim_point" incaslib="&lmvOutCaslib" quiet;
		droptable casdata="pt_internal_org" incaslib="&lmvOutCaslib" quiet;
		droptable casdata="pt_internal_org_hierarchy" incaslib="&lmvOutCaslib" quiet;
		droptable casdata="pt_product" incaslib="&lmvOutCaslib" quiet;
		droptable casdata="pt_product_hierarchy" incaslib="&lmvOutCaslib" quiet;
		droptable casdata="pt_segment" incaslib="&lmvOutCaslib" quiet;
		droptable casdata="pt_segment_hierarchy" incaslib="&lmvOutCaslib" quiet;
		droptable casdata="pt_channel" incaslib="&lmvOutCaslib" quiet;
		droptable casdata="pt_channel_hierarchy" incaslib="&lmvOutCaslib" quiet;
		droptable casdata="promo_calculation" incaslib="&lmvOutCaslib" quiet;
		load data=&lmvPtCaslib..promo_x_dim_point casout='pt_promo_x_dim_point' outcaslib="&lmvOutCaslib" replace;
		load data=&lmvPtCaslib..promo_detail casout='pt_promo_detail' outcaslib="&lmvOutCaslib" replace;
		load data=&lmvPtCaslib..promo_calendar casout='pt_promo_calendar' outcaslib="&lmvOutCaslib" replace;
		load data=&lmvPtCaslib..promo casout='pt_promo' outcaslib="&lmvOutCaslib" replace;
		load data=&lmvPtCaslib..dim_point casout='pt_dim_point' outcaslib="&lmvOutCaslib" replace;
		load data=&lmvPtCaslib..internal_org casout='pt_internal_org' outcaslib="&lmvOutCaslib" replace;
		load data=&lmvPtCaslib..internal_org_hierarchy casout='pt_internal_org_hierarchy' outcaslib="&lmvOutCaslib" replace;
		load data=&lmvPtCaslib..product casout='pt_product' outcaslib="&lmvOutCaslib" replace;
		load data=&lmvPtCaslib..product_hierarchy casout='pt_product_hierarchy' outcaslib="&lmvOutCaslib" replace;
		load data=&lmvPtCaslib..segment casout='pt_segment' outcaslib="&lmvOutCaslib" replace;
		load data=&lmvPtCaslib..segment_hierarchy casout='pt_segment_hierarchy' outcaslib="&lmvOutCaslib" replace;
		load data=&lmvPtCaslib..channel casout='pt_channel' outcaslib="&lmvOutCaslib" replace;
		load data=&lmvPtCaslib..channel_hierarchy casout='pt_channel_hierarchy' outcaslib="&lmvOutCaslib" replace;
		load data=&lmvInLib..channel_lookup casout='pt_channel_lookup' outcaslib="&lmvOutCaslib" replace;
		load data=&lmvPtCaslib..promo_calculation casout='promo_calculation' outcaslib="&lmvOutCaslib" replace;
	quit;

	/*==============================*/
	%let mvPCalRk = ;
	%if %length(&PromoCalculationRk)>0 %then %do;
	proc fedsql sessref=casauto; /*get p_cal_rk*/
	  create table &lmvOutCaslib..pcal{options replace=true} as
	  select p_cal_rk
	  from &lmvOutCaslib..promo_calculation 
		where promo_calculation_rk=%tslit(&PromoCalculationRk);
	quit;
	proc sql noprint;
	select p_cal_rk into :mvPCalRk from casuser.pcal;
	quit;
	%put &mvPCalRk;
	  %if %length(&mvPCalRk)>0 %then %do;
		  proc fedsql sessref=casauto;
			create table &lmvOutCaslib..pt_promo1{options replace=true} as
				select 
				promo_rk,p_cal_rk,trim(promo_id) as promo_id,promo_nm,
				datepart(promo_start_dttm) as start_dt,
				datepart(promo_end_dttm) as end_dt
			from &lmvOutCaslib..PT_PROMO
			where upcase(trim(promo_status_cd))='APPROVED'
				or (p_cal_rk=&mvPCalRk and upcase(trim(promo_status_cd))='DRAFT');
		  quit;
      %end;
	%end;
	%if %length(&PromoCalculationRk)=0 or %length(&mvPCalRk)=0 %then %do;
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..pt_promo1{options replace=true} as
			select 
			promo_rk,p_cal_rk,trim(promo_id) as promo_id,promo_nm,
			datepart(promo_start_dttm) as start_dt,
			datepart(promo_end_dttm) as end_dt
		from &lmvOutCaslib..PT_PROMO
		where upcase(trim(promo_status_cd))='APPROVED';
	quit;
	%end;
/*==============================*/

	proc cas;
	transpose.transpose /
	   table={name="pt_promo_detail", caslib="&lmvOutCaslib", groupby={"promo_rk"}} 
	   attributes={{name="promo_rk"}} 
	   transpose={"promo_dtl_vle"} 
	   id={"promo_dtl_cd"} 
	   casout={name="pt_detail_transposed", caslib="&lmvOutCaslib", replace=true};
	quit;
	
	/* Загрузка Промо */
	data &lmvOutCaslib..promo (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..promo(where=(valid_to_dttm>=&lmvReportDttm.));
	run;
	
	/* создать числовые promo_id для promo_id вида 78b61716-af8e-4deb-97e2-1f79e74c7118 */
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..promo_id_exp{options replace=true} as
		select 
		t2.promo_id
		from &lmvOutCaslib..promo t1 full outer join &lmvOutCaslib..pt_promo1 t2
		on t1.promo_id=inputn(trim(t2.promo_id),'10.')
		where t1.promo_id is null
		;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..max_promo_id{options replace=true} as
		select max(promo_id) as max_promo_id from
		&lmvOutCaslib..promo;
	quit;
	
	/* START: Блок для сортировки полученных после интеграции с ПТ промо акций для обеспечения воспроизводимости результата */
	proc sql noprint;
		create table work.promo_id_exp_sorted as
		select * from &lmvOutCaslib..promo_id_exp
		order by promo_id asc
		;
	quit;

	data work.promo_id_map;
		if _n_ = 1 then set &lmvOutCaslib..max_promo_id;
		keep promo_id_num promo_id;
		set work.promo_id_exp_sorted;
		promo_id_num=max_promo_id+9999+_n_;
	run;

	data &lmvOutCaslib..promo_id_map(replace=yes);
		set  work.promo_id_map;
	run;
	/* END: Блок для сортировки полученных после интеграции с ПТ промо акций для обеспечения воспроизводимости результата */
	
	/*
	data &lmvOutCaslib..promo_id_map /sessref="casauto" single=yes;;
		if _n_ = 1 then set &lmvOutCaslib..max_promo_id;
		keep promo_id_num promo_id;
		set &lmvOutCaslib..promo_id_exp;
		promo_id_num=max_promo_id+9999+_n_;
	run;
	*/
	/*таблица в разрезе промо-акций*/
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..pt_promo2{options replace=true} as
		select 
		t1.promo_rk,t1.p_cal_rk,
		mp.promo_id_num as promo_id,
		t1.promo_nm,
		t1.start_dt,t1.end_dt,
		t2.mechanicstype,
		t2.benefitRadio,
		t2.marketingDigital,
		t2.marketingInStore,
		t2.marketingInStoreDateEnd,
		t2.marketingInStoreDateStart,
		t2.marketingOoh,
		inputn(trim(t2.marketingTrp),'18.') as marketingTrp,
		cast(inputn(trim(t2.marketingTrpDateEnd),'YYMMDD10.') as date) as marketingTrpDateEnd,
		cast(inputn(trim(t2.marketingTrpDateStart),'YYMMDD10.') as date) as marketingTrpDateStart,
		inputn(trim(t2.mechanicsExpertReview),'18.') as mechanicsExpertReview,
		t2.platform as platform,
		case when t2.switcherPrices='true' then 'Y' end as location_based_pricing
		from &lmvOutCaslib..PT_PROMO1 t1 
		inner join &lmvOutCaslib..promo_id_map mp on t1.promo_id=mp.promo_id
		left join &lmvOutCaslib..pt_detail_transposed t2
		on t1.promo_rk=t2.promo_rk
		;
	quit;

	/*таблица в разрезе иерархии промо-акции*/
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..pt_promo3{options replace=true} as
		select t1.promo_id,t1.promo_rk,
		t5.channel_cd as channel_cd,
		t3.int_org_rk as pbo_location_id,
		t3.segment_rk as segment_id
		from &lmvOutCaslib..pt_promo2 t1 left join &lmvOutCaslib..PT_PROMO_X_DIM_POINT t2 on t1.promo_rk=t2.promo_rk
		left join &lmvOutCaslib..PT_DIM_POINT t3 on t2.dim_point_rk=t3.dim_point_rk
		left join &lmvOutCaslib..pt_channel_lookup t5 on t3.channel_rk=t5.member_rk
	;
	quit;

	/*разрезаем таблицу с иерархиями отдельно на ПБО, сегмент и канал*/
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..promopbo_app{options replace=true} as
		select distinct promo_id,pbo_location_id
		from &lmvOutCaslib..pt_promo3;
		create table &lmvOutCaslib..segment_app{options replace=true} as
		select distinct promo_id,segment_id
		from &lmvOutCaslib..pt_promo3;
		create table &lmvOutCaslib..promochn_app{options replace=true} as
		select distinct promo_id,channel_cd
		from &lmvOutCaslib..pt_promo3;
	quit;
	
	data &lmvOutCaslib..media (replace=yes rename=(report_dt=period_dt) drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..media(where=(valid_to_dttm>=&lmvReportDttm.));
	run;
	
	/*нужны отдельные ID для promo_group_id, через которую идёт мэппинг с media*/
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..max_promo_group_id{options replace=true} as
		select max(promo_group_id) as max_promo_group_id from
		&lmvOutCaslib..media;
	quit;

	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..media_ext{options replace=true} as
		select PROMO_ID,MARKETINGTRP,MARKETINGTRPDATEEND,MARKETINGTRPDATESTART from
		&lmvOutCaslib..PT_PROMO2
		where MARKETINGTRP>=0 and MARKETINGTRPDATESTART is not null;
	quit;

	data &lmvOutCaslib..promo_group_id_map /sessref="casauto" single=yes;;
		if _n_ = 1 then set &lmvOutCaslib..max_promo_group_id;
		set &lmvOutCaslib..media_ext;
		promo_group_id_num=max_promo_group_id+8888+_n_;
	run;

	/*подготовка для добавления trp в разрезе недель*/
	data &lmvOutCaslib..media_app;
		set &lmvOutCaslib..promo_group_id_map ;
		format period_dt date9.;
		keep period_dt  promo_group_id_num MARKETINGTRP;
		trp_old=MARKETINGTRP;
		if MARKETINGTRPDATEEND ne . then do;
			do period_dt=intnx('week.2',MARKETINGTRPDATESTART,0) to intnx('week.2',MARKETINGTRPDATEEND,0) by 7;
				MARKETINGTRP=trp_old/(MARKETINGTRPDATEEND-MARKETINGTRPDATESTART+1)*
					( min(MARKETINGTRPDATEEND,intnx('week.2',period_dt,0,'e'))-max(MARKETINGTRPDATESTART,period_dt)+1);
			output;
			end;
		end;
	run;

	/*К товарам нужно подтянуть цены, позиции, флаг подарка*/
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..promo_detail_spl{options replace=true} as
		select distinct t1.promo_rk,t2.promo_id,promo_dtl_cd,promo_dtl_vle,
		scan(promo_dtl_cd,1,'_') as dtl_vle,
		scan(promo_dtl_cd,-2,'_') as first_ind,
		case
		when scan(promo_dtl_cd,1,'_')=scan(promo_dtl_cd,-1,'_') then ' '
		else scan(promo_dtl_cd,-1,'_') end as second_ind
		from &lmvOutCaslib..pt_promo_detail t1 inner join &lmvOutCaslib..PT_PROMO2 t2 on t1.promo_rk=t2.promo_rk;
	quit;

	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..promoprod_app1{options replace=true} as
		select t1.promo_rk,t1.promo_id,
		coalesce(inputn(trim(t1.promo_dtl_vle),'10.'),
				 inputn(trim(t6.promo_dtl_vle),'10.')) as product_id,
		case when t1.first_ind=1 or t1.first_ind is null then 'N' 
			 when t1.first_ind=2 then 'Y' end as gift_flag,
		coalesce(inputn(trim(t3.promo_dtl_vle),'10.'),1) as Qty,
		coalesce(inputn(trim(t4.promo_dtl_vle),'10.'),1) as Pos,
		inputn(trim(t5.promo_dtl_vle),'10.') as Price
		from &lmvOutCaslib..promo_detail_spl t1
		left join &lmvOutCaslib..promo_detail_spl t3 on t1.promo_rk=t3.promo_rk and 
			t1.first_ind=t3.first_ind and t1.second_ind=t3.second_ind and 
			t3.DTL_VLE ='mechPromoSkuQty'
		left join &lmvOutCaslib..promo_detail_spl t4 on t1.promo_rk=t4.promo_rk and 
			t1.first_ind=t4.first_ind and t1.second_ind=t4.second_ind and 
			t4.DTL_VLE ='mechPosition'
		left join &lmvOutCaslib..promo_detail_spl t5 on t1.promo_rk=t5.promo_rk and 
			t1.first_ind=t5.first_ind and t1.second_ind=t5.second_ind and 
			t5.DTL_VLE ='mechPrice'
		left join &lmvOutCaslib..promo_detail_spl t6 on t1.promo_rk=t6.promo_rk and 
			t1.first_ind=t6.first_ind and t1.second_ind=t6.second_ind and 
			t6.DTL_VLE ='mechPromoSkuId'
		where t1.DTL_VLE='mechRegSkuId'
		;
	quit;

	/*добавить channel+segment+promo_group_id*/
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..pt_promo2ext{options replace=true} as
		select 
		t1.promo_rk,t1.p_cal_rk,
		t1.promo_id as promo_id,
		t1.promo_nm,
		t1.start_dt,t1.end_dt,
		t1.mechanicstype,
		t1.platform,
		t1.benefitRadio,
		t1.marketingDigital,
		t1.marketingInStore,
		t1.marketingInStoreDateEnd,
		t1.marketingInStoreDateStart,
		t1.marketingOoh,
		t1.marketingTrp,
		t1.marketingTrpDateEnd,
		t1.marketingTrpDateStart,
		t1.mechanicsExpertReview,
		t1.location_based_pricing,
		t3.channel_cd,
		t2.segment_id,
		t4.promo_group_id_num as promo_group_id
		from &lmvOutCaslib..PT_PROMO2 t1 
		left join &lmvOutCaslib..segment_app t2 on t1.promo_id=t2.promo_id
		left join &lmvOutCaslib..promochn_app t3 on t1.promo_id=t3.promo_id
		left join &lmvOutCaslib..promo_group_id_map t4 on t1.promo_id=t4.promo_id
		where t1.mechanicstype not in ('Delisting') /*Delisting is treated separately*/
		;
	quit;

	/*мэппинг casuser.PROMO*/
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..promo_enh{options replace=true} as
		select 
		coalesce(t1.channel_cd,t2.channel_cd) as channel_cd,
		coalesce(t1.promo_id,t2.promo_id) as promo_id,
		coalesce(t1.promo_mechanics,t2.mechanicstype) as promo_mechanics,
		coalesce(t1.promo_nm,t2.promo_nm) as promo_nm,
		coalesce(t1.segment_id,t2.segment_id) as segment_id,
		coalesce(t1.promo_group_id,t2.promo_group_id,-9999) as promo_group_id,
		t1.promo_price_amt as promo_price_amt, /*??? ????? ?????????*/
		coalesce(t1.start_dt,t2.start_dt) as start_dt,
		coalesce(t1.end_dt,t2.end_dt) as end_dt,
		coalesce(t1.np_gift_price_amt,t2.mechanicsExpertReview) as np_gift_price_amt,
		coalesce(t1.platform,t2.platform) as platform,
		coalesce(t1.location_based_pricing,t2.location_based_pricing) as location_based_pricing,
		case when t2.promo_id is not null then 1 else 0 end as from_pt
		from &lmvOutCaslib..promo t1 full outer join &lmvOutCaslib..pt_promo2ext t2
		on t1.promo_id=t2.promo_id
		;
	quit;
	
	data &lmvOutCaslib..promo_pbo (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..promo_x_pbo(where=(valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data &lmvOutCaslib..promo_prod (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..promo_x_product(where=(valid_to_dttm>=&lmvReportDttm.));
	run;
	
	/*мэппинг casuser.PROMO_PBO*/
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..promo_pbo_enh{options replace=true} as
		select 
		coalesce(t1.promo_id,t2.promo_id) as promo_id,
		coalesce(t1.pbo_location_id,t2.pbo_location_id) as pbo_location_id
		from &lmvOutCaslib..promo_pbo t1 full outer join &lmvOutCaslib..promopbo_app t2
		on t1.promo_id=t2.promo_id
		;
	quit;

	/*мэппинг casuser.PROMO_PROD*/
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..promo_prod_enh{options replace=true} as
		select 
		coalesce(t1.promo_id,t2.promo_id) as promo_id,
		coalesce(t1.product_id,t2.product_id) as product_id,
		coalesce(t1.product_qty,t2.qty) as product_qty,
		coalesce(t1.option_number,t2.pos) as option_number,
		coalesce(t1.gift_flg,t2.gift_flag) as gift_flag,
		t2.Price as price
		from &lmvOutCaslib..promo_prod t1 full outer join &lmvOutCaslib..promoprod_app1 t2
		on t1.promo_id=t2.promo_id
		;
	quit;

	/*мэппинг casuser.media*/
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..media_enh{options replace=true} as
		select 
		coalesce(t1.promo_group_id,t2.promo_group_id_num) as promo_group_id,
		/* Изменено наименование поля на report_dt с period_dt */
		coalesce(t1.period_dt,t2.period_dt) as report_dt,
		coalesce(t1.trp,t2.MARKETINGTRP) as trp
		from &lmvOutCaslib..media t1 full outer join &lmvOutCaslib..media_app t2
		on t1.promo_group_id=t2.promo_group_id_num
		;
	quit;

	/*мэппинг casuser.product_chain - добавляем делистинги*/

	/*pbo_location_id - to leaf level!*/
	
	data CASUSER.PBO_LOC_HIERARCHY (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PBO_LOC_HIERARCHY(where=(valid_to_dttm>=&lmvReportDttm.));
	run;

	proc fedsql sessref=casauto noprint;
	   create table &lmvOutCaslib..pbo_hier_flat{options replace=true} as
			select t1.pbo_location_id, 
				   t2.PBO_LOCATION_ID as LVL3_ID,
				   t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				   1 as LVL1_ID
			from 
			(select * from CASUSER.PBO_LOC_HIERARCHY where pbo_location_lvl=4) as t1
			left join 
			(select * from CASUSER.PBO_LOC_HIERARCHY where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
			;
	quit;

		proc fedsql sessref=casauto noprint;
		create table &lmvOutCaslib..pbo_exp1{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID
			from &lmvOutCaslib..promopbo_app t1 inner join &lmvOutCaslib..pbo_hier_flat t2
				on t1.pbo_location_id=t2.LVL1_ID
		;
				
		create table &lmvOutCaslib..pbo_exp2{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID
			from &lmvOutCaslib..promopbo_app t1 inner join &lmvOutCaslib..pbo_hier_flat t2
				on t1.pbo_location_id=t2.LVL2_ID
		;
		create table &lmvOutCaslib..pbo_exp3{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID
			from &lmvOutCaslib..promopbo_app t1 inner join &lmvOutCaslib..pbo_hier_flat t2
				on t1.pbo_location_id=t2.LVL3_ID
		;
		create table &lmvOutCaslib..pbo_exp4{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID
			from &lmvOutCaslib..promopbo_app t1 inner join &lmvOutCaslib..pbo_hier_flat t2
				on t1.pbo_location_id=t2.pbo_location_id
		;
	quit;

	data &lmvOutCaslib..pbo_exp1(append=force);
		set &lmvOutCaslib..pbo_exp2
			&lmvOutCaslib..pbo_exp3
			&lmvOutCaslib..pbo_exp4;
	run;

	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..product_chain_add1{options replace=true} as
		select distinct
		t1.start_dt as predecessor_end_dt,
		t2.pbo_location_id as predecessor_dim2_id,
		t3.product_id as predecessor_product_id
		from &lmvOutCaslib..PT_PROMO2 t1 
		inner join &lmvOutCaslib..pbo_exp1 t2 on t1.promo_id=t2.promo_id
		inner join &lmvOutCaslib..promoprod_app1 t3 on t1.promo_id=t3.promo_id
		where t1.mechanicstype ='Delisting';
	quit;

	/* Временная подмена таблицы product_chain */
	/* %load_plm(mpOutput = mn_dict.product_chain)*/
	/*%load_product_chain(mpOutput = mn_dict.product_chain);*/
	
	data &lmvOutCaslib..product_chain1;
		/* Временная подмена таблицы product_chain */
		/* set &lmvInLib..PRODUCT_CHAIN; */
		set mn_dict.product_chain;
		/* where valid_to_dttm>=&lmvReportDttm.; применяем усл для версионирования*/
	run;

	data &lmvOutCaslib..product_chain_add2;
		set &lmvOutCaslib..product_chain_add1;
		lifecycle_cd='D';
	run;

/* When new product is introduced, it must enter the PLM as 'N'=new product */
	/*list of promo products*/
	data &lmvOutCaslib..product_attr1;
		set &lmvInLib..PRODUCT_ATTRIBUTES;
		where valid_to_dttm>=&lmvReportDttm.; /*применяем усл для версионирования*/
	run;

	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..promo_prod_list{options replace=true} as
		select distinct product_id
		from &lmvOutCaslib..PRODUCT_ATTR1
		where product_attr_nm='REGULAR_ID' and product_attr_value is not null 
			and product_id ^= inputn(product_attr_value,'8.');
	quit;
/*Для этого набора промо-механик новинки заводятся только для промо-товаров из списка*/
	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..promo_prod_intersect{options replace=true} as
		select distinct t2.product_id as successor_product_id,
				t4.pbo_location_id as successor_dim2_id, 
				t3.start_dt as successor_start_dt, 
				t3.end_dt as predecessor_end_dt
		from &lmvOutCaslib..promo_prod_list t1 inner join
			&lmvOutCaslib..promo_prod_enh t2 
			on t1.product_id=t2.product_id
			left join &lmvOutCaslib..promo_enh t3
			on t2.promo_id=t3.promo_id
			left join &lmvOutCaslib..pbo_exp1 t4 on t2.promo_id=t4.promo_id
				where t3.promo_mechanics in (
				'EVM / Set',
				'Temp price reduction (discount)',
				'Pairs (different categories)',
				'Gift for purchase (Sampling)',
				'Gift for purchase (for product)',
				'Gift for purchase: Non-Product',
				'Gift for purchase (for ordres above X rub)',
				'Bundle',
				'Other: Collaboration'
			) and t3.from_pt=1; /*process only lifecycle from promotool*/
	quit;

/*		Для этого набора промо новинки заводятся для ВСЕХ товаров
				'Product : new launch LTO',
				'Product : new launch Permanent incl item rotation',
				'Product : line-extension' */

	proc fedsql sessref=casauto;
		create table &lmvOutCaslib..promo_prod_intersect3{options replace=true} as
		select distinct t2.product_id as successor_product_id,
				t4.pbo_location_id as successor_dim2_id, 
				t3.start_dt as successor_start_dt, 
				t3.end_dt as predecessor_end_dt
		from 
			&lmvOutCaslib..promo_prod_enh t2 
			left join &lmvOutCaslib..promo_enh t3
			on t2.promo_id=t3.promo_id
			left join &lmvOutCaslib..pbo_exp1 t4 on t2.promo_id=t4.promo_id
				where t3.promo_mechanics in (
				'Product : new launch LTO',
				'Product : new launch Permanent incl item rotation',
				'Product : line-extension'
			) and t3.from_pt=1; /*process only lifecycle from promotool*/
	quit;

	data &lmvOutCaslib..promo_prod_intersect2;
		set &lmvOutCaslib..promo_prod_intersect &lmvOutCaslib..promo_prod_intersect3;
		lifecycle_cd='N';
		scale_factor_pct=100;
	run;

	data &lmvOutCaslib..product_chain_enh;
		drop valid_from_dttm valid_to_dttm;
		set &lmvOutCaslib..product_chain1 
			&lmvOutCaslib..product_chain_add2
			&lmvOutCaslib..promo_prod_intersect2;
	run;

	proc casutil;
 		droptable casdata='product_attr1' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='promo_prod_list' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='promo_prod_intersect' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='promo_prod_intersect2' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='pt_promo1' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='pt_detail_transposed' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='promo' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='promo_id_exp' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='max_promo_id' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='promo_id_map' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='pt_promo2' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='pt_promo3' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='promopbo_app' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='media' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='max_promo_group_id' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='media_ext' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='promo_group_id_map' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='media_app' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='promo_detail_spl' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='promoprod_app1' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='pt_promo2ext' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='promo_pbo' incaslib="&lmvOutCaslib." quiet;
 		droptable casdata='promo_prod' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='product_chain_add1' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='product_chain_add2' incaslib="&lmvOutCaslib." quiet; 
 		droptable casdata='product_chain1' incaslib="&lmvOutCaslib." quiet; 
	quit;
	
%mend add_promotool_marks2;

