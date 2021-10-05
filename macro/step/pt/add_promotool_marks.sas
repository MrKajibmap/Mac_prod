%macro add_promotool_marks(mpOutCaslib=casuser,
							mpPtCaslib=pt);

	%if %sysfunc(sessfound(casauto))=0 %then %do;
			cas casauto;
			caslib _all_ assign;
	%end;
	
	%local lmvPtCaslib lmvOutCaslib lmvReportDttm lmvInLib;

	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	%let lmvPtCaslib=&mpPtCaslib.;
	%let lmvOutCaslib=%sysfunc(upcase(&mpOutCaslib.));
	%let lmvInLib=ETL_IA;

	proc casutil;
		droptable casdata='media_enh' incaslib="&lmvOutCaslib." quiet;
		droptable casdata='promo_prod_enh' incaslib="&lmvOutCaslib." quiet;
		droptable casdata='promo_pbo_enh' incaslib="&lmvOutCaslib." quiet;
		droptable casdata='promo_enh' incaslib="&lmvOutCaslib." quiet;
	  droptable casdata="pt_promo_x_dim_point" incaslib="casuser" quiet;
	  droptable casdata="pt_promo_detail" incaslib="casuser" quiet;
	  droptable casdata="pt_promo_calendar" incaslib="casuser" quiet;
	  droptable casdata="pt_promo" incaslib="casuser" quiet;
	  droptable casdata="pt_dim_point" incaslib="casuser" quiet;
	  droptable casdata="pt_internal_org" incaslib="casuser" quiet;
	  droptable casdata="pt_internal_org_hierarchy" incaslib="casuser" quiet;
	  droptable casdata="pt_product" incaslib="casuser" quiet;
	  droptable casdata="pt_product_hierarchy" incaslib="casuser" quiet;
	  droptable casdata="pt_segment" incaslib="casuser" quiet;
	  droptable casdata="pt_segment_hierarchy" incaslib="casuser" quiet;
	  droptable casdata="pt_channel" incaslib="casuser" quiet;
	  droptable casdata="pt_channel_hierarchy" incaslib="casuser" quiet;
	  load data=&lmvPtCaslib..promo_x_dim_point casout='pt_promo_x_dim_point' outcaslib='casuser' replace;
	  load data=&lmvPtCaslib..promo_detail casout='pt_promo_detail' outcaslib='casuser' replace;
	  load data=&lmvPtCaslib..promo_calendar casout='pt_promo_calendar' outcaslib='casuser' replace;
	  load data=&lmvPtCaslib..promo casout='pt_promo' outcaslib='casuser' replace;
	  load data=&lmvPtCaslib..dim_point casout='pt_dim_point' outcaslib='casuser' replace;
	  load data=&lmvPtCaslib..internal_org casout='pt_internal_org' outcaslib='casuser' replace;
	  load data=&lmvPtCaslib..internal_org_hierarchy casout='pt_internal_org_hierarchy' outcaslib='casuser' replace;
	  load data=&lmvPtCaslib..product casout='pt_product' outcaslib='casuser' replace;
	  load data=&lmvPtCaslib..product_hierarchy casout='pt_product_hierarchy' outcaslib='casuser' replace;
	  load data=&lmvPtCaslib..segment casout='pt_segment' outcaslib='casuser' replace;
	  load data=&lmvPtCaslib..segment_hierarchy casout='pt_segment_hierarchy' outcaslib='casuser' replace;
	  load data=&lmvPtCaslib..channel casout='pt_channel' outcaslib='casuser' replace;
	  load data=&lmvPtCaslib..channel_hierarchy casout='pt_channel_hierarchy' outcaslib='casuser' replace;
	  load data=&lmvInLib..channel_lookup casout='pt_channel_lookup' outcaslib='casuser' replace;
	quit;


	proc fedsql sessref=casauto;
		create table casuser.pt_promo1{options replace=true} as
			select 
			promo_rk,p_cal_rk,trim(promo_id) as promo_id,promo_nm,
			datepart(promo_start_dttm) as start_dt,
			datepart(promo_end_dttm) as end_dt
		from casuser.PT_PROMO
		where trim(promo_status_cd)='approved';
	quit;

	proc cas;
	transpose.transpose /
	   table={name="pt_promo_detail", caslib="casuser", groupby={"promo_rk"}} 
	   attributes={{name="promo_rk"}} 
	   transpose={"promo_dtl_vle"} 
	   id={"promo_dtl_cd"} 
	   casout={name="pt_detail_transposed", caslib="casuser", replace=true};
	quit;
	
	/* Загрузка Промо */
	data casuser.promo (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..promo(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	/* создать числовые promo_id для promo_id вида 78b61716-af8e-4deb-97e2-1f79e74c7118 */
	proc fedsql sessref=casauto;
		create table casuser.promo_id_exp{options replace=true} as
		select 
		t2.promo_id
		from casuser.promo t1 full outer join casuser.pt_promo1 t2
		on t1.promo_id=inputn(trim(t2.promo_id),'10.')
		where t1.promo_id is null
		;
	quit;
	proc fedsql sessref=casauto;
		create table casuser.max_promo_id{options replace=true} as
		select max(promo_id) as max_promo_id from
		casuser.promo;
	quit;

	data casuser.promo_id_map /sessref="casauto" single=yes;;
		if _n_ = 1 then set casuser.max_promo_id;
		keep promo_id_num promo_id;
		set casuser.promo_id_exp;
		promo_id_num=max_promo_id+9999+_n_;
	run;

	/*таблица в разрезе промо-акций*/
	proc fedsql sessref=casauto;
		create table casuser.pt_promo2{options replace=true} as
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
		inputn(trim(t2.mechanicsExpertReview),'18.') as mechanicsExpertReview
		from casuser.PT_PROMO1 t1 
		inner join casuser.promo_id_map mp on t1.promo_id=mp.promo_id
		left join casuser.pt_detail_transposed t2
		on t1.promo_rk=t2.promo_rk
		;
	quit;

	/*таблица в разрезе иерархии промо-акции*/
	proc fedsql sessref=casauto;
		create table casuser.pt_promo3{options replace=true} as
		select t1.promo_id,t1.promo_rk,
		t5.channel_cd as channel_cd,
		t3.int_org_rk as pbo_location_id,
		t3.segment_rk as segment_id
		from casuser.pt_promo2 t1 left join casuser.PT_PROMO_X_DIM_POINT t2 on t1.promo_rk=t2.promo_rk
		left join casuser.PT_DIM_POINT t3 on t2.dim_point_rk=t3.dim_point_rk
		left join casuser.pt_channel_lookup t5 on t3.channel_rk=t5.member_rk
	;
	quit;

	/*разрезаем таблицу с иерархиями отдельно на ПБО, сегмент и канал*/
	proc fedsql sessref=casauto;
		create table casuser.promopbo_app{options replace=true} as
		select distinct promo_id,pbo_location_id
		from casuser.pt_promo3;
		create table casuser.segment_app{options replace=true} as
		select distinct promo_id,segment_id
		from casuser.pt_promo3;
		create table casuser.promochn_app{options replace=true} as
		select distinct promo_id,channel_cd
		from casuser.pt_promo3;
	quit;
	
	data casuser.media (replace=yes rename=(report_dt=period_dt) drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..media(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	/*нужны отдельные ID для promo_group_id, через которую идёт мэппинг с media*/
	proc fedsql sessref=casauto;
		create table casuser.max_promo_group_id{options replace=true} as
		select max(promo_group_id) as max_promo_group_id from
		casuser.media;
	quit;

	proc fedsql sessref=casauto;
		create table casuser.media_ext{options replace=true} as
		select PROMO_ID,MARKETINGTRP,MARKETINGTRPDATEEND,MARKETINGTRPDATESTART from
		casuser.PT_PROMO2
		where MARKETINGTRP>=0 and MARKETINGTRPDATESTART is not null;
	quit;

	data casuser.promo_group_id_map /sessref="casauto" single=yes;;
		if _n_ = 1 then set casuser.max_promo_group_id;
		set casuser.media_ext;
		promo_group_id_num=max_promo_group_id+8888+_n_;
	run;

	/*подготовка для добавления trp в разрезе недель*/
	data casuser.media_app;
		set casuser.promo_group_id_map ;
		format period_dt date9.;
		keep period_dt  promo_group_id_num MARKETINGTRP;
		do period_dt=intnx('week.2',MARKETINGTRPDATESTART,0) to intnx('week.2',MARKETINGTRPDATEEND,0) by 7;
		output;
		end;
	run;

	/*К товарам нужно подтянуть цены, позиции, флаг подарка*/
	proc fedsql sessref=casauto;
		create table casuser.promo_detail_spl{options replace=true} as
		select distinct t1.promo_rk,t2.promo_id,promo_dtl_cd,promo_dtl_vle,
		scan(promo_dtl_cd,1,'_') as dtl_vle,
		scan(promo_dtl_cd,-2,'_') as first_ind,
		case
		when scan(promo_dtl_cd,1,'_')=scan(promo_dtl_cd,-1,'_') then ' '
		else scan(promo_dtl_cd,-1,'_') end as second_ind
		from casuser.pt_promo_detail t1 inner join casuser.PT_PROMO2 t2 on t1.promo_rk=t2.promo_rk;
	quit;

	proc fedsql sessref=casauto;
		create table casuser.promoprod_app1{options replace=true} as
		select t1.promo_rk,t1.promo_id,
		coalesce(inputn(trim(t1.promo_dtl_vle),'10.'),
				 inputn(trim(t6.promo_dtl_vle),'10.')) as product_id,
		case when t1.first_ind=1 or t1.first_ind is null then 'N' 
			 when t1.first_ind=2 then 'Y' end as gift_flag,
		coalesce(inputn(trim(t3.promo_dtl_vle),'10.'),1) as Qty,
		coalesce(inputn(trim(t4.promo_dtl_vle),'10.'),1) as Pos,
		inputn(trim(t5.promo_dtl_vle),'10.') as Price
		from casuser.promo_detail_spl t1
		left join casuser.promo_detail_spl t3 on t1.promo_rk=t3.promo_rk and 
			t1.first_ind=t3.first_ind and t1.second_ind=t3.second_ind and 
			t3.DTL_VLE ='mechPromoSkuQty'
		left join casuser.promo_detail_spl t4 on t1.promo_rk=t4.promo_rk and 
			t1.first_ind=t4.first_ind and t1.second_ind=t4.second_ind and 
			t4.DTL_VLE ='mechPosition'
		left join casuser.promo_detail_spl t5 on t1.promo_rk=t5.promo_rk and 
			t1.first_ind=t5.first_ind and t1.second_ind=t5.second_ind and 
			t5.DTL_VLE ='mechPrice'
		left join casuser.promo_detail_spl t6 on t1.promo_rk=t6.promo_rk and 
			t1.first_ind=t6.first_ind and t1.second_ind=t6.second_ind and 
			t6.DTL_VLE ='mechPromoSkuId'
		where t1.DTL_VLE='mechRegSkuId'
		;
	quit;

	/*добавить channel+segment+promo_group_id*/
	proc fedsql sessref=casauto;
		create table casuser.pt_promo2ext{options replace=true} as
		select 
		t1.promo_rk,t1.p_cal_rk,
		t1.promo_id as promo_id,
		t1.promo_nm,
		t1.start_dt,t1.end_dt,
		t1.mechanicstype,
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
		t3.channel_cd,
		t2.segment_id,
		t4.promo_group_id_num as promo_group_id
		from casuser.PT_PROMO2 t1 
		left join casuser.segment_app t2 on t1.promo_id=t2.promo_id
		left join casuser.promochn_app t3 on t1.promo_id=t3.promo_id
		left join casuser.promo_group_id_map t4 on t1.promo_id=t4.promo_id
		;
	quit;

	/*мэппинг casuser.PROMO*/
	proc fedsql sessref=casauto;
		create table casuser.promo_enh{options replace=true} as
		select 
		coalesce(t1.channel_cd,t2.channel_cd) as channel_cd,
		coalesce(t1.promo_id,t2.promo_id) as promo_id,
		coalesce(t1.promo_mechanics,t2.mechanicstype) as promo_mechanics,
		coalesce(t1.promo_nm,t2.promo_nm) as promo_nm,
		coalesce(t1.segment_id,t2.segment_id) as segment_id,
		coalesce(t1.promo_group_id,t2.promo_group_id,-9999) as promo_group_id,
		t1.promo_price_amt as promo_price_amt, /*��� ����� ���������*/
		coalesce(t1.start_dt,t2.start_dt) as start_dt,
		coalesce(t1.end_dt,t2.end_dt) as end_dt,
		coalesce(t1.np_gift_price_amt,t2.mechanicsExpertReview) as np_gift_price_amt
		from casuser.promo t1 full outer join casuser.pt_promo2ext t2
		on t1.promo_id=t2.promo_id
		;
	quit;
	
	data casuser.promo_pbo (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..promo_x_pbo(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data casuser.promo_prod (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..promo_x_product(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	/*мэппинг casuser.PROMO_PBO*/
	proc fedsql sessref=casauto;
		create table casuser.promo_pbo_enh{options replace=true} as
		select 
		coalesce(t1.promo_id,t2.promo_id) as promo_id,
		coalesce(t1.pbo_location_id,t2.pbo_location_id) as pbo_location_id
		from casuser.promo_pbo t1 full outer join casuser.promopbo_app t2
		on t1.promo_id=t2.promo_id
		;
	quit;

	/*мэппинг casuser.PROMO_PROD*/
	proc fedsql sessref=casauto;
		create table casuser.promo_prod_enh{options replace=true} as
		select 
		coalesce(t1.promo_id,t2.promo_id) as promo_id,
		coalesce(t1.product_id,t2.product_id) as product_id,
		coalesce(t1.product_qty,t2.qty) as product_qty,
		coalesce(t1.option_number,t2.pos) as option_number,
		coalesce(t1.gift_flg,t2.gift_flag) as gift_flag,
		t2.Price as price
		from casuser.promo_prod t1 full outer join casuser.promoprod_app1 t2
		on t1.promo_id=t2.promo_id
		;
	quit;

	/*мэппинг casuser.media*/
	proc fedsql sessref=casauto;
		create table casuser.media_enh{options replace=true} as
		select 
		coalesce(t1.promo_group_id,t2.promo_group_id_num) as promo_group_id,
		/* Изменено наименование поля на report_dt с period_dt */
		coalesce(t1.period_dt,t2.period_dt) as report_dt,
		coalesce(t1.trp,t2.MARKETINGTRP) as trp
		from casuser.media t1 full outer join casuser.media_app t2
		on t1.promo_group_id=t2.promo_group_id_num
		;
	quit;

	proc casutil;
		%if &lmvOutCaslib. ne casuser %then %do;
			promote casdata='media_enh' incaslib='casuser' outcaslib="&lmvOutCaslib.";
			promote casdata='promo_prod_enh' incaslib='casuser' outcaslib="&lmvOutCaslib.";
			promote casdata='promo_pbo_enh' incaslib='casuser' outcaslib="&lmvOutCaslib.";
			promote casdata='promo_enh' incaslib='casuser' outcaslib="&lmvOutCaslib.";
		%end;
		droptable casdata='pt_promo1' incaslib='casuser' quiet;
		droptable casdata='pt_detail_transposed' incaslib='casuser' quiet;
		droptable casdata='promo' incaslib='casuser' quiet;
		droptable casdata='promo_id_exp' incaslib='casuser' quiet;
		droptable casdata='max_promo_id' incaslib='casuser' quiet;
		droptable casdata='promo_id_map' incaslib='casuser' quiet;
		droptable casdata='pt_promo2' incaslib='casuser' quiet;
		droptable casdata='pt_promo3' incaslib='casuser' quiet;
		droptable casdata='promopbo_app' incaslib='casuser' quiet;
		droptable casdata='media' incaslib='casuser' quiet;
		droptable casdata='max_promo_group_id' incaslib='casuser' quiet;
		droptable casdata='media_ext' incaslib='casuser' quiet;
		droptable casdata='promo_group_id_map' incaslib='casuser' quiet;
		droptable casdata='media_app' incaslib='casuser' quiet;
		droptable casdata='promo_detail_spl' incaslib='casuser' quiet;
		droptable casdata='promoprod_app1' incaslib='casuser' quiet;
		droptable casdata='pt_promo2ext' incaslib='casuser' quiet;
		droptable casdata='promo_pbo' incaslib='casuser' quiet;
		droptable casdata='promo_prod' incaslib='casuser' quiet;
	quit;
	
%mend add_promotool_marks;