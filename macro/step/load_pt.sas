/*****************************************************************
*  ВЕРСИЯ:
*     $Id: $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Загружает справочники в PT
*
*  ПАРАМЕТРЫ:
*     Нет
*									
*
******************************************************************
*  Использует: 
*	  %hier_pt;
*	  %bkp_pt;
*	  %load_pt_hierarchy;
*	  %load_pt_dict;
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*	%load_pt;
*
****************************************************************************
*  25-08-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro load_pt;
	/* bkp pt dir */
	*%bkp_pt;
	
	%local mvDatetime 
			lmvReportDttm
	;
	
	%let mvDatetime=&ETL_CURRENT_DTTM.;
	%let lmvReportDttm = &ETL_CURRENT_DTTM.;

	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto;
		caslib _all_ assign;
	%end;

	/* load PT.PRODUCT_HIERARCHY */
	%hier_pt(mpLvl=5, mpIn=etl_ia.PRODUCT_HIERARCHY, mpOut=PT_PRODUCT_HIERARCHY);

	proc sql noprint;
		create table PRODUCT_HIERARCHY_DTTM as
		select *
				,datetime() as valid_from_dttm format=DATETIME25.6
				,&ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6
		from PT_PRODUCT_HIERARCHY 
		;
	quit;

	%load_pt_hierarchy(mpDatetime=&mvDatetime.
					,mpMemberTableNm = pt.product_hierarchy
					);

	/* load PT.INTERNAL_ORG_HIERARCHY */
	%hier_pt(mpLvl=4, mpIn=etl_ia.PBO_LOC_HIERARCHY, mpOut=PT_PBO_LOC_HIERARCHY);

	proc sql noprint;
		create table INTERNAL_ORG_HIERARCHY_DTTM as
		select *, datetime() as valid_from_dttm format=DATETIME25.6, &ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6
		from PT_PBO_LOC_HIERARCHY 
		;
	quit;

	%load_pt_hierarchy(mpDatetime=&mvDatetime.
						,mpMemberTableNm = pt.internal_org_hierarchy
						);

	/* load product */
	/* get new batch for product */
	data etl_ia_product;
		set etl_ia.product(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	data etl_ia_product_hierarchy;
		set etl_ia.product_hierarchy(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data etl_ia_PRODUCT_ATTRIBUTES;
		set etl_ia.PRODUCT_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	/* 
	proc sql noprint;
		create table product as
		select p.PRODUCT_ID as member_rk
				,ifc(h.product_lvl=5,'['||strip(put(p.PRODUCT_ID,best.))||'] '||p.PRODUCT_NM,p.PRODUCT_NM) as member_nm
				,. as order_no
		from etl_ia_product p
		left join etl_ia_product_hierarchy h
			on h.PRODUCT_ID=p.PRODUCT_ID
		;
	quit;
	*/
	proc sql noprint;
		create table product as
		select p.PRODUCT_ID as member_rk
				,ifc(h.product_lvl=5,'['||strip(put(p.PRODUCT_ID,best.))||'] '||p.PRODUCT_NM,p.PRODUCT_NM) as member_nm
				,. as order_no
				, OFFER_TYPE.product_attr_value as OFFER_TYPE
				, ITEM_SIZE.product_attr_value as ITEM_SIZE
				, PRODUCT_GROUP.product_attr_value as PRODUCT_GROUP
				, PRODUCT_SUBGROUP_1.product_attr_value as PRODUCT_SUBGROUP_1
				, PRODUCT_SUBGROUP_2.product_attr_value as PRODUCT_SUBGROUP_2
		from etl_ia_product p
		left join etl_ia_product_hierarchy h
			on h.PRODUCT_ID=p.PRODUCT_ID
		left join etl_ia_PRODUCT_ATTRIBUTES OFFER_TYPE
			on OFFER_TYPE.product_id = p.PRODUCT_ID
			and OFFER_TYPE.product_attr_nm = 'OFFER_TYPE'
		left join etl_ia_PRODUCT_ATTRIBUTES ITEM_SIZE
			on ITEM_SIZE.product_id = p.PRODUCT_ID
			and ITEM_SIZE.product_attr_nm = 'ITEM_SIZE'
		left join etl_ia_PRODUCT_ATTRIBUTES PRODUCT_GROUP
			on PRODUCT_GROUP.product_id = p.PRODUCT_ID
			and PRODUCT_GROUP.product_attr_nm = 'PRODUCT_GROUP'
		left join etl_ia_PRODUCT_ATTRIBUTES PRODUCT_SUBGROUP_1
			on PRODUCT_SUBGROUP_1.product_id = p.PRODUCT_ID
			and PRODUCT_SUBGROUP_1.product_attr_nm = 'PRODUCT_SUBGROUP_1'
		left join etl_ia_PRODUCT_ATTRIBUTES PRODUCT_SUBGROUP_2
			on PRODUCT_SUBGROUP_2.product_id = p.PRODUCT_ID 
			and PRODUCT_SUBGROUP_2.product_attr_nm = 'PRODUCT_SUBGROUP_2'
	;
	quit;
	
	%load_pt_dict(mpDatetime=&mvDatetime.
						,mpMemberTableNm = pt.product
						);


	/*load dict PT.INTERNAL_ORG*/
	data etl_ia_PBO_LOCATION;
		set etl_ia.PBO_LOCATION(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	data etl_ia_PBO_LOC_HIERARCHY;
		set etl_ia.PBO_LOC_HIERARCHY(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	data etl_ia_PBO_LOC_ATTRIBUTES;
		set etl_ia.PBO_LOC_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc sql noprint;
		create table internal_org as
		select  ph4.PBO_LOCATION_ID as member_rk, 
				/*case when ph.pbo_location_lvl=4 then
				'['||catx('] [',strip(put(ph4.PBO_LOCATION_ID,best.)),
					pa1.pbo_loc_attr_value,
					ifc(strip(pa5.pbo_loc_attr_value)='Yes','BREAKFAST',''),
					ifc(strip(pa9.pbo_loc_attr_value)='Yes','McCafe',ifc(strip(pa9.pbo_loc_attr_value)='Yes EOTF','McCafe EOTF','')))
				||'] '||PBO_LOCATION_NM
				else PBO_LOCATION_NM end as member_nm, */
				case
					when ph.pbo_location_lvl=4 then
						'[' || strip(put(ph4.PBO_LOCATION_ID,best.)) || '] ' || PBO_LOCATION_NM
						else PBO_LOCATION_NM
					end as member_nm,
				COMPANY.pbo_loc_attr_value as COMPANY,
				pa1.pbo_loc_attr_value as BUILDING_TYPE,
				pa9.pbo_loc_attr_value as MCCAFE_TYPE,
				pa5.pbo_loc_attr_value as BREAKFAST,
				AGREEMENT_TYPE.pbo_loc_attr_value as AGREEMENT_TYPE,
				PRICE_LEVEL.pbo_loc_attr_value as PRICE_LEVEL,
				OPEN_DATE.pbo_loc_attr_value as OPEN_DATE,
				DRIVE_THRU.pbo_loc_attr_value as DRIVE_THRU,
				DELIVERY.pbo_loc_attr_value as DELIVERY,
				DELIVERY_OPEN_DATE.pbo_loc_attr_value as DELIVERY_OPEN_DATE,
				&mvDatetime. as valid_from_dttm format=DATETIME25.6, 
				&ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6,
				. as order_no
				
		from etl_ia_PBO_LOCATION ph4
		left join etl_ia_PBO_LOC_HIERARCHY ph
			on ph.pbo_location_id=ph4.pbo_location_id
		left join etl_ia_PBO_LOC_ATTRIBUTES pa1
			on pa1.pbo_location_id=ph4.pbo_location_id
			and pa1.pbo_loc_attr_nm='BUILDING_TYPE'
		left join etl_ia_PBO_LOC_ATTRIBUTES pa5
			on pa5.pbo_location_id=ph4.pbo_location_id
			and pa5.pbo_loc_attr_nm='BREAKFAST'
		left join etl_ia_PBO_LOC_ATTRIBUTES pa9
			on pa9.pbo_location_id=ph4.pbo_location_id
			and pa9.pbo_loc_attr_nm='MCCAFE_TYPE'
			left join etl_ia_PBO_LOC_ATTRIBUTES PRICE_LEVEL
			on PRICE_LEVEL.pbo_location_id=ph4.pbo_location_id
			and PRICE_LEVEL.pbo_loc_attr_nm='PRICE_LEVEL'
		left join etl_ia_PBO_LOC_ATTRIBUTES COMPANY
			on COMPANY.pbo_location_id=ph4.pbo_location_id
			and COMPANY.pbo_loc_attr_nm='COMPANY'
			
		left join etl_ia_PBO_LOC_ATTRIBUTES AGREEMENT_TYPE
			on AGREEMENT_TYPE.pbo_location_id=ph4.pbo_location_id
			and AGREEMENT_TYPE.pbo_loc_attr_nm='AGREEMENT_TYPE'
		left join etl_ia_PBO_LOC_ATTRIBUTES OPEN_DATE
			on OPEN_DATE.pbo_location_id=ph4.pbo_location_id
			and OPEN_DATE.pbo_loc_attr_nm='OPEN_DATE'
		left join etl_ia_PBO_LOC_ATTRIBUTES DRIVE_THRU
			on DRIVE_THRU.pbo_location_id=ph4.pbo_location_id
			and DRIVE_THRU.pbo_loc_attr_nm='DRIVE_THRU'
		left join etl_ia_PBO_LOC_ATTRIBUTES DELIVERY
			on DELIVERY.pbo_location_id=ph4.pbo_location_id
			and DELIVERY.pbo_loc_attr_nm='DELIVERY'
		left join etl_ia_PBO_LOC_ATTRIBUTES DELIVERY_OPEN_DATE
			on DELIVERY_OPEN_DATE.pbo_location_id=ph4.pbo_location_id
			and DELIVERY_OPEN_DATE.pbo_loc_attr_nm='DELIVERY_OPEN_DATE'
		;
	quit;

	%load_pt_dict(mpDatetime=&mvDatetime.
				,mpMemberTableNm = pt.internal_org
				);

	
	/*START: LOAD PT_CHANNEL */
	/* bkp etl_ia.lookup_channel 
	data pt_bkp.channel_lookup;
		set etl_ia.channel_lookup;
	run;
	*/
	data etl_ia_channel;
		set etl_ia.channel(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	/* START: generate channel_lookup */
	proc sql noprint;
			/* check for max member_rk in lookup table */
			select max(member_rk) as lmvMaxMemberRk into :lmvMaxMemberRk
			from etl_ia.channel_lookup
			;
			/* подготавливаем входной набор для генерации member_rk */
			create table work.channel_gen as
				select  (ifn(old.member_rk is null, 1, 0)) as flag /* флаг для расчета RK, если он суррогатный */
						,old.member_rk /* существующие rk */
						,batch.channel_cd
						,batch.channel_nm
						,batch.channel_lvl
						,batch.parent_channel_cd
				from etl_ia_channel batch
					left join etl_ia.channel_lookup old
					on batch.channel_cd = old.channel_cd
			;
	quit;
		
	/*calc new member_rk for batch*/
	data work.channel_new(drop=flag);
		set work.channel_gen(where=(flag=1));
		member_rk = _n_ + coalesce(&lmvMaxMemberRk.,0); /* генерирует РК на основе существующего максимального значения RK */
	run;
	
	/* append batch + old */
	proc append base=etl_ia.channel_lookup data=work.channel_new force;
	run;

	/* END: generate channel_lookup */

	proc sql noprint;
		create table channel as
			select lkp.member_rk
					,mn.CHANNEL_NM as member_nm
					,&mvDatetime. as valid_from_dttm format=DATETIME25.6
					,&ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6
					,. as order_no
			from etl_ia_channel mn
				left join etl_ia.channel_lookup lkp
				on mn.channel_cd = lkp.channel_cd
		;
	quit;

	%load_pt_dict(mpDatetime=&mvDatetime.
		,mpMemberTableNm = pt.channel
		);

	/*END: LOAD PT_CHANNEL */

	/*START: LOAD channel_hierarchy */

	
	proc sql noprint;
		create table CHANNEL_HIERARCHY as
		select a.member_rk as CHANNEL_ID
				,a.CHANNEL_LVL
				,b.member_rk as PARENT_CHANNEL_ID
		from etl_ia.channel_lookup a
		left join etl_ia.channel_lookup b
			on b.channel_cd=a.parent_channel_cd
		;
	quit;

	%hier_pt(mpLvl=2, mpIn=WORK.CHANNEL_HIERARCHY, mpOut=PT_CHANNEL_HIERARCHY);
	proc sql;
		create table CHANNEL_HIERARCHY_DTTM as
		select *
				,&mvDatetime. as valid_from_dttm format=DATETIME25.6
				,&ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6
		from PT_CHANNEL_HIERARCHY 
		;
	quit; 

/* 	proc sql noprint; */
/* 		create table CHANNEL_HIERARCHY_DTTM as */
/* 		select  */
/* 				member_rk as prnt_member_rk, */
/* 				member_rk as member_rk,  */
/* 				0 as btwn_lvl_cnt,  */
/* 				'Y' as is_bottom_flg, */
/* 				'Y' as is_top_flg, */
/* 				&mvDatetime. as valid_from_dttm format=DATETIME25.6,  */
/* 				&ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6 */
/* 		from etl_ia.channel_lookup */
/* 		where channel_cd='ALL' */
/* 		; */
/* 	quit; */

	%load_pt_hierarchy(mpDatetime=&mvDatetime.
						,mpMemberTableNm = pt.channel_hierarchy
						);

	/*END: LOAD channel_hierarchy */

	/*START: LOAD PT_SEGMENT */
	data etl_ia_segment;
		set etl_ia.segment(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc sql noprint;
		create table SEGMENT as
		select SEGMENT_ID as member_rk
				,SEGMENT_NM as member_nm
				,&mvDatetime.  as valid_from_dttm format=DATETIME25.6
				,&ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6
				,. as order_no
		from etl_ia_segment
		;
	quit;

	%load_pt_dict(mpDatetime=&mvDatetime.
				,mpMemberTableNm = pt.segment
				);
	/*END: LOAD PT_SEGMENT */
	
	/* START HIERARCHY_SEGMENT */
	/*%hier_pt(mpLvl=2, mpIn=etl_ia.SEGMENT_HIERARCHY, mpOut=PT_SEGMENT_HIERARCHY);

	proc sql noprint;
		create table SEGMENT_HIERARCHY_DTTM as
		select *, datetime() as valid_from_dttm format=DATETIME25.6, &ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6
		from PT_SEGMENT_HIERARCHY
		;
	quit;

	%load_pt_hierarchy(mpDatetime=&mvDatetime.
						,mpMemberTableNm = pt.segment_org_hierarchy
						);
	*/
	/* END HIERARCHY_SEGMENT */
	
	
	/* START PROMO */
	PROC SQL NOPRINT;	
		connect using pt;
			EXECUTE BY pt
				(
					TRUNCATE TABLE public.promo_delta;
				)
			;
		DISCONNECT FROM pt;
	QUIT;
	
	proc sql noprint;
		create table work.promo_diffs as
			select
			"sassrv" as created_by_nm length = 30
			,datetime() as creation_dttm format=datetime.
			,"" as modified_by_nm length = 30
			,. as modified_dttm 
			,1 as p_cal_rk 
			,"" as promo_comment_txt length=300
			,t1.END_DT as promo_end_dttm 
			,put(t1.promo_id, best32.) as promo_id
			,t1.promo_nm 
			,. as promo_rk 
			,t1.START_DT as promo_start_dttm 
			,"approved" as promo_status_cd length =10
			from ia.ia_promo t1
			left join pt.promo t2
			on t1.promo_id = input(t2.promo_id, best32.)
			/* and t1.promo_rk = t2.promo_rk */
			where coalesce(t2.promo_id, "1") = "1"
		;
		select max(promo_rk) into :mvMax_Promo_Rk
		from pt.promo
		;
	quit;

	proc append base=pt.promo_delta data=work.promo_diffs force; 
	run; 

	PROC SQL NOPRINT;	
		connect using pt;
			EXECUTE BY pt
				(
					INSERT INTO public.promo(
					 p_cal_rk, promo_id, promo_nm, promo_start_dttm, promo_end_dttm, promo_comment_txt, promo_status_cd, created_by_nm, creation_dttm, modified_by_nm, modified_dttm)
					select p_cal_rk, promo_id, promo_nm, promo_start_dttm, promo_end_dttm, promo_comment_txt, promo_status_cd, created_by_nm, creation_dttm, modified_by_nm, modified_dttm
					from public.promo_delta
					;
				)
			;
		DISCONNECT FROM pt;
	QUIT;
	
	proc sql noprint;
		create table work.pt_promo as
			select
			t2.promo_rk
			,input(t2.promo_id, best32.) as promo_id
			,t2.promo_nm
			from pt.promo_delta t1
			left join pt.promo t2
			on t1.promo_id = t2.promo_id
;
	quit;

	proc sql noprint;
		create table work.promo_details_full as
		select t1.*
			,t2.product_id
			,t2.product_qty
			,t2.option_number
			,t2.gift_flg
			,t3.member_nm
			,t4.promo_id as promo_id_n
		from work.pt_promo t1
		left join ia.ia_promo t4
			on t1.promo_nm = t4.promo_nm
		left join ETL_IA.PROMO_X_PRODUCT t2
	/* 	on input(t1.promo_id,best32.) = t2.promo_id */
			on t4.promo_id = t2.promo_id
		left join PT.PRODUCT t3
			on t2.product_id = t3.member_rk
	;
	
		create table work.promo_details_qnt as
			select promo_id_n 
					,count(promo_id_n) as cnt
			from work.promo_details_full 
			group by promo_id_n
			order by promo_id_n
	;

		create table work.promo_details_final as
		select t1.*, t2.cnt
			from work.promo_details_full t1
			left join work.promo_details_qnt t2
				on t1.promo_id_n=t2.promo_id_n
	;
			create table work.promo_with_trp as
			select distinct 
					t1.promo_rk
					,avg(t3.trp) as trp
					,t2.promo_nm
					,t2.promo_id as promo_id_n
					,t2.np_gift_price_amt
					,t2.promo_mechanics
			from  work.pt_promo t1
				left join  ia.ia_promo t2
					on t1.promo_nm = t2.promo_nm
				left join  ia.ia_media t3
					on t2.promo_group_id = t3.promo_group_id
				group by t1.promo_rk
					,t2.promo_nm
					,t2.promo_id
					,t2.np_gift_price_amt
					,t2.promo_mechanics
	;
		
		create table work.count_gift_flg as
			select  promo_rk
					,count(gift_flg) as cnt_gift_flg
			from work.promo_details_full
			where gift_flg='Y'
			group by promo_rk
	;
		create table work.promo_final as
			select t1.*
					,coalesce(t2.cnt_gift_flg, 0) as cnt_gift_flg
			from  work.promo_with_trp t1
			left join  work.count_gift_flg t2
 				on t1.promo_rk = t2.promo_rk
	;
	quit;

	data work.promo_trp_trnsp(keep=promo_rk promo_dtl_cd promo_dtl_vle);
		length promo_dtl_cd $30
			promo_dtl_vle $2000
			;
		set  work.promo_final;
		promo_dtl_cd = "marketingTrp";
		promo_dtl_vle = put(trp,best32.);
		output;
		promo_dtl_cd = "mechanicsExpertReview";
		promo_dtl_vle = put(np_gift_price_amt,best32.);
		output;
		promo_dtl_cd =  "mechanicsType";
		promo_dtl_vle = promo_mechanics;
		output;
		if np_gift_price_amt ne . then do;	
			promo_dtl_cd = "benefitRadio";
			promo_dtl_vle = "no product";
			output;	
		end;
		else if cnt_gift_flg > 0 then do;
			promo_dtl_cd = "benefitRadio";
			promo_dtl_vle = "product";
			output;	
		end;
		else do;
			promo_dtl_cd = "benefitRadio";
			promo_dtl_vle = "no";
			output;	
		end;

	run;

	data promo_transp(keep=promo_rk promo_dtl_cd promo_dtl_vle);
	length promo_dtl_cd $30
			promo_dtl_vle $2000
			;
		set work.promo_details_final;
		retain promo_cnt 0 promo_val 0 iter 1;
		
		
		if promo_cnt = cnt and promo_val=promo_id_n then do;
			iter = iter+1;
		end;
		else do;
			iter = 1;
		end;
		
		
		if gift_flg='N' then do;
			promo_dtl_cd = cat("mechPosition_1_",iter);
			promo_dtl_vle = put(option_number,best32.);
			output;
			promo_dtl_cd = cat("mechRegSkuId_1_",iter);
			promo_dtl_vle = put(product_id,best32.);
			output;
			promo_dtl_cd = cat("mechPromoSkuId_1_",iter);
			promo_dtl_vle = put(product_id,best32.);
			output;
			promo_dtl_cd = cat("mechPromoSkuQty_1_",iter);
			promo_dtl_vle = put(product_qty,best32.);
			output;
			promo_dtl_cd = cat("mechPromoSkuTitle_1_",iter);
			promo_dtl_vle = member_nm;
			output;
			promo_dtl_cd = cat("mechRegSkuTitle_1_",iter);
			promo_dtl_vle = member_nm;
			output;
		end;
		else do;
			promo_dtl_cd = cat("mechPosition_2_",iter);
			promo_dtl_vle = put(option_number,best32.);
			output;
			promo_dtl_cd = cat("mechRegSkuId_2_",iter);
			promo_dtl_vle = put(product_id,best32.);
			output;
			promo_dtl_cd = cat("mechPromoSkuId_2_",iter);
			promo_dtl_vle = put(product_id,best32.);
			output;
			promo_dtl_cd = cat("mechPromoSkuQty_2_",iter);
			promo_dtl_vle = put(product_qty,best32.);
			output;
			promo_dtl_cd = cat("mechPromoSkuTitle_2_",iter);
			promo_dtl_vle = member_nm;
			output;
			promo_dtl_cd = cat("mechRegSkuTitle_2_",iter);
			promo_dtl_vle = member_nm;
		end;
		
		/* save previous value */
		promo_cnt = cnt;
		promo_val = promo_id_n;
		
	run;

	proc append base=PT.PROMO_DETAIL data=work.promo_transp force;
	run;

	proc append base=PT.PROMO_DETAIL data=work.promo_trp_trnsp force;
	run;
	
	
	
	
	proc sql noprint;
		create table work.promo_x_pbo_full as
		select distinct /* t1.* */
			t3.pbo_location_id
/* 			,t2.promo_id as promo_id_n */
			,t2.segment_id
			,t5.member_rk as channel_rk_n
		from work.pt_promo t1
		left join ia.ia_promo t2
			on t1.promo_nm = t2.promo_nm
		left join ia.ia_promo_x_pbo t3
			on t3.promo_id = t2.promo_id
		left join ETL_IA.CHANNEL_LOOKUP t5
			on t2.channel_cd = t5.channel_cd
		order by promo_rk
	;
	quit;
	
	/* расчет димпоинта для загрузки для текущих промо */
	data work.dimpoint(drop=channel_rk_n pbo_location_id segment_id);
		set work.promo_x_pbo_full;
		length dp_hash_value $32;
		if channel_rk_n = 1 then do;
			channel_rk = 1;
			channel_lvl1_rk = 1;
			channel_lvl2_rk = .;
		end;
		else do;
			channel_rk = channel_rk_n;
			channel_lvl1_rk = 1;
			channel_lvl2_rk = channel_rk_n;
		end;
		channel_lvl3_rk =.;
		channel_lvl4_rk =.;
		channel_lvl5_rk =.;
		channel_lvl6_rk =.;
		channel_lvl7_rk =.;
		channel_lvl8_rk =.;
		channel_lvl9_rk =.;
		channel_lvl10_rk =.;
	
		
		if pbo_location_id = 1 then do;
			int_org_rk =1;
			int_org_lvl1_rk  = 1;
			int_org_lvl2_rk =.;
			int_org_lvl3_rk =.;
			int_org_lvl4_rk =.;
			int_org_lvl5_rk =.;
			int_org_lvl6_rk =.;
			int_org_lvl7_rk =.;
			int_org_lvl8_rk =.;
			int_org_lvl9_rk =.;
			int_org_lvl10_rk =.;
		end; 
		else do;
			int_org_rk = pbo_location_id;
			int_org_lvl1_rk = 1;
			int_org_lvl2_rk =.;
			int_org_lvl3_rk =.;
			int_org_lvl4_rk = pbo_location_id;
			int_org_lvl5_rk =.;
			int_org_lvl6_rk =.;
			int_org_lvl7_rk =.;
			int_org_lvl8_rk =.;
			int_org_lvl9_rk =.;
			int_org_lvl10_rk =.;
		end;
		
		product_rk =1;
		product_lvl1_rk =1;	
		product_lvl2_rk =.;
		product_lvl3_rk =.;
		product_lvl4_rk =.;
		product_lvl5_rk =.;
		product_lvl6_rk =.;
		product_lvl7_rk =.;
		product_lvl8_rk =.;
		product_lvl9_rk =.;
		product_lvl10_rk =.;
	
		segment_rk = segment_id;
		segment_lvl1_rk = segment_id;
		segment_lvl2_rk =.;
		segment_lvl3_rk =.;
		segment_lvl4_rk =.;
		segment_lvl5_rk =.;
		segment_lvl6_rk =.;
		segment_lvl7_rk =.;
		segment_lvl8_rk =.;
		segment_lvl9_rk =.;
		segment_lvl10_rk =.;
	
		dim_point_id = catx("_", pbo_location_id, product_lvl1_rk,channel_rk_n, segment_id); 
		dp_hash_value = SHA256HEX(catx("_",coalesce(channel_lvl1_rk,0), coalesce(channel_lvl10_rk,0) , coalesce(channel_lvl2_rk,0) , coalesce(channel_lvl3_rk,0) , coalesce(channel_lvl4_rk,0) , coalesce(channel_lvl5_rk,0) , coalesce(channel_lvl6_rk,0) , coalesce(channel_lvl7_rk,0) , coalesce(channel_lvl8_rk,0) , coalesce(channel_lvl9_rk,0) , coalesce(channel_rk,0) , coalesce(int_org_lvl1_rk,0) , coalesce(int_org_lvl10_rk,0) , coalesce(int_org_lvl2_rk,0) , coalesce(int_org_lvl3_rk,0) , coalesce(int_org_lvl4_rk,0) , coalesce(int_org_lvl5_rk,0) , coalesce(int_org_lvl6_rk,0) , coalesce(int_org_lvl7_rk,0) , coalesce(int_org_lvl8_rk,0) , coalesce(int_org_lvl9_rk,0) , coalesce(int_org_rk,0) , coalesce(product_lvl1_rk,0) , coalesce(product_lvl10_rk,0) , coalesce(product_lvl2_rk,0) , coalesce(product_lvl3_rk,0) , coalesce(product_lvl4_rk,0) , coalesce(product_lvl5_rk,0) , coalesce(product_lvl6_rk,0) , coalesce(product_lvl7_rk,0) , coalesce(product_lvl8_rk,0) , coalesce(product_lvl9_rk,0) , coalesce(product_rk,0) , coalesce(segment_lvl1_rk,0) , coalesce(segment_lvl10_rk,0) , coalesce(segment_lvl2_rk,0) , coalesce(segment_lvl3_rk,0) , coalesce(segment_lvl4_rk,0) , coalesce(segment_lvl5_rk,0) , coalesce(segment_lvl6_rk,0) , coalesce(segment_lvl7_rk,0) , coalesce(segment_lvl8_rk,0) , coalesce(segment_lvl9_rk,0) , coalesce(segment_rk,0)));
	run;
	/* расчитываем хеш значений димпоинтов для выявления дельты */
	data pt_dimpoint;
		set pt.dim_point;
		length dp_hash_value $32;
		dp_hash_value = SHA256HEX(catx("_",coalesce(channel_lvl1_rk,0), coalesce(channel_lvl10_rk,0) , coalesce(channel_lvl2_rk,0) , coalesce(channel_lvl3_rk,0) , coalesce(channel_lvl4_rk,0) , coalesce(channel_lvl5_rk,0) , coalesce(channel_lvl6_rk,0) , coalesce(channel_lvl7_rk,0) , coalesce(channel_lvl8_rk,0) , coalesce(channel_lvl9_rk,0) , coalesce(channel_rk,0) , coalesce(int_org_lvl1_rk,0) , coalesce(int_org_lvl10_rk,0) , coalesce(int_org_lvl2_rk,0) , coalesce(int_org_lvl3_rk,0) , coalesce(int_org_lvl4_rk,0) , coalesce(int_org_lvl5_rk,0) , coalesce(int_org_lvl6_rk,0) , coalesce(int_org_lvl7_rk,0) , coalesce(int_org_lvl8_rk,0) , coalesce(int_org_lvl9_rk,0) , coalesce(int_org_rk,0) , coalesce(product_lvl1_rk,0) , coalesce(product_lvl10_rk,0) , coalesce(product_lvl2_rk,0) , coalesce(product_lvl3_rk,0) , coalesce(product_lvl4_rk,0) , coalesce(product_lvl5_rk,0) , coalesce(product_lvl6_rk,0) , coalesce(product_lvl7_rk,0) , coalesce(product_lvl8_rk,0) , coalesce(product_lvl9_rk,0) , coalesce(product_rk,0) , coalesce(segment_lvl1_rk,0) , coalesce(segment_lvl10_rk,0) , coalesce(segment_lvl2_rk,0) , coalesce(segment_lvl3_rk,0) , coalesce(segment_lvl4_rk,0) , coalesce(segment_lvl5_rk,0) , coalesce(segment_lvl6_rk,0) , coalesce(segment_lvl7_rk,0) , coalesce(segment_lvl8_rk,0) , coalesce(segment_lvl9_rk,0) , coalesce(segment_rk,0)));
	run;
	/* выявление дельты через сравнение хешей */
	proc sql noprint;
		create table work.dimpoint_delta as
			select distinct t1.*
			from work.dimpoint t1
				left join pt_dimpoint t2
					on t1.dp_hash_value = t2.dp_hash_value
 			where coalesce(t2.dim_point_id, '0') = '0'  
			;
	quit;

	/* загружаем дельту в ПТ */
	proc sql noprint; 
 		INSERT INTO pt.dim_point( 
		dim_point_id, int_org_rk, int_org_lvl1_rk, int_org_lvl2_rk, int_org_lvl3_rk, int_org_lvl4_rk, int_org_lvl5_rk, int_org_lvl6_rk, int_org_lvl7_rk, int_org_lvl8_rk, int_org_lvl9_rk, int_org_lvl10_rk, product_rk, product_lvl1_rk, product_lvl2_rk, product_lvl3_rk, product_lvl4_rk, product_lvl5_rk, product_lvl6_rk, product_lvl7_rk, product_lvl8_rk, product_lvl9_rk, product_lvl10_rk, channel_rk, channel_lvl1_rk, channel_lvl2_rk, channel_lvl3_rk, channel_lvl4_rk, channel_lvl5_rk, channel_lvl6_rk, channel_lvl7_rk, channel_lvl8_rk, channel_lvl9_rk, channel_lvl10_rk, segment_rk, segment_lvl1_rk, segment_lvl2_rk, segment_lvl3_rk, segment_lvl4_rk, segment_lvl5_rk, segment_lvl6_rk, segment_lvl7_rk, segment_lvl8_rk, segment_lvl9_rk, segment_lvl10_rk) 
  
 		select  
 		dim_point_id, int_org_rk, int_org_lvl1_rk, int_org_lvl2_rk, int_org_lvl3_rk, int_org_lvl4_rk, int_org_lvl5_rk, int_org_lvl6_rk, int_org_lvl7_rk, int_org_lvl8_rk, int_org_lvl9_rk, int_org_lvl10_rk, product_rk, product_lvl1_rk, product_lvl2_rk, product_lvl3_rk, product_lvl4_rk, product_lvl5_rk, product_lvl6_rk, product_lvl7_rk, product_lvl8_rk, product_lvl9_rk, product_lvl10_rk, channel_rk, channel_lvl1_rk, channel_lvl2_rk, channel_lvl3_rk, channel_lvl4_rk, channel_lvl5_rk, channel_lvl6_rk, channel_lvl7_rk, channel_lvl8_rk, channel_lvl9_rk, channel_lvl10_rk, segment_rk, segment_lvl1_rk, segment_lvl2_rk, segment_lvl3_rk, segment_lvl4_rk, segment_lvl5_rk, segment_lvl6_rk, segment_lvl7_rk, segment_lvl8_rk, segment_lvl9_rk, segment_lvl10_rk 
 		from work.dimpoint_delta; 
 	quit; 

	/* снова расчитываем хеш значений димпоинтов для новых загруженных значений */
	data pt_dimpoint_new;
		set pt.dim_point;
		length dp_hash_value $32;
		dp_hash_value = SHA256HEX(catx("_",coalesce(channel_lvl1_rk,0), coalesce(channel_lvl10_rk,0) , coalesce(channel_lvl2_rk,0) , coalesce(channel_lvl3_rk,0) , coalesce(channel_lvl4_rk,0) , coalesce(channel_lvl5_rk,0) , coalesce(channel_lvl6_rk,0) , coalesce(channel_lvl7_rk,0) , coalesce(channel_lvl8_rk,0) , coalesce(channel_lvl9_rk,0) , coalesce(channel_rk,0) , coalesce(int_org_lvl1_rk,0) , coalesce(int_org_lvl10_rk,0) , coalesce(int_org_lvl2_rk,0) , coalesce(int_org_lvl3_rk,0) , coalesce(int_org_lvl4_rk,0) , coalesce(int_org_lvl5_rk,0) , coalesce(int_org_lvl6_rk,0) , coalesce(int_org_lvl7_rk,0) , coalesce(int_org_lvl8_rk,0) , coalesce(int_org_lvl9_rk,0) , coalesce(int_org_rk,0) , coalesce(product_lvl1_rk,0) , coalesce(product_lvl10_rk,0) , coalesce(product_lvl2_rk,0) , coalesce(product_lvl3_rk,0) , coalesce(product_lvl4_rk,0) , coalesce(product_lvl5_rk,0) , coalesce(product_lvl6_rk,0) , coalesce(product_lvl7_rk,0) , coalesce(product_lvl8_rk,0) , coalesce(product_lvl9_rk,0) , coalesce(product_rk,0) , coalesce(segment_lvl1_rk,0) , coalesce(segment_lvl10_rk,0) , coalesce(segment_lvl2_rk,0) , coalesce(segment_lvl3_rk,0) , coalesce(segment_lvl4_rk,0) , coalesce(segment_lvl5_rk,0) , coalesce(segment_lvl6_rk,0) , coalesce(segment_lvl7_rk,0) , coalesce(segment_lvl8_rk,0) , coalesce(segment_lvl9_rk,0) , coalesce(segment_rk,0)));
	run;
	
	/* сборки витрины для загрузки данных в promo_x_dimpoint */
	proc sql noprint;
		create table work.promo_x_dp_init as
		select distinct t1.promo_rk
			,t3.pbo_location_id
			,t2.segment_id
			,t5.member_rk as channel_rk_n
		from work.pt_promo t1
		left join ia.ia_promo t2
			on t1.promo_nm = t2.promo_nm
		left join ia.ia_promo_x_pbo t3
			on t3.promo_id = t2.promo_id
		left join ETL_IA.CHANNEL_LOOKUP t5
			on t2.channel_cd = t5.channel_cd
		order by promo_rk
	;
	quit;
	
	/* добавляем аттрибуты для соединения с пт_димпоинтом */
	proc sql noprint;
		create table work.promo_x_dp_full as
		select distinct 
			t1.*, t2.*
		from work.promo_x_dp_init t1
		left join work.dimpoint t2
			on  t1.pbo_location_id = t2.int_org_rk
			and t1.segment_id = t2.segment_rk
			and	t1.channel_rk_n = t2.channel_rk
			and 1 = t2.product_rk
		order by promo_rk
	;
	quit;
	
	/* рассчитываем хеш для джоина с пт_димпоинт */
	data work.promo_x_dp_hash;
		set work.promo_x_dp_full;
/* 		dp_hash_value = SHA256HEX(catx("_",channel_lvl1_rk, channel_lvl10_rk ,channel_lvl2_rk ,channel_lvl3_rk ,channel_lvl4_rk ,channel_lvl5_rk ,channel_lvl6_rk ,channel_lvl7_rk ,channel_lvl8_rk ,channel_lvl9_rk ,channel_rk ,int_org_lvl1_rk ,int_org_lvl10_rk ,int_org_lvl2_rk ,int_org_lvl3_rk ,int_org_lvl4_rk ,int_org_lvl5_rk ,int_org_lvl6_rk ,int_org_lvl7_rk ,int_org_lvl8_rk ,int_org_lvl9_rk ,int_org_rk ,product_lvl1_rk ,product_lvl10_rk ,product_lvl2_rk ,product_lvl3_rk ,product_lvl4_rk ,product_lvl5_rk ,product_lvl6_rk ,product_lvl7_rk ,product_lvl8_rk ,product_lvl9_rk ,product_rk ,segment_lvl1_rk ,segment_lvl10_rk ,segment_lvl2_rk ,segment_lvl3_rk ,segment_lvl4_rk ,segment_lvl5_rk ,segment_lvl6_rk ,segment_lvl7_rk ,segment_lvl8_rk ,segment_lvl9_rk ,segment_rk)); */
	run;
	
	proc sql noprint;
		create table work.promo_x_dp_fin as
			select distinct 
					t1.promo_rk
					,t2.dim_point_rk

			from work.promo_x_dp_hash t1
			left join work.pt_dimpoint_new t2
				on t1.dp_hash_value=t2.dp_hash_value
		;
	quit;

	proc sql noprint;
		create table work.promo_x_dp_delta as
		select distinct t1.promo_rk
						,t1.dim_point_rk
		from work.promo_x_dp_fin t1
		left join PT.PROMO_X_DIM_POINT t2
			on t1.promo_rk = t2.promo_rk
			and t1.dim_point_rk = t2.dim_point_rk
		where coalesce(t2.promo_rk,0)=0
/* 		and t1.dim_point_rk ne . */
		order by promo_rk
	;
	quit;

	proc sql noprint;
		INSERT INTO pt.promo_x_dim_point(
		dim_point_rk, promo_rk)
		select dim_point_rk, promo_rk
		from work.promo_x_dp_delta
		;
	quit;
	/* END PROMO */
%mend load_pt;