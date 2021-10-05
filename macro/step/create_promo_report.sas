%macro create_promo_report(mpLibName=);

	%local lmvLibName;

	%let lmvLibName = %upcase(&mpLibName.);
	
	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=
						);
	
	proc casutil;
		droptable casdata="promo_report_view" incaslib="casuser" quiet;
		droptable casdata="channel" incaslib="casuser" quiet;
		droptable casdata="pt_channel_hierarchy" incaslib="casuser" quiet;
		droptable casdata="promo_calculation" incaslib="casuser" quiet;
		load data=&lmvLibName..promo_x_dim_point casout='promo_x_dim_point' outcaslib="casuser" replace;
		load data=&lmvLibName..promo_detail casout='promo_detail' outcaslib="casuser" replace;
		load data=&lmvLibName..promo_calendar casout='promo_calendar' outcaslib="casuser" replace;
		load data=&lmvLibName..promo casout='promo' outcaslib="casuser" replace;
		load data=&lmvLibName..dim_point casout='dim_point' outcaslib="casuser" replace;
		load data=&lmvLibName..internal_org casout='internal_org' outcaslib="casuser" replace;
		load data=&lmvLibName..product casout='product' outcaslib="casuser" replace;
		load data=&lmvLibName..segment casout='segment' outcaslib="casuser" replace;
		load data=&lmvLibName..channel casout='channel' outcaslib="casuser" replace;
	run;
	quit;
	
	
	proc fedsql sessref=casauto;
		create table casuser.promo_report_view{options replace=true} as 
		select 
			pr.promo_id
			,pr.promo_nm
			,pr.promo_rk
			,pr.promo_start_dttm
			,pr.promo_end_dttm
			,pr.promo_status_cd
			,pr.creation_dttm
			,pdmt.promo_dtl_vle as mechanicsType
			,pdpl.promo_dtl_vle as platform
			,pdspl.promo_dtl_vle as subPlatform
			,pdben.promo_dtl_vle as benefit
			,pdbenval.promo_dtl_vle as benefitVal
			,pdmrk.promo_dtl_vle as marketingDigital
			,pdmrkst.promo_dtl_vle as marketingInStore
			,pdmrkstooh.promo_dtl_vle as marketingOoh
			,pdmrksttrp.promo_dtl_vle as marketingTrp
		from casuser.promo pr
			left join casuser.promo_detail pdmt
				on pr.promo_rk = pdmt.promo_rk
				and pdmt.promo_dtl_cd = 'mechanicsType'
			left join casuser.promo_detail pdpl
				on pr.promo_rk = pdpl.promo_rk
				and pdpl.promo_dtl_cd = 'platform'
			left join casuser.promo_detail pdspl
				on pr.promo_rk = pdspl.promo_rk
				and pdspl.promo_dtl_cd = 'subPlatform'
			left join casuser.promo_detail pdben
				on pr.promo_rk = pdben.promo_rk
				and pdben.promo_dtl_cd = 'benefitRadio'
			left join casuser.promo_detail pdbenval
				on pr.promo_rk = pdbenval.promo_rk
				and pdbenval.promo_dtl_cd = 'mechanicsExpertReview'
			left join casuser.promo_detail pdmrk
				on pr.promo_rk = pdmrk.promo_rk
				and pdmrk.promo_dtl_cd = 'marketingDigital'
			left join casuser.promo_detail pdmrkst
				on pr.promo_rk = pdmrkst.promo_rk
				and pdmrkst.promo_dtl_cd = 'marketingInStore'
			left join casuser.promo_detail pdmrkstooh
				on pr.promo_rk = pdmrkstooh.promo_rk
				and pdmrkstooh.promo_dtl_cd = 'marketingOoh'
			left join casuser.promo_detail pdmrksttrp
				on pr.promo_rk = pdmrksttrp.promo_rk
				and pdmrksttrp.promo_dtl_cd = 'marketingTrp'	
			
		;
	quit;


	/* Таблица №1 - отдельно список промоакций */
	proc fedsql sessref=casauto;
		create table casuser.PROMO_LIST_REPORT{options replace=true} as 
		SELECT DISTINCT
			 pr.promo_id
			,pr.promo_nm
			,pr.promo_rk
			,pr.promo_start_dttm
			,pr.promo_end_dttm
			,pr.promo_status_cd
			,pr.creation_dttm
			,seg.member_nm as segment_nm
			,ch.member_nm as channel
			,pr.mechanicsType
			,pr.platform
			,pr.subPlatform
			,pr.benefit
			,pr.benefitVal
			,pr.marketingDigital
			,pr.marketingInStore
			,pr.marketingOoh
			,pr.marketingTrp

		FROM casuser.promo_report_view pr
			left join casuser.promo_x_dim_point pxdp
					on pxdp.promo_rk = pr.promo_rk
			left join casuser.dim_point dp
				on dp.dim_point_rk = pxdp.dim_point_rk
			left join casuser.segment seg
				on dp.segment_rk = seg.member_rk
			left join casuser.channel ch
				on dp.channel_rk = ch.member_rk
		;
	quit;

	/* Таблица №2 - отдельно список магазинов для каждой акции
	   (Promo_id + магазины с соотв. параметрами
	   на основе исходной таблицы (имя магазина и т.д.)*/
	proc fedsql sessref=casauto;
		create table casuser.ORG_PROMO_LIST_REPORT{options replace=true} as 
		SELECT DISTINCT
			 pr.promo_rk
			 ,io.member_nm as int_org_nm
			 ,io.member_rk as int_org_rk
		FROM casuser.promo_report_view pr
		left join casuser.promo_x_dim_point pxdp
				on pxdp.promo_rk = pr.promo_rk
		left join casuser.dim_point dp
			on dp.dim_point_rk = pxdp.dim_point_rk
		left join casuser.internal_org io
			on dp.int_org_rk = io.member_rk
			and io.member_nm <> 'All PBO';
	quit;

	/* Таблица №3 - отдельно список продуктов для каждой акции (тоже самое для промо+продукты)*/
		/* Таблица №3 - отдельно список продуктов для каждой акции (тоже самое для промо+продукты)*/
	proc fedsql sessref=casauto;
		create table casuser.PRODUCT_PROMO_LIST_REPORT{options replace=true} as 
		SELECT DISTINCT
			 pr.promo_rk
			 ,prod_nm.promo_dtl_vle as product_nm
			 ,coalesce(pdpos.promo_dtl_vle,'0') as mech_position
		FROM casuser.promo_report_view pr
			left join casuser.promo_detail prod_nm
					on pr.promo_rk = prod_nm.promo_rk
					and prod_nm.promo_dtl_cd like 'mechPromoSkuTitle_%'
					and prod_nm.promo_dtl_cd not like 'null'
			left join casuser.promo_detail pdpos
					on pr.promo_rk = pdpos.promo_rk
					and pdpos.promo_dtl_cd like 'mechPosition%'	
					and substr(prod_nm.promo_dtl_cd,19,3) = substr(pdpos.promo_dtl_cd,14,3) 
					and pdpos.promo_dtl_cd not like 'null'
	;	
	quit;
	
	proc casutil;
		DROPTABLE CASDATA="PROMO_LIST_REPORT" INCASLIB="public" QUIET;
		DROPTABLE CASDATA="ORG_PROMO_LIST_REPORT" INCASLIB="public" QUIET;
		DROPTABLE CASDATA="PRODUCT_PROMO_LIST_REPORT" INCASLIB="public" QUIET;
		promote casdata="PROMO_LIST_REPORT" casout="PROMO_LIST_REPORT" incaslib="casuser" outcaslib="public";
		promote casdata="ORG_PROMO_LIST_REPORT" casout="ORG_PROMO_LIST_REPORT" incaslib="casuser" outcaslib="public";
		promote casdata="PRODUCT_PROMO_LIST_REPORT" casout="PRODUCT_PROMO_LIST_REPORT" incaslib="casuser" outcaslib="public";
	run;
	
	%dp_export_csv(mpInput=public.PROMO_LIST_REPORT, 
	mpTHREAD_CNT=1, mpPath=/data/files/output/);


	%dp_export_csv(mpInput=public.ORG_PROMO_LIST_REPORT, 
	mpTHREAD_CNT=1, mpPath=/data/files/output/);


	%dp_export_csv(mpInput=public.PRODUCT_PROMO_LIST_REPORT, 
	mpTHREAD_CNT=1, mpPath=/data/files/output/);

%mend create_promo_report;