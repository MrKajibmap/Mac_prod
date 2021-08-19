
%macro price_promo_future(
    mpPromoTable         	= CASUSER.PROMO_ENH
    , mpPromoPboTable 	 	= CASUSER.PROMO_PBO_ENH_UNFOLD
    , mpPromoProdTable   	= CASUSER.PROMO_PROD_ENH
    , mpPriceRegFutTable 	= DM_ABT.PRICE_REGULAR_FUTURE
    , mpVatTable		 	= CASUSER.VAT
    , mpLBPTable		 	= CASUSER.LBP
    , mpPboLocAttributes	= CASUSER.PBO_LOC_ATTRIBUTES
    , mpProductAttrTable    = CASUSER.PRODUCT_ATTRIBUTES
    , mpPriceIncreaseTable 	= CASUSER.PRICE_INCREASE
    , mpOutTable	  	 	= DM_ABT.PRICE_PROMO_FUTURE
	, mpPromoClRk			= 
    );

    %local 
        lmvOutTableName
        lmvOutTableCLib
        lmvNPPromoSupList
        lmvDiscountList
        lmvNonProdGiftList
        lmvBogoList
        lmvEVMSetList
        lmvPairsList
        lmvDiscForVolumeList
        lmvNewLaunch
    ;
    
    %let lmvNPPromoSupList = ('NP PROMO SUPPORT', 'PRODUCT : NEW LAUNCH LTO', 'PRODUCT : NEW LAUNCH PERMANENT  INCL ITEM ROTATION', 
                              'PRODUCT : RE-HIT (SAME PRODUCT, NO LINE-EXTENTION)', 'PRODUCT : LINE-EXTENSION');
    %let lmvDiscountList = ('DISCOUNT', 'TEMP PRICE REDUCTION (DISCOUNT)');
    %let lmvNonProdGiftList = ('NON-PRODUCT GIFT', 'GIFT FOR PURCHASE: NON-PRODUCT');
    %let lmvBogoList = ('BOGO / 1+1', 'N+1', '1+1%', 'BUNDLE');
    %let lmvEVMSetList = ('EVM/SET', 'EVM / SET');
    %let lmvPairsList = ('PAIRS', 'PAIRS (DIFFERENT CATEGORIES)');
    %let lmvDiscForVolumeList = ('OTHER: DISCOUNT FOR VOLUME', 'DISCOUNT FOR VOLUME');
    %let lmvProductGiftList = ('PRODUCT GIFT', 'GIFT FOR PURCHASE (FOR PRODUCT)', 'GIFT FOR PURCHASE (SAMPLING)', 'GIFT FOR PURCHASE (FOR ORDRES ABOVE X RUB)');
    %let lmvProductGiftList = ('PRODUCT GIFT', 'GIFT FOR PURCHASE (FOR PRODUCT)', 'GIFT FOR PURCHASE (SAMPLING)', 'GIFT FOR PURCHASE (FOR ORDRES ABOVE X RUB)');
    %let lmvNewLaunch = ('NEW LAUNCH');

    %member_names(mpTable=&mpOutTable, mpLibrefNameKey=lmvOutTableCLib, mpMemberNameKey=lmvOutTableName);

    %if %sysfunc(sessfound(casauto))=0 %then %do;
        cas casauto;
        caslib _all_ assign;
    %end;
    
	proc casutil;
        droptable casdata="PROMO_FILT_SKU_PBO" incaslib="CASUSER" quiet;
        droptable casdata="NPPROMOSUP_OUT" incaslib="CASUSER" quiet;
        droptable casdata="DISCOUNT_OUT" incaslib="CASUSER" quiet;
        droptable casdata="BOGO_1" incaslib="CASUSER" quiet;
        droptable casdata="BOGO_2" incaslib="CASUSER" quiet;
        droptable casdata="BOGO_OUT" incaslib="CASUSER" quiet;
        droptable casdata="EVM_1" incaslib="CASUSER" quiet;
        droptable casdata="EVM_2" incaslib="CASUSER" quiet;
        droptable casdata="EVM_3" incaslib="CASUSER" quiet;
        droptable casdata="EVM_4" incaslib="CASUSER" quiet;
        droptable casdata="EVM_5" incaslib="CASUSER" quiet;
        droptable casdata="EVM_OUT" incaslib="CASUSER" quiet;
        droptable casdata="PRODGIFT_OUT" incaslib="CASUSER" quiet;
        droptable casdata="DISCFORVOLUME_OUT" incaslib="CASUSER" quiet;
        droptable casdata="UNION_MECHANICS" incaslib="CASUSER" quiet;
        droptable casdata="UNION_MECHANICS_1" incaslib="CASUSER" quiet;
        droptable casdata="NEWLAUNCH_OUT" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_REG_MAPPING" incaslib="CASUSER" quiet;
        droptable casdata="UNION_MECHANICS_2" incaslib="CASUSER" quiet;
        droptable casdata="UNION_MECHANICS_3" incaslib="CASUSER" quiet;
        droptable casdata="PRICE_INCREASE_MODIFIED_1" incaslib="CASUSER" quiet;
        droptable casdata="PRICE_INCREASE_MODIFIED_2" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_PROD_TABLE_1" incaslib="CASUSER" quiet;
        droptable casdata="NPPROMOSUP_OUT_1" incaslib="CASUSER" quiet;
    run;
	/* %if %length(&mpPromoClRk.) > 0 %then %do;
		%add_promotool_marks2(
			mpOutCaslib=casuser,
			mpPtCaslib=pt,
			PromoCalculationRk=&mpPromoClRk.
		);
		%let mpPromoTable         	= CASUSER.PROMO_ENH;
		%let mpPromoPboTable 	 	= CASUSER.PROMO_PBO_ENH_UNFOLD;
		%let mpPromoProdTable   	= CASUSER.PROMO_PROD_ENH;
	%end; */
		
	
    proc casutil;  
        droptable casdata="&lmvOutTableName" incaslib="&lmvOutTableCLib" quiet;
    run;

    /* Создание маппинга регулярного и промо товаров */
    data CASUSER.PROMO_REG_MAPPING (keep=PRODUCT_ID TO_PRODUCT_ID);
        set &mpProductAttrTable (where=(PRODUCT_ATTR_NM = 'REGULAR_ID' and PRODUCT_ATTR_VALUE <> ' '));
        TO_PRODUCT_ID = input(PRODUCT_ATTR_VALUE, 4.);
    run;

    data CASUSER.PRICE_INCREASE_MODIFIED_1(drop=L_START_DT);
        set &mpPriceIncreaseTable;
        by PRICE_AREA_NM descending START_DT;
        format L_START_DT END_DT date9.;
        retain L_START_DT;

        L_START_DT = lag(START_DT);

        if first.PRICE_AREA_NM then do;
            END_DT = &ETL_SCD_FUTURE_DT.;
            L_START_DT = .;
            output;
        end;
        else if last.PRICE_AREA_NM then do;
            END_DT = L_START_DT - 1;
            output;
            END_DT = START_DT - 1;
            START_DT = &ETL_SCD_PAST_DT.;
            output;
        end;
        else do;
            END_DT = L_START_DT - 1;
            output;
        end;
    run;

    data CASUSER.PRICE_INCREASE_MODIFIED_2(drop=PERCENT_INCREASE rename=(NEW_PERCENT_INCREASE=PERCENT_INCREASE));
        set CASUSER.PRICE_INCREASE_MODIFIED_1;
        by PRICE_AREA_NM START_DT;
        retain NEW_PERCENT_INCREASE;

        if first.PRICE_AREA_NM then NEW_PERCENT_INCREASE = 1;
        NEW_PERCENT_INCREASE = coalesce(NEW_PERCENT_INCREASE, 1) * coalesce(PERCENT_INCREASE, 1);
    run;

    /* Обработка случаев, когда в &mpPromoProdTable есть дубликаты promo_id - product_id, отличающиеся в GIFT_FLAG. В этом случае договоренность брать с GIFT_FLAG=N */

    proc fedsql sessref=casauto noprint;
        create table CASUSER.PROMO_PROD_TABLE_1{options replace=true} as
            select t1.PRODUCT_ID
                , t1.PROMO_ID
                , t1.PRODUCT_QTY
                , t1.OPTION_NUMBER
                , t1.GIFT_FLAG
                , t1.PRICE
            from &mpPromoProdTable t1

            left join (
                select PRODUCT_ID
                    , PROMO_ID
                    , PRODUCT_QTY
                    , count(*) as count
                from &mpPromoProdTable t2
                group by PRODUCT_ID
                    , PROMO_ID
                    , PRODUCT_QTY
            ) t2
                on t1.PRODUCT_ID = t2.PRODUCT_ID
                    and t1.PROMO_ID = t2.PROMO_ID
                    and t1.PRODUCT_QTY = t2.PRODUCT_QTY

            where not (t2.count = 2 and upper(t1.GIFT_FLAG) = 'Y')
        ;
    quit;

    proc fedsql sessref=casauto noprint;
        create table CASUSER.PROMO_FILT_SKU_PBO{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PROMO_MECHANICS
                , t3.PRODUCT_ID
                , t2.PBO_LOCATION_ID
                , t3.OPTION_NUMBER
                , t3.PRODUCT_QTY
                , t3.GIFT_FLAG
                , max(t1.START_DT, &VF_FC_START_DT_SAS.) as START_DT
                , min(t1.END_DT, &VF_FC_AGG_END_DT_SAS.) as END_DT
                , mean(t3.PRICE * (coalesce(t6.COEFFICIENT, 100) / 100)) as GROSS_PRICE_PT
                , mean(t4.GROSS_PRICE_AMT) as GROSS_PRICE_REG_FUTURE
            from &mpPromoTable t1

            inner join &mpPromoPboTable t2
                on t1.PROMO_ID = t2.PROMO_ID

            inner join CASUSER.PROMO_PROD_TABLE_1 t3
                on t1.PROMO_ID = t3.PROMO_ID

            left join CASUSER.PROMO_REG_MAPPING t0
                on t3.PRODUCT_ID = t0.PRODUCT_ID

            left join &mpPriceRegFutTable t4  /*для случаев, когда в течении интервала цены из &mpPromoProdTable меняется регулярная цена на будещее из &mpPriceRegFutTable*/
                on t0.PRODUCT_ID = t4.PRODUCT_ID
                    and t2.PBO_LOCATION_ID = t4.PBO_LOCATION_ID
                    and ( ( (max(t1.START_DT, &VF_FC_START_DT_SAS.) between t4.START_DT and t4.END_DT) and (min(t1.END_DT, &VF_FC_AGG_END_DT_SAS.) between t4.START_DT and t4.END_DT) )
                    or ( (t4.START_DT < max(t1.START_DT, &VF_FC_START_DT_SAS.)) and (t4.END_DT between max(t1.START_DT, &VF_FC_START_DT_SAS.) and min(t1.END_DT, &VF_FC_AGG_END_DT_SAS.)) )
                    or ( (t4.START_DT between max(t1.START_DT, &VF_FC_START_DT_SAS.) and min(t1.END_DT, &VF_FC_AGG_END_DT_SAS.)) and (t4.END_DT between max(t1.START_DT, &VF_FC_START_DT_SAS.) and min(t1.END_DT, &VF_FC_AGG_END_DT_SAS.)) )
                    or ( (t4.END_DT > min(t1.END_DT, &VF_FC_AGG_END_DT_SAS.)) and (t4.START_DT between max(t1.START_DT, &VF_FC_START_DT_SAS.) and min(t1.END_DT, &VF_FC_AGG_END_DT_SAS.)) ) )

            left join ( select PBO_LOCATION_ID /*таблицы из промо-тула предобрабатываются на соответствующие повышающие коэффициенты */
                            , PBO_LOC_ATTR_VALUE
                        from &mpPboLocAttributes
                        where PBO_LOC_ATTR_NM = 'PRICE_AREA_NAME' ) t5
                on t2.PBO_LOCATION_ID = t5.PBO_LOCATION_ID

            left join &mpLBPTable t6 /*таблицы из промо-тула предобрабатываются на соответствующие повышающие коэффициенты */
                on t5.PBO_LOC_ATTR_VALUE = t6.PRICE_AREA_NM

            /*фильтрация планируемых или уже идущих промо*/
            where ((&VF_FC_START_DT. between t1.START_DT and t1.END_DT)
                or (t1.START_DT between &VF_FC_START_DT. and t1.END_DT))
                and t1.CHANNEL_CD = 'ALL'

            group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        ;
    quit;

    /* NP Promo Support start */
    proc fedsql sessref=casauto noprint;
        create table CASUSER.NPPROMOSUP_OUT_1{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PRODUCT_ID
                , t1.PBO_LOCATION_ID
                , t1.START_DT
                , t1.END_DT
                , round(coalesce(t1.GROSS_PRICE_PT, t1.GROSS_PRICE_REG_FUTURE), 0.01) as GROSS_PRICE_AMT
            from CASUSER.PROMO_FILT_SKU_PBO t1
            where upper(t1.PROMO_MECHANICS) in &lmvNPPromoSupList.
        ;
    quit;
    
    /* Применение таблицы с плановым повышением цен START */

    proc fedsql sessref=casauto noprint;
        create table CASUSER.UNION_MECHANICS_3{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PRODUCT_ID
                , t1.PBO_LOCATION_ID
                , t1.START_DT as START_DT_PRICE
                , t1.END_DT as END_DT_PRICE
                , max(t3.START_DT, &VF_FC_START_DT_SAS.) as START_DT_INC
                , min(t3.END_DT, &VF_FC_AGG_END_DT_SAS.) as END_DT_INC
                , coalesce(t2.PBO_LOC_ATTR_VALUE, 'Price Regular') as PBO_LOC_ATTR_VALUE
                , t3.PERCENT_INCREASE
                , case 
                        when (t1.START_DT between t3.START_DT and t3.END_DT) and (t1.END_DT between t3.START_DT and t3.END_DT) then 1
                        when (t3.START_DT < t1.START_DT) and (t3.END_DT between t1.START_DT and t1.END_DT) then 2
                        when (t3.START_DT between t1.START_DT and t1.END_DT) and (t3.END_DT between t1.START_DT and t1.END_DT) then 3
                        when (t3.END_DT > t1.END_DT) and (t3.START_DT between t1.START_DT and t1.END_DT) then 4
                    end as INTERSECT_TYPE
                , t1.GROSS_PRICE_AMT AS GROSS_PRICE_AMT_TMP
            from CASUSER.NPPROMOSUP_OUT_1 t1

            left join ( select PBO_LOCATION_ID
                            , PBO_LOC_ATTR_VALUE
                        from &mpPboLocAttributes
                        where PBO_LOC_ATTR_NM = 'PRICE_AREA_NAME' ) t2
                on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID

            left join CASUSER.PRICE_INCREASE_MODIFIED_2 t3
                on coalesce(t2.PBO_LOC_ATTR_VALUE, 'Price Regular') = t3.PRICE_AREA_NM
                    and ( ( (t1.START_DT between t3.START_DT and t3.END_DT) and (t1.END_DT between t3.START_DT and t3.END_DT) )
                    or ( (t3.START_DT < t1.START_DT) and (t3.END_DT between t1.START_DT and t1.END_DT) )
                    or ( (t3.START_DT between t1.START_DT and t1.END_DT) and (t3.END_DT between t1.START_DT and t1.END_DT) )
                    or ( (t3.END_DT > t1.END_DT) and (t3.START_DT between t1.START_DT and t1.END_DT) ) )
        ;
    quit;

    data CASUSER.NPPROMOSUP_OUT(keep=CHANNEL_CD PROMO_ID PRODUCT_ID PBO_LOCATION_ID START_DT END_DT GROSS_PRICE_AMT);
        format START_DT END_DT date9.;
        set CASUSER.UNION_MECHANICS_3;
        
        if INTERSECT_TYPE = 1 then do;
            START_DT = START_DT_PRICE;
            END_DT = END_DT_PRICE;
        end;
        if INTERSECT_TYPE = 2 then do;
            START_DT = START_DT_PRICE;
            END_DT = END_DT_INC;
        end;
        if INTERSECT_TYPE = 3 then do;
            START_DT = START_DT_INC;
            END_DT = END_DT_INC;
        end;
        if INTERSECT_TYPE = 4 then do;
            START_DT = START_DT_INC;
            END_DT = END_DT_PRICE;
        end;

        if missing(PERCENT_INCREASE) = 0 and missing(GROSS_PRICE_AMT_TMP) = 0 then do;
            GROSS_PRICE_AMT = round(GROSS_PRICE_AMT_TMP * PERCENT_INCREASE, 0.01);
        end;
        else do;
            GROSS_PRICE_AMT = .;
        end;
    run;

    /* Применение таблицы с плановым повышением цен END */
    /* NP Promo Support end */

    /* Discount, Non-Product Gift start */
    proc fedsql sessref=casauto noprint;
        create table CASUSER.DISCOUNT_OUT{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PRODUCT_ID
                , t1.PBO_LOCATION_ID
                , t1.START_DT
                , t1.END_DT
                , round(coalesce(t1.GROSS_PRICE_PT, t1.GROSS_PRICE_REG_FUTURE, 0), 0.01) as GROSS_PRICE_AMT
            from CASUSER.PROMO_FILT_SKU_PBO t1
            where upper(t1.PROMO_MECHANICS) in &lmvDiscountList
                or upper(t1.PROMO_MECHANICS) in &lmvNonProdGiftList
        ;
    quit;
    /* Discount, Non-Product Gift end */

    /* BOGO/1+1 N+1 1+1% start*/
    proc fedsql sessref=casauto noprint;
        create table CASUSER.BOGO_1{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PRODUCT_ID
                , t1.PBO_LOCATION_ID
                , t1.PRODUCT_QTY
                , t1.START_DT
                , t1.END_DT
                , coalesce(t1.GROSS_PRICE_PT, 0) as GROSS_PRICE_TMP
                , coalesce(t1.GROSS_PRICE_PT, 0) * PRODUCT_QTY as QTY_MULT_PRICE
            from CASUSER.PROMO_FILT_SKU_PBO t1
            where upper(t1.PROMO_MECHANICS) in &lmvBogoList
        ;
    quit;

    proc fedsql sessref=casauto noprint;
        create table CASUSER.BOGO_2{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PBO_LOCATION_ID
                , sum(t1.PRODUCT_QTY) as SUM_QTY
                , sum(t1.QTY_MULT_PRICE) as SUM_QTY_MULT_PRICE
            from CASUSER.BOGO_1 t1
            group by t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PBO_LOCATION_ID
        ;
    quit;

    proc fedsql sessref=casauto noprint;
        create table CASUSER.BOGO_OUT{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PRODUCT_ID
                , t1.PBO_LOCATION_ID
                , t1.START_DT
                , t1.END_DT
                , round(divide(t2.SUM_QTY_MULT_PRICE, t2.SUM_QTY), 0.01) as GROSS_PRICE_AMT
            from CASUSER.BOGO_1 t1

            left join CASUSER.BOGO_2 t2
                on t1.CHANNEL_CD = t2.CHANNEL_CD
                    and t1.PROMO_ID = t2.PROMO_ID
                    and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
        ;
    quit;
    /*BOGO/1+1 N+1 1+1% end*/

    /*EVM/Set, Pairs start*/
    proc fedsql sessref=casauto noprint;
        create table CASUSER.EVM_1{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PRODUCT_ID
                , t1.OPTION_NUMBER
                , t1.PBO_LOCATION_ID
                , t1.START_DT
                , t1.END_DT
                , t1.GROSS_PRICE_PT
                , t1.GROSS_PRICE_REG_FUTURE
            from CASUSER.PROMO_FILT_SKU_PBO t1
            where upper(t1.PROMO_MECHANICS) in &lmvEVMSetList
                or upper(t1.PROMO_MECHANICS) in &lmvPairsList
        ;
    quit;
 
    /* Расчет стоимости комбо-набора*/
    proc fedsql sessref=casauto noprint;
        create table CASUSER.EVM_2{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PBO_LOCATION_ID
                , max(t1.GROSS_PRICE_PT) as GROSS_PRICE_COMBO
            from CASUSER.EVM_1 t1
            where t1.OPTION_NUMBER = 1
            group by t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PBO_LOCATION_ID
        ;
    quit;

    /* Расчет средней рег стоимости позиции */
    proc fedsql sessref=casauto noprint;
        create table CASUSER.EVM_3{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.OPTION_NUMBER
                , t1.PBO_LOCATION_ID
                , mean(t1.GROSS_PRICE_REG_FUTURE) as MEAN_GROSS_PRICE_REG
            from CASUSER.EVM_1 t1
            where missing(t1.GROSS_PRICE_REG_FUTURE) = 0
            group by t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.OPTION_NUMBER
                , t1.PBO_LOCATION_ID
        ;
    quit;
    
    /* Расчет рег стоимости рег-набора */
    proc fedsql sessref=casauto noprint;
        create table CASUSER.EVM_4{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PBO_LOCATION_ID
                , sum(t1.GROSS_PRICE_REG_FUTURE) as REG_GROSS_PRICE_COMBO
            from CASUSER.EVM_1 t1
            group by t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PBO_LOCATION_ID
        ;
    quit;
    
    proc fedsql sessref=casauto noprint;
        create table CASUSER.EVM_5{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.OPTION_NUMBER
                , t1.PBO_LOCATION_ID
                , t1.MEAN_GROSS_PRICE_REG
                , t2.GROSS_PRICE_COMBO
                , t3.REG_GROSS_PRICE_COMBO
                , divide(t2.GROSS_PRICE_COMBO, t3.REG_GROSS_PRICE_COMBO) as DISCOUNT
                , divide(t2.GROSS_PRICE_COMBO, t3.REG_GROSS_PRICE_COMBO) * t1.MEAN_GROSS_PRICE_REG as GROSS_PRICE_POS_AMT
            from CASUSER.EVM_3 t1

            left join CASUSER.EVM_2 t2
                on t1.CHANNEL_CD = t2.CHANNEL_CD
                    and t1.PROMO_ID = t2.PROMO_ID
                    and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID

            left join CASUSER.EVM_4 t3
                on t1.CHANNEL_CD = t3.CHANNEL_CD
                    and t1.PROMO_ID = t3.PROMO_ID
                    and t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID
        ;
    quit;
    
    proc fedsql sessref=casauto noprint;
        create table CASUSER.EVM_OUT{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PRODUCT_ID
                , t1.PBO_LOCATION_ID
                , t1.START_DT
                , t1.END_DT
                , round(t2.GROSS_PRICE_POS_AMT, 0.01) as GROSS_PRICE_AMT
            from CASUSER.EVM_1 t1

            left join CASUSER.EVM_5 t2
                on t1.CHANNEL_CD = t2.CHANNEL_CD
                    and t1.PROMO_ID = t2.PROMO_ID
                    and t1.OPTION_NUMBER = t2.OPTION_NUMBER
                    and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
        ;
    quit;
    /*EVM/Set, Pairs end*/

    /*Other: Discount for volume start*/
    proc fedsql sessref=casauto noprint;
        create table CASUSER.DISCFORVOLUME_OUT{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PRODUCT_ID
                , t1.PBO_LOCATION_ID
                , t1.START_DT
                , t1.END_DT
                , round(t1.GROSS_PRICE_REG_FUTURE, 0.01) as GROSS_PRICE_AMT
            from CASUSER.PROMO_FILT_SKU_PBO t1
            where upper(t1.PROMO_MECHANICS) in &lmvDiscForVolumeList
        ;
    quit;	
    /*Other: Discount for volume end*/

    /* Product Gift start */
    proc fedsql sessref=casauto noprint;
        create table CASUSER.PRODGIFT_OUT{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PRODUCT_ID
                , t1.PBO_LOCATION_ID
                , t1.START_DT
                , t1.END_DT
                , case
                        when t1.GIFT_FLAG = 'Y' then 0
                        else round(coalesce(t1.GROSS_PRICE_PT, t1.GROSS_PRICE_REG_FUTURE, 0), 0.01)
                    end as GROSS_PRICE_AMT
            from CASUSER.PROMO_FILT_SKU_PBO t1
            where upper(t1.PROMO_MECHANICS) in &lmvProductGiftList
        ;
    quit;
    /* Product Gift end */

    /* New launch start */
    proc fedsql sessref=casauto noprint;
        create table CASUSER.NEWLAUNCH_OUT{options replace=true} as
            select t1.CHANNEL_CD
                , t1.PROMO_ID
                , t1.PRODUCT_ID
                , t1.PBO_LOCATION_ID
                , t1.START_DT
                , t1.END_DT
                , round(coalesce(t1.GROSS_PRICE_REG_FUTURE, t1.GROSS_PRICE_PT), 0.01) as GROSS_PRICE_AMT
            from CASUSER.PROMO_FILT_SKU_PBO t1
            where upper(t1.PROMO_MECHANICS) in &lmvNewLaunch
        ;
    quit;

    /* New launch end */

    /*Объединение всех результатов*/

    data CASUSER.UNION_MECHANICS;
        format START_DT END_DT date9.;
        set CASUSER.NPPROMOSUP_OUT
            CASUSER.DISCOUNT_OUT
            CASUSER.BOGO_OUT
            CASUSER.EVM_OUT
            CASUSER.DISCFORVOLUME_OUT
            CASUSER.PRODGIFT_OUT
            CASUSER.NEWLAUNCH_OUT
        ;
    run;

    /*
    Обработка пересечения цен на будущее с изменением НДС на будущее.
    Возможны 4 случая пересечения периодов:
        1) Интервал цены польностью покрывается интервалом НДС
        2) Конец интервала НДС находится внутри интервала цены
        3) И начало и конец интервала НДС находится внутри интервала цены
        4) Начало интервала НДС находится внутри интервала цены
    */
    proc fedsql sessref=casauto noprint;
        create table CASUSER.UNION_MECHANICS_1{options replace=true} as
            select t2.CHANNEL_CD
                , t2.PROMO_ID
                , t2.PRODUCT_ID
                , t2.PBO_LOCATION_ID
                , t2.START_DT as START_DT_PRICE
                , t2.END_DT as END_DT_PRICE
                , t2.GROSS_PRICE_AMT
                , t3.START_DT as START_DT_VAT
                , t3.END_DT as END_DT_VAT
                , t3.VAT
                , case 
                        when (t2.START_DT between t3.START_DT and t3.END_DT) and (t2.END_DT between t3.START_DT and t3.END_DT) then 1
                        when (t3.START_DT < t2.START_DT) and (t3.END_DT between t2.START_DT and t2.END_DT) then 2
                        when (t3.START_DT between t2.START_DT and t2.END_DT) and (t3.END_DT between t2.START_DT and t2.END_DT) then 3
                        when (t3.END_DT > t2.END_DT) and (t3.START_DT between t2.START_DT and t2.END_DT) then 4
                    end as INTERSECT_TYPE
            from CASUSER.UNION_MECHANICS t2/*t1*/

/*            left join CASUSER.PROMO_FILT_SKU_PBO t2
                on  t1.PROMO_ID = t2.PROMO_ID
                    and t1.PRODUCT_ID = t2.PRODUCT_ID
                    and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID commented by Danil M 02JUL2021*/

            left join &mpVatTable t3
                on  t2.PRODUCT_ID = t3.PRODUCT_ID
                    and t2.PBO_LOCATION_ID = t3.PBO_LOCATION_ID
                    and ( ( (t2.START_DT between t3.START_DT and t3.END_DT) and (t2.END_DT between t3.START_DT and t3.END_DT) )
                    or ( (t3.START_DT < t2.START_DT) and (t3.END_DT between t2.START_DT and t2.END_DT) )
                    or ( (t3.START_DT between t2.START_DT and t2.END_DT) and (t3.END_DT between t2.START_DT and t2.END_DT) )
                    or ( (t3.END_DT > t2.END_DT) and (t3.START_DT between t2.START_DT and t2.END_DT) ) )
        ;
    quit;

    data CASUSER.&lmvOutTableName(keep=CHANNEL_CD PROMO_ID PRODUCT_ID PBO_LOCATION_ID START_DT END_DT NET_PRICE_AMT GROSS_PRICE_AMT);
        format START_DT date9. END_DT date9.;
        set CASUSER.UNION_MECHANICS_1;
        
        if INTERSECT_TYPE = 1 then do;
            START_DT = START_DT_PRICE;
            END_DT = END_DT_PRICE;
        end;
        else if INTERSECT_TYPE = 2 then do;
            START_DT = START_DT_PRICE;
            END_DT = END_DT_VAT;
        end;
        else if INTERSECT_TYPE = 3 then do;
            START_DT = START_DT_VAT;
            END_DT = END_DT_VAT;
        end;
        else if INTERSECT_TYPE = 4 then do;
            START_DT = START_DT_VAT;
            END_DT = END_DT_PRICE;
        end;
        else do;
            START_DT = START_DT_PRICE;
            END_DT = END_DT_PRICE;
        end;

        if GROSS_PRICE_AMT ne . and VAT ne . then NET_PRICE_AMT = round(divide(GROSS_PRICE_AMT, (1 + divide(VAT, 100))), 0.01);
        else NET_PRICE_AMT = .;
    run;

    proc casutil;
        promote casdata="&lmvOutTableName" incaslib="CASUSER" outcaslib="&lmvOutTableCLib";
		save incaslib="&lmvOutTableCLib." outcaslib="&lmvOutTableCLib." casdata="&lmvOutTableName." casout="&lmvOutTableName..sashdat" replace; 
    run;
/*
	proc casutil;
        droptable casdata="PROMO_FILT_SKU_PBO" incaslib="CASUSER" quiet;
        droptable casdata="NPPROMOSUP_OUT" incaslib="CASUSER" quiet;
        droptable casdata="DISCOUNT_OUT" incaslib="CASUSER" quiet;
        droptable casdata="BOGO_1" incaslib="CASUSER" quiet;
        droptable casdata="BOGO_2" incaslib="CASUSER" quiet;
        droptable casdata="BOGO_OUT" incaslib="CASUSER" quiet;
        droptable casdata="EVM_1" incaslib="CASUSER" quiet;
        droptable casdata="EVM_2" incaslib="CASUSER" quiet;
        droptable casdata="EVM_3" incaslib="CASUSER" quiet;
        droptable casdata="EVM_4" incaslib="CASUSER" quiet;
        droptable casdata="EVM_5" incaslib="CASUSER" quiet;
        droptable casdata="EVM_OUT" incaslib="CASUSER" quiet;
        droptable casdata="PRODGIFT_OUT" incaslib="CASUSER" quiet;
        droptable casdata="DISCFORVOLUME_OUT" incaslib="CASUSER" quiet;
        droptable casdata="UNION_MECHANICS" incaslib="CASUSER" quiet;
        droptable casdata="UNION_MECHANICS_1" incaslib="CASUSER" quiet;
        droptable casdata="NEWLAUNCH_OUT" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_REG_MAPPING" incaslib="CASUSER" quiet;
        droptable casdata="UNION_MECHANICS_2" incaslib="CASUSER" quiet;
        droptable casdata="UNION_MECHANICS_3" incaslib="CASUSER" quiet;
        droptable casdata="PRICE_INCREASE_MODIFIED_1" incaslib="CASUSER" quiet;
        droptable casdata="PRICE_INCREASE_MODIFIED_2" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_PROD_TABLE_1" incaslib="CASUSER" quiet;
        droptable casdata="NPPROMOSUP_OUT_1" incaslib="CASUSER" quiet;
    run;
*/
%mend price_promo_future;
