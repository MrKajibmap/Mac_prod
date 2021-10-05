/*****************************************************************
*  ВЕРСИЯ:
*   $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*   Реализация алгоритма вычисления промо-цен на прошлое из таблиц промо и фактических цен согласно п.3 постановки McD_price_calculation_v6.
*
*  ПАРАМЕТРЫ:
*   mpPriceRegPastTab    	 - Наименование входящей таблицы с регулярными ценами на прошлое
*   mpPromoTable         	 - Наименование входящего справочника с промо разметкой
*   mpPromoPboTable      	 - Наименование входящего справочника с промо-пбо разметкой
*   mpPromoProdTable     	 - Наименование входящего справочника с промо-скю разметкой
*   mpProductAttrTable       - Наименование входящего справочника с атрибутами скю
*	mpVatTable 		  		 - Наименование входящего справочника НДС
*   mpOutTable 		  		 - Наименование выходящей таблицы с рассчитанными промо-ценами на прошлое
*   mpBatchValue 		  	 - Количество ПБО, используемых в одном батче
*
******************************************************************
*  Использует: 
*	нет
*
*  Устанавливает макропеременные:
*   нет
*
******************************************************************
*  Пример использования:
    mpPriceRegPastTab    = DM_ABT.PRICE_REGULAR_PAST
    , mpPromoTable       = CASUSER.PROMO
    , mpPromoPboTable    = CASUSER.PROMO_PBO_UNFOLD
    , mpPromoProdTable   = CASUSER.PROMO_PROD
    , mpProductAttrTable = CASUSER.PRODUCT_ATTRIBUTES
    , mpVatTable 		 = CASUSER.VAT
    , mpOutTable 		 = CASUSER.PRICE_PROMO_PAST
    , mpBatchValue 		 = 15
    );
*
****************************************************************************
*  30-05-2021 		Мугтасимов Данил 		Начальное кодирование
****************************************************************************/
%macro price_promo_past(
    mpPriceRegPastTab    = DM_ABT.PRICE_REGULAR_PAST
    , mpPromoTable       = CASUSER.PROMO
    , mpPromoPboTable    = CASUSER.PROMO_PBO_UNFOLD
    , mpPromoProdTable   = CASUSER.PROMO_PROD
    , mpProductAttrTable = CASUSER.PRODUCT_ATTRIBUTES
    , mpVatTable 		 = CASUSER.VAT
    , mpOutTable 		 = DM_ABT.PRICE_PROMO_PAST
    , mpBatchValue 		 = 15
    );
    
    %local lmvIterCounter
            lmvPromoList1210
            lmvPromoList345
            lmvPromoList68
            lmvPromoList7
            lmvPboUsedNum
            lmvPboTotalNum
            lmvOutTableName
            lmvOutTableCLib
            lmvBatchValue
            ;
    %let lmvBatchValue = &mpBatchValue.;
    %member_names(mpTable=&mpOutTable, mpLibrefNameKey=lmvOutTableCLib, mpMemberNameKey=lmvOutTableName);
    
    %if %sysfunc(sessfound(casauto))=0 %then %do;
        cas casauto;
        caslib _all_ assign;
    %end;
    
    proc casutil;  
        droptable casdata="&lmvOutTableName" incaslib="&lmvOutTableCLib" quiet;
    run;

    /* Создание маппинга регулярного и промо товаров */
    data CASUSER.PROMO_REG_MAPPING (keep=PRODUCT_ID TO_PRODUCT_ID);
        set &mpProductAttrTable (where=(PRODUCT_ATTR_NM = 'REGULAR_ID' and PRODUCT_ATTR_VALUE <> ' '));
        TO_PRODUCT_ID = input(PRODUCT_ATTR_VALUE, 4.);
    run;
    
    /* Джойн с двумя справочниками. Создание промо-разметки SKU - ПБО - период- Флаг_промо */
    proc fedsql sessref=casauto noprint;
        create table CASUSER.PROMO_FILT_SKU_PBO{options replace=true} as
            select t1.PROMO_ID
                , t1.PROMO_MECHANICS
                , t3.PRODUCT_ID
                , t3.GIFT_FLAG
                , t3.OPTION_NUMBER
                , t2.PBO_LOCATION_ID
                , t1.START_DT
                , t1.END_DT
            from &mpPromoTable t1

            inner join &mpPromoPboTable t2
                on t1.PROMO_ID = t2.PROMO_ID

            inner join &mpPromoProdTable t3
                on t1.PROMO_ID = t3.PROMO_ID

            where t1.CHANNEL_CD = 'ALL'
        ;
    quit;
    
    /* Создание пустой таблицы айдишников ПБО, в которой будут храниться уже посчитанные */
    data CASUSER.PBO_USED;
        set CASUSER.PROMO_FILT_SKU_PBO(keep=PBO_LOCATION_ID);
        where PBO_LOCATION_ID < -1000;
        USED_FLAG = 1;
    run;
    
    proc fedsql sessref=casauto noprint;
        create table CASUSER.PBO_LIST_TMP{options replace=true} as
            select distinct t1.PBO_LOCATION_ID
            from CASUSER.PROMO_FILT_SKU_PBO t1
        ;
    quit;
    
    data _NULL_;
        if 0 then set CASUSER.PBO_USED nobs=n;
        call symputx('lmvPboUsedNum',n);
        stop;
    run;
    data _NULL_;
        if 0 then set CASUSER.PBO_LIST_TMP nobs=n;
        call symputx('lmvPboTotalNum',n);
        stop;
    run;
    %let lmvIterCounter = 1;
    
    %do %while (&lmvPboUsedNum. < &lmvPboTotalNum.);
    
        /* Создание батча PBO start */
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PBO_LIST{options replace=true} as
                select distinct t1.PBO_LOCATION_ID
                from CASUSER.PROMO_FILT_SKU_PBO t1

                left join CASUSER.PBO_USED t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID

                where t2.USED_FLAG = . /*исключение уже посчитанных*/
            ;
        quit;
        
        data CASUSER.PBO_LIST_BATCH;
            set CASUSER.PBO_LIST(obs=&lmvBatchValue.);
            USED_FLAG = 1;
        run;
        
        /* Добавление в список считанных ПБО */
        data CASUSER.PBO_USED(append=yes);
            set CASUSER.PBO_LIST_BATCH;
        run;
    
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_FILT_SKU_PBO_BATCH{options replace=true} as
                select t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , t1.START_DT
                    , t1.END_DT
                    , t1.PROMO_MECHANICS
                    , t1.OPTION_NUMBER
                    , t1.GIFT_FLAG
                from CASUSER.PROMO_FILT_SKU_PBO t1
                inner join CASUSER.PBO_LIST_BATCH t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
            ;
            create table CASUSER.PRICE_BATCH{options replace=true} as
                select t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.START_DT
                    , t1.END_DT
                    , t1.GROSS_PRICE_AMT
                from CASUSER.PRICE t1
                inner join CASUSER.PBO_LIST_BATCH t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
            ;
        quit;

        /* Создание батча PBO end */

        /* Переход от start_dt end_dt интеревалов к подневному списку в ПРОМО разметке*/
        data CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS (rename=(START_DT=DAY_DT) drop=END_DT END_DT_TMP);
            set CASUSER.PROMO_FILT_SKU_PBO_BATCH(where=(START_DT < min(END_DT, &VF_HIST_END_DT_SAS.)));
            format START_DT date9.;
            END_DT_TMP = min(END_DT, &VF_HIST_END_DT_SAS.);
            output;
            do while (START_DT < END_DT_TMP);
                START_DT = intnx('days', START_DT, 1);
                output;
            end;
        run;
        
        /* Переход от start_dt end_dt интеревалов к подневному списку в ФАКТИЧЕСКИХ ценах */
        data CASUSER.PRICE_BATCH_DAYS (rename=(START_DT=DAY_DT) drop=END_DT END_DT_TMP);
            set CASUSER.PRICE_BATCH(where=(START_DT < min(END_DT, &VF_HIST_END_DT_SAS.)));
            format START_DT date9.;
            END_DT_TMP = min(END_DT, &VF_HIST_END_DT_SAS.);
            output;
            do while (START_DT < END_DT_TMP);
                START_DT = intnx('days', START_DT, 1);
                output;
            end;
        run;

/* 		=-=-=-=-=-=-=-=-=-=MECHANICS №1,2,10=-=-=-=-=-=-=-=-=-= */		

        %let lmvPromoList1210 = ('NP PROMO SUPPORT', 'PRODUCT : NEW LAUNCH LTO', 'PRODUCT : NEW LAUNCH PERMANENT  INCL ITEM ROTATION', 'PRODUCT : RE-HIT (SAME PRODUCT, NO LINE-EXTENTION)',
                                'PRODUCT : LINE-EXTENSION', 'DISCOUNT', 'TEMP PRICE REDUCTION (DISCOUNT)', 'PRODUCT GIFT', 'GIFT FOR PURCHASE (FOR PRODUCT)',
                                'GIFT FOR PURCHASE (SAMPLING)', 'GIFT FOR PURCHASE (FOR ORDRES ABOVE X RUB)');

        /* Вычисление средней фактической цены в период промо, когда факт не миссинг*/
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PRICE_BATCH_DAYS_MECH1210_1{options replace=true} as
                select t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , mean(t2.GROSS_PRICE_AMT) as MEAN_GROSS
                from CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t1
                
                left join CASUSER.PRICE_BATCH_DAYS t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                        and t1.PRODUCT_ID = t2.PRODUCT_ID
                        and t1.DAY_DT = t2.DAY_DT

                where upper(t1.PROMO_MECHANICS) in &lmvPromoList1210.
                    and t2.GROSS_PRICE_AMT is not missing

                group by t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
            ;
        quit;

        /* Джойн промо-цены с фактической разметкой. Промо=факт, миссинги факта в дни промо проставляются на среднюю фактическую цену за период */
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PRICE_BATCH_DAYS_MECH1210_2{options replace=true} as
                select t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , t1.DAY_DT
                    , t2.GROSS_PRICE_AMT
                    , t3.MEAN_GROSS
                    , case
                            when t1.GIFT_FLAG = 'Y' then 0
                            else coalesce(t2.GROSS_PRICE_AMT, t3.MEAN_GROSS)
                        end as PROMO_GROSS_PRICE_AMT
                from CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t1

                left join CASUSER.PRICE_BATCH_DAYS t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                        and t1.PRODUCT_ID = t2.PRODUCT_ID
                        and t1.DAY_DT = t2.DAY_DT

                left join CASUSER.PRICE_BATCH_DAYS_MECH1210_1 t3
                    on t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID
                        and t1.PRODUCT_ID = t3.PRODUCT_ID
                        and t1.PROMO_ID = t3.PROMO_ID

                where upper(t1.PROMO_MECHANICS) in &lmvPromoList1210
            ;
        quit;
        
    
/* 		=-=-=-=-=-=-=-=-=-=MECHANICS №3, №4, №5-=-=-=-=-=-=-=-=-= */
    
        %let lmvPromoList345 = ('BOGO / 1+1', 'N+1', '1+1%', 'BUNDLE');

        /*Таблица с факт ценами для каждой даты промо*/
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH345_1{options replace=true} as
                select t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , t1.DAY_DT
                    , t2.GROSS_PRICE_AMT
                    , case	
                            when t2.GROSS_PRICE_AMT is missing then 0
                            else 1
                        end as NONMISS_FLG
                from CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t1

                left join CASUSER.PRICE_BATCH_DAYS t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                        and t1.PRODUCT_ID = t2.PRODUCT_ID
                        and t1.DAY_DT = t2.DAY_DT

                where upper(t1.PROMO_MECHANICS) in &lmvPromoList345
            ;
        quit;
        
        /*Создание словаря с количеством немиссинговых фактических цен и количеством товаров в наборе в промо */

        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH345_DICT{options replace=true} as
                select t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , t1.DAY_DT
                    , sum(t1.NONMISS_FLG) as SUM_NONMISS_FLG
                    , count(t1.PRODUCT_ID) as COUNT_SKUS
                from CASUSER.PROMO_BATCH_DAYS_MECH345_1 t1

                group by t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , t1.DAY_DT
            ;
        quit;
        
        /*Джойн справочника к основной таблице*/
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH345_2{options replace=true} as
                select t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , t1.DAY_DT
                    , t1.GROSS_PRICE_AMT
                    , t1.NONMISS_FLG
                    , t2.SUM_NONMISS_FLG
                    , t2.COUNT_SKUS
            from CASUSER.PROMO_BATCH_DAYS_MECH345_1 t1

            left join CASUSER.PROMO_BATCH_DAYS_MECH345_DICT t2
                on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                    and t1.PROMO_ID = t2.PROMO_ID
                    and t1.DAY_DT = t2.DAY_DT
            ;
        quit;

        /* Подсчет средней цены всего набора внутри одного дня*/
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH345_3{options replace=true} as
                select t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , t1.DAY_DT
                    , mean(t1.GROSS_PRICE_AMT) as DAY_MEAN_GROSS
                from CASUSER.PROMO_BATCH_DAYS_MECH345_2 t1
                group by t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , t1.DAY_DT
            ;
        quit;

        /*Подсчет средней цены внутри промо периода */
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH345_4{options replace=true} as
                select t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , mean(t1.GROSS_PRICE_AMT) as PERIOD_MEAN_GROSS
                from CASUSER.PROMO_BATCH_DAYS_MECH345_2 t1
                where t1.SUM_NONMISS_FLG = t1.COUNT_SKUS
                group by t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
            ;
        quit;

        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH345_5{options replace=true} as
                select t1.PROMO_ID
                    , t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.DAY_DT
                    , t1.GROSS_PRICE_AMT as GROSS_PRICE_AMT_OLD
                    , t2.DAY_MEAN_GROSS
                    , t3.PERIOD_MEAN_GROSS
                    , t1.NONMISS_FLG
                    , t1.SUM_NONMISS_FLG
                    , t1.COUNT_SKUS
                    , case
                            when SUM_NONMISS_FLG = COUNT_SKUS then t2.DAY_MEAN_GROSS
                            else t3.PERIOD_MEAN_GROSS
                        end as PROMO_GROSS_PRICE_AMT
                from CASUSER.PROMO_BATCH_DAYS_MECH345_2 t1

                left join CASUSER.PROMO_BATCH_DAYS_MECH345_3 t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                        and t1.PROMO_ID = t2.PROMO_ID
                        and t1.DAY_DT = t2.DAY_DT

                left join CASUSER.PROMO_BATCH_DAYS_MECH345_4 t3
                    on t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID
                        and t1.PROMO_ID = t3.PROMO_ID
            ;
        quit;


/* 		=-=-=-=-=-=-=-=-=-=MECHANICS №6 №8-=-=-=-=-=-=-=-=-= */

        %let lmvPromoList68 = ('EVM/SET', 'PAIRS', 'EVM / SET', 'PAIRS (DIFFERENT CATEGORIES)');

        /* Вычисление минимальной фактической цены в позиции */
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH68_1{options replace=true} as
                select t1.PBO_LOCATION_ID
                    , t1.OPTION_NUMBER
                    , t1.PROMO_ID
                    , t1.DAY_DT
                    , min(t2.GROSS_PRICE_AMT) as MIN_OPTNUM_GR_PRICE
                from CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t1

                left join CASUSER.PRICE_BATCH_DAYS t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                        and t1.PRODUCT_ID = t2.PRODUCT_ID
                        and t1.DAY_DT = t2.DAY_DT

                where upper(t1.PROMO_MECHANICS) in &lmvPromoList68

                group by t1.PBO_LOCATION_ID
                    , t1.OPTION_NUMBER
                    , t1.PROMO_ID
                    , t1.DAY_DT
            ;
        quit;

        /* Разметка миссинга вычисления с предыщуего шага */
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH68_2{options replace=true} as
                select t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , t1.OPTION_NUMBER
                    , t1.DAY_DT
                    , t1.MIN_OPTNUM_GR_PRICE
                    , case
                            when missing(t1.MIN_OPTNUM_GR_PRICE) = 1 then 0
                            else 1
                        end as NONMISS_FLG
                from CASUSER.PROMO_BATCH_DAYS_MECH68_1 t1
            ;
        quit;

        /* Вычисление количества позиций в промо */                
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH68_3{options replace=true} as
                select t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , t1.DAY_DT
                    , count(t1.OPTION_NUMBER) as COUNT_OPT_NUM
                    , sum(t1.NONMISS_FLG) as SUM_NONMISS_FLG
                from CASUSER.PROMO_BATCH_DAYS_MECH68_2 t1
                group by t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , t1.DAY_DT
            ;
        quit;
        
        /* Вычисление средней минимальной цены позиции на периоде промо */

        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH68_4{options replace=true} as
                select t1.PBO_LOCATION_ID
                    , t1.OPTION_NUMBER
                    , t1.PROMO_ID
                    , t1.DAY_DT
                    , t1.NONMISS_FLG
                    , t2.COUNT_OPT_NUM
                    , t2.SUM_NONMISS_FLG
                    , t1.MIN_OPTNUM_GR_PRICE
                from CASUSER.PROMO_BATCH_DAYS_MECH68_2 t1
                
                left join CASUSER.PROMO_BATCH_DAYS_MECH68_3 t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                        and t1.PROMO_ID = t2.PROMO_ID
                        and t1.DAY_DT = t2.DAY_DT
            ;
        quit;

        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH68_5{options replace=true} as
                select t1.PBO_LOCATION_ID
                    , t1.OPTION_NUMBER
                    , t1.PROMO_ID
                    , mean(t1.MIN_OPTNUM_GR_PRICE) as MN_MIN_OPTNUM_GR_PRICE
                from CASUSER.PROMO_BATCH_DAYS_MECH68_4 t1
                
                where t1.COUNT_OPT_NUM = t1.SUM_NONMISS_FLG
                
                group by t1.PBO_LOCATION_ID
                    , t1.OPTION_NUMBER
                    , t1.PROMO_ID
            ;
        quit;

        /* Вычисление стоимости набора */
        
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH68_6{options replace=true} as
                select t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , sum(t1.MN_MIN_OPTNUM_GR_PRICE) as COMBO_GR_COST
                from CASUSER.PROMO_BATCH_DAYS_MECH68_5 t1
                group by t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
            ;
        quit;

        /* Подтягивание регулярных цен */
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH68_7{options replace=true} as
                select t1.PRODUCT_ID
                    , t2.TO_PRODUCT_ID as REG_PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.OPTION_NUMBER
                    , t1.PROMO_ID
                    , t1.DAY_DT
                    , t3.GROSS_PRICE_AMT as REG_GR_PRICE
                from CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t1

                left join CASUSER.PROMO_REG_MAPPING t2
                    on t1.PRODUCT_ID = t2.PRODUCT_ID

                left join &mpPriceRegPastTab t3
                    on t2.TO_PRODUCT_ID = t3.PRODUCT_ID
                        and t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID
                        and t1.DAY_DT between t3.START_DT and t3.END_DT

                where upper(t1.PROMO_MECHANICS) in &lmvPromoList68
            ;
        quit;

        /* Вычисление средней регулярной цены на позицию в дне */
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH68_8{options replace=true} as
                select t1.PBO_LOCATION_ID
                    , t1.OPTION_NUMBER
                    , t1.PROMO_ID
                    , t1.DAY_DT
                    , mean(t1.REG_GR_PRICE) as MN_DAY_REG_GR_PRICE
                from CASUSER.PROMO_BATCH_DAYS_MECH68_7 t1
                group by t1.PBO_LOCATION_ID
                    , t1.OPTION_NUMBER
                    , t1.PROMO_ID
                    , t1.DAY_DT
            ;
        quit;
        
        /* Вычисление средней регулярной цены на позицию на периоде промо */
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH68_9{options replace=true} as
                select t1.PBO_LOCATION_ID
                    , t1.OPTION_NUMBER
                    , t1.PROMO_ID
                    , mean(t1.MN_DAY_REG_GR_PRICE) as MN_PRD_REG_GR_PRICE
                from CASUSER.PROMO_BATCH_DAYS_MECH68_8 t1
                group by t1.PBO_LOCATION_ID
                    , t1.OPTION_NUMBER
                    , t1.PROMO_ID
            ;
        quit;

        /* Вычисление суммы комбо набора на основе MN_PRD_REG_GR_PRICE */
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH68_10{options replace=true} as
                select t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , sum(t1.MN_PRD_REG_GR_PRICE) as COMBO_REG_GR_COST
                from CASUSER.PROMO_BATCH_DAYS_MECH68_9 t1
                group by t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
            ;
        quit;

        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH68_11{options replace=true} as
                select t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , t1.OPTION_NUMBER
                    , t1.MN_PRD_REG_GR_PRICE
                    , t2.COMBO_REG_GR_COST
                    , t3.COMBO_GR_COST
                    , divide(t3.COMBO_GR_COST, t2.COMBO_REG_GR_COST) * t1.MN_PRD_REG_GR_PRICE as PROMO_GROSS_PRICE_AMT
                from CASUSER.PROMO_BATCH_DAYS_MECH68_9 t1
                
                left join CASUSER.PROMO_BATCH_DAYS_MECH68_10 t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                        and t1.PROMO_ID = t2.PROMO_ID

                left join CASUSER.PROMO_BATCH_DAYS_MECH68_6 t3
                    on t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID
                        and t1.PROMO_ID = t3.PROMO_ID
            ;
        quit;

        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_BATCH_DAYS_MECH68_12{options replace=true} as
                select t1.PRODUCT_ID 
                    , t1.PBO_LOCATION_ID
                    , t1.OPTION_NUMBER
                    , t1.PROMO_ID
                    , t1.DAY_DT
                    , t2.PROMO_GROSS_PRICE_AMT
                from CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t1
                
                left join CASUSER.PROMO_BATCH_DAYS_MECH68_11 t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                        and t1.PROMO_ID = t2.PROMO_ID
                        and t1.OPTION_NUMBER = t2.OPTION_NUMBER
                
                where upper(t1.PROMO_MECHANICS) in &lmvPromoList68
            ;
        quit;	

/* 		=-=-=-=-=-=-=-=-=-=MECHANICS №7-=-=-=-=-=-=-=-=-= */

        %let lmvPromoList7 = ('NON-PRODUCT GIFT', 'GIFT FOR PURCHASE: NON-PRODUCT');

        /* Вычисление средней фактической цены в период промо, когда факт не миссинг*/
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PRICE_BATCH_DAYS_MECH7_1{options replace=true} as
                select t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , mean(t2.GROSS_PRICE_AMT) as MEAN_GROSS
                from CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t1
                
                left join CASUSER.PRICE_BATCH_DAYS t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                        and t1.PRODUCT_ID = t2.PRODUCT_ID
                        and t1.DAY_DT = t2.DAY_DT

                where upper(t1.PROMO_MECHANICS) in &lmvPromoList7
                    and t1.GIFT_FLAG = 'N'
                    and t2.GROSS_PRICE_AMT is not missing

                group by t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
            ;
        quit;

        /* Джойн промо-цены с фактической разметкой. Промо=факт, миссинги факта в дни промо проставляются на среднюю фактическую цену за период */
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PRICE_BATCH_DAYS_MECH7_2{options replace=true} as
                select t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , t1.DAY_DT
                    , t2.GROSS_PRICE_AMT
                    , t3.MEAN_GROSS
                    , coalesce(t2.GROSS_PRICE_AMT, t3.MEAN_GROSS) as PROMO_GROSS_PRICE_AMT
                from CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t1

                left join CASUSER.PRICE_BATCH_DAYS t2
                    on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                        and t1.PRODUCT_ID = t2.PRODUCT_ID
                        and t1.DAY_DT = t2.DAY_DT

                left join CASUSER.PRICE_BATCH_DAYS_MECH7_1 t3
                    on t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID
                        and t1.PRODUCT_ID = t3.PRODUCT_ID
                        and t1.PROMO_ID = t3.PROMO_ID

                where upper(t1.PROMO_MECHANICS) in &lmvPromoList7
                    and t1.GIFT_FLAG = 'N'
            ;
        quit;

/* 		=-=-=-=-=-=-=-=-=-=MECHANICS №7 END-=-=-=-=-=-=-=-=-= */


    /* Объединение всех посчиатанных промо механик в одну таблцицу*/
        data CASUSER.PROMO_PRICE_ALL_MECHANICS(drop=PROMO_GROSS_PRICE_AMT);
            set CASUSER.PRICE_BATCH_DAYS_MECH1210_2 (keep=PROMO_ID PRODUCT_ID PBO_LOCATION_ID DAY_DT PROMO_GROSS_PRICE_AMT)
                CASUSER.PROMO_BATCH_DAYS_MECH345_5 (keep=PROMO_ID PRODUCT_ID PBO_LOCATION_ID DAY_DT PROMO_GROSS_PRICE_AMT)
                CASUSER.PROMO_BATCH_DAYS_MECH68_12 (keep=PROMO_ID PRODUCT_ID PBO_LOCATION_ID DAY_DT PROMO_GROSS_PRICE_AMT)
                CASUSER.PRICE_BATCH_DAYS_MECH7_2 (keep=PROMO_ID PRODUCT_ID PBO_LOCATION_ID DAY_DT PROMO_GROSS_PRICE_AMT)
            ;
            where day_dt between &VF_HIST_START_DT_SAS. and &VF_HIST_END_DT_SAS;
            
            if PROMO_GROSS_PRICE_AMT = . then do;
                GROSS_PRICE_AMT = PROMO_GROSS_PRICE_AMT;
            end;
            else do;
                GROSS_PRICE_AMT = round(PROMO_GROSS_PRICE_AMT, 0.01);
            end;
        run;

        /* Переход от подневной гранулярности к периодной */
        
        data CASUSER.PROMO_INTERVALS(rename=(PRICE_GRO=GROSS_PRICE_AMT));
            set CASUSER.PROMO_PRICE_ALL_MECHANICS;
            by PROMO_ID PBO_LOCATION_ID PRODUCT_ID DAY_DT;
            keep PROMO_ID PBO_LOCATION_ID PRODUCT_ID START_DT END_DT PRICE_GRO;
            format START_DT END_DT date9.;
            retain START_DT END_DT PRICE_GRO L_GROSS_PRICE;
            
            L_GROSS_PRICE = lag(GROSS_PRICE_AMT);
            L_DAY_DT = lag(DAY_DT);
            
            /*первое наблюдение в ряду - сбрасываем хар-ки интервала*/
            if first.PRODUCT_ID then do;
                START_DT = DAY_DT;
                END_DT = .;
                PRICE_GRO = GROSS_PRICE_AMT;
                L_GROSS_PRICE = .z;
                L_DAY_DT = .;
            end;
            
            /*сбрасываем текущий интервал, готовим следующий*/
            if (GROSS_PRICE_AMT ne L_GROSS_PRICE or L_DAY_DT ne DAY_DT - 1) and not first.PRODUCT_ID then do;
                END_DT = L_DAY_DT;
                output;
                START_DT = DAY_DT;
                END_DT = .;
                PRICE_GRO = GROSS_PRICE_AMT;
            end;
            if last.PRODUCT_ID then do;
                END_DT = DAY_DT;
                output;
            end;
        run;

        /* Вычисление цен без НДС */

        /*
        Обработка пересечения цен на будущее с изменением НДС на будущее.
        Возможны 4 случая пересечения периодов:
            1) Интервал цены польностью покрывается интервалом НДС
            2) Конец интервала НДС находится внутри интервала цены
            3) И начало и конец интервала НДС находится внутри интервала цены
            4) Начало интервала НДС находится внутри интервала цены
        */
        proc fedsql sessref=casauto noprint;
            create table CASUSER.PROMO_INTERVALS_OUT_1{options replace=true} as
                select cast('ALL' as CHARACTER) as CHANNEL_CD
                    , t1.PRODUCT_ID
                    , t1.PBO_LOCATION_ID
                    , t1.PROMO_ID
                    , t1.START_DT as START_DT_PRICE
                    , t1.END_DT as END_DT_PRICE
                    , t2.START_DT as START_DT_VAT
                    , t2.END_DT as END_DT_VAT
                    , t2.VAT
                    , case 
                            when (t1.START_DT between t2.START_DT and t2.END_DT) and (t1.END_DT between t2.START_DT and t2.END_DT) then 1
                            when (t2.START_DT < t1.START_DT) and (t2.END_DT between t1.START_DT and t1.END_DT) then 2
                            when (t2.START_DT between t1.START_DT and t1.END_DT) and (t2.END_DT between t1.START_DT and t1.END_DT) then 3
                            when (t2.END_DT > t1.END_DT) and (t2.START_DT between t1.START_DT and t1.END_DT) then 4
                        end as INTERSECT_TYPE
                    , t1.GROSS_PRICE_AMT
                from CASUSER.PROMO_INTERVALS t1

                left join &mpVatTable t2
                    on  t1.PRODUCT_ID = t2.PRODUCT_ID
                        and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
                        and ( ( (t1.START_DT between t2.START_DT and t2.END_DT) and (t1.END_DT between t2.START_DT and t2.END_DT) )
                        or ( (t2.START_DT < t1.START_DT) and (t2.END_DT between t1.START_DT and t1.END_DT) )
                        or ( (t2.START_DT between t1.START_DT and t1.END_DT) and (t2.END_DT between t1.START_DT and t1.END_DT) )
                        or ( (t2.END_DT > t1.END_DT) and (t2.START_DT between t1.START_DT and t1.END_DT) ) )
            ;
        quit;

        data CASUSER.PROMO_INTERVALS_OUT(keep=CHANNEL_CD PRODUCT_ID PBO_LOCATION_ID PROMO_ID START_DT END_DT NET_PRICE_AMT GROSS_PRICE_AMT);
            format START_DT date9. END_DT date9.;
            set CASUSER.PROMO_INTERVALS_OUT_1;
            
            if INTERSECT_TYPE = 1 then do;
                START_DT = START_DT_PRICE;
                END_DT = END_DT_PRICE;
            end;
            if INTERSECT_TYPE = 2 then do;
                START_DT = START_DT_PRICE;
                END_DT = END_DT_VAT;
            end;
            if INTERSECT_TYPE = 3 then do;
                START_DT = START_DT_VAT;
                END_DT = END_DT_VAT;
            end;
            if INTERSECT_TYPE = 4 then do;
                START_DT = START_DT_VAT;
                END_DT = END_DT_PRICE;
            end;
            if missing(VAT) = 0 and missing(GROSS_PRICE_AMT) = 0 then NET_PRICE_AMT = round(divide(GROSS_PRICE_AMT, (1 + divide(VAT, 100))), 0.01);
            else NET_PRICE_AMT = .;
        run;
        /* 	Накопление результативной таблицы */

        data CASUSER.&lmvOutTableName(append=yes);
            set CASUSER.PROMO_INTERVALS_OUT;
        run;

        %let lmvIterCounter = %eval(&lmvIterCounter. + 1);
        
        data _NULL_;
            if 0 then set CASUSER.PBO_USED nobs=n;
            call symputx('lmvPboUsedNum',n);
            stop;
        run;
		
		%if &SYSCC gt 4 %then %do;
			/* Return session in execution mode */
			OPTIONS NOSYNTAXCHECK OBS=MAX;
			/* Закрываем процесс в etl_cfg.cfg_status_table и обновляем ресурс*/
			%tech_update_resource_status(mpStatus=E, mpResource=price_regular_future);
			%tech_log_event(mpMODE=END, mpPROCESS_NM=price_promo_past);
			
			%abort;
		%end;

    %end;

    proc casutil;
        promote casdata="&lmvOutTableName" incaslib="casuser" outcaslib="&lmvOutTableCLib";
		save incaslib="&lmvOutTableCLib." outcaslib="&lmvOutTableCLib." casdata="&lmvOutTableName." casout="&lmvOutTableName..sashdat" replace; 
    run;


    proc casutil;  
        droptable casdata="PROMO_FILT_SKU_PBO" incaslib="CASUSER" quiet;
        droptable casdata="PBO_USED" incaslib="CASUSER" quiet;
        droptable casdata="pbo_list_tmp" incaslib="CASUSER" quiet;
        droptable casdata="pbo_list" incaslib="CASUSER" quiet;
        droptable casdata="PBO_LIST_BATCH" incaslib="CASUSER" quiet;
        droptable casdata="PBO_USED" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_FILT_SKU_PBO_BATCH" incaslib="CASUSER" quiet;
        droptable casdata="PRICE_BATCH" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_FILT_SKU_PBO_BATCH_DAYS" incaslib="CASUSER" quiet;
        droptable casdata="PRICE_BATCH_DAYS" incaslib="CASUSER" quiet;
        
        droptable casdata="PRICE_BATCH_DAYS_MECH1210_1" incaslib="CASUSER" quiet;
        droptable casdata="PRICE_BATCH_DAYS_MECH1210_2" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH345_1" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH345_DICT" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH345_2" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH345_3" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH345_4" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH345_5" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH68_1" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH68_2" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH68_3" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH68_4" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH68_5" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH68_6" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH68_7" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH68_8" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH68_9" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH68_10" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH68_11" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH68_12" incaslib="CASUSER" quiet;        
        droptable casdata="PROMO_BATCH_DAYS_MECH7_1" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_BATCH_DAYS_MECH7_2" incaslib="CASUSER" quiet;        
        droptable casdata="PROMO_PRICE_ALL_MECHANICS" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_INTERVALS" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_INTERVALS_OUT" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_REG_MAPPING" incaslib="CASUSER" quiet;
        droptable casdata="PROMO_INTERVALS_OUT_1" incaslib="CASUSER" quiet;
    run;

%mend price_promo_past;

/*%price_promo_past();*/