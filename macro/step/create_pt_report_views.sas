%macro create_pt_report_views(mpUPTTable=MN_CALC.UPT_SCORING,
							   mpGCTable=MN_CALC.GC_PREDICTION,
							   mpVAPTOut=,
							   mpVAPTGCOut=,
							   mpPromoClRk=);
	cas casauto;
	caslib _all_ assign;
	
	%local
		lmvGCTable
		lmvUPTTable
		lmvVAPTGCOut
		lmvVAPTGCOutLib
		lmvVAPTGCOutTable
		lmvVAPTOut
		lmvVAPTOutLib
		lmvVAPTOutTable
		lmvPromoClRk
		;
	
	%let lmvUPTTable = %upcase(&mpUPTTable.);
	%let lmvGCTable  = %upcase(&mpGCTable.);
	%let lmvVAPTOut    = %upcase(&mpVAPTOut.);
	%let lmvVAPTGCOut = %upcase(&mpVAPTGCOut.);
	
	%let lmvVAPTOutLib = %scan(&lmvVAPTOut., 1, %str(.));
	%let lmvVAPTOutTable = %scan(&lmvVAPTOut., 2, %str(.));
	
	%let lmvVAPTGCOutLib = %scan(&lmvVAPTGCOut., 1, %str(.));
	%let lmvVAPTGCOutTable = %scan(&lmvVAPTGCOut., 2, %str(.));
	
	%let lmvPromoClRk = &mpPromoClRk.;
	
	/* START Создание таблицы casuser.pbo_dictionary для доп.аттрибутов (Макопко/гид/рост и тд) */
	proc casutil;
	  droptable casdata="pbo_dictionary" incaslib="casuser" quiet;
	run;
	
	data CASUSER.PBO_LOCATION (replace=yes drop=valid_from_dttm valid_to_dttm);
		set etl_ia.pbo_location(where=(valid_from_dttm<=&ETL_CURRENT_DTTM. and valid_to_dttm>=&ETL_CURRENT_DTTM.));
	run;
	
	data CASUSER.PBO_LOC_HIERARCHY (replace=yes drop=valid_from_dttm valid_to_dttm);
		set etl_ia.PBO_LOC_HIERARCHY(where=(valid_from_dttm<=&ETL_CURRENT_DTTM. and valid_to_dttm>=&ETL_CURRENT_DTTM.));
	run;

	data CASUSER.PBO_LOC_ATTRIBUTES (replace=yes drop=valid_from_dttm valid_to_dttm);
		set etl_ia.PBO_LOC_ATTRIBUTES(where=(valid_from_dttm<=&ETL_CURRENT_DTTM. and valid_to_dttm>=&ETL_CURRENT_DTTM.));
	run;
	
	
	proc fedsql sessref=casauto noprint;
	   create table casuser.pbo_loc_attr{options replace=true} as
			select distinct *
			from casuser.PBO_LOC_ATTRIBUTES
			;
	quit;

	proc cas;
	transpose.transpose /
	   table={name="pbo_loc_attr", caslib="casuser", groupby={"pbo_location_id"}} 
	   attributes={{name="pbo_location_id"}} 
	   transpose={"PBO_LOC_ATTR_VALUE"} 
	   prefix="A_" 
	   id={"PBO_LOC_ATTR_NM"} 
	   casout={name="attr_transposed", caslib="casuser", replace=true};
	quit;

	proc fedsql sessref=casauto noprint;
	   create table casuser.pbo_hier_flat{options replace=true} as
			select t1.pbo_location_id, 
				   t2.PBO_LOCATION_ID as LVL3_ID,
				   t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				   1 as LVL1_ID
			from 
			(select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=4) as t1
			left join 
			(select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
			;
	quit;

	proc fedsql sessref=casauto noprint;
	   create table casuser.pbo_dictionary{options replace=true} as
	   select t2.pbo_location_id, 
		   coalesce(t2.lvl3_id,-999) as lvl3_id,
		   coalesce(t2.lvl2_id,-99) as lvl2_id,
		   cast(1 as double) as lvl1_id,
		   coalesce(t14.pbo_location_nm,'NA') as pbo_location_nm,
		   coalesce(t13.pbo_location_nm,'NA') as lvl3_nm,
		   coalesce(t12.pbo_location_nm,'NA') as lvl2_nm,
		   cast(inputn(t3.A_OPEN_DATE,'ddmmyy10.') as date) as A_OPEN_DATE,
		   cast(inputn(t3.A_CLOSE_DATE,'ddmmyy10.') as date) as A_CLOSE_DATE,
		   t3.A_PRICE_LEVEL,
		   t3.A_DELIVERY,
		   t3.A_AGREEMENT_TYPE,
		   t3.A_BREAKFAST,
		   t3.A_BUILDING_TYPE,
		   t3.A_COMPANY,
		   t3.A_DRIVE_THRU,
		   t3.A_MCCAFE_TYPE,
		   t3.A_WINDOW_TYPE
	   from casuser.pbo_hier_flat t2
	   left join casuser.attr_transposed t3
	   on t2.pbo_location_id=t3.pbo_location_id
	   left join casuser.pbo_location t14
	   on t2.pbo_location_id=t14.pbo_location_id
	   left join casuser.pbo_location t13
	   on t2.lvl3_id=t13.pbo_location_id
	   left join casuser.pbo_location t12
	   on t2.lvl2_id=t12.pbo_location_id
	   ;
	quit;
	/* END Создание таблицы casuser.pbo_dictionary для доп.аттрибутов (Макопко/гид/рост и тд) */
	
	
	/* START Расчет новых промо-цен на будущее с учетом входного PromoCalculationRk */
	%price_load_data;
	%price_promo_future( mpPromoTable         	= CASUSER.PROMO
				, mpPromoPboTable 	 	= CASUSER.PROMO_PBO_UNFOLD
				, mpPromoProdTable   	= CASUSER.PROMO_PROD
				, mpPriceRegFutTable 	= MN_DICT.PRICE_REGULAR_FUTURE
				, mpVatTable		 	= CASUSER.VAT
				, mpLBPTable		 	= CASUSER.LBP
				, mpPboLocAttributes	= CASUSER.PBO_LOC_ATTRIBUTES
				, mpProductAttrTable    = CASUSER.PRODUCT_ATTRIBUTES
				, mpPriceIncreaseTable 	= CASUSER.PRICE_INCREASE
				, mpOutTable	  	 	= MN_DICT.PRICE_PROMO_FUTURE
				, mpPromoClRk			= &lmvPromoClRk.
				);
	/* END Расчет новых промо-цен на будущее с учетом входного PromoCalculationRk */

	/* Используется выходная таблица  work.lvl_all для загрузки иерархии ПБО*/
	%hier_pt(mpLvl=4, mpIn=etl_ia.PBO_LOC_HIERARCHY, mpOut=PT_PBO_LOC_HIERARCHY);

	/* Удаление целевых таблиц */
	proc casutil;
		/* Удаление целевых таблиц */
		droptable incaslib="casuser" casdata="report_data" quiet;
		droptable incaslib="casuser" casdata="lvl_all_pbo" quiet;
		droptable incaslib="casuser" casdata="UPT_SCORING" quiet;
		droptable incaslib="casuser" casdata="GC_PREDICTION" quiet;
		droptable incaslib="casuser" casdata="INTERNAL_ORG" quiet;
		/* Загрузка в память входных таблиц (GC | UPT после скоринга) и иерархии для пбо*/
		load data=work.lvl_all outcaslib="casuser" casout="lvl_all_pbo" replace;
		load data=&lmvUPTTable. outcaslib="casuser" casout="UPT_SCORING" replace;
		load data=&lmvGCTable. outcaslib="casuser" casout="GC_PREDICTION" replace;
		load data=PT.INTERNAL_ORG outcaslib="casuser" casout="INTERNAL_ORG" replace;
	quit;


	/* Используется выходная таблица  work.lvl_all для загрузки иерархии ПРОДУКТ*/
	%hier_pt(mpLvl=5, mpIn=etl_ia.PRODUCT_HIERARCHY, mpOut=PT_PRODUCT_HIERARCHY);

	proc casutil;
		droptable incaslib="casuser" casdata="lvl_all_prod" quiet;
		load data=work.lvl_all outcaslib="casuser" casout="lvl_all_prod" replace;
	quit;

	/* Подтягиваем к базовому отчету GC доп параметры - иерархию ПБО, аттрибуты пбо */
	proc fedsql SESSREF=casauto noprint;
		create table casuser.report_data_gc{options replace=true} as
			select 	t1.PBO_LOCATION_ID as PBO_LOCATION_ID
					,t1.PROMO_ID 
					,t1.sales_dt as DATE
					,t1.promo as GC_PROMO
					,t1.regular as GC_REGULAR
					,t2.pbo_location_id_1 as PARENT_PBO_LOCATION_ID_1
					,t2.pbo_location_id_2 as PARENT_PBO_LOCATION_ID_2
					,t2.pbo_location_id_3 as PARENT_PBO_LOCATION_ID_3	
					,coalesce(t4.a_agreement_type, t3.agreement_type) as agreement_type
					,coalesce(t4.a_company, t3.company) as company
			from casuser.GC_PREDICTION t1
				left join CASUSER.lvl_all_pbo t2
					on t2.pbo_location_id_4 = t1.PBO_LOCATION_ID
				left join casuser.INTERNAL_ORG t3
					on t3.member_rk = t1.PBO_LOCATION_ID
				left join  casuser.pbo_dictionary t4
					on t4.pbo_location_id = t1.pbo_location_id
		;
	quit;
	
	data casuser.report_data_gc(replace=yes drop=agreement_type_old company_old);
		set casuser.report_data_gc(rename=(agreement_type=agreement_type_old company=company_old));
		length agreement_type $21
			company $20;
		agreement_type = substr(agreement_type_old,1,21);
		company = substr(company_old,1,20);
		PromoClRK = &lmvPromoClRk.;
	run;
	
	data casuser.UPT_SCORING(replace=yes);
		set casuser.UPT_SCORING;
		format pbo_location_id 8.;
	run;
	
	/* Подтягиваем к базовму отчету UPT иерархию продуктов,ПБО, доп аттрибуты */
	proc fedsql SESSREF=casauto noprint;
		create table casuser.report_data{options replace=true} as
			select t1.baseline as UPT_REGULAR
					,t1.delta as UPT_PROMO
					,t1.PBO_LOCATION_ID as PBO_LOCATION_ID
					,t1.product_id as product_id
					,t1.PROMO_ID 
					,t1.sales_dt
					,t2.promo as GC_PROMO
					,t2.regular as GC_REGULAR
					,t1.baseline*t2.regular/1000 as UNITS_REGULAR
					,t1.delta*t2.promo as UNITS_PROMO
					,t3.pbo_location_id_1 as PARENT_PBO_LOCATION_ID_1
					,t3.pbo_location_id_2 as PARENT_PBO_LOCATION_ID_2
					,t3.pbo_location_id_3 as PARENT_PBO_LOCATION_ID_3	
					,t4.product_id_1 as PARENT_PRODUCT_ID_1
					,t4.product_id_2 as PARENT_PRODUCT_ID_2
					,t4.product_id_3 as PARENT_PRODUCT_ID_3
					,t4.product_id_4 as PARENT_PRODUCT_ID_4
					,coalesce(t6.a_agreement_type, t5.agreement_type) as agreement_type
					,coalesce(t6.a_company, t5.company) as company
			from casuser.UPT_SCORING t1
				left join casuser.GC_PREDICTION t2
					on t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID
					and t1.sales_dt = t2.sales_dt
					and t1.promo_id = t2.promo_id
				left join CASUSER.lvl_all_pbo t3
					on t3.pbo_location_id_4 = t1.PBO_LOCATION_ID
				left join CASUSER.LVL_ALL_PROD t4
					on t4.product_id_5 = t1.product_id
				left join casuser.INTERNAL_ORG t5
					on t5.member_rk = t1.PBO_LOCATION_ID
				left join  casuser.pbo_dictionary t6
					on t6.pbo_location_id = t1.pbo_location_id
		;
	quit;
	
	/* Укорачиваем длины полей символьных */
	data casuser.report_data(replace=yes drop=agreement_type_old company_old);
		set casuser.report_data(rename=(agreement_type=agreement_type_old company=company_old));
		length agreement_type $21
			   company $20;
		agreement_type = substr(agreement_type_old,1,21);
		company = substr(company_old,1,20);
	run;
	
	/* Определяем мин. и макс. даты в базовом отчете для последующей протяжки цен(промо- регулярные- косты-) на будущее*/
	proc fedsql SESSREF=casauto noprint;
		create table casuser.report_data_max{options replace=true} as
		select distinct max(sales_dt) as maxDATE, min(sales_dt) as minDATE
		from casuser.report_data
		;
	quit;

	proc sql noprint;
			select maxDATE  format=8., minDATE format=8. into :mvMaxDate, :mvMinDate
			from  casuser.report_data_max
	;
	quit;


	/* Оставляем только нужные нам сочетания ТТ-СКЮ */
	proc fedsql SESSREF=casauto noprint;
		create table casuser.price_reg_fut_extr{options replace=true} as
			select 	t1.*
			from MN_DICT.PRICE_REGULAR_FUTURE t1
				inner join 
						(select distinct PRODUCT_ID, PBO_LOCATION_ID
						from casuser.UPT_SCORING ) t2
					on t1.PRODUCT_ID = t2.product_id
					and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
					and t1.start_dt is not missing
					and t1.end_dt is not missing
		;
	quit;		
	
	/* Раскладываем регулярные цены на дни из интервалов */
	data casuser.price_reg_todate(drop=start_dt end_dt);
		set casuser.price_reg_fut_extr(where=(start_dt ne . and end_dt ne .));
		format sales_dt date9.;
		if start_dt >= &mvMinDate. then do;
			do sales_dt=min(start_dt , &mvMinDate.) to min(end_dt, &mvMaxDate.);
				output;
			end;
		end;
	run;

	proc casutil; 
		droptable incaslib="casuser" casdata="price_reg_fut_extr" quiet; 
	run; 
	quit; 
	

	/*  Присоединяем к витрине регулярные цены */
	proc fedsql SESSREF=casauto noprint;
		create table casuser.report_data_prices_reg{options replace=true} as
			select t1.*
					,t2.net_price_amt as price_regular
			from casuser.report_data t1
				left join casuser.price_reg_todate t2
					on t1.PRODUCT_ID = t2.product_id
					and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
					and t1.sales_dt = t2.sales_dt
		;
	quit;

	proc casutil; 
		droptable incaslib="casuser" casdata="price_reg_todate" quiet; 
	run; 
	quit; 

	/* Оставляем только нужные нам сочетания ТТ-СКЮ */
	proc fedsql SESSREF=casauto noprint;
		create table casuser.price_promo_fut_extr{options replace=true} as
			select 	t1.*
			from MN_DICT.PRICE_PROMO_FUTURE t1
				inner join 
						(select distinct PRODUCT_ID, PBO_LOCATION_ID
						from casuser.UPT_SCORING ) t2
					on t1.PRODUCT_ID = t2.product_id
					and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
					and t1.start_dt is not missing
					and t1.end_dt is not missing
		;
	quit;		

	data casuser.price_promo_todate(drop=start_dt end_dt);
		set casuser.price_promo_fut_extr(where=(start_dt ne . and end_dt ne .));
		format sales_dt date9.;
		if start_dt >= &mvMinDate. then do;
			do sales_dt=min(start_dt , &mvMinDate.) to min(end_dt, &mvMaxDate.);
				output;
			end;
		end;
	run;

	proc casutil; 
		droptable incaslib="casuser" casdata="price_promo_fut_extr" quiet; 
	run; 
	quit; 

	proc fedsql SESSREF=casauto noprint;
		create table casuser.report_data_prices{options replace=true} as
		select 		t1.UPT_REGULAR
					,t1.UPT_PROMO
					,t1.PBO_LOCATION_ID
					,t1.product_id
					,t1.PROMO_ID 
					,t1.sales_dt
					,t1.GC_PROMO
					,t1.GC_REGULAR
					,t1.UNITS_REGULAR
					,t1.UNITS_PROMO
					,t1.PARENT_PBO_LOCATION_ID_1
					,t1.PARENT_PBO_LOCATION_ID_2
					,t1.PARENT_PBO_LOCATION_ID_3	
					/*,t1.PARENT_PBO_LOCATION_ID_4*/
					,t1.PARENT_PRODUCT_ID_1
					,t1.PARENT_PRODUCT_ID_2
					,t1.PARENT_PRODUCT_ID_3
					,t1.PARENT_PRODUCT_ID_4
					,t1.agreement_type
					,t1.company
					,case when coalesce(t2.net_price_amt, t2.gross_price_amt*0.87) is not null then coalesce(t2.net_price_amt, t2.gross_price_amt*0.87)
					else 0
					end as price_promo
					,case when coalesce(t2.net_price_amt, t2.gross_price_amt*0.87) is null then price_regular
					else 0
					end as price_regular
		from casuser.report_data_prices_reg t1
			left join casuser.price_promo_todate t2
				on t1.PRODUCT_ID = t2.product_id
				and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
				and t1.sales_dt = t2.sales_dt
		;
	quit;		


	proc casutil; 
		droptable incaslib="casuser" casdata="price_promo_todate" quiet; 
		droptable incaslib="casuser" casdata="cost_price" quiet; 
	quit; 


	data casuser.cost_price(replace=yes drop=valid_to_dttm valid_from_dttm);
		set etl_ia.cost_price(where=(valid_to_dttm>&ETL_CURRENT_DTTM. and end_dt>intnx('year', &ETL_CURRENT_DT.,0,'b')));
	run;
	
	/* Оставляем только нужные нам сочетания ТТ-СКЮ с последним интервалом цен*/
	proc fedsql SESSREF=casauto noprint;
		create table casuser.cost_price_extr{options replace=true} as
		select src.* 
		from casuser.cost_price src
		inner join (
			select 	t1.product_id
					,t1.pbo_location_id
					,max(t1.start_dt) as start_dt
					,max(t1.end_dt) as end_dt
			from casuser.cost_price t1
				inner join 
						(select distinct PRODUCT_ID, PBO_LOCATION_ID
						from casuser.UPT_SCORING ) t2
					on t1.PRODUCT_ID = t2.product_id
					and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
			group by t1.product_id
					,t1.pbo_location_id ) act
			on src.product_id = act.product_id
			and src.pbo_location_id = act.pbo_location_id
			and src.start_dt = act.start_dt
			and src.end_dt = act.end_dt
		;
	quit;	

	proc casutil; 
		droptable incaslib="casuser" casdata="cost_price" quiet; 
	quit; 

	/* Растягиваем интервалы до максимальной даты отчета */
	data casuser.cost_price_todate(drop=start_dt end_dt);
		set casuser.cost_price_extr(where=(start_dt ne . and end_dt ne .));
		format sales_dt date9.;
		do sales_dt=min(start_dt , &mvMinDate.) to &mvMaxDate.;
			output;
		end;
		/* do sales_dt=min(start_dt,intnx('year', &ETL_CURRENT_DT.,0,'b')) to min(end_dt, &mvMaxDate.);
			output;
		end; */
	run;

	/* добавление аттрибута AGREEMENT_TYPE_PCT, костов на будущее */
	proc fedsql SESSREF=casauto noprint;
		create table casuser.report_data_final{options replace=true} as
		select   t1.*
				,(coalesce(t2.food_cost_amt,0) + coalesce(t2.non_product_cost_amt,0) + coalesce(t2.paper_cost_amt,0)) as cost
				,case when lowcase(AGREEMENT_TYPE) ='mcopco' then 0.3
					  when lowcase(AGREEMENT_TYPE) = 'developmental license' then
						case when lowcase(company)  = 'ooo spp' then 0.35
							when lowcase(company)  = 'ooo razvitie rost' then 0.415 
							when lowcase(company)  = 'ooo gid' then 0.31
						end
				      else 0.25
				end as AGREEMENT_TYPE_PCT
				,&lmvPromoClRk. as PromoClRK
		from casuser.report_data_prices t1
			left join casuser.cost_price_todate t2
				on  t1.PRODUCT_ID = t2.product_id
				and t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
				and t1.sales_dt = t2.sales_dt
		;
	quit;
	
	proc casutil; 
		droptable incaslib="casuser" casdata="cost_price_todate" quiet; 
		droptable incaslib="casuser" casdata="cost_price_extr" quiet; 
	quit; 

	data casuser.product;
		set etl_ia.product(where=(valid_to_dttm>&ETL_CURRENT_DTTM.));
	run;
	
	data casuser.pbo_location;
		set etl_ia.pbo_location(where=(valid_to_dttm>&ETL_CURRENT_DTTM.));
	run;

	/* Формирование CAS-форматов */
	/* Если promo_id = 0, то promo_nm = "Without promo" */
	data CASUSER.promo_format (replace=yes keep=START LABEL Fmtname Type) / single=YES;
		LENGTH START 8
			   LABEL $128
			   Fmtname $32
			   Type $1;
			   
		if _N_ = 1 then do;
			START = 0;
			LABEL = 'Without promo';
			Fmtname= 'promo_name_fmt';
			Type = 'n';
			output;
		end;
		
		set CASUSER.PROMO_ENH;
		START = PROMO_ID;
		LABEL = PROMO_NM;
		Fmtname= 'promo_name_fmt';
		Type = 'n';
		output;
	run;
	
	proc fedsql SESSREF=casauto noprint;
		create table casuser.promo_format{options replace=true} as
		select  distinct t1.*
		from casuser.promo_format t1
		;
	quit;
	
	data CASUSER.product_format (replace=yes keep=START LABEL Fmtname Type);
		set CASUSER.PRODUCT;
		START = PRODUCT_ID;
		LABEL = PRODUCT_NM;
		Fmtname= 'product_name_fmt';
		Type = 'n';
	run;

	data CASUSER.pbo_format (replace=yes keep=START LABEL Fmtname Type);
		set CASUSER.PBO_LOCATION;
		START = PBO_LOCATION_ID;
		LABEL = PBO_LOCATION_NM;
		Fmtname= 'pbo_name_fmt';
		Type = 'n';
	run;

	proc format SESSREF=casauto casfmtlib="FMTDICT" cntlin=CASUSER.promo_format ;
	run;

	proc format SESSREF=casauto casfmtlib="FMTDICT" cntlin=CASUSER.product_format ;
	run;

	proc format SESSREF=casauto casfmtlib="FMTDICT" cntlin=CASUSER.pbo_format ;
	run;
	/* выгрузка форматов в cas */
	cas casauto  savefmtlib fmtlibname=FMTDICT       
	   table="dict_fmts.sashdat" caslib=formats replace;
	/* promote либы с форматами */
	cas casauto promotefmtlib fmtlibname=FMTDICT replace;

	proc casutil;
		droptable incaslib="&lmvVAPTOutLib." casdata="&lmvVAPTOutTable." quiet;
		droptable incaslib="&lmvVAPTGCOutLib." casdata="&lmvVAPTGCOutTable." quiet;
	quit;

	data casuser.VA_PT(DROP=UPT_REGULAR_SVD UPT_PROMO_SVD GC_REGULAR_SVD GC_PROMO_SVD UNITS_REGULAR_SVD UNITS_PROMO_SVD PRICE_REGULAR_SVD PRICE_PROMO_SVD);
		set casuser.report_data_final;
		format pbo_location_id 8.;
		LABEL PROMO_ID="PROMO"
				PBO_LOCATION_ID	="PBO"
				PARENT_PBO_LOCATION_ID_1	=	"PBO 1"
				PARENT_PBO_LOCATION_ID_2	=	"PBO 2"
				PARENT_PBO_LOCATION_ID_3	=	"PBO 3"
				AGREEMENT_TYPE	=	    "AGREEMENT TYPE"
				AGREEMENT_TYPE_PCT	=	"AGREEMENT PERCENT"		
				COMPANY = "COMPANY"
				PRODUCT_ID	=	"PRODUCT"
				PARENT_PRODUCT_ID_1	=	"PRODUCT 1"
				PARENT_PRODUCT_ID_2	=	"PRODUCT 2"
				PARENT_PRODUCT_ID_3	=	"PRODUCT 3"
				PARENT_PRODUCT_ID_4	=	"PRODUCT 4"
				DATE	    =	 "DATE"
				UPT_REGULAR	=	 "UPT REGULAR"
				UPT_PROMO	=	 "UPT PROMO"
				GC_REGULAR	=    "GC REGULAR"
				GC_PROMO	=	 "GC PROMO"
				UNITS_REGULAR = "UNITS REGULAR"
				UNITS_PROMO   =	 "UNITS PROMO"
				PRICE_REGULAR = "PRICE REGULAR"
				PRICE_PROMO   =	 "PRICE PROMO"
				COST	=	     "COST"
		;
		format PBO_LOCATION_ID PARENT_PBO_LOCATION_ID_1 PARENT_PBO_LOCATION_ID_2 PARENT_PBO_LOCATION_ID_3 pbo_name_fmt.
				PRODUCT_ID PARENT_PRODUCT_ID_1 PARENT_PRODUCT_ID_2 PARENT_PRODUCT_ID_3 PARENT_PRODUCT_ID_4 product_name_fmt.
				PROMO_ID promo_name_fmt.;
		/* сохраняем значения для обоих случаев */
		/* Если выводим промо -> promo_id выводим */
		UPT_REGULAR_SVD= UPT_REGULAR;
		UPT_PROMO_SVD = UPT_PROMO;
		GC_REGULAR_SVD = GC_REGULAR;
		GC_PROMO_SVD = GC_PROMO;
		UNITS_REGULAR_SVD = UNITS_REGULAR; 
		UNITS_PROMO_SVD = UNITS_PROMO;
		PRICE_REGULAR_SVD = PRICE_REGULAR; 
		PRICE_PROMO_SVD = PRICE_PROMO;
		/* Выводим промо */
		UPT_REGULAR = 0;
		GC_REGULAR=0;
		UNITS_REGULAR=0;
		PRICE_REGULAR=0;
		output;
		/* Выводим регулярные */
		PROMO_ID = 0;
		UPT_REGULAR = UPT_REGULAR_SVD;
		GC_REGULAR=GC_REGULAR_SVD;
		UNITS_REGULAR= UNITS_REGULAR_SVD;
		PRICE_REGULAR= PRICE_REGULAR_SVD;
		UPT_PROMO = 0;
		GC_PROMO = 0;
		UNITS_PROMO = 0;
		PRICE_PROMO = 0;
		output;
	run;
	
	/*проверить формат для этой таблицы */
	data casuser.VA_PT_GC;
		set casuser.report_data_gc;
		format pbo_location_id 8.;
		
		LABEL   PROMO_ID   =   "PROMO"
				PBO_LOCATION_ID	        =   "PBO"
				PARENT_PBO_LOCATION_ID_1	=	"PBO 1"
				PARENT_PBO_LOCATION_ID_2	=	"PBO 2"
				PARENT_PBO_LOCATION_ID_3	=	"PBO 3"
				AGREEMENT_TYPE	            =	"AGREEMENT TYPE"
				AGREEMENT_TYPE_PCT	        =	"AGREEMENT PERCENT"		
				DATE	    =	"DATE"
				GC_REGULAR	=   "GC REGULAR"
				GC_PROMO	=	"GC PROMO"
		;
		format PBO_LOCATION_ID PARENT_PBO_LOCATION_ID_1 PARENT_PBO_LOCATION_ID_2 PARENT_PBO_LOCATION_ID_3 pbo_name_fmt.
				PROMO_ID promo_name_fmt.;
	run;
	
	proc casutil;
		promote casdata='VA_PT' incaslib='casuser' casout="&lmvVAPTOutTable." outcaslib="&lmvVAPTOutLib.";
		promote casdata='VA_PT_GC' incaslib='casuser' casout="&lmvVAPTGCOutTable." outcaslib="&lmvVAPTGCOutLib.";
	quit;
	
%mend create_pt_report_views;