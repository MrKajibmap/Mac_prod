/*Товарный справочник на CAS с матрицей*/

%macro load_prod_dim;
	%tech_cas_session(mpMode = start
					,mpCasSessNm = casauto
					,mpAssignFlg= y
					,mpAuthinfoUsr=
					);
					
	proc sql ;
	create table work.SKU_sales as 
	select distinct product_id 
	from etl_ia.pmix_sales
	where (valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.)
	and sales_dt >= intnx('year', today(), -1, 'b')
	;
	quit;  

	proc fedsql sessref=casauto;
	create table casuser.SKU_MAT_LIST{options replace=true} as 
	select distinct product_id 
	from mn_short.assort_matrix;
	quit;

		%let lmvInLib=work;
		%let ETL_CURRENT_DT = %sysfunc(date());
		%let ETL_CURRENT_DTTM = %sysfunc(datetime());
		%let lmvReportDt=&ETL_CURRENT_DT.;
		%let lmvReportDttm=&ETL_CURRENT_DTTM.;
		

	data CASUSER.pmix_sales (replace=yes);
	set &lmvInLib..SKU_sales
	;
	run;

	proc fedsql sessref=casauto;
	create table casuser.matrix {options replace=true} as
	select distinct product_id from CASUSER.pmix_sales
	union
	select distinct product_id from casuser.SKU_MAT_LIST;
	quit;

		%let lmvInLibIA=etl_ia;

	data CASUSER.prod_mem_hier_prep (replace=yes drop=valid_from_dttm valid_to_dttm);
	set &lmvInLibIA..product_hierarchy
	(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	/*----------------------------------*/


	proc sql;
	create table work.lvl5 as
	select distinct a.product_id, parent_product_id 
	from etl_ia.product_hierarchy a
	inner join casuser.matrix b on a.product_id = b.product_id
	where product_lvl = 5 and (valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.)
	order by product_id;
	quit;

	proc sql;
	create table work.product_list as 
	select distinct product_id 
	from work.lvl5
	;
	quit;


	proc sql;
	create table work.lvl4_p as
	select distinct a.parent_product_id as product_id, b.parent_product_id 
	from work.lvl5 a
	inner join etl_ia.product_hierarchy b 
	on a.parent_product_id = b.product_id
	where (valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.);

	;
	quit;

	proc sql;
	create table work.lvl4 as 
	select distinct product_id 
	from work.lvl4_p
	order by product_id;
	quit;

	proc append 
	base = work.product_list
	data = work.lvl4;

	proc sql;
	create table work.lvl3_p as
	select distinct a.parent_product_id as product_id, b.parent_product_id 
	from work.lvl4_p a
	inner join etl_ia.product_hierarchy b 
	on a.parent_product_id = b.product_id

	where (valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.);
	;
	quit;

	proc sql;
	create table work.lvl3 as 
	select distinct product_id 
	from work.lvl3_p
	order by product_id;
	quit;

	proc append 
	base = work.product_list
	data = work.lvl3;

	proc sql;
	create table work.lvl2_p as
	select distinct a.parent_product_id as product_id, b.parent_product_id 
	from work.lvl3_p a
	inner join etl_ia.product_hierarchy b 
	on a.parent_product_id = b.product_id

	where (valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.);
	;
	quit;

	proc sql;
	create table work.lvl2 as 
	select distinct product_id 
	from work.lvl2_p
	order by product_id;
	quit;

	proc append 
	base = work.product_list
	data = work.lvl2;



	proc sql;
	create table work.lvl1_p as
	select distinct a.parent_product_id as product_id, b.parent_product_id 
	from work.lvl2_p a
	inner join etl_ia.product_hierarchy b 
	on a.parent_product_id = b.product_id
	where (valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.);
	;
	quit;

	proc sql;
	create table work.lvl1 as 
	select distinct product_id 
	from work.lvl1_p
	order by product_id;
	quit;

	proc append 
	base = work.product_list
	data = work.lvl1;



	data CASUSER.product_list (replace=yes);
	set work.product_list
	;
	run;


	proc fedsql sessref=casauto;
	create table casuser.prod_member_hier  {options replace=true} as
	select cast(t1.Product_id as integer) as MEMBER_ID, cast(parent_product_id as integer) as PARENT_MEMBER_ID, 'Sales' as MEMBER_ASSOC_TYPE_CD,
	'' as VALID_FROM_DTTM, '' as VALID_TO_DTTM
	from CASUSER.prod_mem_hier_prep t1
	inner join CASUSER.product_list t2 on t1.product_id = t2.product_id
	;
	quit;

	/*-----------------------------------------------------------------------*/

	data CASUSER.prod (replace=yes drop=valid_from_dttm valid_to_dttm);
	set &lmvInLibIA..product(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;


	proc fedsql sessref=casauto;
	create table casuser.prod_list  {options replace=true} as
	select t1.product_id as MEMBER_ID, product_nm as MEMBER_NM,
	product_nm as MEMBER_DESC, '' as VALID_FROM_DTTM, '' as VALID_TO_DTTM,
	('LEVEL'||PROduct_lvl) as LEVELNAME, 0 as MODELMEMBER
	from casuser.prod t1
	inner join CASUSER.prod_mem_hier_prep t2 on t1.product_id = t2.product_id
	inner join casuser.product_list t3 on t1.product_id = t3.product_id
	;
	quit;

	proc sql;
	create table work.temp as 
	select * from etl_ia.product_attributes
	where valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
	;
	quit;

	proc sort
	data = work.temp
	out = work.atr_sort;
	by product_id descending product_attr_nm;
	run;

	proc transpose
	data = work.atr_sort
	out = work.prod_attrib
	;
	by product_id;
	id product_attr_nm;
	var product_attr_value
	;
	run;

	data CASUSER.prod_attr (replace=yes);
	set &lmvInLib..prod_attrib
	;
	run;

	proc fedsql sessref=casauto;
	create table casuser.product_member {options replace=true} as
	select cast(MEMBER_ID as integer) as MEMBER_ID, MEMBER_NM, MEMBER_DESC, VALID_FROM_DTTM, VALID_TO_DTTM, LEVELNAME, MODELMEMBER, 
	OFFER_TYPE as	USER_ATTRIB1, PRODUCT_GROUP	as USER_ATTRIB2, PRODUCT_SUBGROUP_1	as USER_ATTRIB3,
	PRODUCT_SUBGROUP_2	as USER_ATTRIB4, ITEM_SIZE as USER_ATTRIB5, PRICE_TIER as USER_ATTRIB6,
	HERO as USER_ATTRIB7, cast(MEMBER_ID as integer) as ID_ATTRIB, member_nm as NAME_ATTRIB
	from casuser.prod_list t1
	left join CASUSER.prod_attr t2 on member_id = product_id;
	quit;
	
	%dp_export_csv(mpInput=casuser.PROD_MEMBER_HIER
				, mpTHREAD_CNT=1
				, mpPath=/data/files/output/dp_files/DIMENSIONS/PRODUCT/);
				
	%dp_export_csv(mpInput=casuser.PRODUCT_MEMBER
				, mpTHREAD_CNT=1
				, mpPath=/data/files/output/dp_files/DIMENSIONS/PRODUCT/);
	
%mend load_prod_dim;