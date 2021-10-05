%macro data_prep_pbo(
		  mpInLib = etl_ia
		, mpReportDttm = &ETL_CURRENT_DTTM.
		, mpOutCasTable = CASUSER.PBO_DICTIONARY
	);

	%macro mDummy;
	%mend mDummy;

	%let lmvReportDttm = &mpReportDttm.;
	%let lmvInLib = &mpInLib.;  

	data CASUSER.PBO_LOCATION (replace=yes drop=valid_from_dttm valid_to_dttm);
	   set &lmvInLib..pbo_location(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.PBO_LOC_HIERARCHY (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PBO_LOC_HIERARCHY(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.PBO_LOC_ATTRIBUTES (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PBO_LOC_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
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
			casout={name="attr_transposed", caslib="casuser", replace=true}
		;
	quit;
	
	proc fedsql sessref=casauto noprint;
		create table casuser.pbo_hier_flat{options replace=true} as
		select 
			 t1.pbo_location_id
		   , t2.PBO_LOCATION_ID as LVL3_ID
		   , t2.PARENT_PBO_LOCATION_ID as LVL2_ID
		   , 1 as LVL1_ID
		from 
			(select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=4) as t1
		left join 
			(select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
		;
	quit;
	
	proc fedsql sessref=casauto noprint;
		create table &mpOutCasTable. {options replace=true} as
		select t2.pbo_location_id
			, coalesce(t2.lvl3_id,-999) as lvl3_id
			, coalesce(t2.lvl2_id,-99) as lvl2_id
			, cast(1 as double) as lvl1_id
			, cast(1 as double) as lvl1_nm
			, coalesce(t14.pbo_location_nm,'NA') as pbo_location_nm
			, coalesce(t13.pbo_location_nm,'NA') as lvl3_nm
			, coalesce(t12.pbo_location_nm,'NA') as lvl2_nm
			, cast(inputn(t3.A_OPEN_DATE,'ddmmyy10.') as date) as A_OPEN_DATE
			, cast(inputn(t3.A_CLOSE_DATE,'ddmmyy10.') as date) as A_CLOSE_DATE
			, t3.A_AGREEMENT_TYPE
			, t3.A_BREAKFAST
			, t3.A_BUILDING_TYPE
			, t3.A_COMPANY
			, t3.A_DELIVERY
			, t3.A_DELIVERY_OPEN_DATE
			, t3.A_DRIVE_THRU
			, t3.A_LATITUDE
			, t3.A_LONGITUDE
			, t3.A_MCCAFE_OPEN_DATE
			, t3.A_MCCAFE_TYPE
			, t3.A_OPS_CONSULTANT
			, t3.A_OPS_DIRECTOR
			, t3.A_OPS_MANAGER
			, t3.A_PRICE_AREA_NAME
			, t3.A_PRICE_LEVEL
			, t3.A_STORE_ID
			, t3.A_WINDOW_TYPE

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

	/* Clear CAS */
/* 	proc casutil incaslib="CASUSER" ; */
/* 		droptable casdata = "attr_transposed" quiet; */
/* 		droptable casdata = "pbo_hier_flat" quiet; */
/* 		droptable casdata = "pbo_loc_attr" quiet; */
/* 		droptable casdata = "PBO_LOC_HIERARCHY" quiet; */
/* 		droptable casdata = "PBO_LOCATION" quiet; */
/* 		droptable casdata = "PBO_LOC_ATTRIBUTES" quiet; */
/* 	run; */

%mend data_prep_pbo;