/*Справочник ресторанов с матрицей в CAS иерархия по типу договора и компании*/


%macro load_loc_dim;
	%tech_cas_session(mpMode = start
					,mpCasSessNm = casauto
					,mpAssignFlg= y
					,mpAuthinfoUsr=
					);
					
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	%let lmvWorkCaslib = mn_short;

	proc casutil;
		droptable casdata="assort_matrix" incaslib="mn_short" quiet;
	run;
	
	data CASUSER.assort_matrix (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set ETL_IA.assort_matrix(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	proc casutil;
		promote casdata="assort_matrix" incaslib="casuser" outcaslib="mn_short";
	run;

	proc fedsql sessref=casauto;
		create table casuser.PBO_MAT_LIST{options replace=true} as 
			select distinct PBO_LOCATION_ID 
			from MN_SHORT.ASSORT_MATRIX;
	quit;


	/*----------------------------------*/

	proc sql;
		create table work.temp as 
			select * 
			from etl_ia.pbo_loc_attributes t1
				inner join casuser.PBO_MAT_LIST t2
					on t1.pbo_location_id = t2.pbo_location_id
			where valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
		;
	quit;

	proc sql;
		create table work.attr_hier_prep as
			select distinct 
				pbo_location_id,
				pbo_loc_attr_nm,
				pbo_loc_attr_value
			from work.temp
			where pbo_loc_attr_nm in ('AGREEMENT_TYPE', 'COMPANY');
	quit;

	proc sql;
		create table work.attr_for_list as
			select distinct 
				pbo_loc_attr_nm, 
				pbo_loc_attr_value
			from work.attr_hier_prep;
	quit;

	proc sql;
		create table work.pbo_lvl4 as
			select distinct
				a.pbo_location_id, 
				put(a.pbo_location_id, 32.) as location_char,
				pbo_loc_attr_value as parent_pbo_location_id,
				'LEVEL4' as LEVELNAME
			from etl_ia.pbo_loc_hierarchy a
				inner join work.attr_hier_prep c
					on a.pbo_location_id = c.pbo_location_id
			where pbo_location_lvl = 4 
				and (valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.) 
				and c.pbo_loc_attr_nm = 'COMPANY'
			order by a.pbo_location_id;
	quit;

	proc sql;
		create table work.pbo_lvl3_temp as
			select distinct 
				t1.pbo_location_id as restic,
				t1.parent_pbo_location_id as pbo_location_id, 
				pbo_loc_attr_value as parent_pbo_location_id
			from work.pbo_lvl4 t1
				inner join work.attr_hier_prep t2 
					on t1.pbo_location_id = t2.pbo_location_id
			where pbo_loc_attr_nm = 'AGREEMENT_TYPE'
	;
	quit;

	proc sql;
		create table work.pbo_lvl3_p as
			select distinct 
				pbo_location_id, 
				parent_pbo_location_id, 
				'LEVEL3' as LEVELNAME
			from work.pbo_lvl3_temp a
		;
	quit;


	proc sql;
		create table work.pbo_lvl2_p as
			select distinct 
				a.parent_pbo_location_id as pbo_location_id, 
				'1' as parent_pbo_location_id, 
				'LEVEL2' as LEVELNAME
			from work.pbo_lvl3_p a
	;
	quit;

	
	proc sql;
		create table work.middle_lvl as
			select distinct  *
			from work.pbo_lvl2_p
		union 
			select distinct * from work.pbo_lvl3_p;
	quit;
	%let max_current_code_id=88888888;
	%if %sysfunc(exist(mn_dict.pbo_loc_id)) %then %do; 
		/*Сохраняем айдишники. Предварителная проверка, чтобы подтянуть те, которых не было в work.id*/
		proc sql noprint;
			create table work.loc_id_to_append as
			select
				t1.pbo_location_id,
				t1.parent_pbo_location_id,
				t1.levelname,
				t2.loc_id
			from work.middle_lvl
				inner join mn_dict.pbo_loc_id
					on t1.pbo_location_id = t2.pbo_location_id
					and t1.parent_pbo_location_id = t2.parent_pbo_location_id
			where t2.loc_id = NULL;
		quit;
		
		%local lmvLocMaxCode   /* Максимальный код loc_id в mn_dict.pbo_loc_id */
			   lmvLocApndCnt;  /* Кол-во записей, которые надо дозаписать в pbo_loc_id */
		
		%let lmvLocApndCnt = %member_obs(mpData=work.loc_id_to_append);
		
		%if &lmvLocApndCnt. gt 0 %then %do;
		
			proc sql noprint;
				select MAX(loc_id) into :lmvLocMaxCode
				from mn_dict.pbo_loc_id;
			quit;
			
			proc sql noprint;
				update work.loc_id_to_append
				set loc_id = monotonic() + &lmvLocMaxCode.;
			quit;
			
			proc append 
				base=mn_dict.pbo_loc_id
				data=work.loc_id_to_append;
			run;
		
		%end;
	%end;
	%else %do;
		create table mn_dict.pbo_loc_id as
			select 
				t1.*,
				(monotonic()+&max_current_code_id.) as loc_id
			from work.middle_lvl t1
		;
	%end;

	proc sql;
		create table work.lvl23_id as
			select 
				t1.loc_id as pbo_location_id,
				coalesce(t2.loc_id, input(t1.parent_pbo_location_id, best8.)) as parent_pbo_location_id,
				t1.levelname
			from mn_dict.pbo_loc_id t1
				left join mn_dict.pbo_loc_id t2 
				on t1.parent_pbo_location_id=t2.pbo_location_id;
	quit;

	proc sql;
		create table work.pbo_lvl1_p as
			select distinct
				1 as pbo_location_id,
				1 as parent_pbo_location_id,
				'LEVEL1' as LEVELNAME
			from work.pbo_lvl2_p 
			;
	quit;

	proc sql;
		create table work.hier_list as 
			select distinct 
				t1.restic as pbo_location_id,
				t2.loc_id as parent_pbo_location_id,
				'LEVEL4' as LEVELNAME
			from work.pbo_lvl3_temp t1
				left join mn_dict.pbo_loc_id t2 
					on t1.pbo_location_id=t2.pbo_location_id 
					and t1.parent_pbo_location_id = t2.parent_pbo_location_id
			;
	quit;


	proc append 
		base = work.hier_list
		data = work.pbo_lvl1_p
		force;
	run;

	proc append 
		base = work.hier_list
		data = work.lvl23_id
		force;
	run;
	
	proc casutil;
		droptable casdata="hier_list" incaslib="casuser" quiet;
		load data=work.hier_list outcaslib='casuser';
	run;


	proc fedsql sessref=casauto;
		create table casuser.location_member_hier{options replace=true} as
			select 
				cast(PBO_LOCATION_id as integer) as MEMBER_ID,
				cast(parent_pbo_location_id as integer) as PARENT_MEMBER_ID,
				'AL_HIER_ATR_FULL' as MEMBER_ASSOC_TYPE_CD,
				'' as VALID_FROM_DTTM,
				'' as VALID_TO_DTTM
			from casuser.hier_list 
			;
	quit;

	/*-----------------------------------------------------------------------*/

	   %let ETL_CURRENT_DTTM = %sysfunc(datetime());
	   %let lmvReportDttm=&ETL_CURRENT_DTTM.;

	proc sql;
		create table work.pbo_location as 
			select * 
			from etl_ia.pbo_location
			where valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
		;
	quit;

	proc sql;
		create table work.pbo_list as
			select 
				t1.pbo_location_id as MEMBER_ID,
				coalesce(t3.pbo_location_nm, t2.pbo_location_id) as MEMBER_NM,
				coalesce(t3.pbo_location_nm, t2.pbo_location_id) as MEMBER_DESC,
				'' as VALID_FROM_DTTM,
				'' as VALID_TO_DTTM,
				t1.LEVELNAME, 
				'RUR' as REPORTING_CURRENCY_CD
			from work.hier_list t1
				left join mn_dict.pbo_loc_id t2 
					on t1.pbo_location_id = t2.loc_id
				left join work.PBO_location t3 
					on t1.pbo_location_id = t3.pbo_location_id
			;
	quit;


	proc sort
		data = work.temp
		out = work.pbo_attr_sort;
		by pbo_location_id descending  pbo_loc_attr_nm;
	run;


	proc transpose
		data = work.pbo_attr_sort
		out = work.pbo_attrib
		;
		by pbo_location_id;
		id pbo_loc_attr_nm;
		var pbo_loc_attr_value
		;
	run;
	
	proc casutil;
		droptable casdata='pbo_list' incaslib='casuser' quiet;
		droptable casdata='pbo_attrib' incaslib='casuser' quiet;
		load data=work.pbo_attrib outcaslib='casuser';
		load data=work.pbo_list outcaslib='casuser';
	run;

	proc fedsql sessref=casauto;
		create table casuser.LOCATION_MEMBER{options replace=true} as
			select 
				cast(MEMBER_ID as integer) as MEMBER_ID,
				MEMBER_NM,
				MEMBER_DESC, 
				VALID_FROM_DTTM,
				VALID_TO_DTTM, LEVELNAME,
				'RUR' as REPORTING_CURRENCY_CD, 
				AGREEMENT_TYPE, 
				BREAKFAST, 
				BUILDING_TYPE,
				CLOSE_DATE, 
				COMPANY, 
				DELIVERY, 
				DELIVERY_OPEN_DATE,
				DRIVE_THRU,
				MCCAFE_OPEN_DATE, 
				MCCAFE_TYPE, 
				OPEN_DATE, 
				OPS_CONSULTANT, 
				OPS_DIRECTOR,
				OPS_MANAGER, 
				PRICE_LEVEL, 
				WINDOW_TYPE, 
				cast(MEMBER_ID as integer) as ATTRIB_ID,
				member_nm as ATTRIB_NAME
			from casuser.pbo_list t1
				left join casuser.pbo_attrib t2
				on member_id = pbo_location_id;
	quit;
	
	%dp_export_csv(mpInput=casuser.LOCATION_MEMBER_HIER
				, mpTHREAD_CNT=1
				, mpPath=/data/files/output/dp_files/DIMENSIONS/LOCATION/);
				
	%dp_export_csv(mpInput=casuser.LOCATION_MEMBER
				, mpTHREAD_CNT=1
				, mpPath=/data/files/output/dp_files/DIMENSIONS/LOCATION/);
				
%mend load_loc_dim;