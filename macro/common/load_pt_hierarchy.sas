/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных в PT
*		До запуска должна быть сформирована таблица work.&TableNm._dttm
*
*  ПАРАМЕТРЫ:
*     Нет
*
******************************************************************
*  Использует: 
*	  нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     load_pt_hierarchy(mpDatetime=&mvDatetime.
*					,mpMemberTableNm = pt.product_hier
*					);
*
****************************************************************************
*  25-08-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro load_pt_hierarchy(mpDatetime=
						,mpMemberTableNm = 
						);

	%local lmvLibrefIn 
			lmvTabNmIn
			lmvDatetime
			lmvCntDiffs
			;

	%member_names (mpTable=&mpMemberTableNm
					,mpLibrefNameKey=lmvLibrefIn
					,mpMemberNameKey=lmvTabNmIn);

	%let lmvDatetime = &mpDatetime.;

	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto;
		caslib _all_ assign;
	%end;

	proc casutil ;
		droptable casdata="&lmvTabNmIn._NEW" incaslib="CASUSER" quiet;
		droptable casdata="&lmvTabNmIn._OLD" incaslib="CASUSER" quiet;
		droptable casdata="hashjoin_config_table" incaslib="CASUSER" quiet;
		droptable casdata="&lmvTabNmIn._delta" incaslib="CASUSER" quiet;
	run;

	/* check for diffs in hier*/
	data CASUSER.&lmvTabNmIn._NEW(replace=yes);
		set &lmvTabNmIn._dttm;
		length hash_key_id hash_value_id $32;
		format hash_key_id hash_value_id $hex32.;
		hash_key_id = SHA256HEX(catx('_',member_rk, prnt_member_rk));
		hash_value_id= SHA256HEX(catx('_',btwn_lvl_cnt, is_bottom_flg, is_top_flg));
	run;
	
	data CASUSER.&lmvTabNmIn._OLD(replace=yes);
		set &lmvLibrefIn..&lmvTabNmIn.;
		length hash_key_id hash_value_id $32;
		format hash_key_id hash_value_id $hex32.;
		hash_key_id = SHA256HEX(catx('_',member_rk, prnt_member_rk));
		hash_value_id= SHA256HEX(catx('_',btwn_lvl_cnt, is_bottom_flg, is_top_flg));
	run;

	proc fedsql SESSREF=casauto noprint;
			create table casuser.hashjoin_config_table as
				select distinct n.hash_key_id, n.hash_value_id, o.valid_from_dttm,
						case 
							when n.hash_value_id = o.hash_value_id
							then 2 /*same values*/
							when n.hash_value_id <> o.hash_value_id and o.hash_value_id <> ' '
							then 3 /*diff values*/
							else 1 /*new*/
						end as flag 
				from CASUSER.&lmvTabNmIn._NEW n
					left join CASUSER.&lmvTabNmIn._OLD o 
						on n.hash_key_id = o.hash_key_id
						and o.valid_to_dttm = timestamp'5999-01-01 00:00:00'
			;
	quit;
			
	proc sql noprint;
			/*extract diffs*/
			select count(*) as cnt into :lmvCntDiffs
			from casuser.hashjoin_config_table
			where flag in (3,1)
			;
	quit;

	%put &=lmvCntDiffs;

	%if &lmvCntDiffs. > 0 %then %do;
		%put "NOTE: The differences between existing table and new batch have founded, Hierarchy table will be replaced." ;
		/* replace table */
		PROC SQL NOPRINT;	
		/*
		CONNECT TO POSTGRES AS CONN (server="10.252.151.3" port=5452 user=pt password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=pt defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=8192");
		*/
		connect using pt;
		/* truncate target table in PT PG schema */
				EXECUTE BY pt
					(
						TRUNCATE TABLE public.&lmvTabNmIn.
					)
				;
		QUIT;

		data work.&lmvTabNmIn._NEW(drop = hash_key_id hash_value_id);
			set casuser.&lmvTabNmIn._NEW;
		run;

		proc append base=&lmvLibrefIn..&lmvTabNmIn. data=work.&lmvTabNmIn._NEW force; 
		run;
	%end;

	%symdel lmvCntDiffs;

%mend load_pt_hierarchy;