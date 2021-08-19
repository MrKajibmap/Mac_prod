/********************************************************
* НАЗНАЧЕНИЕ:
*    Макрос для обработки пришедших данных в PostgreSQL (Новые данные из последней выгрузки и изменения в старых)
*
*
* ПАРАМЕТРЫ:
*    mpResource - Наименование загружаемого ресурса
*
*********************************************************
* ОПИСАНИЕ АЛГОРИТМА:
*   1)  Проверка на существование ресурса mpResource в БД
*   2)  Обновление статуса ресурсов в cfg_resource_registry:
*       2.1) Закрытие всех "подвисших" ресурсов со статусом выгрузки(P)
*       2.2) Обновление статуса выгрузки для доступных(A) и ошибочных(E) данных на выгружаемые(P)
*       2.3) Закрытие статуса(C) неактуальных выгрузок
*   3)  Выборка самых свежих данных из выгружаемых(P)
*       3.1) Если таких данных нет, выход из макроса
*   4)  Получение структуры таблицы ресурса (ключи и поля) проверка её корректности
*   5)  Формирование списка ключей и фактов ресурса
*   6)  Выгрузка Снапшота (Полностью=FULL или частично=WINDOW) и расчёт хешей
*   7)  Расчёт хешей для нового набора данных из (3)
*   8)  Сравнение хешей старого набора набора и пришедших данных:
*       8.1) Если хеши фактов равны, то данные не менялись (Флаг равен 2)
*       8.2) Если хеши фактов не равны и при этом есть старые факты, то данные изменились (Флаг равен 3)
*       8.3) В ином случае данные являются новыми (Флаг равен 1)
*   9)  Формирование Дельты (по Флагу 1 или 3) и Снупа (по флагу 3)
*   10) Формирование хеш таблицы Снупа с хешом ключей и даты валидности этих данных.
*       Если на прошлом шаге Снуп не был сформирован, шаг 11 не будет выполняется
*   11) Сопоставление хешей старых данных и найденных изменений. В изменённых строках устанавливается дата окончания актуальности.
*   12) Выгрузка в PostgreSQL Дельты 
*   13) Транзакция изменения данных:
*       13.1) Удаление данных, которые были подвержены изменениям
*       13.2) Добавление Дельты в данные ресурса
*   14) Если Снуп был непустым, добавление в данные ресурса актуализированных данных из (11)
*   15) Выгрузка в PostgreSQL Снупа и Снапшота
*   16) Пометка ресурса как загруженного(L)
*********************************************************/
%macro fmk_load_etl_ia(mpResource=);

	%local lmvResource
			lmvResId
			lmvResNm
			lmvDateTime
			lmvPkList
			lmvColumnsList
			lmvResTypeLoad
			lmvFieldTmFrame
			lmvTmFrameVal
			lmvActualRowsCount
	;
	
	%let lmvResource = %lowcase(&mpResource.);
	%let lmvDateTime = %sysfunc(dhms(%sysfunc(date()), %sysfunc(hour(%sysfunc(time()))), %sysfunc(minute(%sysfunc(time()))), %sysfunc(second(%sysfunc(time()))) ));
	%tech_log_event(mpMODE=START, mpPROCESS_NM=fmk_load_etl_ia_&lmvResource.);
	
	/* Проверка на валидность входных параметров (Сущ-е ресурса) */
	proc sql noprint;
		select 	resource_id
				,resource_nm 
		into 	:lmvResId
				,:lmvResNm
		from etl_cfg.cfg_resource
		where lowcase(resource_nm) = "&lmvResource."
	;
	quit;
	%if %length(&lmvResNm.) eq 0 %then %do;
		%put ERROR: Invalid input parameters;
		%abort;
	%end;
	
	
	proc sql noprint;
		connect using etl_cfg;
		 execute by etl_cfg(
			/* Закрываем все "подвисшие" выгрузки */
			update etl_cfg.cfg_resource_registry
			set status_cd = 'C' where resource_id = &lmvResId.
										and status_cd = 'P' 
			;
			/* Обновляем статус в "Загружается"*/
			update etl_cfg.cfg_resource_registry
			set status_cd = 'P' where exec_dttm = (select max(exec_dttm) as max 
														from etl_cfg.cfg_resource_registry 
														where resource_id = &lmvResId.
														and status_cd in ('A', 'E') )
			;
			/* Закрываем все "старые" выгрузки */
			update etl_cfg.cfg_resource_registry
			set status_cd = 'C' where resource_id = &lmvResId.
										and status_cd in ('A', 'E') 
			);
			
		disconnect from etl_cfg;
	quit;

	/* Забираем самую свежую выгрузку, помеченную статусом 'P' */
	proc sql noprint;
		create table work.tmp_&lmvResource._delta as
			select * 
					,'N' as etl_delta_cd length=1
					/* ,(dhms(date(), hour(time()), minute(time()), second(time()))) as valid_from_dttm length=8 format=datetime. */
					,&lmvDateTime. as valid_from_dttm length=8 format=datetime.
					,&ETL_SCD_FUTURE_DTTM. as valid_to_dttm length=8 format=datetime.
			from etl_stg.stg_&lmvResource.
			where etl_extract_id = select extract_id						
									from etl_cfg.cfg_resource_registry 
									where exec_dttm = select max(exec_dttm) as max 
														from etl_cfg.cfg_resource_registry 
														where resource_id = &lmvResId.
														and status_cd in ('P')  
	;													
	quit;
	
	/* Подсчет кол-ва строк во входном наборе данных */
	%let lmvCntRowsSource = %member_obs(mpData=work.tmp_&lmvResource._delta);
	
	/* Нет данных новых - выходим */
	%if &lmvCntRowsSource. eq 0 %then %do;
		%put WARNING: There are no data to load (Fresh extract_id does not exist) for resource = &lmvResource. (&lmvResId.) 
		in etl_stg.stg_&lmvResource. . Check out etl_cfg.cfg_resource_registry  for existence of new snapshot.;
		%return;
	%end;

	/* Определение типа загрузки для ресурса */
	proc sql noprint;
		select 	resource_type_load
				,field_time_frame
				,time_frame_value 
		into 	:lmvResTypeLoad
				,:lmvFieldTmFrame
				,:lmvTmFrameVal
		from etl_cfg.cfg_resource_type_load
		where lowcase(resource_nm) = "&lmvResource."
	;
	quit;
	%if %length(&lmvResTypeLoad.) eq 0 %then %do;
		%put ERROR: Invalid value for resource type load were expected;
		%abort;
	%end;
		
	proc sql noprint;
		/* Выявляем ключи/поля для таблицы, необходимые для расчета хеш-сумм */
		connect using etl_ia;
		create table work.cols_&lmvResource. as
		select * from connection to etl_ia 
		(
		select c.column_name, c.data_type, ccu.constraint_name, tc.constraint_type
					 from information_schema.columns as c 
					 left join information_schema.constraint_column_usage as ccu
						on c.table_name = ccu.table_name
							and c.column_name = ccu.column_name
					left join information_schema.table_constraints as tc
						on c.table_name = tc.table_name
						and ccu.constraint_name = tc.constraint_name
					 where c.table_name=%str(%')&lmvResource.%str(%')
					 and c.table_schema='etl_ia'
		)
		;
		disconnect from etl_ia;
	quit;
	
	/*Проверка на корректность структуры таблицы */
	%if %member_obs(mpData=work.cols_&lmvResource.) eq 0 %then %do;
		%put ERROR: Invalid structure in table "etl_ia.&lmvResource." (&lmvResId.).;
		%abort;
	%end;

	proc sql noprint;
		select column_name into :lmvPkList separated by ","
		from work.cols_&lmvResource.
		where constraint_type = 'PRIMARY KEY'
			and column_name not in ('valid_from_dttm', 'valid_to_dttm')
		;
		select column_name into :lmvColumnsList separated by ","
		from work.cols_&lmvResource.
		where constraint_type not eq 'PRIMARY KEY'
			and column_name not in ('valid_from_dttm', 'valid_to_dttm')
		;
		select column_name into :lmvFullColumnsList separated by ","
		from work.cols_&lmvResource.
		;
	quit;
	%put &=lmvPkList &=lmvColumnsList;
	
	%if %length(&lmvPkList.) gt 0 %then %do;
		/* Забираем окно из главной таблицы со старыми данными и расчитываем хеши (по фактам и по ключам)*/
		data work.tmp_&lmvResource._snap_hsh;
			length pk_hash value_hash $32;
			format  pk_hash value_hash $hex32.;
			%if %upcase(&lmvResTypeLoad.) eq FULL %then %do;
				set etl_ia.&lmvResource.(where=(valid_from_dttm<=&ETL_CURRENT_DTTM. and valid_to_dttm>=&ETL_CURRENT_DTTM.));
			%end;
			%else %if %upcase(&lmvResTypeLoad.) eq WINDOW %then %do;
				set etl_ia.&lmvResource.(where=( (&lmvFieldTmFrame. >= today()-&lmvTmFrameVal.) and (valid_from_dttm<=&ETL_CURRENT_DTTM. and valid_to_dttm>=&ETL_CURRENT_DTTM.)));
			%end;
			pk_hash = SHA256HEX(catx('_', &lmvPkList.));
			/* Проверка на наличие фактов */
			%if %length(&lmvColumnsList.) gt 0 %then %do;
				value_hash= SHA256HEX(catx('_', &lmvColumnsList.));
			%end;
			%else %do;
				value_hash= SHA256HEX('');
			%end;
		run;
		
		/*Расчитываем хеш для новой пачки данных */
		data work.tmp_&lmvResource._delta_hsh;
			length pk_hash value_hash $32;
			format pk_hash value_hash $hex32.;
			set work.tmp_&lmvResource._delta;
			pk_hash = SHA256HEX(catx('_', &lmvPkList.));
			/* Проверка на наличие фактов */
			%if %length(&lmvColumnsList.) gt 0 %then %do;
				value_hash= SHA256HEX(catx('_', &lmvColumnsList.));
			%end;
			%else %do;
				value_hash= SHA256HEX('');
			%end;
		run;
		
		proc sql noprint;
			create table work.&lmvResource._hash_cfg as
				select distinct n.pk_hash, n.value_hash, o.valid_from_dttm,
						case 
							when n.value_hash = o.value_hash
							then 2 /*same values*/
							when n.value_hash ne o.value_hash and o.value_hash ne ' '
							then 3 /*diff values*/
							else 1 /*new*/
						end as flag 
				from work.tmp_&lmvResource._delta_hsh n
					left join work.tmp_&lmvResource._snap_hsh o 
						on n.pk_hash = o.pk_hash
						and o.valid_to_dttm = &ETL_SCD_FUTURE_DTTM.
				where calculated flag ne 2
			;
			/*extract delta*/
			create table work.&lmvResource._chkd_dlt as
				select distinct mn.*
				from work.tmp_&lmvResource._delta_hsh mn
				inner join work.&lmvResource._hash_cfg h
					on mn.pk_hash =h.pk_hash
				where h.flag in (1,3) 
			;
			/*extract diffs (snup)*/
			create table work.&lmvResource._chkd_snup as
				select distinct mn.*
				from work.tmp_&lmvResource._snap_hsh mn
				inner join work.&lmvResource._hash_cfg h
					on mn.pk_hash =h.pk_hash
				where h.flag = 3 
			;
		quit;
		
		/*create table for dcl hash*/
		data work.&lmvResource._hash_diffs (drop=valid_from_dttm pk_hash);
			length hash_value_dttm_id $32;
			set work.&lmvResource._chkd_snup(keep=valid_from_dttm pk_hash);
			hash_value_dttm_id = SHA256HEX(catx('_',pk_hash,valid_from_dttm));
		run; 
		/* update with hash */
		
		%let lmvHashDiffsLength = %member_obs(mpData=work.&lmvResource._hash_diffs);

		%if &lmvHashDiffsLength gt 0 %then %do;
			%put There are some difference in &lmvResource.;
			data work.&lmvResource.(drop=rc hash_value_dttm_id);
				length hash_value_dttm_id $32 pk_hash $32;
				if _n_=1 then do;
					DECLARE HASH H (DATASET:"work.&lmvResource._hash_diffs");
					RC=H.DEFINEKEY("hash_value_dttm_id");
					RC=H.DEFINEDATA();
					RC=H.DEFINEDONE();
					call missing(hash_value_dttm_id);
				end;
				set tmp_&lmvResource._snap_hsh;
				/* set etl_ia.&lmvResource.; */
				pk_hash = SHA256HEX(catx('_', &lmvPkList.));
				hash_value_dttm_id = SHA256HEX(catx('_',pk_hash,valid_from_dttm));
				rc=h.find();
				if (rc=0) then do;
					valid_to_dttm = &lmvDateTime.;
				end;
			run;
			
			%fmk_load_etl_ia_error_check(mpResId = &lmvResId. ,mpResource = &lmvResource.);		
		%end;
		
		/* загружаем дельту */
		%cmn_load_etl_ia_artefacts(mpMode=delta
								  ,mpTargetTableNm=&lmvResource.
								  ,mpInputTableNm=work.&lmvResource._chkd_dlt
								  );
	%end;
		
	proc sql noprint;
		connect using etl_ia;
		execute by etl_ia 
		(
			BEGIN;
			%if %length(&lmvPkList.) gt 0 %then %do;
				/* Удаление лишних строк */
				%if &lmvHashDiffsLength. gt 0 %then %do;
					%if %upcase(&lmvResTypeLoad.) eq FULL %then %do;
						DELETE FROM etl_ia.&lmvResource.
						WHERE valid_to_dttm = &ETL_SCD_FUTURE_DTTM_DB.;
					%end;
					%else %if %upcase(&lmvResTypeLoad.) eq WINDOW %then %do;
						DELETE FROM etl_ia.&lmvResource.
						WHERE &lmvFieldTmFrame. >= current_date - &lmvTmFrameVal. and valid_to_dttm = &ETL_SCD_FUTURE_DTTM_DB.;
					%end;
				%end;
				
				/* добавление новых строк */
				INSERT INTO etl_ia.&lmvResource.
				SELECT &lmvFullColumnsList.
				FROM etl_ia.&lmvResource._delta;
			%end;
			%else %do;
				TRUNCATE etl_ia.&lmvResource.;
			%end;
			
			COMMIT;
		);
		disconnect from etl_ia;
	quit;
	%fmk_load_etl_ia_error_check(mpResId = &lmvResId. ,mpResource = &lmvResource.);
	
	%if %length(&lmvPkList.) gt 0 %then %do;
		%if &lmvHashDiffsLength. gt 0 %then %do;
			proc append base=etl_ia.&lmvResource.(&ETL_PG_BULKLOAD.) data=work.&lmvResource.(drop=pk_hash value_hash) force;
			run;
		%end;
	%end;
	%else %do;
		proc append base=etl_ia.&lmvResource. (&ETL_PG_BULKLOAD.) data=work.tmp_&lmvResource._delta (drop=etl_delta_cd) force;
		run;
	%end;
	%fmk_load_etl_ia_error_check(mpResId = &lmvResId. ,mpResource = &lmvResource.);

	%if %length(&lmvPkList.) gt 0 %then %do;
		/* Загружаем snup */
		%cmn_load_etl_ia_artefacts(mpMode=snup
									,mpTargetTableNm=&lmvResource.
									,mpInputTableNm=work.&lmvResource._chkd_snup
									);
		/* Загружаем snap */
		%cmn_load_etl_ia_artefacts(mpMode=snap
									,mpTargetTableNm=&lmvResource.
									,mpInputTableNm= work.tmp_&lmvResource._snap_hsh
									);
		%fmk_load_etl_ia_error_check(mpResId = &lmvResId. ,mpResource = &lmvResource.);

		
		/* Calc uploaded/updated rows */
		%let lmvCntRowsTarget = %eval( %member_obs(mpData=work.tmp_&lmvResource._delta) + %member_obs(mpData=work.&lmvResource._hash_diffs) );
		%let lmvCntUpdatedRows = %member_obs(mpData=work.&lmvResource._chkd_snup);
		
		%put Count of updated rows in table &lmvResource. = &lmvCntUpdatedRows. and count of uploaded rows  = &lmvCntRowsTarget.;
	%end;
	%else %do;
		%let lmvCntRowsTarget =%member_obs(mpData=work.tmp_&lmvResource._delta);
		%let lmvCntUpdatedRows = 0;
		
		%put Table &lmvResource. was fully uploaded from etl_stg (There are no primary keys);
	%end;
	
	proc sql noprint;
		SELECT COUNT(*) INTO :lmvActualRowsCount
		FROM ETL_IA.&lmvResource.
		WHERE valid_to_dttm>datetime();
	quit;
	
	%fmk_load_etl_ia_error_check(mpResId = &lmvResId. ,mpResource = &lmvResource.);
	
	proc sql noprint;
		connect using etl_cfg;
		execute by etl_cfg(
		update etl_cfg.cfg_resource_registry 
		set status_cd = 'L'
			,uploaded_from_source=&lmvCntRowsSource.
			,uploaded_to_target=&lmvCntRowsTarget.
			,updated=&lmvCntUpdatedRows.
			,actual_count=&lmvActualRowsCount.
		where exec_dttm = (select max(exec_dttm) as max
					from etl_cfg.cfg_resource_registry where resource_id = &lmvResId. and status_cd = 'P');
		)
	;
	disconnect from etl_cfg;
	quit;
	
	%tech_log_event(mpMODE=END, mpPROCESS_NM=fmk_load_etl_ia_&lmvResource.);
	
%mend fmk_load_etl_ia;