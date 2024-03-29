%macro tech_main_schedule;
	/* 0. Этап проверки интеграционого слоя на доступность новых ресурсов к загрузке в систему SAS */
	options nomprint nomlogic nosymbolgen;
	proc sql noprint;
		create table work.ia_resources as
			select resource_name
			from IA.ETL_PROCESS_LOG
			where 
			/* IA_STATUS_CD = "L" and  */
			datepart(IA_FINISH_DTTM) = date()
			/* and  */
			/* (SAS_START_DTTM is null or SAS_STATUS_CD="E") */
		;
		create table work.ready_to_downl_res as
			select main_res.resource_nm
			from work.ia_resources ia_res
			inner join ETL_CFG.CFG_RESOURCE main_res
				on upcase(ia_res.resource_name) = upcase(main_res.resource_nm)
		;
		create table work.ready_to_downl_res_checked as 
			select resource_nm
			from (
				select lowcase(resource_nm) as resource_nm
				from work.ready_to_downl_res
				/* Добавляем форсированные процессы */
				union 
				select lowcase(resource_nm) as resource_nm
				from ETL_CFG.CFG_RESOURCE
					where forced_load_flag eq 1
				except 
				select lowcase(resource_nm) as resource_nm
				from ETL_CFG.CFG_STATUS_TABLE
					where datepart(processed_dttm) eq date()
				)
		;
	quit;

	/* Открываем ресурсы */
	%if %member_obs(mpData=work.ready_to_downl_res_checked) gt 0 %then %do;
		%do i=1 %to %member_obs(mpData=work.ready_to_downl_res_checked);
			data _null_;
				set work.ready_to_downl_res_checked(obs=&i firstobs=&i);
				call symputx('res_nm', resource_nm);
			run;
			%tech_open_resource(mpResource=&res_nm.);
		%end;
	%end;
	
	/* Перезапуск упавших ресурсов */
	%if &ETL_RESTART_FAILED_PROCESSES.=YES %then %do;
		proc sql;
			update etl_cfg.cfg_status_table
			set status_cd = 'A',
				retries_cnt = retries_cnt + 1
			where status_cd ='E' AND retries_cnt <= &ETL_RETRIES_COUNT.;
		quit;
	%end;

	/* 1. Получаем полный список по модулям */
	proc sql;
	   create table work.full_list_modules as
	   select
		  cfg_resource.resource_id length = 8   
			 label = 'resource_id',
		  cfg_resource.resource_nm length = 40   
			 format = $40.
			 informat = $40.
			 label = 'resource_nm',
		  cfg_resource.macro_nm length = 200   
			 format = $200.
			 informat = $200.
			 label = 'macro_nm',
		  cfg_resource.module_nm length = 32   
			 format = $32.
			 informat = $32.
			 label = 'module_nm',
		  cfg_schedule_rule.rule_cond length = 1000   
			 format = $1000.
			 informat = $1000.
			 label = 'rule_cond',
		  cfg_schedule_rule.rule_start_hour length = 8   
			 format = $8.
			 informat = $8.
	   from
		  ETL_CFG.cfg_resource as cfg_resource inner join 
		  ETL_CFG.cfg_schedule_rule as cfg_schedule_rule
			 on
			 (
				upcase(cfg_resource.module_nm) = upcase(cfg_schedule_rule.rule_nm)
			 )
	   ;
	quit;

	/* 2. Проверка выполнения условий по модулям */
	%tech_schedule_check_rule(mpInput=work.full_list_modules, mpOutput=work.full_list_modules_checked);

	/* 3. Добавим условия запуска по ресурсам */

	proc sql;
	   create table work.resource_rules_list as
	   select
		  t1.resource_id length = 8   
			 label = 'resource_id',
		  t1.resource_nm length = 40   
			 format = $40.
			 informat = $40.
			 label = 'resource_nm',
		  t1.macro_nm length = 200   
			 format = $200.
			 informat = $200.
			 label = 'macro_nm',
		  t1.module_nm length = 32   
			 format = $32.
			 informat = $32.
			 label = 'module_nm',
		  t2.rule_cond length = 1000   
			 format = $1000.
			 informat = $1000.
			 label = 'rule_cond',
		  t2.rule_start_hour length = 8   
			 format = $8.
			 informat = $8.
	   from
		  work.full_list_modules_checked as t1 left join 
		  ETL_CFG.cfg_schedule_rule as t2
			 on
			 (
				full_list_modules_checked.resource_nm = t2.rule_nm
			 )
	   where
		  t1.macro_nm NOT IS MISSING 
	   ;
	quit;

	/* 4. Проверка по новым правилам */
	%tech_schedule_check_rule(mpInput=work.resource_rules_list, mpOutput=work.resource_rules_list_checked);
	/* 5. Загруженные сегодня и загружаемые ресурсы */
	proc sql;
	   create table work.loaded_in_process_resources as
		  select
			 resource_id,
			 resource_nm,
			 status_cd,
			 processed_dttm
	   from etl_cfg.cfg_status_table
		  where ( 
			 datepart(processed_dttm) = today()
			 AND
			 (
			 status_cd not in ( "A" , "C"))
			 )
			 OR
			 (
			 status_cd in ( "P")
			 )
	   ;
	quit;

	/* 6. Отсеиваем ресурсы из п.5 */
	proc sql;
	   create table work.resources_to_start as
	   select
		  t1.resource_id length = 8   
			 label = 'resource_id',
		  t1.resource_nm length = 40   
			 format = $40.
			 informat = $40.
			 label = 'resource_nm',
		  t1.macro_nm length = 200   
			 format = $200.
			 informat = $200.
			 label = 'macro_nm',
		  t1.module_nm length = 32   
			 format = $32.
			 informat = $32.
			 label = 'module_nm',
		  t1.rule_cond length = 1000   
			 format = $1000.
			 informat = $1000.
			 label = 'rule_cond'
	   from
		  work.resource_rules_list_checked as t1
		  left join 
		  work.loaded_in_process_resources as t2
			 on
			 (
				t1.resource_id = t2.resource_id
			 ) 
	   where 
		  COALESCE(t2.resource_id ,1) = 1
	   ;
	quit;
	
	data resources_to_start;
		set resources_to_start;
		macro_nm=scan(macro_nm, -1, "/"); /*получаем только имена скриптов*/
		resource_nm = tranwrd(tranwrd(lowcase(resource_nm), 'ia_', ''),'stg_', '');
	run;

		data open_res;
			set resources_to_start;
			seq = _N_;
		run;
		
		%macro m_schedule_check;

			%let seq=&seq;
			%let macro_nm = %trim(&macro_nm.);
			%let module_nm = %lowcase(%trim(&module_nm.));
			%let resource_nm = %trim(&resource_nm.);
			
			filename par_&seq temp;
			
			data _null_;
				file par_&seq;       
				/* Загрузка данных из IA в ETL_STG */
				%if &module_nm. eq etl_stg %then %do;
					put "%nrstr(%%)fmk100_load_&module_nm.(mpResource=&resource_nm.)";
				%end;
				/* Загрузка данных из ETL_STG в ETL_IA*/
				%else %if &module_nm. eq etl_ia %then %do;
					put "%nrstr(%%)fmk200_load_&module_nm.(mpResource=&resource_nm.)";
				%end;
				/* Загрузка данных в DP */
				%else %if &module_nm. eq dp_int or &module_nm. eq dp_fact or &module_nm. eq dp_seed %then %do;
					put "%nrstr(%%)rtp011_load_to_dp(mpJobNm=&resource_nm.)";
				%end;
				/* Запуск остальных процессов (индивидуальных) */
				%else %do;
					put "%nrstr(%%)&macro_nm.";
				%end;
			run;
			/* Разделяем запуски для корректного создания логов */
			%if &module_nm. eq etl_stg or &module_nm. eq etl_ia or &module_nm. eq dp_int or &module_nm. eq dp_fact or &module_nm. eq dp_seed %then %do;
				systask command 
				"""/opt/sas/mcd_config/bin/start_sas.sh"" ""%sysfunc(pathname(par_&seq))"" &module_nm. &macro_nm._&resource_nm."
				taskname=task_&seq status=rc_&seq;
			%end;
			%else %do;
				systask command 
				"""/opt/sas/mcd_config/bin/start_sas.sh"" ""%sysfunc(pathname(par_&seq))"" &module_nm. &macro_nm."
				taskname=task_&seq status=rc_&seq;
			%end;
			
			%if &SYSRC ne 0 %then %do;
				%put WARNING: сессия не была запущена успешно;
			%end;
			%else %do;
				%put NOTE: Процесс &macro_nm. запущен;
			%end;
		%mend m_schedule_check;


		%util_loop_data( 
		 mpLoopMacro       =  m_schedule_check,
		 mpData            =  open_res);	
		
		%macro m_schedule_check_waitfor;
			task_&seq
		%mend m_schedule_check_waitfor;

		waitfor _all_
		%util_loop_data( 
			 mpLoopMacro       =  m_schedule_check_waitfor,
			 mpData            =  open_res);
			
%mend tech_main_schedule;
