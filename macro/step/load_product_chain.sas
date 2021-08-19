%macro load_product_chain(mpOutput = mn_dict.product_chain);
	%tech_cas_session(mpMode = start
							,mpCasSessNm = casauto
							,mpAssignFlg= y
							,mpAuthinfoUsr=
							);
							
	%local 
			lmvOutLibref
			lmvOutTabName
			;
			
	%member_names (mpTable=&mpOutput, mpLibrefNameKey=lmvOutLibref, mpMemberNameKey=lmvOutTabName);
	
	proc casutil;
	  droptable casdata="&lmvOutTabName." incaslib="&lmvOutLibref." quiet;
	run;
	quit;
	
	proc sql noprint;
		create table ia_assort_matrix_history_date
		as select 
		PBO_LOCATION_ID, PRODUCT_ID, 
		datepart(start_dt) as start_dt format=date9.,
		datepart(end_dt) as end_dt format=date9.
		from ia.ia_assort_matrix_history
		;
	run;

	proc sort data=work.ia_assort_matrix_history_date out= work.ia_assort_matrix_history_sorted;
		by PBO_LOCATION_ID PRODUCT_ID start_dt end_dt ;
	run;

	data work.gp_tmp_product_chain_step1;
	  set work.ia_assort_matrix_history_sorted;
	  by PBO_LOCATION_ID PRODUCT_ID;
	  length show_prev_end_Date flag buff_per 8;
	  format show_prev_end_Date new_buffer_variable date9.;
	  retain new_buffer_variable buff_per;
	  /* Начало новый группы тт-скю */
	  if  first.PRODUCT_ID then do; /* first.location and */
		/*сохраняем первый элемент группы в буфер */
		new_buffer_variable = .;
		buff_per = 1;
	  end;
	  /* Проверка на нахождении внутри группы для начала алгоритма*/
	  if new_buffer_variable ne . then do;
		buff_per = buff_per + 1;
		if (start_dt - new_buffer_variable <=14) and (start_dt - new_buffer_variable >= 0) then do;
		  flag=1;
		end;
	  end;
	  /* Сохраняем пред. значение конца в новую переменную*/
	  show_prev_end_Date = new_buffer_variable;
	  /* Сбрасываем пред. значение в буфер */
	  new_buffer_variable = end_dt;
	run;

	proc sort data=work.gp_tmp_product_chain_step1 out= work.gp_tmp_product_chain_step2;
		by descending PBO_LOCATION_ID descending PRODUCT_ID descending buff_per ;
	run;

	data work.gp_tmp_product_chain_step3; 
	  set work.gp_tmp_product_chain_step2;
	  by descending PBO_LOCATION_ID
		 descending PRODUCT_ID
		 descending buff_per;
	  retain buff_next_flag;
	  
	  if first.PRODUCT_ID then do;
		next_flag = .;
		buff_next_flag = flag;
	  end;
	  else do;
	  next_flag = buff_next_flag;
	  buff_next_flag = flag;
	  end;
	run;

	proc sort data=work.gp_tmp_product_chain_step3 out= work.gp_tmp_product_chain_step4;
		by  PBO_LOCATION_ID  PRODUCT_ID  buff_per ;
	run;

	proc sql noprint;
		create table work.gp_tmp_product_chain_step5 as
			select 
					t1.*,
					t2.cnt_rows
			from  work.gp_tmp_product_chain_step4 t1
				left join ( select
					a.PBO_LOCATION_ID, a.PRODUCT_ID, max(a.buff_per) as cnt_rows
					from work.gp_tmp_product_chain_step4 a
					group by a.PBO_LOCATION_ID, a.PRODUCT_ID ) t2
				on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and t1.PRODUCT_ID = t2.PRODUCT_ID
		;
	quit;

	proc sql noprint;
		create table work.gp_tmp_product_chain_step6 as
			select 
				a.PBO_LOCATION_ID,
				a.PRODUCT_ID,
				a.start_dt,
				a.end_dt,
				a.buff_per,
				a.cnt_rows,
				a.flag,
				a.next_flag,
				case when a.cnt_rows >= 2 and a.next_flag = 1 
					then 1 else a.flag 
				end as fin_flag 
			from  work.gp_tmp_product_chain_step5 a
			order by
				a.PBO_LOCATION_ID,
				a.PRODUCT_ID,
				a.start_dt,
				a.end_dt
		;
	quit;

	data work.gp_tmp_product_chain_step7;
	  set work.gp_tmp_product_chain_step6;
	  by PBO_LOCATION_ID PRODUCT_ID;
	  retain group_num buff_2;

	  if first.PRODUCT_ID then do;
		 group_num = 1;
		 buff_2 = fin_flag;
	  end;

	  else do;
			if (buff_2 = fin_flag) and buff_2 ne . then do;
				buff_2 = fin_flag;
				group_num = group_num;
			end;
			else do;
			group_num = group_num + 1;
			buff_2 = fin_flag;
			end;
	  end;
	run;

	proc sql noprint;
		create table work.gp_tmp_product_chain_step8 as
			select 
				a.PBO_LOCATION_ID
				, a.PRODUCT_ID
				, a.group_num
				, min(a.start_dt) as group_start_dt format=date9.
				, max(a.end_dt) as group_end_dt format=date9.
			from work.gp_tmp_product_chain_step7 a
			group by a.PBO_LOCATION_ID
					, a.PRODUCT_ID
					, a.group_num
			order by a.PBO_LOCATION_ID
					, a.PRODUCT_ID
					, a.group_num
		;
	quit;

	proc sql noprint;
		create table work.gp_tmp_product_chain_fin as
			select 
				'N' as lifecycle_cd
				,a.PRODUCT_ID as predecessor_product_id
				,a.PBO_LOCATION_ID as successor_dim2_id
				,a.PRODUCT_ID as successor_product_id
				,a.PBO_LOCATION_ID as predecessor_dim2_id
				,a.group_start_dt as successor_start_dt
				,a.group_end_dt as predecessor_end_dt
				,100 as scale_factor_pct
		from work.gp_tmp_product_chain_step8 a
		order by a.PBO_LOCATION_ID
				, a.PRODUCT_ID
				, a.group_num
		;
	quit;

	data &lmvOutLibref..&lmvOutTabName.;
		set work.gp_tmp_product_chain_fin;
	run;
	
	/*
	proc casutil; 
	  promote casdata="gp_tmp_product_chain_fin" casout="&lmvOutTabName." incaslib="casuser" outcaslib="&lmvOutLibref.";
	run;
	*/
	
%mend load_product_chain;