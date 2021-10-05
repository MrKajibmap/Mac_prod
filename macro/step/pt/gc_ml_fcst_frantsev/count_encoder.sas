
/* LEVENC / TARGENC */
/* PROPENCODE / COUNTENCODE / TARGETENCODE */
/**************************************************************
Макрос сначала считает количество вхождений всех переменных из &target_var_list
на уровне группировки по &count_var_list
Затем дополнительно производит агрегацию на уровень &group_by_list
с использованием функций &agg_func_list
**************************************************************/
%macro count_encoder(inp_data, 
			target_var_list,
			group_by_list,
			agg_func_list=,
			outp_data=casuser.promo_product_features);
	%local i j k var;
	option nosymbolgen mprint nomlogic notes;

	proc fedsql sessref=casauto;
		create table casuser.tmp_cnt{options replace=true} as
		select
			%unquote(&group_by_list.)
			,count(1) as total
		from &inp_data.
		group by %unquote(&group_by_list.)
		;	
	quit;

	%do k=1 %to %sysfunc(countw(&target_var_list.)); 
		%let var = %qscan(&target_var_list.,&k.,',;');
		%put &var.;
		proc fedsql sessref=casauto;
			create table casuser.tmp_cnt_enc_&var.{options replace=true} as
			select %unquote(&group_by_list.)
				,&var.
				,count(1) as cnt_&var.
			from &inp_data.
			group by %unquote(&group_by_list.), &var.
			;	
		quit;

		proc fedsql sessref=casauto;
			create table casuser.tmp_cnt_enc_&var.{options replace=true} as
			select
				%do i=1 %to %sysfunc(countw(&group_by_list.)); 
					%let by = %qscan(&group_by_list.,&i.,',;');
					%if &i. > 1 %then ,;
					t1.&by.
				%end;
				,t1.total
				%if %sysevalf(%superq(agg_func_list)^= ,boolean) %then %do;
					%do j=1 %to %sysfunc(countw(&agg_func_list.)); 
						%let agg = %qscan(&agg_func_list.,&j.,',;');
						,&agg.(cnt_&var.) as &agg._cnt_enc_&var.
						,&agg.(cast(cnt_&var. as DOUBLE)/t1.total) as &agg._freq_enc_&var.
					%end;
				%end;
				%else %do;
/* TODO: сделать транспонирование в этом случае */
					,cnt_&var. as cnt_enc_&var.
					,cast(cnt_&var. as DOUBLE)/t1.total as freq_enc_&var.
				%end;
			from casuser.tmp_cnt as t1
			inner join casuser.tmp_cnt_enc_&var. as t2
			on
				%do i=1 %to %sysfunc(countw(&group_by_list.)); 
					%let by = %qscan(&group_by_list.,&i.,',;');
					%if &i. > 1 %then and;					
					t1.&by. = t2.&by.
				%end;
				%if %sysevalf(%superq(agg_func_list)^= ,boolean) %then %do;
					group by 
						%do i=1 %to %sysfunc(countw(&group_by_list.)); 
							%let by = %qscan(&group_by_list.,&i.,',;');
							%if &i. > 1 %then ,;
							t1.&by.
						%end;
						,t1.total
				%end;
			;	
		quit;

		proc fedsql sessref=casauto;
			create table casuser.tmp_cnt{options replace=true} as
			select distinct t1.*
				%if %sysevalf(%superq(agg_func_list)^= ,boolean) %then %do;
					%do j=1 %to %sysfunc(countw(&agg_func_list.)); 
						%let agg = %qscan(&agg_func_list.,&j.,',;');
						,t2.&agg._cnt_enc_&var.
						,t2.&agg._freq_enc_&var.
					%end;
				%end;
				%else %do;
					,t2.cnt_enc_&var.
					,t2.freq_enc_&var.
				%end;
			from casuser.tmp_cnt as t1
			left join casuser.tmp_cnt_enc_&var. as t2
			on 
				%do i=1 %to %sysfunc(countw(&group_by_list.)); 
					%let by = %qscan(&group_by_list.,&i.,',;');
					%if &i. > 1 %then and;					
					t1.&by. = t2.&by.
				%end;
			;
		quit;
	%end;

	data &outp_data.;
		set casuser.tmp_cnt(drop=total);
	run;

%mend count_encoder;

/* Пример работы макроса */
/* 	%count_encoder(inp_data = casuser.promo_by_product,  */
/* 				target_var_list = %str(prod_lvl2_id), */
/* 				target_var_list = %str(product_id,prod_lvl4_id,prod_lvl3_id,prod_lvl2_id,a_offer_type),  */
/* 				group_by_list = %str(pbo_location_id,channel_cd,sales_dt), */
/* 				agg_func_list = %str(min,max,mean), */
/* 				outp_data = casuser.test); */