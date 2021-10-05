%macro ABT_ML_PBO_CAT(lib_out, tb_out);

	proc casutil;
		droptable casdata="&tb_out." incaslib="&lib_out." quiet;
		droptable casdata="&tb_out._FL" incaslib="&lib_out." quiet;
	run;

	%if not %sysfunc(exist(casuser.pbo_dictionary_ml)) %then %do;
		data casuser.pbo_dictionary_ml;
			set mn_calc.pbo_dictionary_ml;
		run;	
	%end;

	proc fedsql sessref=casauto;
		create table casuser.pbo_loc_cat{options replace=true} as
			select 
				pbo_location_id
				,lvl3_id as pbo_loc_lvl3
				,lvl2_id as pbo_loc_lvl2
				,case when a_delivery='No' then 'No'
					else 'Yes' end as pbo_loc_delivery_cat
				,a_breakfast as pbo_loc_breakfast_cat
				,a_building_type as pbo_loc_building_cat
				,case when a_mccafe_type='No' then 'No'
					else 'Yes' end as pbo_loc_mccafe_cat
				,a_drive_thru as pbo_loc_drivethru_cat
				,a_window_type as pbo_loc_window_cat
			from casuser.pbo_dictionary_ml
		;	
	quit;

	proc casutil;
		promote incaslib='casuser' casdata="pbo_loc_cat" outcaslib="&lib_out." casout="&tb_out";
	run;


	data casuser.&tb_out._FL;
		format feature_nm $40. feature_type $10. use 2.;
		feature_nm='pbo_loc_lvl3'; feature_type = 'cat'; use=1;output;
		feature_nm='pbo_loc_lvl2'; feature_type = 'cat'; use=1;output;
		feature_nm='pbo_loc_delivery_cat'; feature_type = 'cat'; use=1;output;
		feature_nm='pbo_loc_breakfast_cat'; feature_type = 'cat'; use=1;output;
		feature_nm='pbo_loc_building_cat'; feature_type = 'cat'; use=1;output;
		feature_nm='pbo_loc_mccafe_cat'; feature_type = 'cat'; use=1;output;
		feature_nm='pbo_loc_drivethru_cat'; feature_type = 'cat'; use=1;output;
		feature_nm='pbo_loc_window_cat'; feature_type = 'cat'; use=1;output;


	run;
	proc casutil;
		promote incaslib="casuser" casdata="&tb_out._FL" 
					outcaslib="&lib_out." casout="&tb_out._FL";
	quit;
%mend;