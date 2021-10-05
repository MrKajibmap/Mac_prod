%macro ABT_ML_LAGS_MEAN_AVG(lib_in,tb_in, lib_out,tb_out);
	options nosymbolgen nomprint nomlogic;

		proc casutil;
			droptable casdata="&tb_out." incaslib="&lib_out." quiet;
			droptable casdata="&tb_out._FL" incaslib="&lib_out." quiet;
		run;


	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name = "&tb_in.",
			caslib = "&lib_in.", 
			groupBy = {
				{name = 'pbo_location_id'},
				{name = 'channel_cd'}
			}
		},
		series = {{name='target'}},
		interval='day',
		timeId = {name='sales_dt'},
		trimId = "left", 
		code=
			%unquote(%str(%"))			
			%let names=; 																				
			%let minlag=35; 																			
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30 90 180 365;															
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
				%let window=%scan(&window_list.,&ic.); 													
				%let intnm=%rtp_namet(&window);        													
				%let intnm=%sysfunc(strip(&intnm.));
				do t = %eval(&lag+&window) to _length_; 												
					lag_&intnm._avg[t]=mean(%rtp_argt(target,t,%eval(&lag),%eval(&lag+&window-1)));
					lag_&intnm._med[t]=median(%rtp_argt(target,t,%eval(&lag),%eval(&lag+&window-1)));
					
				end;
				%let names={name=%tslit(lag_&intnm._avg)}, &names;
				%let names={name=%tslit(lag_&intnm._med)}, &names;
		
			%end; 		

			do t = 1 to _length_; 																				
				MD_lag_7_avg[t]=  mean(%rtp_argt(target,t,1,8));
				MD_lag_7_med[t]=median(%rtp_argt(target,t,1,8));
			end;
			%let names={name=%tslit(MD_lag_7_avg)}, &names;
			%let names={name=%tslit(MD_lag_7_med)}, &names;
			
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))  																		
		,
		arrayOut={
			table={name="&tb_out.", replace=true, caslib="casuser"},
			arrays={&names}
		}
	;
	run;
	quit;

	proc casutil;
		promote incaslib="casuser" casdata="&tb_out." 
					outcaslib="&lib_out." casout="&tb_out.";
	quit;

/* 	список добавочных полей и их тип для фореста */
	data casuser.&tb_out._FL;
		format feature_nm $40. feature_type $10. use 2.;
		feature_nm='MD_lag_7_avg'; feature_type = 'num'; use=1;output;
		feature_nm='MD_lag_7_med'; feature_type = 'num'; use=1;output;
		feature_nm='lag_halfyear_avg'; feature_type = 'num'; use=1;output;
		feature_nm='lag_halfyear_med'; feature_type = 'num'; use=1;output;
		feature_nm='lag_month_avg'; feature_type = 'num'; use=1;output;
		feature_nm='lag_month_med'; feature_type = 'num'; use=1;output;
		feature_nm='lag_qtr_avg'; feature_type = 'num'; use=1;output;
		feature_nm='lag_qtr_med'; feature_type = 'num'; use=1;output;
		feature_nm='lag_week_avg'; feature_type = 'num'; use=1;output;
		feature_nm='lag_week_med'; feature_type = 'num'; use=1;output;
		feature_nm='lag_year_avg'; feature_type = 'num'; use=1;output;
		feature_nm='lag_year_med'; feature_type = 'num'; use=1;output;

	run;
	proc casutil;
		promote incaslib="casuser" casdata="&tb_out._FL" 
					outcaslib="&lib_out." casout="&tb_out._FL";
	quit;

	options symbolgen mprint mlogic;
%mend ABT_ML_LAGS_MEAN_AVG;