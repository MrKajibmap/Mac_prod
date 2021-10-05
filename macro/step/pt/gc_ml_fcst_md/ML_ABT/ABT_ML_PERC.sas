%macro ABT_ML_PERC(lib_out,tb_out);

	options nosymbolgen nomprint nomlogic;

	proc casutil;
		droptable casdata="&tb_out." incaslib="&lib_out." quiet;
		droptable casdata="&tb_out._FL" incaslib="&lib_out." quiet;
	run;

	%let lib = mn_short;
	%if not %sysfunc(exist(&lib..weather)) %then %do;
		%let lib = casuser;
		data &lib..weather;
			set ia.ia_weather;
			report_dt = datepart(report_dt);
		run;
	%end;

	proc cas;
		timeData.runTimeCode result=r /
			table = {
			name ='weather',
			caslib = "&lib.", 
			groupBy = {
				{name = 'pbo_location_id'}
			}
		},
		series = {{name='precipitation'}},
		interval='day',
		timeId = {name='report_dt'},
		trimId = "left", 
		code=
			%unquote(%str(%"))			
			%let names=; 																				
			%let minlag=35; 																			
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30;															
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
				%let window=%scan(&window_list.,&ic.); 													
				%let intnm=%rtp_namet(&window);        													
				%let intnm=%sysfunc(strip(&intnm.));
				do t = %eval(&lag+&window) to _length_; 												
					prec_&intnm._std[t]=std(%rtp_argt(precipitation,t,%eval(&lag),%eval(&lag+&window-1)));
					prec_&intnm._avg[t]=mean(%rtp_argt(precipitation,t,%eval(&lag),%eval(&lag+&window-1)));
				end;
				%let names={name=%tslit(prec_&intnm._std)}, &names;
				%let names={name=%tslit(prec_&intnm._avg)}, &names;
			%end; 																						
			
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
	
	data casuser.&tb_out.;
		set casuser.&tb_out.;
		sales_dt = report_dt;
		drop report_dt;
	run;

	proc casutil;
		promote incaslib='casuser' casdata="&tb_out." outcaslib="&lib_out." casout="&tb_out";
	run;
	
	data casuser.&tb_out._FL;
		format feature_nm $40. feature_type $10. use 2.;
		feature_nm='prec_month_avg'; feature_type = 'num'; use=1;output;
		feature_nm='prec_month_std'; feature_type = 'num'; use=1;output;
		feature_nm='prec_week_avg'; feature_type = 'num'; use=1;output;
		feature_nm='prec_week_std'; feature_type = 'num'; use=1;output;
		feature_nm='precipitation'; feature_type = 'num'; use=1;output;
	run;
	proc casutil;
		promote incaslib="casuser" casdata="&tb_out._FL" 
					outcaslib="&lib_out." casout="&tb_out._FL";
	quit;	

options symbolgen mprint mlogic;
%mend ABT_ML_PERC;