data casuser.gc_ml_may_af_v0;
	set mn_calc.gc_ml_may_new;
run;

data casuser.fcst_gc_ml_v0;
set
	casuser.gc_ml_dec_af_v0(where=(sales_dt >= '1dec2020'd and sales_dt < '1jan2021'd))
	casuser.gc_ml_jan_af_v0(where=(sales_dt >= '1jan2021'd and sales_dt < '1feb2021'd))
	casuser.gc_ml_mar_af_v0(where=(sales_dt >= '1mar2021'd and sales_dt < '1apr2021'd))
	casuser.gc_ml_may_af_v0(where=(sales_dt >= '1may2021'd and sales_dt < '1jun2021'd))
;
run;

proc casutil;
	droptable           
		casdata		= "fcst_gc_ml_v0" 
		incaslib	= "max_casl" 
	;                 
run;

proc casutil;
/* 	promote            */
	save
		casdata		= "DM_TRAIN_TRP_GC_MP" 
		incaslib	= "CASUSER" 
		casout		= "DM_TRAIN_TRP_GC_MP"  
		outcaslib	= "max_casl"
/* 	replace */
	;                 
run;

	proc fedsql sessref=casauto;
		create table casuser.t1{options replace=true} as
			select sales_dt, sum(gc_predict) as gc_predict
			from casuser.fcst_gc_ml_v0
			group by sales_dt
		;	
	quit;

proc sgplot data=casuser.t1
/* 	(where=(sales_dt>='1may2021'd and sales_dt<'1jun2021'd)) */
	;
	series x=sales_dt y=gc_predict;
run;