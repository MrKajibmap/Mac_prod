%macro rtp012_load_fact_gc_last_year;

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_log_event(mpMode=START, mpProcess_Nm=load_fact_gc_last_year);		
	
	%tech_update_resource_status(mpStatus=P, mpResource=load_fact_gc_last_year);

	
	%load_fact_gc_last_year(mpOutput=mn_dict.load_fact_gc_last_year,
							mpOutPath=/data/files/output/dp_files/)
	
	
	
	%tech_update_resource_status(mpStatus=L, mpResource=load_fact_gc_last_year);
	%tech_open_resource(mpResource=LOAD_COMP_GC_MONTH_LAST_YEAR_FACT);
	
	%tech_log_event(mpMode=END, mpProcess_Nm=load_fact_gc_last_year);	
	
%mend rtp012_load_fact_gc_last_year;