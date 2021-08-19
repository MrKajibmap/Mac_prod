%macro dp001_generate_facts;

	%tech_log_event(mpMode=START, mpProcess_Nm=dp_generate_facts);	

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
	%tech_update_resource_status(mpStatus=P, mpResource=dp_generate_facts);
	
	%dp_load_facts(mpFactGcMnth = mn_dict.fact_gc_month,
						mpFactPmixMnth = mn_dict.fact_pmix_month,
						mpFactUptMnth = mn_dict.fact_upt_month,
						mpPath = /data/files/output/dp_files/
						);
						
	%tech_update_resource_status(mpStatus=L, mpResource=dp_generate_facts);
	
	%tech_open_resource(mpResource=LOAD_COMP_GC_MONTH_FACT);
	%tech_open_resource(mpResource=LOAD_COMP_SALE_MONTH_FACT);
	%tech_open_resource(mpResource=LOAD_COMP_UPT_MONT_FACT);
	
	
	%tech_open_resource(mpResource=LOAD_PRODUCT_DIMENSION);
	%tech_open_resource(mpResource=LOAD_LOCATION_AGRTYPE_FULL_DIMENSION);
	
	
	%tech_log_event(mpMode=END, mpProcess_Nm=dp_generate_facts);	

%mend dp001_generate_facts;