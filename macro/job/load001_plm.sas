%macro load001_plm;
	%tech_log_event(mpMode=START, mpProcess_Nm=load_plm);
	
	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=load_plm);
					
	%load_plm(mpOutput = mn_dict.product_chain);
	
	%tech_update_resource_status(mpStatus=L, mpResource=load_plm);

	%tech_log_event(mpMode=END, mpProcess_Nm=load_plm);
%mend load001_plm;