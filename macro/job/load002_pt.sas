%macro load002_pt;

	%tech_log_event(mpMode=START, mpProcess_Nm=load_pt);
	
	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=load_pt);
					
	%load_pt;
	
	%tech_update_resource_status(mpStatus=L, mpResource=load_pt);

	%tech_log_event(mpMode=END, mpProcess_Nm=load_pt);

%mend load002_pt;