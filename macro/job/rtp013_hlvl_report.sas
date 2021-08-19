%macro rtp013_hlvl_report;

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_log_event(mpMode=START, mpProcess_Nm=hlvl_report);		
	
	%tech_update_resource_status(mpStatus=P, mpResource=hlvl_report);

	
	%hlvl_report;
	
	
	%tech_update_resource_status(mpStatus=L, mpResource=hlvl_report);
	%tech_open_resource(mpResource=LOAD_HLVL_REP);
	
	%tech_log_event(mpMode=END, mpProcess_Nm=hlvl_report);	
	
%mend rtp013_hlvl_report;