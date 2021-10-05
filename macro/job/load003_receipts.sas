%macro load003_receipts;

	%tech_log_event(mpMode=START, mpProcess_Nm=load_receipts);
	
	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=load_receipts);
					
	%load_receipts;
	
	%tech_update_resource_status(mpStatus=L, mpResource=load_receipts);

	%tech_log_event(mpMode=END, mpProcess_Nm=load_receipts);

%mend load003_receipts;