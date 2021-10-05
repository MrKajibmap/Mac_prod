%macro fmk200_load_etl_ia(mpResource=);

	%local lmvResource
			;
	
	%let lmvResource=&mpResource.;
	
	%tech_update_resource_status(mpStatus=P, mpResource=STG_&lmvResource.);
	
	/* %fmk_load_etl_ia(mpResource=&lmvResource); */
	%fmk_load_etl_ia_pg(mpResource=&lmvResource);
	
	%tech_update_resource_status(mpStatus=L, mpResource=STG_&lmvResource.);
	
	%tech_open_resource(mpResource=&lmvResource);
	%tech_open_resource(mpResource=&lmvResource._rtp);
	
%mend fmk200_load_etl_ia;