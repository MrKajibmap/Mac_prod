/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для выгрузки данных из DP
*
*  ПАРАМЕТРЫ:
*     mpPlanAreaNm -		имя области планирования
*	
******************************************************************
*  Использует: 
*	  нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*    %export_pa_data(mpPlanAreaNm=GC_MONTH_Level1 Exchange Rate Set);
*
****************************************************************************
*  08-09-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro echo_file(mpFileRef);
   data _null_;
      infile &mpFileRef;
      input;
      put _infile_;
   run;
%mend echo_file;

%macro export_pa_data(mpPlanAreaNm=); 
	cas casauto sessopts=(metrics=true);
	caslib _all_ assign;
	
	proc casutil;
		droptable incaslib="CASUSERHDFS" casdata="planningAreaExtract" quiet;
	run;
	
	%global SYS_PROCHTTP_STATUS_CODE SYS_PROCHTTP_STATUS_PHRASE;
	%let SYS_PROCHTTP_STATUS_CODE=;
	%let SYS_PROCHTTP_STATUS_PHRASE=;
	%let SERVICESBASEURL=10.252.151.9/;
	
	filename resp TEMP;
	/* Извлечение списка всех доступных planningAreaName*/
	/*
	proc http 
		url="&lmvAPI_URL/planning/planningAreas/"
		method='get'
		oauth_bearer = sas_services
		out = resp;
		headers 
		'Accept' = 'application/vnd.sas.collection+json';
	run;
	
	libname respjson JSON fileref=resp;
	data _NULL_;
	infile resp;
	input;
	putlog _INFILE_;
	run;
	*/
	/*
    proc http
      method="POST"
      OAUTH_BEARER=SAS_SERVICES
      url="&SERVICESBASEURL.retailAnalytics/dataExtracts/jobs"
	  in=
			"{
			  ""version"": 0,
			  ""name"": ""string"",
			  ""tasks"": [
			    {
			      ""dataExtractName"": ""planningScope"",
			      ""userDefinedExtractName"": ""planningAreaExtract"",
			      ""format"": ""SASDATASET"",
			      ""locationType"": ""CASLIB"",
			      ""location"": ""null"",
			      ""parameters"": {
			        ""planningAreaName"": ""&mpPlanAreaNm""
			      },
			      ""version"": 1
			    }
			  ]
			}"
      out=resp;
      headers
      "Accept"="application/vnd.sas.retail.data.extract.job.detail+json"
      "Content-Type"="application/vnd.sas.retail.data.extract.job.detail+json";
    run;
		
		  proc http
      method="POST"
      OAUTH_BEARER=SAS_SERVICES
      url="&SERVICESBASEURL.retailAnalytics/dataExtracts/jobs"
	  in=
			"{
			  ""version"": 1,
			  ""name"": ""string"",
			  ""tasks"": [
			    {
			      ""dataExtractName"": ""planningScope"",
			      ""userDefinedExtractName"": ""planningAreaExtract"",
			      ""format"": ""SASDATASET"",
			      ""locationType"": ""CASLIB"",
			      ""location"": ""null"",
			      ""parameters"": {
			        ""planningAreaName"": ""&mpPlanAreaNm""
			      },
			      ""version"": 1
			    }
			  ]
			}"
      out=resp;
      headers
      "Accept"="application/vnd.sas.retail.data.extract.job.detail+json"
      "Content-Type"="application/vnd.sas.retail.data.extract.job.detail+json";
    run;
	*/
	
	
	 proc http
      method="POST"
      OAUTH_BEARER=SAS_SERVICES
      url="&SERVICESBASEURL.retailAnalytics/dataExtracts/jobs"
	  in=
			"{
			  ""version"": 0,
			  ""name"": ""string"",
			  ""tasks"": [
			    {
			      ""dataExtractName"": ""planningScope"",
			      ""userDefinedExtractName"": ""planningAreaExtract"",
			      ""format"": ""csv"",
			      ""locationType"": ""fileSystem"",
			      ""location"": ""/data/dm_rep/"",
			      ""parameters"": {
			        ""planningAreaName"": ""&mpPlanAreaNm""
			      },
			      ""version"": 1
			    }
			  ]
			}"
      out=resp;
      headers
      "Accept"="application/vnd.sas.retail.data.extract.job.detail+json"
      "Content-Type"="application/vnd.sas.retail.data.extract.job.detail+json";
    run;
	
	/*Show status in log */
	libname respjson JSON fileref=resp;
	%put &=SYS_PROCHTTP_STATUS_CODE &=SYS_PROCHTTP_STATUS_PHRASE;
	%echo_File(resp);


	%local stateUri;
	%let stateUri=;
	  data _null_;
	    set respjson.tasks_links;
		if rel='state' then 
	   		call symput('stateUri', uri);
	  run;
	
	%local jobState;
	
	%do %until(&jobState ^= running);
	
		proc casutil;
			list TABLES incaslib='casuserhdfs' ;
		quit;

	  proc http
	    method="GET"
	    url="&SERVICESBASEURL.&stateUri"
	    out=resp
		OAUTH_BEARER=SAS_SERVICES;
      run;
	  %put Response status: &SYS_PROCHTTP_STATUS_CODE;
	
	  %echo_File(resp);
	  data _null_;
	    infile resp;
		input;
	    call symput('jobState', _infile_);
	  run;
	
	  %put jobState = &jobState;	
	
	  data _null_;
	    call sleep(10000);
	  run;
	
	%end;
	
	%if not (&jobState = completed) %then %do;
	  %put ERROR: An invalid response was received.;
	  %abort;
	%end;
%mend export_pa_data;