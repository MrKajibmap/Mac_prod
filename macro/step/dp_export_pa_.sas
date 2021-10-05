/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для выгрузки данных из DP в указанную директорию/каслибу
*
*  ПАРАМЕТРЫ:
*     mpPlanAreaNm		область планирования
*	,mpOutTable			наименование выходной таблицы (где имя будет именем csv файла в режиме CSV)
*	,mpMode				режим работы макроса - CASLIB / CSV
*	,mpPath				путь для экспорта CSV /data/dm_rep/
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
*    dp_export_pa(mpPlanAreaNm=COMP_SALE_MONTH
*						,mpOutTable=casuser.dp_out_planArea_extr_pmix
*						,mpMode=caslib_csv
*						,mpPath = /data/dm_rep/); 
*
****************************************************************************
*  08-09-2020  Борзунов     Начальное кодирование
****************************************************************************/

%macro dp_export_pa(mpPlanAreaNm=COMP_SALE_MONTH
						,mpOutTable=casuser.dp_out_planArea_extr_pmix
						,mpMode=caslib_csv
						,mpPath = /data/dm_rep/); 
						
	cas casauto sessopts=(metrics=true);
	caslib _all_ assign;
	
	
	
	%global SYS_PROCHTTP_STATUS_CODE 
			SYS_PROCHTTP_STATUS_PHRASE
			;
	
	%let SYS_PROCHTTP_STATUS_CODE=;
	%let SYS_PROCHTTP_STATUS_PHRASE=;
	
	%local lmvApiUrl
			lmvPlanAreaNm
			lmvPlanAreaNmNonkomp
			lmvOutTable
			lmvMode
			lmvPath
			lmvOutLibrefNm
			lmvOutTabNameNm
			;
			
	%let lmvOutTable = &mpOutTable.;
	%member_names (mpTable=&lmvOutTable, mpLibrefNameKey=lmvOutLibrefNm, mpMemberNameKey=lmvOutTabNameNm); 
			
	%let lmvApiUrl = 10.252.151.3;
	%let lmvPlanAreaNm = &mpPlanAreaNm.;
	%let lmvOutTable = &mpOutTable.;
	%let lmvMode = %upcase(&mpMode.);
	%let lmvPath = &mpPath.;
	
	filename resp TEMP;
	/* Извлечение списка всех доступных planningAreaName*/
	/*
	proc http 
		url="&lmvApiUrl./planning/planningAreas/"
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
	
	%if &lmvMode. = CASLIB %then %do;
		proc casutil;
			droptable incaslib="CASUSERHDFS" casdata="&lmvOutTabNameNm." quiet;
		run;
		
		proc http
			method="POST"
			OAUTH_BEARER=SAS_SERVICES
			url="&lmvApiUrl./retailAnalytics/dataExtracts/jobs"
			in=
				"{
				  ""version"": 0,
				  ""name"": ""string"",
				  ""tasks"": [
					{
					  ""dataExtractName"": ""planningScope"",
					  ""userDefinedExtractName"": ""&lmvOutTabNameNm."",
					  ""format"": ""SASDATASET"",
					  ""locationType"": ""CASLIB"",
					  ""location"": ""null"",
					  ""parameters"": {
						""planningAreaName"": ""&lmvPlanAreaNm.""
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
	%end;
	%else %if &lmvMode. = CSV %then %do;
		proc http
			method="POST"
			OAUTH_BEARER=SAS_SERVICES
			url="&lmvApiUrl./retailAnalytics/dataExtracts/jobs"
			in=
				"{
				  ""version"": 0,
				  ""name"": ""string"",
				  ""tasks"": [
					{
					  ""dataExtractName"": ""planningScope"",
					  ""userDefinedExtractName"": ""&lmvOutTabNameNm."",
					  ""format"": ""csv"",
					  ""locationType"": ""fileSystem"",
					  ""location"": ""&lmvPath."",
					  ""parameters"": {
						""planningAreaName"": ""&lmvPlanAreaNm.""
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
	%end;
	
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
	    url="&lmvApiUrl./&stateUri"
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
	
	%if &lmvMode. = CASLIB %then %do;
		proc casutil;
			droptable casdata="&lmvOutTabNameNm." incaslib="&lmvOutLibrefNm." quiet;
		quit;

		data &lmvOutLibrefNm..&lmvOutTabNameNm.(promote=yes);
			set casuserh.&lmvOutTabNameNm.;
			Length p1 varchar(4) p2 varchar(3) p_all varchar(9) Date 8;
			format Date date9.;
			p1 = substr(Time, 1, 4);
			p2 = substr(Time, 5, 3);
			p_all = '01'||p2||p1;
			Date = input(p_all, date9.);
			drop p1 p2 p_all time;
		run;
	%end;
%mend dp_export_pa;