/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для вызова процессов в DP
*
*  ПАРАМЕТРЫ:
*     mpJobName -		имя Model Process
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
*    %dp_jobexecution(mpJobName=Ruspmi Load Product Dimension);
*
****************************************************************************
*  08-09-2020  Борзунов     Начальное кодирование
****************************************************************************/

%macro dp_jobexecution(mpJobName=Ruspmi Load Product Dimension
						, mpAuth=NO
						);

	%local lmvJobName 
			lmvUrl 
			lmvJobUrl
			stateUri
			jobState
			;
			
	%let lmvUrl=&CUR_API_URL.;
	%let lmvJobName = %sysfunc(upcase(&mpJobName.));
	%let lmvAuthFlg = %sysfunc(upcase(&mpAuth.));
	/* Получение токена аутентификации */
	%if &lmvAuthFlg. = YES %then %do;
		%tech_get_token(mpUsername=&SYS_ADM_USER., mpOutToken=tmp_token);
	%end;
	/******************Get the processtemplate details*****************/
	filename jsn temp;
	proc http
		url="&lmvUrl./retailAnalytics/processModels/"
		method="GET"
		%if &lmvAuthFlg. = NO %then %do;
			out=jsn 
			OAUTH_BEARER=SAS_SERVICES ;
		%end;
		%else %do;
			out=jsn;
			headers 
				"Authorization"="bearer &tmp_token.";
		%end;
	run;

	libname posts JSON fileref=jsn ;

	proc datasets noprint;
	   copy in= posts out=work memtype=data;
	   run; 
	quit;

	proc sql noprint;
		create table process_template as 
		select a.*,b.href,b.uri
		from ITEMS as a left join ITEMS_EXECUTE as b
		on a.ordinal_items=b.ordinal_items
		;
	quit;

	/******************Get the processtemplate url*****************/
	proc sql noprint;
		select %str(href) into: lmvJobUrl
		from process_template
		where upcase(name) = "&lmvJobName";
	quit;

	%put &=lmvJobUrl;
	/* Проверка на существование DP-процесса */
	%if %length(&lmvJobUrl.) eq 0 %then %do;
		%put ERROR: Provided DP Proccess name does not exist.;
		%global SYSCC;
		%let SYSCC = 1012;
		%return;
	%end;

	/******************Execute the Process template*****************/
	filename resp TEMP;
	proc http
		url="&lmvUrl.&lmvJobUrl."
		method="POST"
		%if &lmvAuthFlg. = NO %then %do;
			OAUTH_BEARER=SAS_SERVICES 
			out=resp;
			headers
				"Content-Type" ="application/vnd.sas.retail.process.data+json";
		%end;
		%else %do;
			out=resp;
			headers 
				"Authorization"="bearer &tmp_token."
				"Content-Type" ="application/vnd.sas.retail.process.data+json";
		%end;
	run;

	libname respjson JSON fileref=resp;
	%put &=SYS_PROCHTTP_STATUS_CODE &=SYS_PROCHTTP_STATUS_PHRASE;
	%echo_File(resp);
	
	%let stateUri=;
		data _null_;
			set respjson.links;
			if rel='state' then 
				call symput('stateUri', uri);
		run;
			
	%do %until(&jobState ^= running);
			
		  proc http
			method="GET"
			url="&lmvUrl./&stateUri"
			%if &lmvAuthFlg. = NO %then %do;
				out=resp
				OAUTH_BEARER=SAS_SERVICES ;
			%end;
			%else %do;
				out=resp;
				headers 
					"Authorization"="bearer &tmp_token.";
			%end;
		  run;
		  %put Response status: &SYS_PROCHTTP_STATUS_CODE;
		
		  %echo_File(resp);
		  libname respjs1 JSON fileref=resp;
		  data _null_;
			 set respjs1.root;
			call symputx('jobState', state);
		  run;
		
		  %put jobState = &jobState;	
		
		  data _null_;
			call sleep(50000);
		  run;
	
	%end;

	%if not (&jobState = completed) %then %do;
		%put ERROR: An invalid response was received.;
		%global SYSCC;
		%let SYSCC = 1012;
		%return;
	%end;
	
%mend dp_jobexecution;