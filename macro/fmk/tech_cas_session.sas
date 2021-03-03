/***************************************************************************
* Наименование:      
*   %tech_cas_session
*
* Назначение:
*   Скрипт для запуска и остановки сессий CAS
*
* Входные параметры:
*	mpCasSessNm=				+ Наименование CAS-сессии
*	mpAssignFlg=				+ Флаг о подключении caslib (y/n)
*	mpMode=						+ Статус сессии: запущен (start) или завершён (end)
*	mpAuthinfoUsr=				+ Юзер, через которого будет выполнена аутентификация для CAS
* Выходные параметры:
*   нет
*
* Глобальные макропеременные:
*
* Пример вызова: 
*	tech_cas_session(mpMode = start
*						,mpCasSessNm = casauto
*						,mpAssignFlg= y
*						,mpAuthinfoUsr=&SYSUSERID.
*						);
*	);
*
* Версия:
*   1	-	13.01.2021	- Alexey Samsonov, Initial version
*   2	-	14.02.2021	- rusnib, форматирование кода, добавление mpAuthinfoUsr
***************************************************************************/

%macro tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=
						);

	%local lmvMode 
			lmvAssignFlg
			lmvCasSessName
			lmvAuthinfoUsr
	;
	
	%let lmvMode = %upcase(&mpMode.);
	%let lmvAssignFlg = %upcase(&mpAssignFlg.);
	%let lmvCasSessName = %upcase(&mpCasSessNm.);
	%let lmvAuthinfoUsr = %lowcase(&mpAuthinfoUsr.);
	%if &lmvMode. = START %then %do;
		%if %sysfunc(SESSFOUND(&lmvCasSessName)) = 0 %then %do; 
			cas &lmvCasSessName. 
			%if %length(&lmvAuthinfoUsr.) gt 0 %then %do; authinfo="/home/&lmvAuthinfoUsr./.authinfo_cas" %end;
			sessopts=(metrics=true) ;
			
			%if &lmvAssignFlg. = Y %then %do;
				caslib _ALL_ ASSIGN SESSREF=&lmvCasSessName.;
			%end;
		%end;
	%end;
	%if &lmvMode. = END %then %do;
		cas &lmvCasSessName. terminate;
	%end;

%mend tech_cas_session;