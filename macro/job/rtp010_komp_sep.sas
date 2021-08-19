/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для получения таргет таблиц с прогнозами
*	
*
*  ПАРАМЕТРЫ:
*     Нет
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
*     %rtp010_komp_sep;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp010_komp_sep;

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_log_event(mpMode=START, mpProcess_Nm=rtp_komp_sep);				
	%tech_update_resource_status(mpStatus=P, mpResource=rtp_out_integration);
	
		%rtp_komp_sep(mpInPmixLt=mn_short.plan_pmix_month,
						mpInGcLt=mn_short.plan_gc_month, 
						mpInUptLt=mn_short.plan_upt_month, 
						mpInPmixSt=mn_short.plan_pmix_day,
						mpInGcSt=mn_short.plan_gc_day, 
						mpInUptSt=mn_short.plan_upt_day, 
						mpPathOut=/data/files/output/dp_files/);
	
	
	%tech_update_resource_status(mpStatus=L, mpResource=rtp_out_integration);
	%tech_open_resource(mpResource=rtp_komp_sep);
	
	/* OPEN DP_INTEGRATION RESOURCES */
	%tech_open_resource(mpResource=LOAD_COMP_GC_MONTH_FORC);
	%tech_open_resource(mpResource=LOAD_COMP_SALE_MONTH_FORC);
	%tech_open_resource(mpResource=LOAD_COMP_UPT_MONTH_FORC);
	%tech_open_resource(mpResource=LOAD_GC_FACT_LAST_YEAR);
	%tech_open_resource(mpResource=hlvl_report);
	%tech_open_resource(mpResource=LOAD_COMP_SALE_DAY);
	%tech_open_resource(mpResource=LOAD_COMP_GC_DAY_FORECST);
	
	
	%tech_log_event(mpMode=END, mpProcess_Nm=rtp_komp_sep);	
	
%mend rtp010_komp_sep;