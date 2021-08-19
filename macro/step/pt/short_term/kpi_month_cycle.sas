cas casauto;
caslib _all_ assign;

%include "/opt/sas/mcd_config/macro/step/pt/short_term/mCycleAccuracyAnalysis.sas";

%mCycleAccuracyAnalysis(
	  lmvInputTable = MAX_CASL.GC_FCST_VS_ACT_DEC
	  , lmvKPI = GC
	  , lmvOutTablePostfix = DEC
	);
	
	
data CASUSER.KPI_MONTH_CYCLE_DEC (promote=yes);
set WORK.KPI_MONTH_CYCLE_DEC;
run;

proc sql;
create table WORK.KPI_MONTH_CYCLE as
select 
	  pbo_location_id
	, BIAS_SAS
	, sum_gc_act
	, sum_gc_sas_fcst
	, sum_gc_sas_err
	, PBO_LOCATION_NM
	, LVL3_NM
	, A_OPEN_DATE
	, A_CLOSE_DATE
	, A_BUILDING_TYPE
from WORK.KPI_MONTH_CYCLE_DEC
order by KPI_MONTH_CYCLE_DEC desc
;
quit;

