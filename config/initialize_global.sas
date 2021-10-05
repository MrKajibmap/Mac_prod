/*********************************************************************************
* ВЕРСИЯ:
*   $Id$
**********************************************************************************/

/*===================================== GLOBAL ===================================*/
/* Здесь определяются глобальные переменные, общие для всех фаз                   */
/*================================================================================*/

%global ETL_DBMS;                   /* Имя СУБД, используемой для ETL (oracle, db2) */
%global ETL_DBCS;                   /* Использовать или нет Double Byte Character Set (0|1) */
%global ETL_ROOT;                   /* Корневой каталог размещения всего, связанного с ETL */
%global ETL_DATA_ROOT;              /* Путь к данным среды (корневой каталог) */
%global ETL_CDATA_ROOT;             /* Путь к общим данным (корневой каталог) */
%global ETL_LOGS;					/* Путь к логам */
%global ETL_FILE_STATIC_ROOT;       /* Путь к статическим (не меняющимся) файлам */
%global ETL_FILE_INPUT_ROOT;        /* Путь к входным файлам (корневой каталог) */
%global ETL_FILE_OUTPUT_ROOT;       /* Путь к выходным файлам (корневой каталог) */

%global ETL_SCD_FUTURE_DT;			/* Дата, которой закрываются текущие версии записей SCD в днях */
%global ETL_SCD_PAST_DT;			/* Дата начала для версий, корректируемых вручную 1 января 1960 года */
%global ETL_SCD_FUTURE_DTTM;        /* Дата, которой закрываются текущие версии записей SCD */
%global ETL_SCD_FUTURE_DTTM_DB;		/* Дата, которой закрываются текущие версии записей SCD в формате DB*/
%global ETL_CURRENT_DT;             /* Текущая дата для ETL-процессов */
%global ETL_RETRIES_COUNT;		  /* Кол-во перезапусков для упавших процессов */
%global ETL_CURRENT_DTTM;           /* Текущее дата-время для ETL-процессов */
%global ETL_CURRENT_DTTM_DB;		/* Текущее дата-время для ETL-процессов в формате DB*/

%global DEPTH_STORED_VERSIONS;		/* Глубина хранения версия в директориях etl_stg etl_ia */
%global IORC_SOK;                   /* Константа - номер ошибки _SOK */
%global IORC_DSENOM;                /* Константа - номер ошибки _DSENOM */
%global IORC_DSENMR;                /* Константа - номер ошибки _DSENMR */


%global ETL_SYS_CONNECT_OPTIONS;    /* Параметры подключения к системной схеме ETL_SYS */
%global ETL_IA_CONNECT_OPTIONS;     /* Параметры подключения к системной схеме ETL_IA */
%global ETL_STG_CONNECT_OPTIONS;    /* Параметры подключения к системной схеме ETL_STG */
%global DM_REP_CONNECT_OPTIONS;		/* Параметры подключения к системной схеме DM_REP */
%global DM_ABT_CONNECT_OPTIONS;		/* Параметры подключения к системной схеме DM_ABT */
%global IA_CONNECT_OPTIONS;         /* Параметры подключения к системной схеме IA */
%global IA_CONNECT_SCHEMA;          /* Параметры подключения к системной схеме IA */

%global ETL_PG_BULKLOAD;			/* Опции BULKLOAD (только для работы со схемами Postgresql) */
%global ETL_BULKLOAD_OPTIONS;       /* Опции BULKLOAD (только для работы со схемами ETL) */
%global ETL_RESTART_FAILED_PROCESSES;     /* Флаг для перезапуска упавших процессов выгрузки */

%global SYS_OS_FAMILY;              /* Семейство текущей ОС (WIN, UNIX) */

%global CUR_API_URL;				/* Текущий API URL */
%global RTP_START_DATE;				/* Начальная дата для отбора данных в рамках процесса расчета краткосрочного прогноза */
%global VF_START_DATE;				/* Начальная дата для отбора данных в рамках процесса расчета долгосрочного прогноза */
%global VF_FC_HORIZ;				/* Число интервалов для прогнозирования */
%global VF_FC_START_DT;				/* Дата начала прогноза (должна быть понедельником)*/
%global VF_FC_START_DT_SAS;			/* Дата начала прогноза (должна быть понедельником) в формате yymmdd10. */
%global VF_FC_START_MONTH_SAS;		/* Начало месяца, с которого начинается прогноз */
%global VF_HIST_END_DT;				/* Дата конца истории */
%global VF_FC_END_DT;				/* Понедельник последней недели, на которую должен быть прогноз */
%global VF_FC_AGG_END_DT;			/* Воскресенье последней недели, на которую должен быть прогноз */
%global VF_FC_AGG_END_DT_SAS;		/* воскресенье последней недели, на которую должен быть прогноз в формате yymmdd10.*/
%global VF_HIST_START_DT;			/* Дата начала истории */
%global VF_HIST_START_DT_SAS;		/* Дата начала истории (должна быть понедельником) в формате yymmdd10. */
%global VF_FC_END_SHORT_DT;			/* Дата конца истории краткосрочного прогноза в формате date'2020-09-17'*/
%global VF_FC_END_SHORT_DT_SAS;		/* Дата конца истории краткосрочного прогноза в формате SAS*/
%global VF_PMIX_ID;					/* ID VF-проекта, построенного на pmix_sal_abt*/
%global VF_PMIX_PROJ_NM;			/* ID VF-проекта, построенного на pmix_sal_abt*/
%global VF_PBO_ID;					/* Наименование VF-проекта, построенного на pbo_sal_abt*/
%global VF_PBO_PROJ_NM;				/* Наименование VF-проекта, построенного на pbo_sal_abt*/
%global VF_GC_NM;					/* Наименование VF-проекта для прогнозирования GC*/
%global VF_PBO_NM;					/* Наименование VF-проекта для прогнозирования PBO*/
%global SYS_ADM_USER;				/* Имя пользователя, от имени которого запускаются регламентные процессы системы */

%global RTP_TRAIN_FLG_PMIX;			/* Флаг запуска обучения моделей PMIX (Y/N)*/
%global RTP_TRAIN_FLG_MC;			/* Флаг запуска обучения моделей MC (Y/N)*/
%global RTP_PROMO_MECH_TRANSF_FILE; /* Путь до csv-файла управляющей промо-таблицы */
%global SAS_START_CMD;              /* Путь к start_sas */

%global TG_BOT_TOKEN;				/* Токен для бота телеграм */

/*===================================== GLOBAL ===================================*/
/* Здесь назначаются глобальные переменные						                  */
/*================================================================================*/

%let ETL_DBMS                       =  postgres;
%let ETL_ROOT                       =  /opt/sas/mcd_config;
%let ETL_DATA_ROOT                  =  /data;
%let ETL_LOGS						=  /data/logs;

%let ETL_FILE_STATIC_ROOT           =  &ETL_DATA_ROOT./files/static;
%let ETL_FILE_INPUT_ROOT            =  &ETL_DATA_ROOT./files/input;
%let ETL_FILE_OUTPUT_ROOT           =  &ETL_DATA_ROOT./files/output;

%let ETL_SCD_PAST_DT				=  %sysfunc(putn( '01JAN1960'd, best.));
%let ETL_SCD_FUTURE_DT            	=  %sysfunc(putn('01Jan5999'd, best.));
%let ETL_SCD_FUTURE_DTTM            =  %sysfunc(putn('01Jan5999 00:00:00'dt, best.));
%let ETL_SCD_FUTURE_DTTM_DB			=  %str(date%')01Jan5999 00:00:00%str(%');
%let ETL_CURRENT_DT                 =  %sysfunc(date());
%let ETL_RETRIES_COUNT              =  3;
%let ETL_CURRENT_DTTM               =  %sysfunc(datetime());
%let ETL_CURRENT_DTTM_DB			=  %str(%')%sysfunc(putn(%sysfunc(datepart(%sysfunc(datetime()))),yymmdd10.))%str( )%sysfunc(putn(%sysfunc(timepart(%sysfunc(datetime()))), time.))%str(%');

%let DEPTH_STORED_VERSIONS			=  5;

%let ETL_PG_BULKLOAD				=  bulkload=yes bl_default_dir="/data/pg_blk/" bl_psql_path="/usr/pgsql-11/bin/psql" BL_FORMAT=CSV BL_ESCAPE=ON BL_DELETE_DATAFILE=YES;
%let ETL_BULKLOAD_OPTIONS           =  BULKLOAD=NO BL_DEFAULT_DIR="&ETL_DATA_ROOT./sqlldr/" BL_DELETE_DATAFILE=YES;
%let ETL_RESTART_FAILED_PROCESSES   =  YES;

%let CUR_API_URL					=  10.252.151.9;

%let VF_FC_HORIZ					=  104;
%let VF_FC_START_DT 				= date%str(%')%sysfunc(putn(%sysfunc(intnx(week.2,%sysfunc(date()),0,b)),yymmdd10.))%str(%'); 
%let VF_FC_START_DT_SAS				= %sysfunc(inputn(%scan(%bquote(&VF_FC_START_DT.),2,%str(%')),yymmdd10.));
%let VF_FC_START_MONTH_SAS 			= %sysfunc(intnx(month,&VF_FC_START_DT_SAS,0,b));
%let VF_HIST_END_DT 				= %sysfunc(intnx(day,&VF_FC_START_DT_SAS,-1),yymmddd10.);	
%let VF_HIST_END_DT_SAS				= %sysfunc(inputn(&VF_HIST_END_DT.,yymmdd10.));	
%let VF_FC_END_DT 					= %sysfunc(intnx(day,&VF_FC_START_DT_sas,7*(&VF_FC_HORIZ-1)),yymmddd10.);		
%let VF_FC_AGG_END_DT 				= %sysfunc(intnx(day,&VF_FC_START_DT_sas,7*&VF_FC_HORIZ-1),yymmddd10.);
%let VF_FC_AGG_END_DT_SAS 			= %sysfunc(intnx(day,&VF_FC_START_DT_sas,7*&VF_FC_HORIZ-1));
%let VF_HIST_START_DT 				= date'2017-01-02';
%let VF_HIST_START_DT_SAS			= %sysfunc(inputn(%scan(%bquote(&VF_HIST_START_DT),2,%str(%')),yymmdd10.));
%let VF_FC_END_SHORT_DT_SAS			= %sysfunc(intnx(day, &VF_FC_START_DT_SAS., 90));
%let VF_FC_END_SHORT_DT 			= date%str(%')%sysfunc(putn(&VF_FC_END_SHORT_DT_SAS.,yymmdd10.))%str(%');
%let VF_PMIX_ID						= a55383f4-9bbb-4b48-93cd-538b7a96ead9;
%let VF_PBO_ID 						= 50a3762d-58b0-4848-9125-cf3e9df0891d;
%let VF_PMIX_PROJ_NM				= nm_abt_pmix;
%let VF_PBO_PROJ_NM					= nm_abt_pbo;
%let VF_GC_NM						= mn_gc_shortterm;
%let VF_PBO_NM						= mn_pbo_shortterm;
%let SYS_ADM_USER					= ru-pkonakov;

%let RTP_TRAIN_FLG_PMIX				= N;
%let RTP_TRAIN_FLG_MC				= N;
%let RTP_PROMO_MECH_TRANSF_FILE		= /data/files/input/PROMO_MECH_TRANSFORMATION.csv;
%let SAS_START_CMD                  =  &ETL_ROOT/config/start_sas.cmd;
%let VF_START_DATE					= %sysfunc(intnx(month,&VF_HIST_START_DT_SAS.,10,b));
%let RTP_START_DATE					= %sysfunc(intnx(year,&etl_current_dt.,-4,s)); 

%let TG_BOT_TOKEN                   = 1819897875:AAFYeGThLwMPmc81964-BdyFWOErzq0waX0;
/*===================================== GLOBAL ===================================*/
/* Здесь исполняются глобальные назначения                                        */
/*================================================================================*/
libname ETL_FMT "&ETL_ROOT/format" filelockwait=10;

options
   lrecl          =  30000
   append         =  (sasautos="&ETL_ROOT/macro/common")
   append         =  (sasautos="&ETL_ROOT/macro/dbms/oracle")
   append         =  (sasautos="&ETL_ROOT/macro/dbms/postgres")
   append         =  (sasautos="&ETL_ROOT/macro/etl")
   append         =  (sasautos="&ETL_ROOT/macro/job")
   append         =  (sasautos="&ETL_ROOT/macro/step")
   append         =  (sasautos="&ETL_ROOT/macro/fmk")
   fmtsearch      =  (FORMATS WORK ETL_FMT)
   cmplib         =  ETL_FMT.fcmp
   compress       =  binary
   /*mprint*/
   mprintnest
   mlogic
   mlogicnest
   /*symbolgen*/
   mrecall
   source
   source2
   fullstimer
   msglevel    = 'I'
   missing     = '.'
   varinitchk     =  NOTE
;

/* Настройка логирования */
%log4sas;

/* Назначение переменных, которые не должны выводиться в лог */
%log_disable;
%let ETL_SYS_CONNECT_OPTIONS        =  server="&CUR_API_URL." port=5452 user=etl_sys password="{SAS002}DCB5DA3808FAC9EE26380F5007B9E276" database=postgres defer=yes;
%let ETL_IA_CONNECT_OPTIONS         =  server="&CUR_API_URL." port=5452 user=etl_ia password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=postgres defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=32768";
%let ETL_STG_CONNECT_OPTIONS        =  server="&CUR_API_URL." port=5452 user=etl_stg password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=postgres defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=32768";
%let ETL_CFG_CONNECT_OPTIONS        =  server="&CUR_API_URL." port=5452 user=etl_cfg password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=postgres defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=32768";
%let IA_CONNECT_OPTIONS             =  DEFER=YES  PATH=WARE  USER=SAS_USER  PASSWORD="{SAS002}C4A120480F3F302F49249CD238FA3D0F" UPDATE_LOCK_TYPE=row;
%let IA_CONNECT_SCHEMA              =  sas_interf;
%log_enable;

/*==================================== LIBNAMES ==================================*/
/* Здесь исполняются назначения библиотек                                         */
/*================================================================================*/

libname etl_sys postgres &ETL_SYS_CONNECT_OPTIONS schema=etl_sys;

libname etl_ia postgres &ETL_IA_CONNECT_OPTIONS schema=etl_ia;

libname etl_stg postgres &ETL_STG_CONNECT_OPTIONS schema=etl_stg;

libname etl_cfg postgres &ETL_CFG_CONNECT_OPTIONS schema=etl_cfg;

/* libname pt postgres server="10.252.151.3" port=5452 user=pt password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=pt defer=yes schema=public readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=8192"; */

libname pt postgres server="&CUR_API_URL." port=5452 user=pt password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=pt defer=yes schema=public readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=8192"; 
libname pt_prod postgres server="&CUR_API_URL." port=5452 user=pt password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=pt defer=yes schema=public readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=8192";

LIBNAME ia ORACLE &IA_CONNECT_OPTIONS SCHEMA=&IA_CONNECT_SCHEMA.;

libname ETL_TMP "/data/ETL_TMP";
libname MN_CALC "/data/MN_CALC";