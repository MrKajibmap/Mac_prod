/* %tech_init_config_files(mpSchema=etl_cfg); */

%macro tech_init_config_files(mpSchema=);

	%local
			lmvSchema
	;
	
	%let lmvSchema = &mpSchema.;
	
	proc sql noprint;
		connect using &lmvSchema.;
		execute by &lmvSchema. (
			truncate &lmvSchema..cfg_resource;
			truncate &lmvSchema..cfg_resource_type_load;
			truncate &lmvSchema..cfg_schedule_rule;
		);
	quit;
	
	data work.imp_cfg_resource; 
	   infile "&ETL_FILE_STATIC_ROOT/etl_cfg/SF_CFG_RESOURCE.csv"
			  lrecl = 256
			  delimiter = ';'
			  dsd
			  missover
			  firstobs = 2
			  encoding = "utf8"; 
	   ; 
	   attrib resource_id length = 8;
		  
	   attrib resource_nm length = $40
		  format = $40.
		  informat = $40.;  
		  
	   attrib macro_nm length = $200
		  format = $200.
		  informat = $200.; 
	   attrib module_nm length = $32
		  format = $32.
		  informat = $32.; 
	   attrib forced_load_flag length = 8;
	   
	   input resource_id resource_nm macro_nm module_nm forced_load_flag; 
	run; 
	
	proc sql;
		insert into &lmvSchema..cfg_resource
		select * from work.imp_cfg_resource
		;
	quit;
	
	data work.imp_cfg_resource_type_load; 
	   infile "&ETL_FILE_STATIC_ROOT/etl_cfg/SF_CFG_RESOURCE_TYPE_LOAD.csv"
			  lrecl = 256
			  delimiter = ';'
			  dsd
			  missover
			  firstobs = 2
			  encoding = "utf8"; 
	   ; 
	   attrib resource_nm length = $40
		  format = $40.
		  informat = $40.;  
		  
		  
	   attrib resource_type_load length = $32
		  format = $32.
		  informat = $32.;  
		  
	   attrib field_time_frame length = $32
		  format = $32.
		  informat = $32.; 
	   attrib time_frame_value length = 8;
	   
	   input resource_nm resource_type_load field_time_frame time_frame_value; 
	  
	run; 
	
	proc sql;
		insert into &lmvSchema..cfg_resource_type_load
		select * from work.imp_cfg_resource_type_load
		;
	quit;
	
	data work.imp_cfg_schedule_rule; 
	   infile "&ETL_FILE_STATIC_ROOT/etl_cfg/SF_CFG_SCHEDULE_RULE.csv"
			  lrecl = 256
			  delimiter = ';'
			  dsd
			  missover
			  firstobs = 2
			  encoding = "utf8"; 
	   ; 
	   attrib rule_id length = 8;
		  
	   attrib rule_nm length = $40
		  format = $40.
		  informat = $40.;  
		  
	   attrib rule_desc length = $256
		  format = $256.
		  informat = $256.; 
	   attrib rule_cond length = $1000
		  format = $1000.
		  informat = $1000.; 
	   attrib rule_start_hour length = $8
		  format = $8.
		  informat = $8.; 
	   
	   input rule_id rule_nm rule_desc rule_cond rule_start_hour; 
	   
	run; 
	
	proc sql;
		insert into &lmvSchema..cfg_schedule_rule
		select * from work.imp_cfg_schedule_rule
		;
	quit;
	
%mend tech_init_config_files;