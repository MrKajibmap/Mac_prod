%macro price_unfold;
	%tech_cas_session(mpMode = start
                        ,mpCasSessNm = casauto
                        ,mpAssignFlg= y);

	/* Подготовка регулярных цен в разрезе SKU-ПБО-день */
	data casuser.price_reg_past;
		set MN_DICT.PRICE_REGULAR_PAST;
		retain _past 1;
		if start_dt ne . and end_dt ne . then
		do period_dt=max(start_dt,&VF_HIST_START_DT_SAS) to min(end_dt,&VF_FC_AGG_END_DT_SAS);
		output;
		end;
	run;
	data casuser.price_reg_future;
		set MN_DICT.PRICE_REGULAR_FUTURE;
		retain _past 0;
		if start_dt ne . and end_dt ne . then
		do period_dt=max(start_dt,&VF_HIST_START_DT_SAS) to min(end_dt,&VF_FC_AGG_END_DT_SAS);
		output;
		end;
	run;

	data casuser.prices_flat1;
		format period_dt date9. product_id pbo_location_id 32.;
		set casuser.price_reg_past casuser.price_reg_future;
		by product_id pbo_location_id period_dt _past;
		if first.period_dt then output;
	run;

	proc casutil;
		droptable casdata="price_reg_past" incaslib="casuser" quiet;
		droptable casdata="price_reg_future" incaslib="casuser" quiet;
	run;

	/* Подготовка промо-цен в разрезе ID_промо-SKU-ПБО-день */
	data casuser.price_promo_past;
		set MN_DICT.PRICE_promo_past;
		drop channel_cd;
		retain _past 1;
		if start_dt ne . and end_dt ne . and upcase(channel_cd)='ALL' then
		do period_dt=max(start_dt,&VF_HIST_START_DT_SAS) to min(end_dt,&VF_FC_AGG_END_DT_SAS);
		output;
		end;
	run;

	data casuser.price_promo_future;
		set MN_DICT.PRICE_promo_future;
		drop channel_cd;
		retain _past 0;
		if start_dt ne . and end_dt ne . and upcase(channel_cd)='ALL' then
		do period_dt=max(start_dt,&VF_HIST_START_DT_SAS) to min(end_dt,&VF_FC_AGG_END_DT_SAS);
		output;
		end;
	run;

	data casuser.prices_flat2;
		format period_dt date9. product_id pbo_location_id 32.;
		set casuser.price_promo_past casuser.price_promo_future;
		by product_id pbo_location_id promo_id period_dt _past;
		if first.period_dt then output;
	run;

	proc casutil;
		droptable casdata="PRICE_promo_past" incaslib="casuser" quiet;
		droptable casdata="price_promo_future" incaslib="casuser" quiet;
	run;

	/* Агрегация промо-цен до SKU-ПБО-день,
		то есть устранение разреза ID_промо */
	proc fedsql sessref=casauto;
		create table casuser.prices_flat2_nopromo{options replace=true} as
		select period_dt, product_id, pbo_location_id,
			avg(GROSS_PRICE_AMT) as A_GROSS_PRICE_AMT,
			avg(NET_PRICE_AMT) as A_NET_PRICE_AMT,
			min(GROSS_PRICE_AMT) as M_GROSS_PRICE_AMT,
			min(NET_PRICE_AMT) as M_NET_PRICE_AMT,
			count(*) as promo_ct
		from casuser.prices_flat2
		group by 1,2,3
	;
	quit;

	proc casutil;
		droptable casdata="PRICES_flat2" incaslib="casuser" quiet;
	run;

	/* Объединение промо- и регулярных цен, расчет скидок */
	data casuser.price_full_sku_pbo_day;
		merge 
			casuser.prices_flat1 (
				rename=( 
					net_price_amt = price_reg_net 			
					gross_price_amt = price_reg_gross
					)
				) 
			casuser.prices_flat2_nopromo (
				rename=( 
					M_GROSS_PRICE_AMT = price_promo_gross 	
					M_NET_PRICE_AMT = price_promo_net
					)
				)
			;
		by 
			product_id 
			pbo_location_id 
			period_dt
			;
		keep 
			product_id 
			pbo_location_id 
			period_dt 
			price_reg_gross 
			price_promo_gross
			discount_gross_rur 
			discount_gross_pct 
			price_reg_net
			price_promo_net
			discount_net_rur 
			discount_net_pct 
			promo_ct 
			price_gross
			price_net
			;
		/* GROSS-prices */
		if price_promo_gross>0 then do;
			price_gross			= price_promo_gross ;
			discount_gross_rur	= max(0, price_reg_gross - price_promo_gross);
			discount_gross_pct 	= divide(discount_gross_rur, price_reg_gross);
		end;
		else do;
			price_gross			= price_reg_gross;
			discount_gross_rur	= 0;
			discount_gross_pct	= 0;
		end;
		/* NET-prices */
		if price_promo_net>0 then do;
			price_net			= price_promo_net ;
			discount_net_rur	= max(0, price_reg_net - price_promo_net);
			discount_net_pct	= divide(discount_net_rur, price_reg_net);
		end;
		else do;
			price_net			= price_reg_net;
			discount_net_rur	= 0;
			discount_net_pct	= 0;
		end;
		promo_ct = coalesce(promo_ct,0);
	run;

	proc casutil;
		droptable casdata="price_full_sku_pbo_day" incaslib="mn_dict" quiet;
		promote casdata="price_full_sku_pbo_day" incaslib="casuser" casout="price_full_sku_pbo_day" outcaslib="mn_dict";
		save incaslib="mn_dict" outcaslib="mn_dict" casdata="price_full_sku_pbo_day" casout="price_full_sku_pbo_day.sashdat" replace; 
		droptable casdata="price_full_sku_pbo_day" incaslib="mn_dict" quiet;
	run;
%mend price_unfold;