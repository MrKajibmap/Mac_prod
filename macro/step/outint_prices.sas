
%macro outint_prices;

	data casuser.price_regular_full(replace=yes);
		set mn_dict.price_regular_past 
			mn_dict.price_regular_future(where=(year(start_dt) = year(date())));
		where start_dt ne . and end_dt ne .;
	run;

	data casuser.price_promo_full(replace=yes);
		set mn_dict.price_promo_past
			mn_dict.price_promo_future(where=(year(start_dt) = year(date())));
		where start_dt ne . and end_dt ne .;
	run;


	proc fedsql sessref=casauto noprint;
				create table casuser.price_regular_full{options replace=true} as
					select product_id
						, pbo_location_id
						, CHANNEL_CD
						,START_DT
						, END_DT
						, GROSS_PRICE_AMT
						, NET_PRICE_AMT
	from casuser.price_regular_full
	;
	quit;


	proc fedsql sessref=casauto noprint;
				create table casuser.price_promo_full{options replace=true} as
					select PROMO_ID
						,product_id
						, pbo_location_id
						, CHANNEL_CD
						,START_DT
						, END_DT
						, GROSS_PRICE_AMT
						, NET_PRICE_AMT
	from casuser.price_promo_full
	;
	quit;


	proc casutil;
			promote casdata='price_promo_full' incaslib='casuser' casout='price_promo_full'
		outcaslib='public';
			promote casdata='price_regular_full' incaslib='casuser' casout='price_regular_full'
		outcaslib='public';
	run;
	quit;

	%dp_export_csv(mpInput=public.price_promo_full, 
	mpTHREAD_CNT=1, mpPath=/data/files/output/);


	%dp_export_csv(mpInput=public.price_regular_full, 
	mpTHREAD_CNT=1, mpPath=/data/files/output/);
	
%mend outint_prices;