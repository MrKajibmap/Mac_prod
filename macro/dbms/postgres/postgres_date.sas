%macro postgres_date(mpSASDate);
   %if (%is_blank(mpSASDate)) or (&mpSASDate eq .) %then %do;
      NULL
   %end;
   %else %do;
/*  ������� to_date() �� ������������� � ������ � ���������, � ������� - �� */
   		DATE %unquote(%str(%')%sysfunc(putn(&mpSASDate, e8601da.))%str(%'))
   %end;
%mend;