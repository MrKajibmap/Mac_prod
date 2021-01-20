%macro postgres_timestamp (mpSASDatetime);
   %if (%is_blank(mpSASDatetime)) or (&mpSASDatetime eq .) %then %do;
      NULL
   %end;
   %else %do;
/*  ������� to_timestamp() �� ������������� � ������ � ���������, � ������� - �� */
      TIMESTAMP %unquote(%str(%')%sysfunc(putn(&mpSASDatetime, e8601dt.))%str(%'))
   %end;
%mend;