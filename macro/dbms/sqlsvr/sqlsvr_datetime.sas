/*****************************************************************
*  ������:
*     $Id: f1ed22c45e57ae7e84a14ff4bcaa1637efdcdda6 $
*
******************************************************************
*  ����������:
*     ���� ������ ����� ������������ ������ � ������ ������ ������������� �������
*     � ���� SQL Server � ����� datetime.
*     � ������ ���� (��� � � ���������) ������ ���� ������ �� ����������.
*
******************************************************************/

%macro sqlsvr_datetime(mpSASDatetime);
   %if (%is_blank(mpSASDatetime)) or (&mpSASDatetime eq .) %then %do;
      NULL
   %end;
   %else %do;
      convert(datetime, %unquote(%str(%')%sysfunc(strip(%sysfunc(putn(&mpSASDatetime, B8601DT19.3))))%str(%')), 126)
   %end;
%mend sqlsvr_datetime;