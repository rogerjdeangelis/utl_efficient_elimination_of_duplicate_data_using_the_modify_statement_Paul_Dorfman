Efficient elimination of duplicate data using the modify statement Paul Dorfman

see
https://tinyurl.com/yaoczzrm
https://github.com/rogerjdeangelis/utl_efficient_elimination_of_duplicate_data_using_the_modify_statement_Paul_Dorfman

Very usefull Paper

Paul M. Dorfman Independent Consultant, Jacksonville, FL
SUG Denver 2018
Paper 2426-2018
Efficient Elimination of Duplicate Data Using the MODIFY Statement


BENCHMARKS
==========

    Remove 103,320 duplicates from  5,000,000 observations with 145 variables

    1.  155 seconds PROC SORT NODUPKEY
    2.   37 seconds SAS using Paul's Method
    3.   43 seconds WPS using Paul's Method (probably less cacheing- I ran have code separately?)

HAVE
====

WORK.HAVE  5,000,001 observations with 145 variables

Keep only the most recent date in each duplicate group

          Looking for Duplicate Keys   Want most Recent    140 Satelite Fact Variables
         ===========================   ================
    Obs    KEY1    KEY2       KEY3         DATE            X1 .. X70    Y1     Y70

DUP   1    3534    53440    3FCF35344    07AUG1995         75 ..  14    $74    $99   * remove this one;
DUP   2    3534    53440    3FCF35344    08AUG1995         61 ..  91    $88    $31   * keep this one. most recent

      3    E7D7    7D74E    3FB6E7D74    09AUG1995         94 ..  21    $7     $24
      4    863E    63EED    3FD8863EE    11AUG1995         80 ..  99    $32    $27
      5    11D5    1D5D4    3FB911D5D    13AUG1995         51 ..  29    $39    $96

EXAMPLE OUTPUT ( record 1 is removed)

                  KEYS
           =======================       =========
    Obs    KEY1    KEY2       KEY3         DATE            X1 .. X70    Y1     Y70

      2    3534    53440    3FCF35344    08AUG1995         61 ..  91    $88    $31   * Only

      3    E7D7    7D74E    3FB6E7D74    09AUG1995         94 ..  21    $7     $24
      4    863E    63EED    3FD8863EE    11AUG1995         80 ..  99    $32    $27
      5    11D5    1D5D4    3FB911D5D    13AUG1995         51 ..  29    $39    $96


PROCESS (all the code)
======================

* add a record. RID  will be used later with sas 'point=rid' option to remove the dups;
data havRid / view=havRid ;
set have (keep=key1 key2 key3 Date) ;  * keep just keys and data, drop satelite variables;
rid = _n_ ;
run;quit;

* sort so we can locate the dups using 'not last.key3';
proc sort data=havRid out=havRidSrt equals;
by key1 key2 key3 /* date are in order on the input */ ;
run;quit;

/*
NOTE: The data set WORK.HAVRIDSRT has 5000000 observations and 5 variables.
NOTE: PROCEDURE SORT used (Total process time):
      real time           34.18 seconds
*/

* mark the last of a dup group for deletion;;
data havDup (keep=rid) ;
  set havRidSrt ;
  by key1 key2 key3 ;
  if not last.key3 ;  * if not last we have dups keep the 10,320 RIDs;
run;quit;

/*
NOTE: There were 5000000 observations read from the data set WORK.HAVRIDSRT.
NOTE: The data set WORK.HAVDUP has 10320 observations and 1 variables.
NOTE: DATA statement used (Total process time):
      real time           1.86 seconds
      user cpu time       0.93 seconds
*/


* put in order for fast 'point=rid processing - we don't want to do scattered reads;
proc sort data=havDup out=havDupRid noequals;
by rid ;
run ;

/*

NOTE: The data set WORK.HAVDUPRID has 10320 observations and 1 variables.
NOTE: PROCEDURE SORT used (Total process time):
      real time           0.02 seconds
*/

* point to the dups and remove them;
data have ;
  set havDupRid ;
  modify have point=rid ;
  remove ;
run;quit;

/*
NOTE: The data set WORK.HAVE has been updated.   10320 observations deleted.
NOTE: DATA statement used (Total process time):
      real time           1.42 seconds
*/

OUTPUT
======


*                _              _       _
 _ __ ___   __ _| | _____    __| | __ _| |_ __ _
| '_ ` _ \ / _` | |/ / _ \  / _` |/ _` | __/ _` |
| | | | | | (_| |   <  __/ | (_| | (_| | || (_| |
|_| |_| |_|\__,_|_|\_\___|  \__,_|\__,_|\__\__,_|

;

* probably need a power workstation (any post 2007 laptop or desktop);

data have;
  call streaminit(1234);
  length key1 $4  key2 $5 key3 $9;
  retain date;
  array x[70] x1-x70 (70*1);
  array y[70] $8 y1-y70 (70*'12345678');

  do xx=1 to 5000000;* 20000000;
    hex=put(uniform(1234),hex16.);
    key1=substr(hex,5,4);
    key2=substr(hex,6,5);
    key3=substr(hex,1,9);
    date=2*mod(xx,1000) + 13000;
    do yy=1 to 70;
       x[yy]=int(100*rand('uniform'));
       y[yy]=cats('$',int(100*rand('uniform')));
    end;
    output;
    if xx=1 then do;
        date=date+1;
        do zz=1 to 70;
          x[zz]=int(100*rand('uniform'));
          y[zz]=cats('$',int(100*rand('uniform')));
        end;
        output;
    end;
  end;
  drop xx ;
run;quit;

NOTE: The data set WORK.HAVE has 5000001 observations and 147 variables.
NOTE: DATA statement used (Total process time):
      real time           1:31.26
      cpu time            1:31.01


proc print data=have(obs=5) width=min;
format date date9.;
var key1 key2 key3 date x1 x70 y1 y70;
run;quit;

proc sort data=have out=havSrt equals nodupkey;
by key1 key2 key3;
run;quit;

*          _       _   _
 ___  ___ | |_   _| |_(_) ___  _ __
/ __|/ _ \| | | | | __| |/ _ \| '_ \
\__ \ (_) | | |_| | |_| | (_) | | | |
|___/\___/|_|\__,_|\__|_|\___/|_| |_|

;

* SAS;

%utl_submit_wps64('
data have;
  call streaminit(1234);
  length key1 $4  key2 $5 key3 $9;
  retain date;
  array x[70] x1-x70 (70*1);
  array y[70] $8 y1-y70 (70*"12345678");

  do xx=1 to 5000000;* 20000000;
    hex=put(uniform(1234),hex16.);
    key1=substr(hex,5,4);
    key2=substr(hex,6,5);
    key3=substr(hex,1,9);
    date=2*mod(xx,1000) + 13000;
    do yy=1 to 70;
       x[yy]=int(100*rand("uniform"));
       y[yy]=cats("$",int(100*rand("uniform")));
    end;
    output;
    if xx=1 then do;
        date=date+1;
        do zz=1 to 70;
          x[zz]=int(100*rand("uniform"));
          y[zz]=cats("$",int(100*rand("uniform")));
        end;
        output;
    end;
  end;
  drop xx ;
run;quit;

data havRid / view=havRid ;
set have (keep=key1 key2 key3 Date) ;  * keep just keys and data, drop satelite variables;
rid = _n_ ;
run;quit;

proc sort data=havRid out=havRidSrt equals;
by key1 key2 key3 ; * alreade in date order  ;
run;quit;

data havDup (keep=rid) ;
  set havRidSrt ;
  by key1 key2 key3 ;
  if not last.key3 ;  * if not last we have dups keep the 10,320 RIDs;
run;quit;

proc sort data=havDup out=havDupRid noequals;
by rid ;
run ;

data have ;
  set havDupRid ;
  modify have point=rid ;
  remove ;
run;quit;
');

