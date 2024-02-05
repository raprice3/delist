%* This macro creates a dataset that contains each delisting return;
%* For delistings with missing delisting returns, a replacement value is computed;
%* as the average delisting return of similar delistings (i.e., same three-digit delisting code);
%* This macro is used in the portret macro;

%* The output file is named "dlret" in the work directory by default;
%* If a library/filename is provided, then you can save this dataset for multiple executions of;
%* the portret macro, e.g. out=st1.dlret;

%* freq is the frequency of return data, either daily (freq=d) or monthly (freq=m), corresponding;
%* to the daily and monthly stock event files (dse, mse) that contain delisting information;

%* Note that whenever a delisting occurs, there is a record (may be;
%* missing return) in the  msf/dsf file.  If not so, some delisting;
%* returns would not merge, and we would have to create a separate;
%* file of delistings that did not merge, and stack the datasets;
%* together. Fortunately this is not necessary.;

%* The next proc sql statement shows this;
%* proc sql;
%*     select d.*, r.dlret as dlretret, r.ret, r.date;
%*         from dlret d left join ret r;
%*         on d.permno=r.permno and month(d.dldate)=month(r.date);
%*         where year(d.dldate)=1991;


%macro dlret_rv(out=work.dlret,freq=m,weeklib=);
    
proc sql;
    %* replacement values for missing delisting returns;
    create table rv as
    select dlstcd, avg(dlret) as rv, prt(dlret) as pvalue
    from crsp.dse
    where dlret not in (. .S) and dlstcd ne . and dlstcd > 199
    group by dlstcd;

    %* must use dse, not mse, because mse contains partial month returns;
    %* while dse only contains delisting returns;

    %* Note that there are some dlstcds that do not have any delisting;
    %* returns.  For example, 470 and 480 have either missing dlret or;
    %* only partial monthly returns.;
    %* In this code, i leave observations with missing dlret and no valid;
    %* rv missing.;

    %* Also note that code 550 has a distribution where 10% of delistings;
    %* are -1 return, many are 0 return and some are positive.  Average and;
    %* median are 0 (or insignificant);
    %* However, there are several very large positive returns, around 1200%;
    %* I choose not to winsorize these (Positive returns), and just use 0 as;
    %* the replacement value since it is the mean and median;
quit;

%* set replacement value to 0 if rv is insignificant at 10 pct level;
data rv;
    set rv;
    if pvalue > 0.1 then rv=0;
run;

%if "&freq"="w" %then %do;
    proc sql;
	%* select all delistings from the event file;
	create table dlret as
	    select permno, date, dlpdt, dlstcd, dlret
	    from &weeklib..&freq.se
	    where dlstcd > 199;

    %end;
    %else %do;

    proc sql;
	%* select all delistings from the event file;
	create table dlret as
	    select permno, date, dlpdt, dlstcd, dlret
	    from crsp.&freq.se
	    where dlstcd > 199;
    %end;


proc sql;   
    %* merge the replacement value with the dlret file;
    create table dlret as
    select d.*, r.rv 
    from dlret d left join rv r
    on d.dlstcd = r.dlstcd;
quit;

%* If the monthly delisting return is a partial month return then;
%* compound the rv with pmr.  If monthly delisting return is missing,;
%* set dlret=rv;

data dlret;
    set dlret;
    %* partial month returns exclude delistings, identified by dlpdt le date, include rv;
    if (not missing(dlpdt) and dlpdt le date) and not missing(dlret) and not missing(rv) then dlret2 = (1+dlret)*(1+rv)-1;

    %* in some cases rv is missing but dlret is not (contains partial month return);
    %* so use partial month return and assume dlret=0;
    else if (not missing(dlpdt) and dlpdt le date) and not missing(dlret) and missing(rv) then dlret2 = dlret;

    else if (dlpdt > date) and (missing(dlret)) then dlret2=rv;

    %*there are a fair number of delistings with missing dlpdt;
    else if missing(dlpdt) and missing(dlret) then dlret2=rv;

    %* There are some missing dlret with missing code .T (not trading on exchange);
    %* and .P (price not available for 10 days);
    %* It is most likely that these wont merge anyway, but I assign these to rv;
    else if dlret in (.T .P) then dlret2=rv;

    else dlret2=dlret;
    %* This takes care of most delistings.  There are still a few with missing dlret;
    %* and rv is missing.  These will effectively be excluded from the sample;

run;

data &out;
    set dlret;
run;

%mend;
