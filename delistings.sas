* This code shows how to incorporate delistings into monthly return data;
* following following Beaver, McNichols, and Price (2007, JAE);

* Note: to merge delistings with daily return data, it is relatively simple;
* because daily delisting returns do not have "partial month returns" as does;
* the monthly file.  So you do not need to use the dlpdt le dldate test to;
* know if a daily delisting return is truly missing;
* If a daily delisting return is missing, set it equal to the replacement value;
* Also, if there is a daily delisting return, the daily stock return will be;
* missing, so you just need to set ret=dlret;

************************************************************;
**** FIRST, WITH THE MACRO;

proc sql;
    create table monthlyreturns as
	select permno, date, ret
	from crsp.msf
	where year(date)=2003 and permno<12000; *an arbitrary restriction of the sample for illustration purposes;

%dlret_rv(out=dlret,freq=m);

proc sql;
    create table monthlyreturns as
        select a.*, d.dlret2, d.dlstcd
        from monthlyreturns a left join dlret d
        on a.permno=d.permno and month(a.date)=month(d.date) and year(a.date)=year(d.date)
        order by a.permno, a.date;

data monthlyreturns;
    set monthlyreturns;
    if not missing(dlret2) then do;
        if not missing(ret) then ret=(1+ret)*(1+dlret2)-1;
        else if missing(ret) then ret=dlret2;
        end;
run;

* Sample Output
/*
The MEANS Procedure

Variable    Label         N            Mean         Std Dev         Minimum         Maximum
-------------------------------------------------------------------------------------------
RET         Returns    3859       0.0418905       0.1820073      -0.9913043       5.1785717
ret_orig               3842       0.0429354       0.1800152      -0.5890411       5.1785717
dlret2                   20      -0.1684432       0.3543438      -0.9913043       0.3333333
-------------------------------------------------------------------------------------------
*/

************************************************************;
**** NEXT, WITHOUT THE MACRO (much of this code is contained in the macro;



* The dataset with monthly return data;
proc sql;
    create table monthlyreturns as
	select permno, date, ret
	from crsp.msf
	where year(date)=2003 and permno<12000; *an arbitrary restriction of the sample for illustration purposes;

* The monthly delisting dataset;
data delist;
    set crsp.mse;
    where dlstcd > 199;
    keep permno date dlstcd dlpdt dlret;
run;

* Compute replacement values for missing delisting returns;
* using daily delisting returns;
proc sql;
    create table rvtemp as
	select * from crsp.dse
	where dlstcd > 199 and 1960 le year(DATE) le 2020
	order by dlstcd;
        * modify year range as needed;

proc univariate data=rvtemp noprint;
    var dlret;
    output out=rv mean=mean_dlret probt=mean_pvalue;
    * could use median=median_dlret probm=median_pvalue;
    * if you do not like mean delisting returns as the replacement value;
    by dlstcd;
run;

* require replacement values to be statistically significant;
data rv;
    set rv;
    if mean_pvalue le 0.05 then rv = mean_dlret; * adjust p-value as desired;
    else rv = 0; * adjust as desired;
    keep dlstcd rv;
run;

* Merge replacement values with delisting returns;

proc sql;
    create table delist as
	select a.*, b.rv
	from delist a left join rv b
	on a.dlstcd = b.dlstcd;

proc sql;
    create table monthlyreturns as
	select a.*, b.dlret, b.dlstcd, b.rv, b.date as dldate, b.dlpdt
	from monthlyreturns a left join delist b
	on (a.permno = b.permno)
	and (month(a.date)= month(b.date))
	and (year(a.date) = year(b.date));
    quit;
    

data monthlyreturns;
    set monthlyreturns;
    ret_orig = ret;
    
    ** First, use replacement values where necessary;
    if not missing(dlstcd) and missing(dlret) then dlret=rv;
    * note, this will happen when the delisting occurs on the last day of the month;
    * and ret is not missing, but the delisting return is unknown;

    else if not missing(dlstcd) and dlpdt le dldate and not missing(dlret) then dlret=(1+dlret)*(1+rv)-1;
    * If delisting return is a partial month return, it is identified;
    * by CRSP by the dlpdt being set to a date less than or equal to;
    * the delisting date;
    * Could use a single replacement value as in Shumway, like -0.35. (Sloan, 1996, used -1.0);
    * would only do single replacement value for a subset of delisting codes > 499;

    ** Second, incorporate delistings into monthly return measure;
    if not missing(dlstcd) and missing(ret) then ret=dlret;
    else if not missing(dlstcd) and not missing(ret) then ret=(1+ret)*(1+dlret)-1;
run;

proc print;
run;
