use fatca;

CREATE TABLE IF NOT EXISTS fatcamessage (SendingCompanyIN STRING,TransmittingCountry STRING,ReceivingCountry STRING,Contact STRING,MessageRefId STRING, ReportingPeriod STRING, Timestamp STRING)
ROW FORMAT
   SERDE 'oracle.hadoop.xquery.hive.OXMLSerDe'
STORED AS
   INPUTFORMAT 'oracle.hadoop.xquery.hive.OXMLInputFormat'
   OUTPUTFORMAT 'oracle.hadoop.xquery.hive.OXMLOutputFormat'
TBLPROPERTIES(
        "oxh-namespace.ftc" = "urn:oecd:ties:fatca:v2",
        "oxh-namespace.sfa" = "urn:oecd:ties:stffatcatypes:v2",
        "oxh-elements" = "ftc:MessageSpec",
        "oxh-column.MessageRefId" = "./sfa:MessageRefId",
        "oxh-column.SendingCompanyIN" = "./sfa:SendingCompanyIN",
        "oxh-column.TransmittingCountry" = "./sfa:TransmittingCountry",
        "oxh-column.ReceivingCountry" = "./sfa:ReceivingCountry",
        "oxh-column.Contact" = "./sfa:Contact",		
        "oxh-column.ReportingPeriod" = "./sfa:ReportingPeriod",
        "oxh-column.Timestamp" = "./sfa:Timestamp"
);
load data local inpath '/home/ibietl/mk/mkamdar/data.xml' into table fatcamessage;

CREATE TABLE IF NOT EXISTS fatcareportingInstitution (DocRefId STRING,MessageRefId STRING,ReportingPeriod STRING,ResCountryCode STRING)
ROW FORMAT
   SERDE 'oracle.hadoop.xquery.hive.OXMLSerDe'
STORED AS
   INPUTFORMAT 'oracle.hadoop.xquery.hive.OXMLInputFormat'
   OUTPUTFORMAT 'oracle.hadoop.xquery.hive.OXMLOutputFormat'
TBLPROPERTIES(
        "oxh-namespace.ftc" = "urn:oecd:ties:fatca:v2",
        "oxh-namespace.sfa" = "urn:oecd:ties:stffatcatypes:v2",
        "oxh-elements" = "ftc:FATCA_OECD",
        "oxh-column.DocRefId" = "./ftc:FATCA/ftc:ReportingFI/ftc:DocSpec/ftc:DocRefId",
        "oxh-column.MessageRefId" = "./ftc:MessageSpec/sfa:MessageRefId",
        "oxh-column.ReportingPeriod" = "./ftc:MessageSpec/sfa:ReportingPeriod",
        "oxh-column.ResCountryCode" = "./ftc:FATCA/ftc:ReportingFI/sfa:Address/sfa:CountryCode"        
);
load data local inpath '/home/ibietl/mk/mkamdar/data.xml' into table fatcareportingInstitution;

--working
CREATE TABLE  IF NOT EXISTS stg_reportinggroup (InstDocrefId STRING,reportinggroup STRING)
ROW FORMAT
   SERDE 'oracle.hadoop.xquery.hive.OXMLSerDe'
STORED AS
   INPUTFORMAT 'oracle.hadoop.xquery.hive.OXMLInputFormat'
   OUTPUTFORMAT 'oracle.hadoop.xquery.hive.OXMLOutputFormat'
TBLPROPERTIES(
        "oxh-namespace.ftc" = "urn:oecd:ties:fatca:v2",
        "oxh-namespace.sfa" = "urn:oecd:ties:stffatcatypes:v2",
        "oxh-elements" = "ftc:FATCA",
        "oxh-column.InstDocrefId" = "./ftc:ReportingFI/ftc:DocSpec/ftc:DocRefId",        
        "oxh-column.reportinggroup" = "fn:serialize(./ftc:ReportingGroup)"
);

load data local inpath '/home/ibietl/mk/mkamdar/data.xml' into table stg_reportinggroup;


--Creating Accountreportreporting group

CREATE TABLE IF NOT EXISTS stg_reportinggroupaccountreport as 
SELECT instdocrefid, t.accountreport FROM stg_reportinggroup LATERAL VIEW 
xml_table(
struct("ftc", "urn:oecd:ties:fatca:v2", "sfa", "urn:oecd:ties:stffatcatypes:v2" ),
"
let $reportinggroup :=./ftc:ReportingGroup
for $accountreport in $reportinggroup/ftc:AccountReport
return
<r>
{fn:serialize($accountreport)}
</r>
", 
stg_reportinggroup.reportinggroup, 
struct(".")) t AS accountreport;

CREATE TABLE IF NOT EXISTS stg_reportinggroupSponsor as 
SELECT instdocrefid, t.sponsor FROM stg_reportinggroup LATERAL VIEW 
xml_table(
struct("ftc", "urn:oecd:ties:fatca:v2", "sfa", "urn:oecd:ties:stffatcatypes:v2" ),
"
let $reportinggroup :=./ftc:ReportingGroup
for $sponsor in $reportinggroup/ftc:Sponsor
return
<r>
{fn:serialize($sponsor)}
</r>
", 
stg_reportinggroup.reportinggroup, 
struct(".")) t AS sponsor;

CREATE TABLE IF NOT EXISTS stg_reportinggroupintermediary as 
SELECT instdocrefid, t.intermediary FROM stg_reportinggroup LATERAL VIEW 
xml_table(
struct("ftc", "urn:oecd:ties:fatca:v2", "sfa", "urn:oecd:ties:stffatcatypes:v2" ),
"
let $reportinggroup :=./ftc:ReportingGroup
for $intermediary in $reportinggroup/ftc:Intermediary
return
<r>
{fn:serialize($intermediary)}
</r>
", 
stg_reportinggroup.reportinggroup, 
struct(".")) t AS intermediary;

CREATE TABLE IF NOT EXISTS stg_reportinggroupnillreport as 
SELECT instdocrefid, t.nillreport FROM stg_reportinggroup LATERAL VIEW 
xml_table(
struct("ftc", "urn:oecd:ties:fatca:v2", "sfa", "urn:oecd:ties:stffatcatypes:v2" ),
"
let $reportinggroup :=./ftc:ReportingGroup
for $nillreport in $reportinggroup/ftc:NillReport
return
<r>
{fn:serialize($nillreport)}
</r>
", 
stg_reportinggroup.reportinggroup, 
struct(".")) t AS nillreport;

CREATE TABLE IF NOT EXISTS stg_reportinggrouppoolreport as 
SELECT instdocrefid, t.poolreport FROM stg_reportinggroup LATERAL VIEW 
xml_table(
struct("ftc", "urn:oecd:ties:fatca:v2", "sfa", "urn:oecd:ties:stffatcatypes:v2" ),
"
let $reportinggroup :=./ftc:ReportingGroup
for $poolreport in $reportinggroup/ftc:PoolReport
return
<r>
{fn:serialize($poolreport)}
</r>
", 
stg_reportinggroup.reportinggroup, 
struct(".")) t AS poolreport;

--Extracting Account report information
CREATE TABLE ACCOUNTREPORTDETAILS AS 
SELECT  t.docrefid,InstDocrefId,t.Entity,t.AccountCount,t.PoolReportType, t.BalanceAmt,t.BalanceType, t.BalanceCurrCode, t.AccountNumber, t.AccountHolderTypeInd,t.AccountHolderTypeOrg, t.ResCountryCodeInd,t.ResCountryCodeOrg, t.Birthdate, t.Birthcity, t.BirthcitySubEnt, t.BirthCountryCode, t.Nationality,t.AccountClosed,t.CARPoolReportReportingFIGIIN,t.CARPoolReportMessageRefId,t.CARPoolReportDocRefId,t.AdditionalItemName,t.AdditionalItemValue
FROM stg_reportinggroupaccountreport LATERAL VIEW 
xml_table(
struct("ftc", "urn:oecd:ties:fatca:v2", "sfa", "urn:oecd:ties:stffatcatypes:v2" ),
"
let $accountreport :=./ftc:AccountReport
for $account in $accountreport
return
<r>
{$account/ftc:DocSpec/ftc:DocRefId}
{$account}
{$account/ftc:AccountCount}
{$account/ftc:AccountPoolReportType}
{$account/ftc:AccountBalance}
{$account/ftc:AccountBalance}
{$account/ftc:AccountBalance/@currCode/data()}
{$account/ftc:AccountNumber}
{$account/ftc:AccountHolder/ftc:Individual}
{$account/ftc:AccountHolder/ftc:Organisation}
{$account/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode}
{$account/ftc:AccountHolder/ftc:Organisation/sfa:ResCountryCode}
{$account/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:BirthDate}
{$account/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:City}
{$account/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:CitySubentity}
{$account/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:CountryInfo}
{$account/ftc:AccountHolder/ftc:Individual/sfa:Nationality}
{$account/ftc:AccountClosed}
{$account/ftc:CARRef/ftc:PoolReportReportingFIGIIN}
{$account/ftc:CARRef/ftc:PoolReportMessageRefId}
{$account/ftc:CARRef/ftc:PoolReportDocRefId}
{$account/ftc:AdditionalData/ftc:AdditionalItem/ftc:ItemName}
{$account/ftc:AdditionalData/ftc:AdditionalItem/ftc:ItemContent}
</r>
", 
stg_reportinggroupaccountreport.accountreport, 
struct(
"./ftc:DocRefId",
"./ftc:AccountReport/name()",
"./ftc:AccountCount",
"./ftc:AccountPoolReportType",
"./ftc:AccountBalance",
"./ftc:AccountBalance/name()",
"./ftc:AccountBalance/@currCode",
"./ftc:AccountNumber",
"./ftc:Individual/name()",
"./ftc:Organisation/name()",
"./ftc:Individual/sfa:ResCountryCode",
"./ftc:Organisation/sfa:ResCountryCode",
"./sfa:BirthDate",
"./sfa:City",
"./sfa:CitySubentity",
"./sfa:CountryInfo",
"./sfa:Nationality",
"./ftc:AccountClosed",
"./ftc:PoolReportReportingFIGIIN",
"./ftc:PoolReportMessageRefId",
"./ftc:PoolReportDocRefId",
"./ftc:ItemName",
"./ftc:ItemContent")
) t AS docrefid,Entity,AccountCount,PoolReportType, BalanceAmt,BalanceType, BalanceCurrCode, AccountNumber, AccountHolderTypeInd,AccountHolderTypeOrg, ResCountryCodeInd,ResCountryCodeOrg, Birthdate, Birthcity, BirthcitySubEnt, BirthCountryCode, Nationality,AccountClosed,CARPoolReportReportingFIGIIN,CARPoolReportMessageRefId,CARPoolReportDocRefId,AdditionalItemName,AdditionalItemValue;
--extracting Sponsor information
CREATE TABLE SPONSORREPORTDETAILS AS 
SELECT  t.docrefid,InstDocrefId,t.Entity,t.AccountCount,t.PoolReportType, t.BalanceAmt,t.BalanceType, t.BalanceCurrCode, t.AccountNumber, t.AccountHolderTypeInd,t.AccountHolderTypeOrg, t.ResCountryCodeInd,t.ResCountryCodeOrg, t.Birthdate, t.Birthcity, t.BirthcitySubEnt, t.BirthCountryCode, t.Nationality,t.AccountClosed,t.CARPoolReportReportingFIGIIN,t.CARPoolReportMessageRefId,t.CARPoolReportDocRefId,t.AdditionalItemName,t.AdditionalItemValue
FROM stg_reportinggroupsponsor LATERAL VIEW 
xml_table(
struct("ftc", "urn:oecd:ties:fatca:v2", "sfa", "urn:oecd:ties:stffatcatypes:v2" ),
"
let $sponsorreport :=./ftc:Sponsor
for $sponsor in $sponsorreport
return
<r>
{$sponsor/ftc:DocSpec/ftc:DocRefId}
{$sponsor}
{$sponsor/ftc:AccountCount}
{$sponsor/ftc:AccountPoolReportType}
{$sponsor/ftc:AccountBalance}
{$sponsor/ftc:AccountBalance}
{$sponsor/ftc:AccountBalance/@currCode/data()}
{$sponsor/ftc:AccountNumber}
{$sponsor/ftc:AccountHolder/ftc:Individual}
{$sponsor/ftc:AccountHolder/ftc:Organisation}
{$sponsor/sfa:ResCountryCode}
{$sponsor/ftc:AccountHolder/ftc:Organisation/sfa:ResCountryCode}
{$sponsor/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:BirthDate}
{$sponsor/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:City}
{$sponsor/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:CitySubentity}
{$sponsor/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:CountryInfo}
{$sponsor/ftc:AccountHolder/ftc:Individual/sfa:Nationality}
{$sponsor/ftc:AccountClosed}
{$sponsor/ftc:CARRef/ftc:PoolReportReportingFIGIIN}
{$sponsor/ftc:CARRef/ftc:PoolReportMessageRefId}
{$sponsor/ftc:CARRef/ftc:PoolReportDocRefId}
{$sponsor/ftc:AdditionalData/ftc:AdditionalItem/ftc:ItemName}
{$sponsor/ftc:AdditionalData/ftc:AdditionalItem/ftc:ItemContent}
</r>
", 
stg_reportinggroupsponsor.sponsor, 
struct(
"./ftc:DocRefId",
"./ftc:Sponsor/name()",
"./ftc:AccountCount",
"./ftc:AccountPoolReportType",
"./ftc:AccountBalance",
"./ftc:AccountBalance/name()",
"./ftc:AccountBalance/@currCode",
"./ftc:AccountNumber",
"./ftc:Individual/name()",
"./ftc:Organisation/name()",
"./sfa:ResCountryCode",
"./ftc:Organisation/sfa:ResCountryCode",
"./sfa:BirthDate",
"./sfa:City",
"./sfa:CitySubentity",
"./sfa:CountryInfo",
"./sfa:Nationality",
"./ftc:AccountClosed",
"./ftc:PoolReportReportingFIGIIN",
"./ftc:PoolReportMessageRefId",
"./ftc:PoolReportDocRefId",
"./ftc:ItemName",
"./ftc:ItemContent")
) t AS docrefid,Entity,AccountCount,PoolReportType, BalanceAmt,BalanceType, BalanceCurrCode, AccountNumber, AccountHolderTypeInd,AccountHolderTypeOrg, ResCountryCodeInd,ResCountryCodeOrg, Birthdate, Birthcity, BirthcitySubEnt, BirthCountryCode, Nationality,AccountClosed,CARPoolReportReportingFIGIIN,CARPoolReportMessageRefId,CARPoolReportDocRefId,AdditionalItemName,AdditionalItemValue;
--extracting intermidiary information
CREATE TABLE INTERMEDIARYREPORTDETAILS AS 
SELECT  t.docrefid,InstDocrefId,t.Entity,t.AccountCount,t.PoolReportType, t.BalanceAmt,t.BalanceType, t.BalanceCurrCode, t.AccountNumber, t.AccountHolderTypeInd,t.AccountHolderTypeOrg, t.ResCountryCodeInd,t.ResCountryCodeOrg, t.Birthdate, t.Birthcity, t.BirthcitySubEnt, t.BirthCountryCode, t.Nationality,t.AccountClosed,t.CARPoolReportReportingFIGIIN,t.CARPoolReportMessageRefId,t.CARPoolReportDocRefId,t.AdditionalItemName,t.AdditionalItemValue
FROM stg_reportinggroupintermediary LATERAL VIEW 
xml_table(
struct("ftc", "urn:oecd:ties:fatca:v2", "sfa", "urn:oecd:ties:stffatcatypes:v2" ),
"
let $intermediaryreport :=./ftc:Intermediary
for $intermediary in $intermediaryreport
return
<r>
{$intermediary/ftc:DocSpec/ftc:DocRefId}
{$intermediary}
{$intermediary/ftc:AccountCount}
{$intermediary/ftc:AccountPoolReportType}
{$intermediary/ftc:AccountBalance}
{$intermediary/ftc:AccountBalance}
{$intermediary/ftc:AccountBalance/@currCode/data()}
{$intermediary/ftc:AccountNumber}
{$intermediary/ftc:AccountHolder/ftc:Individual}
{$intermediary/ftc:AccountHolder/ftc:Organisation}
{$intermediary/sfa:ResCountryCode}
{$intermediary/ftc:AccountHolder/ftc:Organisation/sfa:ResCountryCode}
{$intermediary/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:BirthDate}
{$intermediary/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:City}
{$intermediary/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:CitySubentity}
{$intermediary/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:CountryInfo}
{$intermediary/ftc:AccountHolder/ftc:Individual/sfa:Nationality}
{$intermediary/ftc:AccountClosed}
{$intermediary/ftc:CARRef/ftc:PoolReportReportingFIGIIN}
{$intermediary/ftc:CARRef/ftc:PoolReportMessageRefId}
{$intermediary/ftc:CARRef/ftc:PoolReportDocRefId}
{$intermediary/ftc:AdditionalData/ftc:AdditionalItem/ftc:ItemName}
{$intermediary/ftc:AdditionalData/ftc:AdditionalItem/ftc:ItemContent}
</r>
", 
stg_reportinggroupintermediary.intermediary, 
struct(
"./ftc:DocRefId",
"./ftc:Intermediary/name()",
"./ftc:AccountCount",
"./ftc:AccountPoolReportType",
"./ftc:AccountBalance",
"./ftc:AccountBalance/name()",
"./ftc:AccountBalance/@currCode",
"./ftc:AccountNumber",
"./ftc:Individual/name()",
"./ftc:Organisation/name()",
"./sfa:ResCountryCode",
"./ftc:Organisation/sfa:ResCountryCode",
"./sfa:BirthDate",
"./sfa:City",
"./sfa:CitySubentity",
"./sfa:CountryInfo",
"./sfa:Nationality",
"./ftc:AccountClosed",
"./ftc:PoolReportReportingFIGIIN",
"./ftc:PoolReportMessageRefId",
"./ftc:PoolReportDocRefId",
"./ftc:ItemName",
"./ftc:ItemContent")
) t AS docrefid,Entity,AccountCount,PoolReportType, BalanceAmt,BalanceType, BalanceCurrCode, AccountNumber, AccountHolderTypeInd,AccountHolderTypeOrg, ResCountryCodeInd,ResCountryCodeOrg, Birthdate, Birthcity, BirthcitySubEnt, BirthCountryCode, Nationality,AccountClosed,CARPoolReportReportingFIGIIN,CARPoolReportMessageRefId,CARPoolReportDocRefId,AdditionalItemName,AdditionalItemValue;
--Extracting nill report information
CREATE TABLE NILLREPORTDETAILS AS 
SELECT  t.docrefid,InstDocrefId,t.Entity,t.AccountCount,t.PoolReportType, t.BalanceAmt,t.BalanceType, t.BalanceCurrCode, t.AccountNumber, t.AccountHolderTypeInd,t.AccountHolderTypeOrg, t.ResCountryCodeInd,t.ResCountryCodeOrg, t.Birthdate, t.Birthcity, t.BirthcitySubEnt, t.BirthCountryCode, t.Nationality,t.AccountClosed,t.CARPoolReportReportingFIGIIN,t.CARPoolReportMessageRefId,t.CARPoolReportDocRefId,t.AdditionalItemName,t.AdditionalItemValue
FROM stg_reportinggroupnillreport LATERAL VIEW 
xml_table(
struct("ftc", "urn:oecd:ties:fatca:v2", "sfa", "urn:oecd:ties:stffatcatypes:v2" ),
"
let $nillreport :=./ftc:NillReport
for $nillrpt in $nillreport
return
<r>
{$nillrpt/ftc:DocSpec/ftc:DocRefId}
{$nillrpt}
{$nillrpt/ftc:AccountCount}
{$nillrpt/ftc:AccountPoolReportType}
{$nillrpt/ftc:AccountBalance}
{$nillrpt/ftc:AccountBalance}
{$nillrpt/ftc:AccountBalance/@currCode/data()}
{$nillrpt/ftc:AccountNumber}
{$nillrpt/ftc:AccountHolder/ftc:Individual}
{$nillrpt/ftc:AccountHolder/ftc:Organisation}
{$nillrpt/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode}
{$nillrpt/ftc:AccountHolder/ftc:Organisation/sfa:ResCountryCode}
{$nillrpt/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:BirthDate}
{$nillrpt/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:City}
{$nillrpt/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:CitySubentity}
{$nillrpt/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:CountryInfo}
{$nillrpt/ftc:AccountHolder/ftc:Individual/sfa:Nationality}
{$nillrpt/ftc:AccountClosed}
{$nillrpt/ftc:CARRef/ftc:PoolReportReportingFIGIIN}
{$nillrpt/ftc:CARRef/ftc:PoolReportMessageRefId}
{$nillrpt/ftc:CARRef/ftc:PoolReportDocRefId}
{$nillrpt/ftc:AdditionalData/ftc:AdditionalItem/ftc:ItemName}
{$nillrpt/ftc:AdditionalData/ftc:AdditionalItem/ftc:ItemContent}
</r>
", 
stg_reportinggroupnillreport.nillreport, 
struct(
"./ftc:DocRefId",
"./ftc:NillReport/name()",
"./ftc:AccountCount",
"./ftc:AccountPoolReportType",
"./ftc:AccountBalance",
"./ftc:AccountBalance/name()",
"./ftc:AccountBalance/@currCode",
"./ftc:AccountNumber",
"./ftc:Individual/name()",
"./ftc:Organisation/name()",
"./ftc:Individual/sfa:ResCountryCode",
"./ftc:Organisation/sfa:ResCountryCode",
"./sfa:BirthDate",
"./sfa:City",
"./sfa:CitySubentity",
"./sfa:CountryInfo",
"./sfa:Nationality",
"./ftc:AccountClosed",
"./ftc:PoolReportReportingFIGIIN",
"./ftc:PoolReportMessageRefId",
"./ftc:PoolReportDocRefId",
"./ftc:ItemName",
"./ftc:ItemContent")
) t AS docrefid,Entity,AccountCount,PoolReportType, BalanceAmt,BalanceType, BalanceCurrCode, AccountNumber, AccountHolderTypeInd,AccountHolderTypeOrg, ResCountryCodeInd,ResCountryCodeOrg, Birthdate, Birthcity, BirthcitySubEnt, BirthCountryCode, Nationality,AccountClosed,CARPoolReportReportingFIGIIN,CARPoolReportMessageRefId,CARPoolReportDocRefId,AdditionalItemName,AdditionalItemValue;
--extracting information from pool report
CREATE TABLE POOLREPORTDETAILS AS 
SELECT  t.docrefid,InstDocrefId,t.Entity,t.AccountCount,t.PoolReportType, t.BalanceAmt,t.BalanceType, t.BalanceCurrCode, t.AccountNumber, t.AccountHolderTypeInd,t.AccountHolderTypeOrg, t.ResCountryCodeInd,t.ResCountryCodeOrg, t.Birthdate, t.Birthcity, t.BirthcitySubEnt, t.BirthCountryCode, t.Nationality,t.AccountClosed,t.CARPoolReportReportingFIGIIN,t.CARPoolReportMessageRefId,t.CARPoolReportDocRefId,t.AdditionalItemName,t.AdditionalItemValue
FROM stg_reportinggrouppoolreport LATERAL VIEW 
xml_table(
struct("ftc", "urn:oecd:ties:fatca:v2", "sfa", "urn:oecd:ties:stffatcatypes:v2" ),
"
let $poolreport :=./ftc:PoolReport
for $poolrpt in $poolreport
return
<r>
{$poolrpt/ftc:DocSpec/ftc:DocRefId}
{$poolrpt}
{$poolrpt/ftc:AccountCount}
{$poolrpt/ftc:AccountPoolReportType}
{$poolrpt/ftc:PoolBalance}
{$poolrpt/ftc:PoolBalance}
{$poolrpt/ftc:PoolBalance/@currCode/data()}
{$poolrpt/ftc:AccountNumber}
{$poolrpt/ftc:AccountHolder/ftc:Individual}
{$poolrpt/ftc:AccountHolder/ftc:Organisation}
{$poolrpt/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode}
{$poolrpt/ftc:AccountHolder/ftc:Organisation/sfa:ResCountryCode}
{$poolrpt/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:BirthDate}
{$poolrpt/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:City}
{$poolrpt/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:CitySubentity}
{$poolrpt/ftc:AccountHolder/ftc:Individual/sfa:ResCountryCode/sfa:BirthInfo/sfa:CountryInfo}
{$poolrpt/ftc:AccountHolder/ftc:Individual/sfa:Nationality}
{$poolrpt/ftc:AccountClosed}
{$poolrpt/ftc:CARRef/ftc:PoolReportReportingFIGIIN}
{$poolrpt/ftc:CARRef/ftc:PoolReportMessageRefId}
{$poolrpt/ftc:CARRef/ftc:PoolReportDocRefId}
{$poolrpt/ftc:AdditionalData/ftc:AdditionalItem/ftc:ItemName}
{$poolrpt/ftc:AdditionalData/ftc:AdditionalItem/ftc:ItemContent}
</r>
", 
stg_reportinggrouppoolreport.poolreport, 
struct(
"./ftc:DocRefId",
"./ftc:PoolReport/name()",
"./ftc:AccountCount",
"./ftc:AccountPoolReportType",
"./ftc:PoolBalance",
"./ftc:PoolBalance/name()",
"./ftc:PoolBalance/@currCode",
"./ftc:AccountNumber",
"./ftc:Individual/name()",
"./ftc:Organisation/name()",
"./ftc:Individual/sfa:ResCountryCode",
"./ftc:Organisation/sfa:ResCountryCode",
"./sfa:BirthDate",
"./sfa:City",
"./sfa:CitySubentity",
"./sfa:CountryInfo",
"./sfa:Nationality",
"./ftc:AccountClosed",
"./ftc:PoolReportReportingFIGIIN",
"./ftc:PoolReportMessageRefId",
"./ftc:PoolReportDocRefId",
"./ftc:ItemName",
"./ftc:ItemContent")
) t AS docrefid,Entity,AccountCount,PoolReportType, BalanceAmt,BalanceType, BalanceCurrCode, AccountNumber, AccountHolderTypeInd,AccountHolderTypeOrg, ResCountryCodeInd,ResCountryCodeOrg, Birthdate, Birthcity, BirthcitySubEnt, BirthCountryCode, Nationality,AccountClosed,CARPoolReportReportingFIGIIN,CARPoolReportMessageRefId,CARPoolReportDocRefId,AdditionalItemName,AdditionalItemValue;

--consolidating data from all details tables
CREATE TABLE FATCAREPORTINGGROUPRAW AS  
SELECT * FROM ACCOUNTREPORTDETAILS;
INSERT INTO FATCAREPORTINGGROUPRAW 
SELECT * FROM SPONSORREPORTDETAILS;
INSERT INTO FATCAREPORTINGGROUPRAW 
SELECT * FROM INTERMEDIARYREPORTDETAILS;
INSERT INTO FATCAREPORTINGGROUPRAW 
SELECT * FROM NILLREPORTDETAILS;
INSERT INTO FATCAREPORTINGGROUPRAW 
SELECT * FROM POOLREPORTDETAILS;


--Refining data

create table fatcareportinggroup as
select docrefid, substring(Entity,5) as entity,AccountCount,PoolReportType, BalanceAmt,substring(BalanceType,5) as balancetype, BalanceCurrCode, AccountNumber,
case 
   when (coalesce( AccountHolderTypeInd,AccountHolderTypeOrg) = 'ftc:Organisation') 
   then 'Organisation' 
   when (coalesce( AccountHolderTypeInd,AccountHolderTypeOrg) = 'ftc:Individual') 
   then 'Individual'  
   else ''
end as accountholdertype,
coalesce( ResCountryCodeInd,ResCountryCodeOrg) as  ResCountryCode, Birthdate, Birthcity, BirthcitySubEnt, BirthCountryCode, Nationality,AccountClosed,CARPoolReportReportingFIGIIN,CARPoolReportMessageRefId,CARPoolReportDocRefId,AdditionalItemName,AdditionalItemValue from fatcareportinggroupraw;
