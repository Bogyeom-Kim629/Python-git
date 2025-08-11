"""
Author: Bogyeom Kim
Date: 2025-08-01
Purpose: Korea Daily update
"""

# from sqlalchemy import create_engine
# from snowflake.connector.pandas_tools import write_pandas
# import pandas as pd

import os
from datetime import datetime
import pandas as pd
import pytz
import snowflake.connector
from dateutil.relativedelta import relativedelta

# run([sys.executable, './I&A Combination Numbers.py']) # updating the combination list of I&A used in procedures.
# run([sys.executable, './Surgeon Pivot Numbers.py']) # updating the pivot information made up from procedures.

today_date = datetime.today()
cq = pd.Period(freq='D', year=today_date.year, month=today_date.month,
               day=today_date.day)  # this variable means Today.(not quarter...)
currentquarter = cq.strftime('%F-Q%q')  # this variable means this quarter.

lq_date = today_date - relativedelta(months=3)
lq = pd.Period(freq='D', year=lq_date.year, month=lq_date.month, day=lq_date.day)  # this variable means 3mothe ago day.
lastquarter = lq.strftime('%F-Q%q')  # this variable means last quarter.

kst = pytz.timezone('Asia/Seoul')
current_date = datetime.now(kst)
today = str(current_date.strftime('%Y-%m-%d %H:%M:%S'))  # this variable means Today.(YYYY-MM-DD hh:mm:ss)

print('Snowflake Connector Version:', snowflake.connector.__version__)
_password = os.environ.get("INTUUSER")

ctx: object = snowflake.connector.connect(
    user='bkim2',
    password=_password,
    account='intuitive',
    role='EDW_SALESLAB_RW',
    warehouse='REPORTING_WH',
    database='SALESLAB',
    authenticator='https://intusurg.okta.com'
)
cs = ctx.cursor()

# updating Access Limiting table.
name = 'edwlabs.saleslab.KoreaSecurity'

squery = '''
create or replace table
'''

aquery = ''' AS '''

query = """
with 
subordinate_under_csd as ( -- Based on "vw_AccountTeam", listingup all CSR / CSM / CTA / SSM under CSD
    select
        accountrole.csdnetworkid,
        listagg(distinct accountrole.csrnetworkid, ', ') within group (order by accountrole.csrnetworkid) as all_csr,
        listagg(distinct accountrole.csmnetworkid, ', ') within group (order by accountrole.csmnetworkid) as all_csm,
        listagg(distinct accountrole.ctanetworkid, ', ') within group (order by accountrole.ctanetworkid) as all_cta,
        listagg(distinct accountrole.ssmnetworkid, ', ') within group (order by accountrole.ssmnetworkid) as all_ssm,
        concat(
          ifnull(all_csr,''), ', ', 
          ifnull(all_csm,''), ', ',
          ifnull(all_cta,''), ', ',
          ifnull(all_ssm,'')
        ) as all_subordinate -- this column measn the list of all subordinates under CSD
    FROM 
        "EDW"."MASTER"."VW_ACCOUNT" account
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNTTEAM" accountrole
            ON account.accountguid = accountrole.accountguid
    WHERE 
        account.recordtype = 'Hospital'
        and account.capitalregion = 'Asia : Korea'
        and accountrole.csdnetworkid is not null
    group by
        accountrole.csdnetworkid
),
subordinate_under_cvp as ( -- Based on "vw_AccountTeam", listingup all CSD under CVP
    select
        accountrole.cvpnetworkid,
        listagg(distinct accountrole.csdnetworkid, ', ') within group (order by accountrole.csdnetworkid) as all_csd
    FROM 
        "EDW"."MASTER"."VW_ACCOUNT" account
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNTTEAM" accountrole
            ON account.accountguid = accountrole.accountguid
    WHERE 
        account.recordtype = 'Hospital'
        and account.capitalregion = 'Asia : Korea'
        and accountrole.cvpnetworkid is not null
    group by
        accountrole.cvpnetworkid
),
subordinate_under_asd as ( -- Based on "vw_AccountTeam", listingup all ASM under ASD
    select
        accountrole.asdnetworkid,
        listagg(distinct accountrole.asmnetworkid, ', ') within group (order by accountrole.asmnetworkid) as all_asm
    FROM 
        "EDW"."MASTER"."VW_ACCOUNT" account
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNTTEAM" accountrole
            ON account.accountguid = accountrole.accountguid
    WHERE 
        account.recordtype = 'Hospital'
        and account.capitalregion = 'Asia : Korea'
        and accountrole.asdnetworkid is not null
    group by
        accountrole.asdnetworkid
),
subordinate_under_avp as ( -- Based on "vw_AccountTeam", listingup all ASD under AVP
    select
        accountrole.avpnetworkid,
        listagg(distinct accountrole.asdnetworkid, ', ') within group (order by accountrole.asdnetworkid) as all_asd
    FROM 
        "EDW"."MASTER"."VW_ACCOUNT" account
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNTTEAM" accountrole
            ON account.accountguid = accountrole.accountguid
    WHERE 
        account.recordtype = 'Hospital'
        and account.capitalregion = 'Asia : Korea'
        and accountrole.avpnetworkid is not null
    group by
        accountrole.avpnetworkid  
),
all_users as ( -- Listing all users under Sales Directors & VP各Director
    select
        listagg(distinct summary.subordinate_list) within group (order by summary.subordinate_list) as all_usr,
        length(all_usr) length -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)
    from
        (
        select
            'All Subordinate under CSD' as section, -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)
            listagg(distinct subordinate_under_csd.all_subordinate) within group (order by subordinate_under_csd.all_subordinate) as subordinate_list,
            length(subordinate_list) length -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)
        from
            subordinate_under_csd

        union all

        select
            'All Subordinate under CVP' as section, -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)    
            listagg(distinct subordinate_under_cvp.all_csd) within group (order by subordinate_under_cvp.all_csd) as subordinate_list,
            length(subordinate_list) length -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)
        from
            subordinate_under_cvp

        union all

        select
            'All Subordinate under ASD' as section, -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)    
            listagg(distinct subordinate_under_asd.all_asm) within group (order by subordinate_under_asd.all_asm) as subordinate_list,
            length(subordinate_list) length -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)
        from
            subordinate_under_asd

        union all

        select
            'All Subordinate under AVP' as section, -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)    
            listagg(distinct subordinate_under_avp.all_asd) within group (order by subordinate_under_avp.all_asd) as subordinate_list,
            length(subordinate_list) length -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)
        from
            subordinate_under_avp
        ) as summary
)



select -- 17481 account in all over the world (Commented by Taisei Watanabe @2022/11/20)
    account.accountid,
    account.accountguid,  
    all_users.all_usr as ALLUSERS,
    concat(
      ifnull(subordinate_under_csd.all_csr,''), ', ',
      ifnull(subordinate_under_csd.all_csm,''), ', ',
      ifnull(subordinate_under_csd.all_cta,''), ', ',
      ifnull(subordinate_under_csd.all_ssm,''), ', ',
      ifnull(subordinate_under_cvp.all_csd,''), ', ',
      ifnull(subordinate_under_asd.all_asm,''), ', ',
      ifnull(subordinate_under_avp.all_asd,'')
    ) as WHOCANACCESS -- listing Users who can access each account
FROM 
    "EDW"."MASTER"."VW_ACCOUNT" account
        LEFT JOIN "EDW"."MASTER"."VW_ACCOUNTTEAM" accountrole
        ON account.accountguid = accountrole.accountguid
        
        left outer join subordinate_under_csd
        on accountrole.csdnetworkid = subordinate_under_csd.csdnetworkid

        left outer join subordinate_under_cvp
        on accountrole.cvpnetworkid = subordinate_under_cvp.cvpnetworkid

        left outer join subordinate_under_asd
        on accountrole.asdnetworkid = subordinate_under_asd.asdnetworkid

        left outer join subordinate_under_avp
        on accountrole.avpnetworkid = subordinate_under_avp.avpnetworkid  
        
        cross join all_users
WHERE 
    account.recordtype = 'Hospital'
"""

cs.execute(squery + name + aquery + query)
print('{} updated'.format(name))

# updating Access Limiting table.
name = 'edwlabs.saleslab.KoreaSecurity2'

squery = '''
create or replace table
'''

aquery = ''' AS '''

query = """
with 
subordinate_under_csd as ( -- Based on "vw_AccountTeam", listingup all CSR / CSM / CTA / SSM under CSD
    select
        accountrole.csdnetworkid,
        listagg(distinct accountrole.csrnetworkid, ', ') within group (order by accountrole.csrnetworkid) as all_csr,
        listagg(distinct accountrole.csmnetworkid, ', ') within group (order by accountrole.csmnetworkid) as all_csm,
        listagg(distinct accountrole.ctanetworkid, ', ') within group (order by accountrole.ctanetworkid) as all_cta,
        listagg(distinct accountrole.ssmnetworkid, ', ') within group (order by accountrole.ssmnetworkid) as all_ssm,
        concat(
          ifnull(all_csr,''), ', ', 
          ifnull(all_csm,''), ', ',
          ifnull(all_cta,''), ', ',
          ifnull(all_ssm,'')
        ) as all_subordinate -- this column measn the list of all subordinates under CSD
    FROM 
        "EDW"."MASTER"."VW_ACCOUNT" account
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNTTEAM" accountrole
            ON account.accountguid = accountrole.accountguid
    WHERE 
        account.recordtype = 'Hospital'
        and account.capitalregion = 'Asia : Korea'
        and accountrole.csdnetworkid is not null
    group by
        accountrole.csdnetworkid
),
subordinate_under_cvp as ( -- Based on "vw_AccountTeam", listingup all CSD under CVP
    select
        accountrole.cvpnetworkid,
        listagg(distinct accountrole.csdnetworkid, ', ') within group (order by accountrole.csdnetworkid) as all_csd
    FROM 
        "EDW"."MASTER"."VW_ACCOUNT" account
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNTTEAM" accountrole
            ON account.accountguid = accountrole.accountguid
    WHERE 
        account.recordtype = 'Hospital'
        and account.capitalregion = 'Asia : Korea'
        and accountrole.cvpnetworkid is not null
    group by
        accountrole.cvpnetworkid
),
subordinate_under_asd as ( -- Based on "vw_AccountTeam", listingup all ASM under ASD
    select
        accountrole.asdnetworkid,
        listagg(distinct accountrole.asmnetworkid, ', ') within group (order by accountrole.asmnetworkid) as all_asm
    FROM 
        "EDW"."MASTER"."VW_ACCOUNT" account
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNTTEAM" accountrole
            ON account.accountguid = accountrole.accountguid
    WHERE 
        account.recordtype = 'Hospital'
        and account.capitalregion = 'Asia : Korea'
        and accountrole.asdnetworkid is not null
    group by
        accountrole.asdnetworkid
),
subordinate_under_avp as ( -- Based on "vw_AccountTeam", listingup all ASD under AVP
    select
        accountrole.avpnetworkid,
        listagg(distinct accountrole.asdnetworkid, ', ') within group (order by accountrole.asdnetworkid) as all_asd
    FROM 
        "EDW"."MASTER"."VW_ACCOUNT" account
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNTTEAM" accountrole
            ON account.accountguid = accountrole.accountguid
    WHERE 
        account.recordtype = 'Hospital'
        and account.capitalregion = 'Asia : Korea'
        and accountrole.avpnetworkid is not null
    group by
        accountrole.avpnetworkid  
),
all_users as ( -- Listing all users under Sales Directors & VP各Director
    select
        listagg(distinct summary.subordinate_list) within group (order by summary.subordinate_list) as all_usr,
        length(all_usr) length -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)
    from
        (
        select
            'All Subordinate under CSD' as section, -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)
            listagg(distinct subordinate_under_csd.all_subordinate) within group (order by subordinate_under_csd.all_subordinate) as subordinate_list,
            length(subordinate_list) length -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)
        from
            subordinate_under_csd

        union all

        select
            'All Subordinate under CVP' as section, -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)    
            listagg(distinct subordinate_under_cvp.all_csd) within group (order by subordinate_under_cvp.all_csd) as subordinate_list,
            length(subordinate_list) length -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)
        from
            subordinate_under_cvp

        union all

        select
            'All Subordinate under ASD' as section, -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)    
            listagg(distinct subordinate_under_asd.all_asm) within group (order by subordinate_under_asd.all_asm) as subordinate_list,
            length(subordinate_list) length -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)
        from
            subordinate_under_asd

        union all

        select
            'All Subordinate under AVP' as section, -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)    
            listagg(distinct subordinate_under_avp.all_asd) within group (order by subordinate_under_avp.all_asd) as subordinate_list,
            length(subordinate_list) length -- outputting for referrence (Commented by Taisei Watanabe @2022/11/22)
        from
            subordinate_under_avp
        ) as summary
)



select -- 17481 account in all over the world (Commented by Taisei Watanabe @2022/11/20)
    account.accountid,
    account.accountguid,  
    all_users.all_usr as ALLUSERS,
    account.csdnetworkid,
    account.csmnetworkid,
    account.csrnetworkid
    
      

FROM 
    "EDW"."MASTER"."VW_ACCOUNT" account
        LEFT JOIN "EDW"."MASTER"."VW_ACCOUNTTEAM" accountrole
        ON account.accountguid = accountrole.accountguid
        
        left outer join subordinate_under_csd
        on accountrole.csdnetworkid = subordinate_under_csd.csdnetworkid

        left outer join subordinate_under_cvp
        on accountrole.cvpnetworkid = subordinate_under_cvp.cvpnetworkid

        left outer join subordinate_under_asd
        on accountrole.asdnetworkid = subordinate_under_asd.asdnetworkid

        left outer join subordinate_under_avp
        on accountrole.avpnetworkid = subordinate_under_avp.avpnetworkid  
        
        cross join all_users
WHERE 
    account.recordtype = 'Hospital' AND
    account.clinicalregion = 'Asia : Korea'

"""

cs.execute(squery + name + aquery + query)
print('{} updated'.format(name))


name = 'edwlabs.saleslab.KoreaSurgeonPopulationShift'

squery = '''
create or replace table
'''

aquery = ''' AS '''

query = """

WITH
quarter AS (
		SELECT DISTINCT 
		     edwlabs.saleslab.shquarter(d.calendar_dt) AS "Quarter",
		     year(d.calendar_dt) AS "Year",
		     quarter(d.calendar_dt) AS "Q"
		FROM "EDW"."MASTER"."VW_SFDC_DIMDATE" d
		WHERE d.calendar_dt >= '2015-01-01' and d.calendar_dt < (SELECT dateadd(month, 3, edwlabs.saleslab.THISQUARTERFIRSTDAY()))
),
procedures AS (
		SELECT
		    p.recordid as "ProcedureGUID",
		    p.proceduredatelocal as "ProcedureDateLocal",
		    p.surgeonguid as "SurgeonGUID",
		    systemmodel
		FROM 
		    "EDW"."PROCEDURES"."VW_PROCEDURES" p
            left join EDW.PROCEDURES.VW_PROCEDURESUMMARY ps on p.casenumber = ps.casenumber
		WHERE 
		    EXISTS(SELECT accountguid FROM "EDW"."MASTER"."VW_ACCOUNT" account WHERE p.accountguid  = account.accountguid AND account.recordtype = 'Hospital')
		    and p.status = 'Completed' and ps.clinicalregion = 'Asia : Korea'
),

first_procedure AS (
		SELECT
			"SurgeonGUID",
			MIN("ProcedureDateLocal") AS "ProcedureDateLocal",
			MIN(CASE WHEN p.systemmodel = 'da Vinci Si' THEN "ProcedureDateLocal" ELSE NULL END) as "ProcedureDateLocal_Si",
            MIN(CASE WHEN p.systemmodel = 'da Vinci X' THEN "ProcedureDateLocal" ELSE NULL END) as "ProcedureDateLocal_X",
            MIN(CASE WHEN p.systemmodel = 'da Vinci Xi' THEN "ProcedureDateLocal" ELSE NULL END) as "ProcedureDateLocal_Xi",
            MIN(CASE WHEN p.systemmodel = 'da Vinci SP' THEN "ProcedureDateLocal" ELSE NULL END) "ProcedureDateLocal_SP"
		FROM
			procedures p
		GROUP BY 
			"SurgeonGUID" 
),

pastoneyear AS (
		SELECT
		 "SurgeonGUID",
		 "ProcedureDateLocal",
		 quarter."Quarter",
		 quarter."Year",
		 quarter."Q"
		FROM 
			 first_procedure p
		CROSS JOIN quarter
		WHERE 
			edwlabs.saleslab.shquarter("ProcedureDateLocal") <= quarter."Quarter"
)


SELECT
    sps."SurgeonGUID",
    sps."Quarter",
    sps."QuarterProcedureCount",
    sps."Past 1 Year Procedures",
    sps."SurgeonPopulation",
    
    IFNULL(LAG(sps."SurgeonPopulation") OVER (PARTITION BY sps."SurgeonGUID" ORDER BY sps."SurgeonGUID", sps."Quarter"), 'Stalled') AS "-1 Quarter SurgeonPopulation",
    IFNULL(LAG(sps."SurgeonPopulation_Si") OVER (PARTITION BY sps."SurgeonGUID" ORDER BY sps."SurgeonGUID", sps."Quarter"), 'Stalled') AS "-1 Quarter SurgeonPopulation_Si",
    IFNULL(LAG(sps."SurgeonPopulation_X") OVER (PARTITION BY sps."SurgeonGUID" ORDER BY sps."SurgeonGUID", sps."Quarter"), 'Stalled') AS "-1 Quarter SurgeonPopulation_X",
    IFNULL(LAG(sps."SurgeonPopulation_Xi") OVER (PARTITION BY sps."SurgeonGUID" ORDER BY sps."SurgeonGUID", sps."Quarter"), 'Stalled') AS "-1 Quarter SurgeonPopulation_Xi",
    IFNULL(LAG(sps."SurgeonPopulation_SP") OVER (PARTITION BY sps."SurgeonGUID" ORDER BY sps."SurgeonGUID", sps."Quarter"), 'Stalled') AS "-1 Quarter SurgeonPopulation_SP",
    
    IFNULL(LAG(sps."SurgeonPopulation", 2) OVER (PARTITION BY sps."SurgeonGUID" ORDER BY sps."SurgeonGUID", sps."Quarter"), 'Stalled') AS "-2 Quarter SurgeonPopulation",
    IFNULL(LAG(sps."SurgeonPopulation", 3) OVER (PARTITION BY sps."SurgeonGUID" ORDER BY sps."SurgeonGUID", sps."Quarter"), 'Stalled') AS "-3 Quarter SurgeonPopulation",
    IFNULL(LAG(sps."SurgeonPopulation", 4) OVER (PARTITION BY sps."SurgeonGUID" ORDER BY sps."SurgeonGUID", sps."Quarter"), 'Stalled') AS "-4 Quarter SurgeonPopulation",
    sps."DetailedSurgeonPopulation",
    IFNULL(LAG(sps."DetailedSurgeonPopulation") OVER (PARTITION BY sps."SurgeonGUID" ORDER BY sps."SurgeonGUID", sps."Quarter"), 'Stalled') AS "-1 Quarter DetailedSurgeonPopulation",
    IFNULL(LAG(sps."DetailedSurgeonPopulation", 2) OVER (PARTITION BY sps."SurgeonGUID" ORDER BY sps."SurgeonGUID", sps."Quarter"), 'Stalled') AS "-2 Quarter DetailedSurgeonPopulation",
    IFNULL(LAG(sps."DetailedSurgeonPopulation", 3) OVER (PARTITION BY sps."SurgeonGUID" ORDER BY sps."SurgeonGUID", sps."Quarter"), 'Stalled') AS "-3 Quarter DetailedSurgeonPopulation",
    IFNULL(LAG(sps."DetailedSurgeonPopulation", 4) OVER (PARTITION BY sps."SurgeonGUID" ORDER BY sps."SurgeonGUID", sps."Quarter"), 'Stalled') AS "-4 Quarter DetailedSurgeonPopulation",
    sps."Stalled",
    sps."SurgeonName",
    sps."SurgeonSpeciality",
  //  CASE
    //    WHEN sps."SurgeonSpeciality" = 'URO:URO' OR sps."SurgeonSpeciality" = 'URO:URO/ONC' THEN 'URO'
      //  WHEN sps."SurgeonSpeciality" = 'GYN:GYN' OR sps."SurgeonSpeciality" = 'GYN:GYO' THEN 'GYN'
        //WHEN sps."SurgeonSpeciality" = 'GEN:GASTRIC' OR sps."SurgeonSpeciality" = 'GEN:GEN' THEN 'UGI'
       // WHEN sps."SurgeonSpeciality" = 'GEN:CR' THEN 'LGI'
       // WHEN sps."SurgeonSpeciality" = 'THORACIC' THEN 'THORACIC'
       // WHEN sps."SurgeonSpeciality" = 'CARDIAC' THEN 'CARDIAC'
       // ELSE 'Other' END AS "SurgeonSpeciality2",
    sps."SurgeonCategory",
    sps."AccountGUID",
    sps."AccountName",
    sps."CustomerType",
    sps."ClinicalRegion", -- updating for showing global data.
    sps."CSDName",
    sps."CSMName",
    sps."CSRName",
    sps."SSMName",
    CASE WHEN edwlabs.saleslab.shquarter(exist."ProcedureDateLocal")<=sps."Quarter" THEN 1 ELSE 0 END AS exist_flg,
    CASE WHEN edwlabs.saleslab.shquarter(exist."ProcedureDateLocal_Si")<=sps."Quarter" THEN 1 ELSE 0 END AS exist_flg_Si,
    CASE WHEN edwlabs.saleslab.shquarter(exist."ProcedureDateLocal_X")<=sps."Quarter" THEN 1 ELSE 0 END AS exist_flg_X,
    CASE WHEN edwlabs.saleslab.shquarter(exist."ProcedureDateLocal_Xi")<=sps."Quarter" THEN 1 ELSE 0 END AS exist_flg_Xi,
    CASE WHEN edwlabs.saleslab.shquarter(exist."ProcedureDateLocal_SP")<=sps."Quarter" THEN 1 ELSE 0 END AS exist_flg_SP
FROM (
		SELECT
			sps."SurgeonGUID",
			sps."Quarter",
			sps."QuarterProcedureCount",
			sps."Past 1 Year Procedures",
			CASE
                WHEN sps."QuarterProcedureCount" >= 70 THEN 'SuperMega'
                WHEN sps."QuarterProcedureCount" >= 50 THEN 'MegaUser'
                WHEN sps."QuarterProcedureCount" >= 30 THEN 'SuperUser'
				WHEN sps."QuarterProcedureCount" >= 13 THEN 'Sustainable'
				WHEN sps."QuarterProcedureCount" >= 7 THEN 'Bubble'
				WHEN sps."QuarterProcedureCount" >= 1 THEN 'Dabbler'
				WHEN (sps."QuarterProcedureCount" IS NULL OR sps."QuarterProcedureCount" = 0) THEN 'Stalled'
			END AS "SurgeonPopulation",
			CASE
				WHEN sps."QuarterProcedureCount_Si" >= 13 THEN 'Sustainable'
				WHEN sps."QuarterProcedureCount_Si" >= 7 THEN 'Bubble'
				WHEN sps."QuarterProcedureCount_Si" >= 1 THEN 'Dabbler'
				WHEN (sps."QuarterProcedureCount_Si" IS NULL OR sps."QuarterProcedureCount_Si" = 0) THEN 'Stalled'
			END AS "SurgeonPopulation_Si",
			CASE
				WHEN sps."QuarterProcedureCount_X" >= 13 THEN 'Sustainable'
				WHEN sps."QuarterProcedureCount_X" >= 7 THEN 'Bubble'
				WHEN sps."QuarterProcedureCount_X" >= 1 THEN 'Dabbler'
				WHEN (sps."QuarterProcedureCount_X" IS NULL OR sps."QuarterProcedureCount_X" = 0) THEN 'Stalled'
			END AS "SurgeonPopulation_X",
			CASE
				WHEN sps."QuarterProcedureCount_Xi" >= 13 THEN 'Sustainable'
				WHEN sps."QuarterProcedureCount_Xi" >= 7 THEN 'Bubble'
				WHEN sps."QuarterProcedureCount_Xi" >= 1 THEN 'Dabbler'
				WHEN (sps."QuarterProcedureCount_Xi" IS NULL OR sps."QuarterProcedureCount_Xi" = 0) THEN 'Stalled'
			END AS "SurgeonPopulation_Xi",
			CASE
				WHEN sps."QuarterProcedureCount_SP" >= 13 THEN 'Sustainable'
				WHEN sps."QuarterProcedureCount_SP" >= 7 THEN 'Bubble'
				WHEN sps."QuarterProcedureCount_SP" >= 1 THEN 'Dabbler'
				WHEN (sps."QuarterProcedureCount_SP" IS NULL OR sps."QuarterProcedureCount_SP" = 0) THEN 'Stalled'
			END AS "SurgeonPopulation_SP",
			
			CASE
				WHEN sps."QuarterProcedureCount" >= 20 THEN 'Super'
				WHEN sps."QuarterProcedureCount" >= 13 THEN 'Sustainable'
				WHEN sps."QuarterProcedureCount" >= 10 THEN 'Upper Bubble'
				WHEN sps."QuarterProcedureCount" >= 7 THEN 'Lower Bubble'
				WHEN sps."QuarterProcedureCount" >= 4 THEN 'Upper Dabbler'
				WHEN sps."QuarterProcedureCount" >= 1 THEN 'Lower Dabbler'
				WHEN (sps."QuarterProcedureCount" IS NULL OR sps."QuarterProcedureCount" = 0) THEN 'Stalled'
			END AS "DetailedSurgeonPopulation",
			CASE
				WHEN (sps."QuarterProcedureCount" IS NULL OR sps."QuarterProcedureCount" = 0) THEN 'Stalled'
				ELSE 'Non Stalled'
			END AS "Stalled",
			sps."SurgeonName",
			sps."SurgeonSpeciality",
			sps."SurgeonCategory",
			sps."AccountGUID",
			sps."AccountName",
			sps."CustomerType",
			sps."ClinicalRegion", -- updating for showing global data.
			sps."CSDName",
			sps."CSMName",
			sps."CSRName",
			sps."SSMName"
		FROM (
				SELECT
					pastoneyear."SurgeonGUID",
					pastoneyear."Quarter",
					
					CASE WHEN 
						pastoneyear."Quarter" = edwlabs.saleslab.shquarter(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) 
					THEN
						quarterprocedures."QuarterProcedureCount" / forcal.dayssofar * forcal.workingday 
					ELSE
						quarterprocedures."QuarterProcedureCount" 
					END AS "QuarterProcedureCount",
					CASE WHEN 
						pastoneyear."Quarter" = edwlabs.saleslab.shquarter(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) 
					THEN
						quarterprocedures."QuarterProcedureCount_Si" / forcal.dayssofar * forcal.workingday 
					ELSE
						quarterprocedures."QuarterProcedureCount_Si" 
					END AS "QuarterProcedureCount_Si",
					CASE WHEN 
						pastoneyear."Quarter" = edwlabs.saleslab.shquarter(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) 
					THEN
						quarterprocedures."QuarterProcedureCount_X" / forcal.dayssofar * forcal.workingday 
					ELSE
						quarterprocedures."QuarterProcedureCount_X" 
					END AS "QuarterProcedureCount_X",
					CASE WHEN 
						pastoneyear."Quarter" = edwlabs.saleslab.shquarter(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) 
					THEN
						quarterprocedures."QuarterProcedureCount_Xi" / forcal.dayssofar * forcal.workingday 
					ELSE
						quarterprocedures."QuarterProcedureCount_Xi" 
					END AS "QuarterProcedureCount_Xi",
					CASE WHEN 
						pastoneyear."Quarter" = edwlabs.saleslab.shquarter(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) 
					THEN
						quarterprocedures."QuarterProcedureCount_SP" / forcal.dayssofar * forcal.workingday 
					ELSE
						quarterprocedures."QuarterProcedureCount_SP" 
					END AS "QuarterProcedureCount_SP",
					
					pastoneyear."Past 1 Year Procedures",
					surgeon.fullname AS "SurgeonName",
					surgeon.specialtyname as "SurgeonSpeciality",
					surgeon.accountguid as "AccountGUID",
					surgeon.surgeoncategory as "SurgeonCategory",
					account.accountname as "AccountName",
					account.customertype as "CustomerType",
					account.clinicalregion as "ClinicalRegion", -- updating for showing global data.
					accountroles.csdname as "CSDName",
					accountroles.csmname as "CSMName",
					accountroles.csrname as "CSRName",
					accountroles.ssmname as "SSMName"
				FROM (
						SELECT
							pastoneyear."SurgeonGUID",
							pastoneyear."Quarter",
							COUNT(pastoneyear."ProcedureGUID") AS "Past 1 Year Procedures"
						FROM (
								SELECT
									pastoneyear."SurgeonGUID",
									pastoneyear."Quarter",
									pastoneyearprocedures."ProcedureGUID",
									pastoneyearprocedures."ProcedureDateLocal"
								FROM
									pastoneyear
								LEFT JOIN
									procedures AS pastoneyearprocedures 
									ON 
										pastoneyear."SurgeonGUID" = pastoneyearprocedures."SurgeonGUID" 
										AND pastoneyearprocedures."ProcedureDateLocal" >= (
											                                    SELECT
																					dateadd(month, -9, TO_DATE(CONCAT(CONCAT(to_char(pastoneyear."Year"),
																					CASE 
																					WHEN pastoneyear."Q" = 1 THEN '-01'
																					WHEN pastoneyear."Q" = 2 THEN '-04'
																					WHEN pastoneyear."Q" = 3 THEN '-07'
																					WHEN pastoneyear."Q" = 4 THEN '-10'
																					END), '-01')))
											                                        )
										AND pastoneyearprocedures."ProcedureDateLocal" < (
										                                            SELECT
										                                                dateadd(month, 3, TO_DATE(CONCAT(CONCAT(to_char(pastoneyear."Year"),
                                                                                        CASE 
                                                                                        WHEN pastoneyear."Q" = 1 THEN '-01'
                                                                                        WHEN pastoneyear."Q" = 2 THEN '-04'
                                                                                        WHEN pastoneyear."Q" = 3 THEN '-07'
                                                                                        WHEN pastoneyear."Q" = 4 THEN '-10'
                                                                                        END), '-01')))
																							)
							) pastoneyear
						GROUP BY
							pastoneyear."SurgeonGUID",
							pastoneyear."Quarter"
 				) pastoneyear

				LEFT JOIN "EDW"."MASTER"."VW_CONTACT" surgeon 
					ON pastoneyear."SurgeonGUID" = surgeon.contactguid
				LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account 
					ON surgeon.accountguid = account.accountguid
				LEFT JOIN "EDW"."MASTER"."VW_ACCOUNTTEAM" accountroles 
					ON account.accountguid = accountroles.accountguid

				LEFT JOIN (
					    SELECT
					        sps."SurgeonGUID",
					        sps."ProcedureQuarter",
					        COUNT(sps."ProcedureGUID") AS "QuarterProcedureCount",
					        COUNT(sps."ProcedureGUID_Si") AS "QuarterProcedureCount_Si",
					        COUNT(sps."ProcedureGUID_X") AS "QuarterProcedureCount_X",
					        COUNT(sps."ProcedureGUID_Xi") AS "QuarterProcedureCount_Xi",
					        COUNT(sps."ProcedureGUID_SP") AS "QuarterProcedureCount_SP"
					    FROM (
					            SELECT
					                p.recordid as "ProcedureGUID",
					                CASE WHEN p.systemmodel = 'da Vinci Si' THEN p.recordid ELSE NULL END as "ProcedureGUID_Si",
					                CASE WHEN p.systemmodel = 'da Vinci X' THEN p.recordid ELSE NULL END as "ProcedureGUID_X",
					                CASE WHEN p.systemmodel = 'da Vinci Xi' THEN p.recordid ELSE NULL END as "ProcedureGUID_Xi",
					                CASE WHEN p.systemmodel = 'da Vinci SP' THEN p.recordid ELSE NULL END "ProcedureGUID_SP",
					                p.casenumber as "ProcedureNumber",
					                p.proceduredatelocal as "ProcedureDateLocal",
					                edwlabs.saleslab.shquarter(p.proceduredatelocal) AS "ProcedureQuarter",
					                p.surgeonguid as "SurgeonGUID",
					                p.businesscategoryname as "BusinessCategory",
					                p.subject as "Subject",
					                p.procedurename as "ProcedureName",
					                p.systemmodel
					            FROM 
					                "EDW"."PROCEDURES"."VW_PROCEDURES" p
					            WHERE 
					                EXISTS(SELECT accountguid FROM "EDW"."MASTER"."VW_ACCOUNT" account WHERE 
					                       --account.capitalregion = 'Asia : Japan' AND 
					                       p.accountguid  = account.accountguid AND account.recordtype = 'Hospital')
					                and p.status = 'Completed' 
					                AND p.proceduredatelocal <= TO_DATE(dateadd(day, (-dayofweek(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) - 2), convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())))
					        ) sps
					    GROUP BY
					        sps."SurgeonGUID",
					        sps."ProcedureQuarter"
						) quarterprocedures 
					ON 
						quarterprocedures."SurgeonGUID" = pastoneyear."SurgeonGUID" 
						AND pastoneyear."Quarter" = quarterprocedures."ProcedureQuarter"

	            CROSS JOIN (
						select * from table(edwlabs.saleslab.Korearunrate())
	            		) forcal
				WHERE 
				   EXISTS(SELECT accountguid FROM "EDW"."MASTER"."VW_ACCOUNT" account WHERE 
				          --account.capitalregion = 'Asia : Japan' AND 
				          surgeon.accountguid  = account.accountguid AND account.recordtype = 'Hospital')
			) sps
	) sps
LEFT JOIN first_procedure as exist
	ON sps."SurgeonGUID" = exist."SurgeonGUID"

"""

cs.execute(squery + name + aquery + query)
print('{} updated'.format(name))

name = 'edwlabs.saleslab.KoreaProceduresHistorical'

squery = '''
create or replace table
'''

aquery = ''' AS '''

query = """

SELECT
    convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp()) AS "ExtractedDate",
    p.businesscategoryname as "BusinessCategory",
    p.subject as "Subject",
    p.procedurename as "ProcedureName",
    p.proceduredurationinseconds / 60 AS "ProcedureDurationMin",
    edwlabs.saleslab.shquarter(IFNULL(p.proceduredatelocal, p.SCHEDULEDSTARTDATE)) as "Quarter",
    edwlabs.saleslab.weekcal(IFNULL(p.proceduredatelocal, p.SCHEDULEDSTARTDATE)) as "Week",
    edwlabs.saleslab.weekcal(IFNULL(p.proceduredatelocal, p.SCHEDULEDSTARTDATE)) - edwlabs.saleslab.weekcal(edwlabs.saleslab.THISQUARTERFIRSTDAY()) + 1 AS "Week of the Quarter",
    IFNULL(p.proceduredatelocal, p.SCHEDULEDSTARTDATE) as "ProcedureDateLocal",
    p.status as "Status",
    CASE
        WHEN  concat(p.businesscategoryname,p.subject) IN(
                   'General SurgeryVentral Hernia','General SurgeryInguinal Hernia','General SurgeryCholecystectomy'
                 ,'General SurgeryColon Resection','General SurgeryRectal Resection','General SurgeryCholecystectomy',
                 'GynecologyOophorectomy','GynecologyOther Gynecology','GynecologyHysterectomy - Malignant',
                 'GynecologyOvarian Cystectomy','GynecologySacrocolpopexy','GynecologyHysterectomy - Benign',
                 'GynecologyMyomectomy','GynecologyEndometriosis')
                 then 'Growth'
                 else 'Others' 
    END AS "GP/Others",
    p.casenumber as "ProcedureNumber",
    p.recordid as "ProcedureGUID",
    account.accountid as "AccountID",
    account.accountguid as "AccountGUID",
    account.accountname as "AccountName",
    account.ENDCUSTOMERSTATEPROVINCE AS "Prefecture",
    accountrolesactive.ssmname as "SSMName",
    accountrolesactive.csdname as "CSDName",
    accountrolesactive.csmname as "CSMName",
    accountrolesactive.csrname as "CSRName",
    contact.fullname as "SurgeonName",
    contact.contactguid as "ContactGUID",
    contact.specialtyname as "SurgeonSpeciality",
    contact.surgeoncategory as "SurgeonCategory",
    CASE 
        WHEN p.status = 'OnSite' THEN 'OnSite' 
        ELSE IFNULL(surgeonpopulation."SurgeonPopulation", 'No Procedure in Last Quarter') 
    END AS "SurgeonPopulation(LastQuarter)",
    traineddate."FullName" AS "TrainedFrom",
    traineddate."CSRGUID" AS "TrainingCSRGUID",
    traineddate."EndDateTime" AS "EndDateTime",
    traineddate."EventStatus",
    aq."Other - Forecast",
    aq."Growth - Forecast",
    aq."All - Forecast"
FROM 
    "EDW"."PROCEDURES"."VW_PROCEDURES" p
    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account 
    ON p.accountguid  = account.accountguid
        LEFT JOIN "EDW"."MASTER"."VW_ACCOUNTTEAM" accountrolesactive 
        ON p.accountguid  = accountrolesactive.accountguid
            LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact 
            ON contact.contactguid = p.surgeonguid
                //LEFT JOIN edwlabs.saleslab.JapanNewProcedureClassification npc
                //ON p.subject = npc.subject
                //AND p.businesscategoryname = npc.businesscategory
                //AND p.procedurename = npc.procedurename
                LEFT JOIN
                (
                    SELECT
                        previousquartercount."SurgeonGUID",
                        CASE 
                            WHEN previousquartercount."PreviousQuarterProceduresCount - All" >= 13 THEN 'Sustainable'
                            WHEN previousquartercount."PreviousQuarterProceduresCount - All" < 13 AND previousquartercount."PreviousQuarterProceduresCount - All" >= 7 THEN 'Bubble'
                            WHEN previousquartercount."PreviousQuarterProceduresCount - All" < 7 THEN 'Dabbler'
                            ELSE 'No Procedure in Last Quarter'
                        END AS "SurgeonPopulation"
                    FROM
                    (
                        SELECT
                            quarterlypivot."SurgeonGUID",
                            COUNT(quarterlypivot."ProcedureNumber") as "PreviousQuarterProceduresCount - All"
                        FROM
                        (
                            SELECT
                                p.casenumber as "ProcedureNumber",
                                edwlabs.saleslab.shquarter(IFNULL(p.proceduredatelocal, p.SCHEDULEDSTARTDATE)) AS "QUARTER",
                                p.businesscategoryname as "BusinessCategory",
                                p.subject as "Subject",
                                p.surgeonid as "SurgeonGUID"
                            FROM 
                                "EDW"."PROCEDURES"."VW_PROCEDURES" p
                            WHERE 
                                IFNULL(p.proceduredatelocal, p.SCHEDULEDSTARTDATE) >= dateadd(month, -3, edwlabs.saleslab.THISQUARTERFIRSTDAY())
                                and p.status = 'Completed' 
                                and IFNULL(p.proceduredatelocal, p.SCHEDULEDSTARTDATE) < edwlabs.saleslab.THISQUARTERFIRSTDAY()
                        ) quarterlypivot
                        GROUP BY 
                            "SurgeonGUID"
                    ) previousquartercount
                ) surgeonpopulation 
                ON contact.contactid = surgeonpopulation."SurgeonGUID"

                LEFT JOIN
                (
                    SELECT
                        er."EndDate" AS "EndDateTime",
                        edwlabs.saleslab.shquarter(er."EndDate") AS "EndDateTimeQuarter",
                        er."CSRName" AS "FullName",
                        er."CSRGUID",
                        er."EventType",
                        er."SurgeonName",
                        er."AttendeeGUID" AS "SurgeonGUID",
                        er."Status" AS "EventStatus"
                    FROM
                    (
                        SELECT
                            tr."EndDate",
                            tr."CSRName",
                            tr."CSRGUID",
                            tr."EventType",
                            tr."SurgeonName",
                            tr."AttendeeGUID",
                            tr."Status",
                            ROW_NUMBER() OVER (PARTITION BY tr."AttendeeGUID" ORDER BY tr."EndDate" DESC) as "seqnum"
                        FROM
                        (
                            SELECT
                                tr.CERTIFICATIONDATE AS "EndDate",
                                IFNULL(u.fullname, csr.fullname) AS "CSRName",
                                IFNULL(tr.csr, er.CSRGUID) AS "CSRGUID",
                                tr.eventtype as "EventType",
                                tr.CONTACTFULLNAME AS "SurgeonName",
                                tr.contact AS "AttendeeGUID",
                                'Completed' AS "Status",
                                'Certification' AS "Label"
                            FROM 
                                "EDW"."TRAINING"."VW_TRNCERTIFICATION" tr
                                    LEFT JOIN "EDW"."MASTER"."VW_USER" u ON tr.csr = u.userid
                                    LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact ON tr.contact = contact.contactguid
                                    LEFT JOIN "EDW"."TRAINING"."VW_EVENTREGISTRATION" er ON tr.eventregistration = er.RECORDID
                                    LEFT JOIN "EDW"."MASTER"."VW_USER" csr ON er.csrguid = csr.userid
                            WHERE 
                                tr.eventtype = 'Technology Training Multi-Port' and contact.ENDCUSTOMERMAILINGCOUNTRY = 'South Korea' and er.eventstatus = 'Completed' and tr.CERTIFICATIONDATE >= '2019-01-01'
                                and tr.role = 'Surgeon'
                        ) tr
                    ) er
                    WHERE
                        er."seqnum" = 1
                ) traineddate 
                ON traineddate."SurgeonGUID" = contact.contactguid

                LEFT JOIN
                (
                    SELECT
                        aq.accountguid,
                        aq."Quarter",
                        SUM("Other - Forecast") AS "Other - Forecast",
                        SUM("Growth - Forecast") AS "Growth - Forecast",
                        SUM("All - Forecast") AS "All - Forecast"
                    FROM
                    (
                        SELECT
                            aq.accountguid,
                            aq."Quarter",
                            CASE WHEN aq."Bucket" = 'Other' THEN IFNULL(aq.forecast, 0) ELSE 0 END AS "Other - Forecast",
                            CASE WHEN aq."Bucket" = 'Growth' THEN IFNULL(aq.forecast, 0) ELSE 0 END AS "Growth - Forecast",
                            IFNULL(aq.forecast, 0) AS "All - Forecast"
                        FROM
                        (
                            SELECT
                                aq.accountguid,
                                CASE WHEN aq.businesscategory = 'General Surgery' THEN 'Growth' ELSE aq.businesscategory END AS "Bucket",
                                CONCAT(CONCAT(aq.fiscalyear, '-'), aq.quarter) AS "Quarter",
                                aq.forecast
                            FROM 
                                "EDW"."PROCEDURES"."VW_ACCOUNTQUOTA" aq
                            WHERE 
                                EXISTS(SELECT accountguid FROM "EDW"."MASTER"."VW_ACCOUNT" account WHERE account.capitalregion = 'Asia : Korea' AND aq.accountguid  = account.accountguid AND account.recordtype = 'Hospital')
                        ) aq
                    ) aq
                    GROUP BY
                        aq.accountguid,
                        aq."Quarter"
                ) aq 
                ON aq.accountguid = account.accountguid AND aq."Quarter" = edwlabs.saleslab.shquarter(IFNULL(p.proceduredatelocal, p.SCHEDULEDSTARTDATE))


WHERE 
    EXISTS(SELECT accountguid FROM "EDW"."MASTER"."VW_ACCOUNT" account WHERE account.capitalregion = 'Asia : Korea' AND p.accountguid  = account.accountguid AND account.recordtype = 'Hospital')
    and IFNULL(p.proceduredatelocal, p.SCHEDULEDSTARTDATE) >= edwlabs.saleslab.THISQUARTERFIRSTDAY() 
    and IFNULL(p.proceduredatelocal, p.SCHEDULEDSTARTDATE) < dateadd(month, 3, edwlabs.saleslab.THISQUARTERFIRSTDAY())



"""

cs.execute(squery + name + aquery + query)
print('{} updated'.format(name))

name = 'edwlabs.saleslab.KoreaProcedureDurationSurgeonPopulation'

squery = '''
create or replace table
'''

aquery = ''' AS '''

query = """
SELECT 
    account.capitalregion AS "Region",
    p.casenumber as "CaseNumber",
    p.recordid as "ProcedureGUID",
    p.surgeonguid as "SurgeonGUID",
    p.businesscategoryname as "BusinessCategory",
    p.subject as "Subject",
    p.procedurename as "ProcedureName",
    p.proceduredurationinseconds / 60 AS "ProcedureMins",
    p.proceduredurationinseconds / 60 / 60 AS "ProcedureHour",
    p.proceduredatelocal as "ProcedureDateLocal",
    edwlabs.saleslab.shquarter(p.proceduredatelocal) AS "Quarter",
    p.status as "Status",
    contact.fullname as "SurgeonName",
    sp."SurgeonPopulation",
    sp."SurgeonPopulation (Runrate)",
    contact.specialtyname as "SurgeonSpeciality",
    account.ENDCUSTOMERSTATEPROVINCE AS "Prefecture",
    account.accountid as "AccountID",
    account.accountguid as "AccountGUID",
    account.accountname as "AccountName",
    account.customertype as "CustomerType",
    CASE 
        WHEN account.tier1 = TRUE THEN 'Tier 1'
        WHEN account.tier1 = FALSE THEN 'Non-Tier 1'
    END AS "Tier 1",
    accountrole.asdname as "ASDName",
    accountrole.asmname as "ASMName",
    accountrole.csdname as "CSDName",
    accountrole.csmname as "CSMName",
    accountrole.csrname as "CSRName",
    accountrole.ssmname as "SSMName",
    installedbase.name as "Name",
    installedbase.model as "Model",
    installedbase.installbaseguid AS "InstallBaseGUID",
    installedbase.financeinstalleddate as "FinanceInstalledDate",
    account.parentaccountname as "IDN",
    CASE 
        WHEN p.proceduredatelocal <= TO_DATE(dateadd(day, (-dayofweek(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) - 2), convert_timezone('America/Los_Angeles', 'Asia/Tokyo', current_timestamp()))) 
        	AND QUARTER(p.proceduredatelocal) = QUARTER(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) 
        	AND YEAR(p.proceduredatelocal) = YEAR(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) 
        	AND p.status = 'Completed' THEN 1 / forcal.dayssofar * forcal.workingday
        WHEN YEAR(p.proceduredatelocal) < YEAR(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) 
        	AND p.status = 'Completed' THEN 1
        WHEN QUARTER(p.proceduredatelocal) < QUARTER(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) 
        	AND p.status = 'Completed' THEN 1
        ELSE 0 
    END AS "Procedure Volume (RR for CQ)",
    CASE
        WHEN concat(p.businesscategoryname,p.Subject) in (
                 'General SurgeryVentral Hernia','General SurgeryInguinal Hernia','General SurgeryCholecystectomy'
                 ,'General SurgeryColon Resection','General SurgeryRectal Resection','General SurgeryCholecystectomy',
                 'GynecologyOophorectomy','GynecologyOther Gynecology','GynecologyHysterectomy - Malignant',
                 'GynecologyOvarian Cystectomy','GynecologySacrocolpopexy','GynecologyHysterectomy - Benign',
                 'GynecologyMyomectomy','GynecologyEndometriosis') then 'Growth'
else 'Others'
       
    END AS "Bucket"

FROM 
    "EDW"."PROCEDURES"."VW_PROCEDURES" p
        LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact ON p.surgeonguid = contact.CONTACTGUID
        LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON p.accountguid = account.ACCOUNTGUID
        LEFT JOIN "EDW"."MASTER"."VW_ACCOUNTTEAM" accountrole ON account.accountguid = accountrole.accountguid
        LEFT JOIN "EDW"."MASTER"."VW_INSTALLBASE" installedbase ON p.installedbaseguid = installedbase.installbaseguid
        
      
        CROSS JOIN (
            select * from table(edwlabs.saleslab.Korearunrate())
        ) forcal

		LEFT JOIN
		(
		SELECT
			surgeonpopulation."SurgeonGUID",
			surgeonpopulation."Quarter",
			CASE 
				WHEN COUNT(DISTINCT surgeonpopulation."ProcedureGUID") >= 13 THEN 'Sustainable+'
				WHEN COUNT(DISTINCT surgeonpopulation."ProcedureGUID") >= 7 THEN 'Bubble'
				WHEN COUNT(DISTINCT surgeonpopulation."ProcedureGUID") = 0 THEN 'Non-Active'
				ELSE 'Dabbler' 
			END AS "SurgeonPopulation",
			COUNT(DISTINCT surgeonpopulation."ProcedureGUID") AS "ProcedureVolume",
			CASE 
				WHEN SUM(surgeonpopulation."Procedure Volume (RR for CQ)") >= 13 THEN 'Sustainable+'
				WHEN SUM(surgeonpopulation."Procedure Volume (RR for CQ)") >= 7 THEN 'Bubble'
				WHEN SUM(surgeonpopulation."Procedure Volume (RR for CQ)") = 0 THEN 'Non-Active'
				ELSE 'Dabbler' 
			END AS "SurgeonPopulation (Runrate)",
			SUM(surgeonpopulation."Procedure Volume (RR for CQ)") AS "ProcedureVolumeRunrate"
		FROM
			(
			SELECT 
				p.recordid as "ProcedureGUID",
				edwlabs.saleslab.shquarter(p.proceduredatelocal) AS "Quarter",
				p.surgeonguid as "SurgeonGUID",
				CASE 
					WHEN p.proceduredatelocal <= TO_DATE(dateadd(day, (-dayofweek(convert_timezone('America/Los_Angeles', 'Asia/Tokyo', current_timestamp())) - 2), convert_timezone('America/Los_Angeles', 'Asia/Tokyo', current_timestamp()))) 
						AND QUARTER(p.proceduredatelocal) = QUARTER(convert_timezone('America/Los_Angeles', 'Asia/Tokyo', current_timestamp())) 
						AND YEAR(p.proceduredatelocal) = YEAR(convert_timezone('America/Los_Angeles', 'Asia/Tokyo', current_timestamp())) 
						AND p.status = 'Completed' THEN 1 / forcal.dayssofar * forcal.workingday
					WHEN YEAR(p.proceduredatelocal) < YEAR(convert_timezone('America/Los_Angeles', 'Asia/Tokyo', current_timestamp())) 
						AND p.status = 'Completed' THEN 1
					WHEN QUARTER(p.proceduredatelocal) < QUARTER(convert_timezone('America/Los_Angeles', 'Asia/Tokyo', current_timestamp())) 
						AND p.status = 'Completed' THEN 1
					ELSE 0 
				END AS "Procedure Volume (RR for CQ)"
			FROM 
				"EDW"."PROCEDURES"."VW_PROCEDURES" p
					LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account 
					ON p.accountguid = account.ACCOUNTGUID

					CROSS JOIN (
					select * from table(edwlabs.saleslab.japanrunrate())
					) forcal
			WHERE 
                account.recordtype = 'Hospital' 
				AND p.proceduredatelocal >= 
					CONCAT(to_char(Year(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) -4), '-01-01')
			) surgeonpopulation
		GROUP BY
			surgeonpopulation."SurgeonGUID",
			surgeonpopulation."Quarter"
		) sp 
		ON sp."SurgeonGUID" = p.surgeonguid 
		AND sp."Quarter" = edwlabs.saleslab.shquarter(p.proceduredatelocal)
WHERE 
    account.recordtype = 'Hospital' 
    AND p.proceduredatelocal >= 
		CONCAT(to_char(Year(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) -4), '-01-01')

"""

cs.execute(squery + name + aquery + query)
print('{} updated'.format(name))

name = 'edwlabs.saleslab.KoreaSocket'

squery = '''
create or replace table
'''

aquery = ''' AS '''

query = """
WITH
procedures AS (
	SELECT
		p.accountguid,
		COUNT(
            CASE 
                WHEN p.proceduredatelocal >= (edwlabs.saleslab.THISQUARTERFIRSTDAY())
                    and p.proceduredatelocal <= TO_DATE(dateadd(day, (-dayofweek(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) - 2), convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp()))) 
                THEN p.recordid
                ELSE NULL
            END) as "Completed Procedures",
        COUNT(
            CASE 
                WHEN p.proceduredatelocal >= (dateadd(month, -3, edwlabs.saleslab.THISQUARTERFIRSTDAY()))
                    and p.proceduredatelocal < (edwlabs.saleslab.THISQUARTERFIRSTDAY())
                THEN p.recordid
                ELSE NULL
            END) as "P.Q. Completed Procedures",
        COUNT(
            CASE 
                WHEN YEAR(p.proceduredatelocal) = YEAR(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp()))-1
                THEN p.recordid
                ELSE NULL
            END) as "P.Y. Completed Procedures"
	FROM 
		"EDW"."PROCEDURES"."VW_PROCEDURES" p
	LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account 
		ON p.accountguid = account.accountguid
	WHERE account.clinicalregion = 'Asia : Korea' 
		and account.recordtype = 'Hospital' 
		and p.status = 'Completed'
		and YEAR(p.proceduredatelocal) >= YEAR(convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp()))-1
	GROUP BY p.accountguid
),
opportunity_item_category_cpq AS (
    select 
        item_summary.*
    from
        (
        select
            opportunity.ACCOUNTID,
            opportunity.ACCOUNTGUID,
            opportunity.accountname,
            oli.SYSTEMSERIALNUMBERCPQ AS Systemname,
            case 
                when opportunity.GREENFIELD = true then 'Greenfield'
                else oli.dealtype
            end as category,
            ib.financeinstalleddate,
            ib.model,
            RANK() OVER (PARTITION BY opportunity.ACCOUNTGUID 
                         ORDER BY ib.financeinstalleddate DESC, oli.SYSTEMSERIALNUMBERCPQ
                        ) AS rank -- It's supposed that OPPORTUNITYLINEITEMID is numbered automatically. by taisei watanabe 2022/08/05
        from
            EDW.OPPORTUNITY.VW_OPPORTUNITYLINEITEM as oli
        inner join EDW.OPPORTUNITY.VW_OPPORTUNITY as opportunity
            on opportunity.OPPORTUNITYID = oli.OPPORTUNITYID
        LEFT JOIN EDW.MASTER.VW_INSTALLBASE AS ib
        	ON oli.SYSTEMSERIALNUMBERCPQ = ib.name
        	AND opportunity.ACCOUNTGUID = ib.accountguid
        where
            not (oli.SYSTEMSERIALNUMBERCPQ is null) 
            and oli.DELETED = false
            and oli.SYSTEMLINE = true
            and oli.DEALTYPE in ('Incremental') -- to decide the category, using only 'Trade-in' & 'Incremental'. by taisei watanabe 2023/02/09
            and oli.DEALTYPE is not null
        ) as item_summary
    where
        item_summary.rank = 1 -- This condition must be implemented to extract distinct 1 record for a account & a system name. by taisei watanabe 2023/01/25
        AND category in ('Incremental')
),
incremental_procedures AS (
    SELECT
        opportunity_item_category_cpq.*,
        COUNT(p.procedureguid) AS procedure_1year_before
    FROM 
        opportunity_item_category_cpq
    LEFT JOIN EDW.PROCEDURES.VW_PROCEDURESUMMARY AS p
        ON opportunity_item_category_cpq.accountguid = p.accountguid
        AND opportunity_item_category_cpq.financeinstalleddate > p.localproceduredate
        AND dateadd('year',-1,opportunity_item_category_cpq.financeinstalleddate) <= p.localproceduredate
    GROUP BY
        ALL
)


SELECT 
    account."AccountID",
    account.accountguid as "AccountGUID",
    account."AccountName", 
    thirdparty."TAM", 
    account."CustomerType", 
    account."Prefecture",
    account."ASDName",
    account."ASMName",
    account."CSDName",
    account."CSMName",
    account."CSRName",
    account."KAD",
    account."IDN",
    account."CUSTOMERSEGMENTATION",
    account."Tier 1",
    account.tiersegmentaion,
    account."Top 200",
    opportunity."MinCloseDate",
    edwlabs.saleslab.shquarter(opportunity."MinCloseDate") AS "Pipeline Quarter (Nearest)",
    e."Operational Roll Out",
    e."Clinical Validation",
    e."Executive Validation",
    e."Market Awareness",
    e."Quantify the Impact",
    ibcount."# GEN 4",
    ibcount."# GEN 3",
    runrate."Completed Procedures",
    runrate."LastUpdatedDay",
    runrate."WorkingDay",
    runrate."DaysSoFar",
    runrate."Runrate",
    runrate."P.Q. Completed Procedures" * 4 AS "LastQuarterProcedure * 4",
    runrate."P.Y. Completed Procedures" AS "LastYearProcedure",
    runrate."Runrate" * 4 AS "Runrate * 4",
    (runrate."Runrate" * 4 - (IFNULL(ibcount."# GEN 4", 0)+IFNULL(ibcount."# GEN 3",0)) ) AS "Overflow/Underflow - Runrate * 4",
    (runrate."P.Q. Completed Procedures" * 4 - (IFNULL(ibcount."# GEN 4", 0)+IFNULL(ibcount."# GEN 3",0)) ) AS "Overflow/Underflow - LastQuarterProcedure * 4",
    pathway."Discover",
    pathway."Validate",
    pathway."Contract",
    pathway."Deliver",
    pathway."Pivot",
    contact."Number of Surgeon Contacts",
    bam."BAM Volume (Past 1 Year)",
    incremental_procedures.procedure_1year_before,
    ibcount.latestinstalleddate
FROM (
		SELECT
			account.accountid as "AccountID",
			account.accountguid,
			account.accountname as "AccountName",
			account.customertype as "CustomerType",
            account.customersegmentation as "CUSTOMERSEGMENTATION",
			CASE 
				WHEN account.tiertype = 'Tier 1' THEN 'Tier 1'
                WHEN account.tiertype = 'Tier 2' THEN 'Tier 2'
				else 'Others'
			END AS "Tier 1",
            tier.tiersegmentaion,
			CASE WHEN account.japancustomersegment LIKE '%2022 Top 300 Greenfield%' THEN 'Top 200' ELSE 'Non-Top 200' END AS "Top 200", -- update to 2022 Top300 GF (2022/03/02 taisei watanabe)
			account.parentaccountname AS "IDN",
			accountrole.asdname as "ASDName",
			accountrole.asmname as "ASMName",
			accountrole.csdname as "CSDName",
			accountrole.csmname as "CSMName",
			accountrole.csrname as "CSRName",
            account.KEYCUSTOMERDIRECTORIDN as "KAD",
            account.ENDCUSTOMERSTATEPROVINCE AS "Prefecture"
		FROM
			"EDW"."MASTER"."VW_ACCOUNT" account
		LEFT JOIN "EDW"."MASTER"."VW_ACCOUNTTEAM" accountrole 
			ON account.accountguid = accountrole.accountguid
        LEFT JOIN EDWLABS.SALESLAB.KOREA_TIERSEGMANTATION2023 tier
            ON account.accountid = tier.accountid
		WHERE 
		 account.capitalregion = 'Asia : Korea' 
		 and account.recordtype = 'Hospital'
    ) account 

    LEFT JOIN(
        SELECT 
            thirdparty.accountguid,
            SUM(thirdparty.totalvolume) AS "TAM"
        FROM 
            (
                SELECT DISTINCT
                    thirdparty.AccountguID,
                    thirdparty.proceduresubject,
                    thirdparty.TOTALVOLUME,
                    thirdparty.date
                FROM
                    "EDW"."SALESSHARED"."VW_ACCOUNT3RDPARTYPROCEDURES" thirdparty 
                WHERE 
                    EXISTS (SELECT accountguid FROM "EDW"."MASTER"."VW_ACCOUNT" account WHERE account.ClinicalRegion = 'Asia : Korea' AND account.RecordType = 'Hospital' AND account.AccountGUID = thirdparty.AccountGUID) 
                    and thirdparty.Date = 
                        (
                        SELECT DISTINCT
                            MAX(thirdparty.Date)
                        FROM 
                            "EDW"."SALESSHARED"."VW_ACCOUNT3RDPARTYPROCEDURES" thirdparty 
                        WHERE 
                            EXISTS (SELECT accountguid FROM "EDW"."MASTER"."VW_ACCOUNT" account WHERE account.ClinicalRegion = 'Asia : Korea' AND account.RecordType = 'Hospital' AND account.AccountGUID = thirdparty.AccountGUID)
                        )
            ) thirdparty
              
        WHERE 
            thirdparty.date = 
                    (
                    SELECT DISTINCT
                        MAX(thirdparty.date)
                    FROM 
                        "EDW"."SALESSHARED"."VW_ACCOUNT3RDPARTYPROCEDURES" thirdparty 
                    WHERE 
                        EXISTS (SELECT accountguid FROM "EDW"."MASTER"."VW_ACCOUNT" account WHERE account.capitalregion = 'Asia : Korea' AND account.recordtype = 'Hospital' AND account.accountguid = thirdparty.accountguid)
                    )
            and EXISTS(SELECT accountguid FROM "EDW"."MASTER"."VW_ACCOUNT" account WHERE account.capitalregion = 'Asia : Korea' AND account.recordtype = 'Hospital' AND thirdparty.accountguid  = account.accountguid)
            //and d.dv21 = 'T'
        GROUP BY
            thirdparty.accountguid
    ) thirdparty 
    	ON account.accountguid = thirdparty.accountguid


    LEFT JOIN (
        SELECT
            "CloseDate" as "MinCloseDate",
            "AccountID",
            "AccountGUID",
            "OpportunityGUID"
        FROM 
            (
            SELECT
                opportunity.closedate as "CloseDate",
                opportunity.opportunityid as "OpportunityGUID",
                account.accountid as "AccountID",
                account.accountguid as "AccountGUID",
                ROW_NUMBER() OVER (PARTITION BY account.accountguid ORDER BY opportunity.closedate) as "seqnum"
            FROM 
                "EDW"."OPPORTUNITY"."VW_OPPORTUNITY" opportunity
                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON opportunity.accountguid = account.accountguid
            WHERE 
                account.endcustomercountry = 'South Korea' and opportunity.forecastcategory = 'Pipeline' and opportunity.closedate >=
                    (edwlabs.saleslab.thisquarterfirstday()) 
                --and opportunity.PRODUCTTYPE = 'System'--delete filter @tatsuro hidaka, 11/14 
            ) 
        WHERE 
            "seqnum" = 1
    ) opportunity 
    ON account.accountguid = opportunity."AccountGUID"


    LEFT JOIN (
        SELECT
            accountguid,
            SUM("Operational Roll Out") as "Operational Roll Out",
            SUM("Clinical Validation") as "Clinical Validation",
            SUM("Executive Validation") as "Executive Validation",
            SUM("Market Awareness") as "Market Awareness",
            SUM("Quantify the Impact") as "Quantify the Impact"
        FROM (
	            SELECT 
	                account.accountguid,
	                case when e.pillar = 'Operational Roll Out' then 1 else NULL end as "Operational Roll Out",
	                case when e.pillar = 'Clinical Validation' then 1 else NULL end as "Clinical Validation",
	                case when e.pillar = 'Executive Validation' then 1 else NULL end as "Executive Validation",
	                case when e.pillar = 'Market Awareness' then 1 else NULL end as "Market Awareness",
	                case when e.pillar = 'Quantify the Impact' then 1 else NULL end as "Quantify the Impact"
	            FROM 
	                "EDW"."TRAINING"."VW_EVENT" e
	                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account 
	                    ON e.accountid = account.accountguid 
	            WHERE 
	                EXISTS (SELECT opportunityid FROM "EDW"."OPPORTUNITY"."VW_OPPORTUNITY" opportunity WHERE opportunity.forecastcategory = 'Pipeline' and e.relatedtoid = opportunity.opportunityid)
	                and account.capitalregion = 'Asia : Korea' 
	                and account.recordtype = 'Hospital' 
	                and e.relatedobjecttype = 'Opportunity' 
	                and e.enddatetime >= dateadd(year, -1, convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) 
	                and e.eventstatus = 'Completed' 
	                and e.pillar IN('Clinical Validation', 'Executive Validation', 'Operational Roll Out', 'Market Awareness', 'Quantify the Impact')
            )
        GROUP BY 
            accountguid
    ) e 
    	on account.accountguid = e.accountguid

    LEFT JOIN (
		    SELECT
		        accountguid,
		        SUM("Discover") as "Discover",
		        SUM("Validate") as "Validate",
		        SUM("Contract") as "Contract",
		        SUM("Deliver") as "Deliver",
		        SUM("Pivot") as "Pivot"
		    FROM 
		        (SELECT 
		            account.accountguid,
		            case when e.pillar = 'Discover' then 1 else NULL end as "Discover",
		            case when e.pillar = 'Validate' then 1 else NULL end as "Validate",
		            case when e.pillar = 'Contract' then 1 else NULL end as "Contract",
		            case when e.pillar = 'Deliver' then 1 else NULL end as "Deliver",
		            case when e.pillar = 'Pivot' then 1 else NULL end as "Pivot"
		        FROM 
		            "EDW"."TRAINING"."VW_EVENT" e
		                LEFT JOIN "EDW"."OPPORTUNITY"."VW_MILESTONEPATHWAY" pathway ON e.relatedtoid = pathway.MILESTONEPATHWAYGUID
		                LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact ON pathway.surgeonguid = contact.contactguid
		                LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON contact.accountguid = account.accountguid
		        WHERE 
		            account.capitalregion = 'Asia : Korea' 
		            and account.recordtype = 'Hospital' 
		            and e.relatedobjecttype = 'Pathway' 
		            and e.enddatetime 
		                >= dateadd(year, -1, convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) 
		            and e.eventstatus = 'Completed' 
		            and e.pillar IN('Discover', 'Validate', 'Contract', 'Deliver', 'Pivot') 
		            and contact.isititle LIKE '%Surgeon%' 
		            and contact.ENDCUSTOMERMAILINGCOUNTRY = 'South Korea'
		        )
		    GROUP BY 
		        accountguid
    ) pathway 
    	on account.accountguid = pathway.accountguid

    LEFT JOIN (
		    SELECT
			    contact.accountguid,
			    COUNT(contact.accountid) as "Number of Surgeon Contacts"
		    FROM "EDW"."MASTER"."VW_CONTACT" contact
		    WHERE contact.ENDCUSTOMERMAILINGCOUNTRY = 'South Korea' and contact.status != 'Inactive' and contact.isititle LIKE '%Surgeon%'
		    GROUP BY contact.accountguid
    ) contact 
    	ON account.accountguid = contact.accountguid

    LEFT JOIN (
		    SELECT
			    ibcount."AccountGUID",
			    COUNT(ibcount."InstallBaseGUID") as "IBCount",
			    SUM(ibcount."Gen 4") as "# GEN 4",
			    SUM(ibcount."Gen 3") as "# GEN 3",
                MAX(ibcount.financeinstalleddate) AS latestinstalleddate
		    FROM (
				    SELECT
					    installedbase.accountguid AS "AccountGUID",
					    installedbase.installbaseguid as "InstallBaseGUID",
					    case when installedbase.model = 'da Vinci Xi' or installedbase.model = 'da Vinci X'  or installedbase.model = 'da Vinci SP' then 1 ELSE NULL END AS "Gen 4",
					    case when installedbase.model = 'da Vinci Si' or installedbase.model = 'da Vinci S' then 1 ELSE NULL END AS "Gen 3",
                        installedbase.financeinstalleddate
				    FROM 
				    	"EDW"."MASTER"."VW_INSTALLBASE" installedbase
				    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account 
				    	ON installedbase.accountguid = account.accountguid
				    WHERE installedbase.type = 'System' and installedbase.country = 'South Korea' and installedbase.systemuse = 'OR' and installedbase.financedeinstalleddate IS NULL
				    and installedbase.inactive = FALSE and account.capitalregion = 'Asia : Korea' and account.recordtype = 'Hospital'
		    	) ibcount
		    GROUP BY
		    	ibcount."AccountGUID"
    	) ibcount ON ibcount."AccountGUID" = account.accountguid


    FULL JOIN (
		    SELECT
				account.accountid as "AccountID",
				account.accountguid,
				procedures."Completed Procedures",
				procedures."P.Q. Completed Procedures",
				procedures."P.Y. Completed Procedures",
				forcal.LASTUPDATEDAY as "LastUpdatedDay",
				forcal.workingday as "WorkingDay",
				forcal.dayssofar as "DaysSoFar",
				(procedures."Completed Procedures" / forcal.dayssofar * forcal.workingday) as "Runrate"
			FROM 
			  "EDW"."MASTER"."VW_ACCOUNT" account
			LEFT JOIN procedures
				ON account.accountguid = procedures.accountguid

			CROSS JOIN (
					select * from table(edwlabs.saleslab.japanrunrate())
			) forcal
			WHERE account.capitalregion = 'Asia : Korea' 
				and account.recordtype = 'Hospital'
    ) runrate 
    	ON account.accountguid = runrate.accountguid

    LEFT JOIN (
		    SELECT
				bam."AccountGUID",
				COUNT(bam.activityid) as "BAM Volume (Past 1 Year)"
		    FROM (
		      SELECT
		          e.activityid,
		          e.accountid as "AccountGUID"
		      FROM 
		        "EDW"."TRAINING"."VW_EVENT" e
		      LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account 
		      	ON e.accountid = account.accountguid
		      WHERE 
		        EXISTS (SELECT opportunityid FROM "EDW"."OPPORTUNITY"."VW_OPPORTUNITY" opportunity WHERE e.relatedtoid = opportunity.opportunityid)
		        and account.capitalregion = 'Asia : Korea' 
		        and account.recordtype = 'Hospital' 
		        and e.relatedobjecttype = 'Opportunity' 
		        and convert_timezone('GMT', 'Asia/Seoul', e.enddatetime) >= dateadd(year, -1, convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp())) 
		        and e.eventstatus = 'Completed' 
		        and e.pillar = 'Executive Validation'
		        and e.salesstep = 'Business Alignment Meeting' 
		        and convert_timezone('GMT', 'Asia/Seoul', e.enddatetime) >= '2021-02-08'
		      ) bam
		    GROUP BY bam."AccountGUID"
    ) bam 
    	ON bam."AccountGUID" = account.accountguid
    LEFT JOIN incremental_procedures
        ON incremental_procedures.accountguid = account.accountguid

 

"""

cs.execute(squery + name + aquery + query)
print('{} updated'.format(name))

name = 'edwlabs.saleslab.KoreaAccountUtilization'

squery = '''
create or replace table
'''

aquery = ''' AS '''

query = """
WITH calendar_tmp AS (
    SELECT DISTINCT
        ib.ACCOUNTGUID,
        ib.accountname,
        ib.name AS "SystemName",
        ib.model AS "SystemModel",
        main."Year-Quarter" AS quarter,
        main.CURRENT_QUARTER_FLAG
    FROM EDW.MASTER.VW_INSTALLBASE AS ib
    CROSS JOIN (
        SELECT DISTINCT
            YEAR(calendar_dt) || '-Q' || QUARTER(calendar_dt) AS "Year-Quarter",
            CURRENT_QUARTER_FLAG
        FROM EDW.MASTER.VW_DIMDATE
        WHERE YEAR(calendar_dt) BETWEEN 2000 AND YEAR(current_date)
    ) main
    WHERE ib.accountguid IN (
        SELECT accountguid 
        FROM EDW.MASTER.VW_ACCOUNT 
        WHERE clinicalregion = 'Asia : Korea'
    )
),
Fleet_Customer_historical AS (
    SELECT DISTINCT
        ct.quarter,
        ct.CURRENT_QUARTER_FLAG,
        ct.ACCOUNTGUID,
        ct."SystemName",
        ct."SystemModel",
        Ifnull(COUNT(ib.INSTALLBASEGUID),0) AS number_of_ib,
        CASE 
            WHEN COUNT(ib.INSTALLBASEGUID) > 1 THEN 'Fleet'
            ELSE 'Others'
        END AS Fleet_or_Not,
        LISTAGG(DISTINCT ib.model, ', ') WITHIN GROUP (ORDER BY ib.model DESC) AS combination
    FROM calendar_tmp AS ct
    LEFT JOIN EDW.MASTER.VW_INSTALLBASE AS ib ON ct.quarter >= (
        CASE 
            WHEN ib.accountid = '21191' AND ib.name = 'SG757' THEN '2012-Q1'
            WHEN ib.accountid = '115501' AND ib.name = 'SG757' THEN '2018-Q2'
            WHEN ib.accountid = '21179' AND ib.name = 'SK1689' THEN '2023-Q1'
            WHEN ib.accountid = '26465' AND ib.name = 'SK1689' THEN '2018-Q1'
            WHEN ib.accountid = '117611' AND ib.name = 'SP0036' THEN '2019-Q1'
            WHEN ib.accountid = '21194' AND ib.name = 'SP0036' THEN '2018-Q4'
            WHEN ib.accountid = '21183' AND ib.name = 'SP0040' THEN '2019-Q1'
            WHEN ib.name = 'SG838' THEN '2010-Q4'
            WHEN ib.name = 'SH2309' THEN '2020-Q4'
            WHEN ib.name = 'RSH0139' AND ib.inactive THEN CONCAT(YEAR(ib.FINANCEINSTALLEDDATE), '-Q', QUARTER(ib.FINANCEINSTALLEDDATE))
            WHEN ib.name = 'RSH0139' AND NOT ib.inactive THEN CONCAT(YEAR(ib.installdate), '-Q', QUARTER(ib.installdate))
            WHEN ib.FINANCEINSTALLEDDATE IS NULL THEN '9999-Q4'
            ELSE CONCAT(YEAR(ib.FINANCEINSTALLEDDATE), '-Q', QUARTER(ib.FINANCEINSTALLEDDATE))
        END
    ) AND ct.quarter < (
        CASE 
            WHEN ib.name = 'RSH0139' AND ib.inactive THEN CONCAT(YEAR(ib.removedate), '-Q', QUARTER(ib.removedate))
            WHEN ib.name = 'SH2009' THEN CONCAT(YEAR(ib.removedate), '-Q', QUARTER(ib.removedate))
            WHEN ib.name = 'RSH0175' THEN CONCAT(YEAR(ib.removedate), '-Q', QUARTER(ib.removedate))
            WHEN ib.FINANCEDEINSTALLEDDATE IS NULL THEN '9999-Q4'
            ELSE CONCAT(YEAR(ib.FINANCEDEINSTALLEDDATE), '-Q', QUARTER(ib.FINANCEDEINSTALLEDDATE))
        END
    ) AND ct.ACCOUNTGUID = ib.accountguid AND ct."SystemName" = ib.name
     WHERE ib.type = 'System'
      //AND ac.accounttype NOT IN ('Internal', 'Distributor')
      AND ib.model <> 'da Vinci'
      AND ib.systemuse = 'OR'
    GROUP BY ct.quarter, ct.CURRENT_QUARTER_FLAG, ct.ACCOUNTGUID, ct."SystemName",ct."SystemModel"
)
SELECT DISTINCT
    main11.quarter,
    main11.accountguid,
    main11.accountname,
    main11.CURRENT_QUARTER_FLAG,
    main11."SystemName",
    main11."SystemModel",
    ib2."InstalldateQ",
    PQIB.PQ_IB,  -- Using LAG from CQ_IB
    CQIB.CQ_IB,
    proc.CQ_AS,
    PQAS.PQ_AS,
    PQAS.PQ_RR,
    proc.CQ_RR,     
    CASE
        WHEN PQIB.PQ_IB = 0 AND CQ_RR = 0 THEN 0
        WHEN PQIB.PQ_IB = 0 AND CQ_RR >= 1 THEN CQ_RR / COALESCE(PQIB.PQ_IB, 1)
        ELSE CQ_RR / COALESCE(PQIB.PQ_IB, 1)
    END AS "SystemUtilization"
FROM (
    SELECT
        main3."Year-Quarter" AS quarter,
        main3.CURRENT_QUARTER_FLAG,
        main2.ACCOUNTGUID,
        main2.accountname,
        main2."SystemName",
        main2."SystemModel"
    FROM (
        SELECT
            ib.ACCOUNTGUID,
            ib.accountname,
            ib.name AS "SystemName",
            ib.model AS "SystemModel"
        FROM EDW.MASTER.VW_INSTALLBASE AS ib
        WHERE ib.accountguid IN (
            SELECT accountguid 
            FROM EDW.MASTER.VW_ACCOUNT 
            WHERE clinicalregion = 'Asia : Korea'
        )
    ) main2
    CROSS JOIN (
        SELECT DISTINCT
            YEAR(calendar_dt) || '-Q' || QUARTER(calendar_dt) AS "Year-Quarter",
            CURRENT_QUARTER_FLAG
        FROM EDW.MASTER.VW_DIMDATE
        WHERE YEAR(calendar_dt) BETWEEN 2000 AND 2025
    ) main3
) main11
LEFT JOIN (
    SELECT distinct
        Year(pr.localproceduredate) || '-Q' || Quarter(pr.localproceduredate) AS YearQuarter,
        pr.accountguid,
        pr.accountname,
        pr.systemname,
        COUNT(DISTINCT pr.systemname) AS CQ_AS, -- Current Active System
        SUM(pr.runrate) AS CQ_RR
    FROM EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
    WHERE clinicalregion = 'Asia : Korea'
    GROUP BY Year(pr.localproceduredate) || '-Q' || Quarter(pr.localproceduredate), pr.accountguid, pr.accountname, pr.systemname
) proc ON proc.YearQuarter = main11.quarter AND main11.accountguid = proc.accountguid AND main11."SystemName" = proc.systemname
LEFT JOIN (
    SELECT fc.ACCOUNTGUID, 
           fc."SystemName",
           fc.quarter, 
           fc.CURRENT_QUARTER_FLAG,
           fc.NUMBER_OF_IB AS CQ_IB,
           //LAG(fc.NUMBER_OF_IB, 1) OVER (PARTITION BY fc.ACCOUNTGUID ORDER BY fc.CURRENT_QUARTER_FLAG) AS PQ_IB -- Get PQ_IB from previous quarter's CQ_IB
    FROM Fleet_Customer_historical fc
    ORDER BY fc.CURRENT_QUARTER_FLAG, fc.ACCOUNTGUID
) CQIB ON CQIB.ACCOUNTGUID = main11.accountguid AND CQIB.quarter = main11.quarter AND CQIB."SystemName" = main11."SystemName"
LEFT JOIN (
    SELECT fc.ACCOUNTGUID, 
           fc."SystemName",
           fc.quarter, 
           fc.CURRENT_QUARTER_FLAG,
           fc.NUMBER_OF_IB AS PQ_IB,
           //LAG(fc.NUMBER_OF_IB, 1) OVER (PARTITION BY fc.ACCOUNTGUID ORDER BY fc.CURRENT_QUARTER_FLAG) AS PQ_IB -- Get PQ_IB from previous quarter's CQ_IB
    FROM Fleet_Customer_historical fc
    ORDER BY fc.CURRENT_QUARTER_FLAG, fc.ACCOUNTGUID 
) PQIB ON PQIB.ACCOUNTGUID = main11.accountguid AND PQIB.CURRENT_QUARTER_FLAG+1 = main11.CURRENT_QUARTER_FLAG AND PQIB."SystemName" = main11."SystemName"
LEFT JOIN (
    SELECT
        main1.YearQuarter,
        main1.accountguid,
        main1.accountname,
        main1.sfdccurrentquarterflag,
        main1.systemname,
        main1.CQ_AS AS PQ_AS,
        main1."RunRate" AS PQ_RR
    FROM (
        SELECT Year(localproceduredate) || '-Q' || Quarter(localproceduredate) AS YearQuarter,
               accountguid,
               accountname,
               sfdccurrentquarterflag,
               systemname,
               COUNT(DISTINCT systemname) AS CQ_AS, -- Current Active System
               SUM(runrate) AS "RunRate"
        FROM EDW.PROCEDURES.VW_PROCEDURESUMMARY
        WHERE clinicalregion = 'Asia : Korea'
        GROUP BY Year(localproceduredate) || '-Q' || Quarter(localproceduredate), accountguid, accountname, sfdccurrentquarterflag, systemname
    ) main1
) PQAS ON PQAS.ACCOUNTGUID = main11.accountguid AND PQAS.sfdccurrentquarterflag+1 = main11.CURRENT_QUARTER_FLAG AND PQAS.systemname = main11."SystemName"
LEFT JOIN 
    (SELECT distinct
            ib.name,
            ib.accountguid,
            CASE
             WHEN ib.accountid = '21191' AND ib.name = 'SG757' THEN '2012-Q1'
            WHEN ib.accountid = '115501' AND ib.name = 'SG757' THEN '2018-Q2'
            WHEN ib.accountid = '21179' AND ib.name = 'SK1689' THEN '2023-Q1'
            WHEN ib.accountid = '26465' AND ib.name = 'SK1689' THEN '2018-Q1'
            WHEN ib.accountid = '117611' AND ib.name = 'SP0036' THEN '2019-Q1'
            WHEN ib.accountid = '21194' AND ib.name = 'SP0036' THEN '2018-Q4'
            WHEN ib.accountid = '21183' AND ib.name = 'SP0040' THEN '2019-Q1'
            WHEN ib.name = 'SG838' THEN '2010-Q4'
            WHEN ib.name = 'SH2309' THEN '2020-Q4'
            WHEN ib.name = 'RSH0139' AND ib.inactive THEN CONCAT(YEAR(ib.FINANCEINSTALLEDDATE), '-Q', QUARTER(ib.FINANCEINSTALLEDDATE))
            WHEN ib.name = 'RSH0139' AND NOT ib.inactive THEN CONCAT(YEAR(ib.installdate), '-Q', QUARTER(ib.installdate))
            WHEN ib.FINANCEINSTALLEDDATE IS NULL THEN '9999-Q4'
            ELSE CONCAT(YEAR(ib.FINANCEINSTALLEDDATE), '-Q', QUARTER(ib.FINANCEINSTALLEDDATE)) END AS "InstalldateQ"
            
    FROM
            EDW.MASTER.VW_INSTALLBASE ib) ib2 ON main11."SystemName" = ib2.name AND main11.accountguid = ib2.accountguid



WHERE 
main11."SystemModel" NOT IN ('da Vinci SimNow','Intuitive Hub')
AND LEFT(main11.quarter,4) >= 2000
"""

# updated by 241213 by bogyeom adding Mia
cs.execute(squery + name + aquery + query)
print('{} updated'.format(name))

name = 'edwlabs.saleslab.Korea_MIA_Data'

squery = '''
create or replace table
'''

aquery = ''' AS '''

query = """
with users as
 (
 
  select 
  sfdc_contact_id,
  uma_user_id,
  contact_name,
  Surgeon_specialty,
  registration_completed_date,
  isi_title,
  mailing_country
  from "PRODUCT_ANALYTICS_LAB"."COMMON"."DIM_USER"
 ),
  
 procedures as
 
 (
  select a.procedure_name, a.subject__c, b.system_name, a.procedure_date_local, c.sfdc_account_id, c.account_name, a.casenumber, c.BILLING_COUNTRY, aa.runrate, aa.Category, aa.customersegmentation, aa.ProcedureSubject, aa.ClinicalRegion, aa.calqtroffset, aa.CVP, aa.CSD, aa.ugsrname, aa.spsmname, aa.tiertype, aa.systemmodelname
  from "PRODUCT_ANALYTICS_LAB"."COMMON"."FCT_PROCEDURE" a,
  "PRODUCT_ANALYTICS_LAB"."COMMON"."DIM_SYSTEM_INSTALL" b,
   "PRODUCT_ANALYTICS_LAB"."COMMON"."DIM_ACCOUNT" c,
   "EDW"."PROCEDURES"."VW_PROCEDURESUMMARY" aa
        
  where a.sk_system_install_id = b.sk_system_install_id
  and a.SK_ACCOUNT_ID = c.sk_account_id
  and a.procedure_name = aa.casenumber

and year(aa.localproceduredate)>2022

  ), 
  
 user_cnts as
(
  select f.sk_user_id
         ,d.sfdc_contact_id
         , contact_name
         , count(distinct f.casenumber) cnts
  from "PRODUCT_ANALYTICS_LAB"."COMMON"."FCT_PROCEDURE" f
      join "PRODUCT_ANALYTICS_LAB"."COMMON"."DIM_USER" d on f.SK_USER_ID = d.SK_USER_ID
  where --quarter(to_date(PROCEDURE_DATE_LOCAL)) = 4 and year(to_date(PROCEDURE_DATE_LOCAL)) = 2021
        to_date(PROCEDURE_DATE_LOCAL) >= dateadd(quarter, -1,date_trunc('quarter', current_date))
        and to_date(PROCEDURE_DATE_LOCAL) <= dateadd(day, -1,date_trunc('quarter', current_date))
  group by 1, 2,3
)
, productivity_groups as
(
  select t.*
          , case when t.cnts >= 70 then 'Super Mega User'
               when t.cnts>=50 and t.cnts<=69 then 'Mega User'
               when t.cnts>=30 and t.cnts<=49 then 'Super User'
               when t.cnts>=13 and t.cnts<=29 then 'Sustainable User'
               when t.cnts>=7 and t.cnts<=12 then 'Bubble'
               when t.cnts>=1 and t.cnts<=6 then 'Dabbler'
               else 'Untrained/Stalled' end as productivity_group
  from user_cnts t
 ),
 
 cloud_data as
 (
   select * from "PRODUCT_ANALYTICS_LAB"."ANALYTICS"."VW_ADDITIONAL_SURGEON_DETAILS" 
 ),
 
 case_details_usage as (
        select u.sk_user_id, f.umaid_derived, f.class casenumber
                , min(event_date) first_viewed_date
                , sum(1) number_of_views_bySurgeon
                , count(distinct INTUITIVESESSIONID) number_of_sessions_bySurgeon
        from "PRODUCT_ANALYTICS_LAB"."ANALYTICS"."FACT_WEB_ANALYTICS" f 
                join "PRODUCT_ANALYTICS_LAB"."COMMON"."DIM_USER" u on f.umaid_derived = u.UMA_USER_ID --use uma dervied for accuracy
        where f.APPNAME_OUTER = 'MIA'
                and f.kingdom = 'case_details'
                and f.family = 'load' -- only page views
                and f.class is not null -- filer records with null casenumber
        group by 1,2,3
   ),

morpheus_cl_name_only as 
(
    select mp.procedureid as morpheus_proc_id, sfa.remotefe_procedure_id__c, sfa.name, sfa.procedure_date_gmt__c, sfa.procedure_date_local__c, mp.logontimestampgmt, mp.logofftimestampgmt, ps.starttimegmt, timediff(minute, ps.starttimegmt, logontimestampgmt) as timediff_between_sources, s.cloud_login_enabled_date
    from morpheus.morpheus.surgeonlogeventbyprocedureid mp
    join morpheus.morpheus.proceduresummaries ps on mp.procedureid = ps.procedureid
    left join procedures360.meta_gxp.procedures mgp on mgp.caseuid = ps.caseuid -- only dv4 procs
    --left join dvstream.rfe.procedures p on mgp.meta_procedureid=p.procedureid
    left join SFDC.SFDC.SFA_PROCEDURES__C sfa on sfa.remotefe_procedure_id__c=mgp.meta_procedureid --p.procedureid -- to get cases only in sfdc
    left join product_analytics_lab.common.dim_system s on s.system_name = ps.systemname -- to get CLE date
    where mp.uuid > 0
 
    and timediff(minute, ps.starttimegmt, logontimestampgmt) >= -120
    and sfa.procedure_date_gmt__c >=s.cloud_login_enabled_date
    --and sfa.casenumber is not null -- if filtering by status__c we dont need to filter by casenumber is not null
    and sfa.status__c in ('OnSite','Complete','Completed')
),
 
 final as 
 (select a.*
  , b.registration_completed_date
  , b.isi_title
  , b.surgeon_specialty
  , b.mailing_country
  , c.subject__c
  , c.Category 
  , c.ProcedureSubject
  , c.ClinicalRegion
  , c.ugsrname
  , c.spsmname
  , c.CSD
  , c.runrate
  , c.CVP
  , c.system_name
  , c.sfdc_account_id
  , c.account_name
  , c.billing_country
  , c.systemmodelname
  , c.customersegmentation
  , c.tiertype
  , c.calqtroffset
  , d.productivity_group
  , e.first_viewed_date
  , e.number_of_views_bysurgeon
  , e.number_of_sessions_bysurgeon

  , m.name as morpheus_casenumber
  , m.procedure_date_local__c as morphues_procedure_date_local
  , case when (date_part(year, to_date(a.procedure_date_local__c)) = date_part(year, current_date()) 
  and date_part(quarter, current_date()-90) = date_part(quarter, to_date(a.procedure_date_local__c))) or 
  
  (date_part(year, to_date(a.procedure_date_local__c)) = date_part(year, current_date()-365) 
  and date_part(quarter, current_date()) in('1') and date_part(quarter, to_date(a.procedure_date_local__c)) = '4')
  then 1 else 0 end as prior_completed_quarter_flag
  
  
   , case when (date_part(year, to_date(a.procedure_date_local__c)) = date_part(year, current_date()) 
  and date_part(quarter, current_date()-180) = date_part(quarter, to_date(a.procedure_date_local__c))) or 
  
  (date_part(year, to_date(a.procedure_date_local__c)) = date_part(year, current_date()-365) 
  and date_part(quarter, current_date()) in('1') and date_part(quarter, to_date(a.procedure_date_local__c)) = '3') or
  
  (date_part(year, to_date(a.procedure_date_local__c)) = date_part(year, current_date()-365) 
  and date_part(quarter, current_date()) in('2') and date_part(quarter, to_date(a.procedure_date_local__c)) = '4')
  
  
  then 1 else 0 end as two_prior_completed_quarters_flag
  ,'Q'||date_part(quarter,to_date(a.first_cloud_login))||'-'||date_part(year,to_date(a.first_cloud_login)) as First_Cloud_Login_Quarter
 from cloud_data a
  
 left join users b on a.sfdc_contact_id = b.sfdc_contact_id
 left join procedures c on a.casenumber = c.procedure_name
 left join productivity_groups d on a.sfdc_contact_id = d.sfdc_contact_id
 left join case_details_usage e on e.umaid_derived = b.uma_user_id  and e.casenumber = c.casenumber
 left join morpheus_cl_name_only m on m.name = a.casenumber 
 where a.sfdc_contact_id not in ('368855','41287','25033','356385','25034')
  //left join 
 )
 
 select a.*, b.registration_count  from final a
cross join 
(select count(sfdc_contact_id) as registration_count from "PRODUCT_ANALYTICS_LAB"."COMMON"."DIM_USER"
where registration_completed_date is not null and mia_invited_flag = 1 
) b

WHERE a.clinicalregion = 'Asia : Korea'
 --where a.cloud_login_case||a.cloud_eligible_case <> 'NN'
"""

# updated by 241213 by bogyeom adding Mia
cs.execute(squery + name + aquery + query)
print('{} updated'.format(name))

# query = '''
# SELECT DISTINCT
# MAX("EXTRACTEDDATE")
# FROM EDWLABS.SALESLAB.Korea_Historical_Event_Data
# '''
#
# output = cs.execute(query)
# data = output.fetchall()
# columnnames = [a_tuple[0] for a_tuple in output.description]
# df = pd.DataFrame(data, columns=columnnames)
# cro_date = df.iloc[0]['MAX("EXTRACTEDDATE")']
###
name = 'EDWLABS.SALESLAB.Korea_Historical_Event_Data'

squery = '''
Insert into
'''
aquery = '''  '''

query = '''
with 
Opportunity_LineItem_Info as (
    select
        oli.OPPORTUNITYID,
        listagg(distinct oli.dealtype_aligned, ',') within group (order by oli.dealtype_aligned) as deal_type_d, 
        listagg(distinct oli.product_type_name, ',') within group (order by oli.product_type_name) as product_type_name_d, -- this column is needed to ignore the sort of same kind of Product name. (commented by taisei watanabe @2022/10/25)
        case 
            when product_type_name_d like 'da Vinci SP%da Vinci Xi%da Vinci Xi%' then 'da Vinci SP & da Vinci Xi'
            when product_type_name_d like 'da Vinci SP%da Vinci X%da Vinci Xi%' then 'da Vinci SP & da Vinci X & da Vinci Xi'   
            when product_type_name_d like 'da Vinci SP%da Vinci Xi%' then 'da Vinci SP & da Vinci Xi'   
            when product_type_name_d like 'da Vinci SP%da Vinci X%' then 'da Vinci SP & da Vinci X'   
            when product_type_name_d like 'da Vinci Xi%da Vinci Xi%' then 'da Vinci Xi'   
            when product_type_name_d like 'da Vinci X%da Vinci Xi%' then 'da Vinci X & da Vinci Xi'   
            when product_type_name_d like 'da Vinci Xi%' then 'da Vinci Xi' 
            when product_type_name_d like 'da Vinci X%' then 'da Vinci X' 
            when product_type_name_d like 'da Vinci SP%' then 'da Vinci SP'
            else 'Others'
        end product_type
    from
        (
        select
            item_tmp.*,
            case 
                when item_tmp.SYSTEMCONFIGURATIONCPQ like 'da Vinci Xi%' then 'da Vinci Xi' 
                when item_tmp.SYSTEMCONFIGURATIONCPQ like 'da Vinci X%' then 'da Vinci X' 
                when item_tmp.SYSTEMCONFIGURATIONCPQ like 'da Vinci SP%' then 'da Vinci SP'
                else null
            end as product_type_name,
            case 
                when item_tmp.dealtype = 'Trade-In' then 'Trade-in'
                else item_tmp.dealtype
            end dealtype_aligned -- Patching the master data bug... (commented by taisei watanabe @2022/10/25)
        from
            EDW.OPPORTUNITY.VW_OPPORTUNITYLINEITEM as item_tmp
        ) as oli
    where
        oli.DELETED = false
        and oli.SYSTEMLINE = true
        and oli.dealtype <> 'Non System Deal'
    group by
        oli.OPPORTUNITYID
)


//Capital側SQL
SELECT
    'Capital' as CAPITAL_OR_CLINICAL,
    convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp()) as ExtractedDate,
    eventt.activityid,
    eventt.relatedtoid,
    eventt.relatedobjecttype as RelatedObjectType,
    opportunity.name as OpportunityName,
    opportunity.closedate as CloseDate,
    edwlabs.saleslab.shquarter(opportunity.closedate) AS CloseQuarter,
    opportunity.deleted as OPPORTUNITY_ISDELETED,
    opportunity.status as OPPORTUNITY_STATUS,
    opportunity.createddate as CreatedDate,
    opportunity.closed as ClosedFlag,
    opportunity.won as IsWonFlag,
    opportunity.forecastcategory as ForecastCategory,
    opportunity.forecastcategoryname as ForecastCategoryName,
    opportunity.transactionstructure as TransactionStructure,
    opportunity.avpcall as AVPCall,
    opportunity.stage AS Color,
    opportunity.amount as Amount,
    opportunity.expectedamount as ExpectedAmount,
    opportunity.probability as ProbabilityPercent,
    opportunity.quantity as Quantity,
    opportunity.financingpartnername as FinancePartner,
    opportunity.budgetcycle as BudgetCycle,
    opportunity.money as Money,
    opportunity.method as Method,
    oli.product_type as ProductName,              -- After CPQ implementation, opportunity.product was de-activated. (commented by taisei watanabe @2022/10/25)
    opportunity.productfamily as ProductFamily,   -- After CPQ implementation, this column was de-activated. (commented by taisei watanabe @2022/10/25)
    opportunity.producttype as ProductType,       -- After CPQ implementation, this column was de-activated. (commented by taisei watanabe @2022/10/25)
    case 
        WHEN opportunity.GREENFIELD = true then 'Greenfield'
        else oli.DEAL_TYPE_D 
    end as OpportunityType,                       -- After CPQ implementation, opportunity.opportunitytype was de-activated. (commented by taisei watanabe @2022/10/25)
   
   
    CASE
        WHEN opportunity.GREENFIELD <> true AND account.tiertype = 'Tier 1' THEN 'Existing - Tier 1'
        WHEN opportunity.GREENFIELD <> true AND account.tiertype <> 'Tier 1' THEN 'Existing - Non-Tier 1'
        WHEN opportunity.GREENFIELD = true AND account.tiertype = 'Tier 1' THEN 'Greenfield - Tier 1'
        WHEN opportunity.GREENFIELD = true AND account.japancustomersegment LIKE '%2022 Top 300 Greenfield%' THEN 'Greenfield - Top 300'
        WHEN opportunity.GREENFIELD = true THEN 'Greenfield - Other GF'
        ELSE 'Others' 
    END AS CapitalCustomerType, -- this column is needed to update ever year because the definition of Top GF is updated annualy. (commented by taisei watanabe @2022/10/25)
    opportunity.accountguid,
    account.accountname as AccountName,
    account.customertype as CustomerType,
    CASE 
        WHEN account.tiertype = 'Tier 1' THEN 'Tier 1'
        WHEN account.tiertype <> 'Tier 1' THEN 'Non-Tier 1'
    END AS top_Tier,
    CASE 
        WHEN account.japancustomersegment LIKE '%2022 Top 300 Greenfield%' THEN 'Top 300' 
        ELSE 'Non-Top 300' 
    END AS Top_GF, -- Column name couldn't be updated. (by taisei watanabe @2022/10/14) 
    eventt.subject,
    eventt.assignedtoid,
    assignedto.fullname as AssignedTo,
    eventt.contact,
    contactperson.fullname AS ContactPersonName,
    contactperson.specialtyname AS SurgeonSpeciality,
    contactperson.surgeoncategory AS SurgeonCategory,
    null as type,                             -- this column is prepared for clinical team.
    null as SurgeonGUID,                      -- this column is prepared for clinical team.
    null as event_isdeleted,                  -- this column is prepared for clinical team.
    null as PathwayName_or_OpporunityName,    -- this column is prepared for clinical team.
    null as PathwayType,                      -- this column is prepared for clinical team.
    eventt.createdbyid,                       
    createdby.fullname as CreatedBy,
    eventt.salesstep as SalesStep,
    convert_timezone('GMT', 'Asia/Seoul', eventt.eventenddatetime) as EventEndDateTime,
    eventt.eventstatus,
    eventt.description,
    eventt.pillar,
    eventt.salessteptype,
    eventt.priority
FROM 
    "EDW"."TRAINING"."VW_EVENT" eventt 
        LEFT JOIN (
        SELECT DISTINCT
         opportunity.opportunityid,
         opportunity.name,
         opportunity.accountguid,
         opportunity.closedate,
         opportunity.deleted,
         opportunity.status,
         opportunity.createddate,
         opportunity.closed,
         opportunity.won,
         opportunity.forecastcategory,
         opportunity.forecastcategoryname,
         opportunity.transactionstructure,
         opportunity.avpcall,
         opportunity.stage,
         opportunity.amount,
         opportunity.expectedamount,
         opportunity.probability,
         opportunity.quantity,
         opportunity.financingpartnername,
         opportunity.budgetcycle,
         opportunity.money,
         opportunity.method,
         opportunity.product,
         opportunity.productfamily,
         opportunity.producttype,
         opportunity.opportunitytype,
         opportunity.GREENFIELD
        FROM 
         EDW.OPPORTUNITY.VW_OPPORTUNITY as opportunity
        ) as opportunity
        ON eventt.relatedtoid = opportunity.opportunityid
        LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contactperson on contactperson.contactguid = eventt.contact
        LEFT JOIN "EDW"."MASTER"."VW_USER" assignedto ON eventt.assignedtoid = assignedto.userid
        LEFT JOIN "EDW"."MASTER"."VW_USER" createdby on createdby.userid = eventt.createdbyid
        LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account on account.accountguid = opportunity.accountguid
        
        LEFT JOIN Opportunity_LineItem_Info as oli 
        on eventt.relatedtoid = oli.opportunityid
WHERE 
    eventt.relatedobjecttype = 'Opportunity' 
    and account.capitalregion in ('Asia : Korea') 
    and convert_timezone('GMT', 'Asia/Seoul', eventt.eventenddatetime) >= edwlabs.saleslab.THISQUARTERFIRSTDAY() -- 今四半期のActivityをログとして取得する。
    and convert_timezone('GMT', 'Asia/Seoul', eventt.eventenddatetime) < dateadd(month, 3, edwlabs.saleslab.THISQUARTERFIRSTDAY())

union all

//Clinical側SQL
SELECT
    'Clinical' as CAPITAL_OR_CLINICAL,
    convert_timezone('America/Los_Angeles', 'Asia/Seoul', current_timestamp()) AS ExtractedDate,
    e.activityid,
    e.relatedtoid,
    e.RelatedObjectType AS "RelatedObjectType",
    null as "OpportunityName",
    null as "CloseDate",
    null as "CloseQuarter",
    null as "IsDeleted",
    null as "Stage",
    null as "CreatedDate",
    null as "ClosedFlag",
    null as "IsWonFlag",
    null as "ForecastCategory",
    null as "ForecastCategoryName",
    null as "TransactionStructure",
    null as "AVPCall",
    null as "Color",
    null as "Amount",
    null as "ExpectedAmount",
    null as "ProbabilityPercent",
    null as "Quantity",
    null as "FinancePartner",
    null as "BudgetCycle",
    null as "Money",
    null as "Method",
    null as "ProductName",              
    null as "ProductFamily",   
    null as "ProductType",       
    null as "OpportunityType",                       
    null as "CapitalCustomerType", 
    CASE 
        WHEN e.RelatedObjectType = 'Pathway' THEN pa.accountguid
        WHEN e.RelatedObjectType = 'Opportunity' AND e.SalesStep = 'Business Alignment Meeting' THEN opportunity."AccountGUID"
        ELSE NULL 
    END AccountGUID,
    null as "AccountName",
    null as "CustomerType",
    null as Top_Tier,
    null as Top_GF,
    e.subject,
    e.assignedtoid,
    null as "AssignedTo",
    null as contact,
    null as "ContactPersonName",
    null as "SurgeonSpeciality",
    null as "SurgeonCategory",
    CASE 
        WHEN e.RelatedObjectType = 'Pathway' AND e.SalesStep = 'Directional QTI' THEN 'Pathway'
        WHEN e.RelatedObjectType = 'Pathway' AND e.SalesStep = 'Peer-to-peer event' THEN 'Pathway'
        WHEN e.RelatedObjectType = 'Opportunity' AND e.SalesStep = 'Business Alignment Meeting' THEN 'BAM'
        WHEN rt.RecordTypeName = 'Advanced Technology' THEN 'Advanced Technology'
        WHEN e.RelatedObjectType = 'Pathway' AND p.PathwayName IS NOT NULL THEN 'Pathway'
        ELSE NULL 
    END type,
    CASE 
        WHEN e.RelatedObjectType = 'Pathway' THEN p.surgeonguid
        WHEN e.RelatedObjectType = 'Opportunity' AND e.SalesStep = 'Business Alignment Meeting' THEN forc.contactguid
        ELSE NULL 
    END SurgeonGUID,
    e.deleted as event_isdeleted,   
    IFNULL(p.PathwayName, opportunity."OpportunityName") AS PathwayName_or_OpporunityName,
    IFNULL(rt.RecordTypeName, NULL) AS PathwayType,
    e.createdbyid,
    null as "CreatedBy",   
    e.SalesStep,
    e.DUEDATEONLY AS EventEndDateTime,
    e.eventstatus,
    e.description,
    e.pillar,
    e.salessteptype,
    e.priority
FROM 
    "EDW"."TRAINING"."VW_EVENT" e
        LEFT JOIN "EDW"."MASTER"."VW_USER" u on u.UserID = e.CreatedByID
        LEFT JOIN "EDW"."MASTER"."VW_USER" assignedto on assignedto.UserID = e.AssignedToID
        LEFT JOIN "EDW"."MASTER"."VW_CONTACT" forc on forc.ContactGUID = e.NameID
        LEFT JOIN "EDW"."OPPORTUNITY"."VW_MILESTONEPATHWAY" p on p.MILESTONEPATHWAYGUID = e.RelatedToID
        LEFT JOIN "EDW"."MASTER"."VW_CONTACT" pc ON pc.ContactGUID = p.SurgeonGUID
        LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" pa ON pc.AccountGUID = pa.AccountGUID 
        LEFT JOIN "EDW"."MASTER"."VW_RECORDTYPE" rt on rt.RecordTypeID = p.RecordTypeID        
        LEFT JOIN 
        (
        SELECT DISTINCT
            opportunity.opportunityid as "OpportunityGUID",
            opportunity.accountguid as "AccountGUID",
            opportunity.name as "OpportunityName"
        FROM 
            "EDW"."OPPORTUNITY"."VW_OPPORTUNITY" opportunity
        ) opportunity
        ON e.RelatedToID = opportunity."OpportunityGUID"
        LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" oa ON opportunity."AccountGUID" = oa.AccountGUID 
WHERE 
    e.relatedobjecttype IN ('Opportunity', 'Pathway')
    and ifnull(pa.capitalregion,oa.capitalregion) in ('Asia : Korea') 
    and e.DUEDATEONLY >= edwlabs.saleslab.THISQUARTERFIRSTDAY() -- 今四半期のActivityをログとして取得する。
    and e.DUEDATEONLY < dateadd(month, 3, edwlabs.saleslab.THISQUARTERFIRSTDAY())

'''
#
# if df.iloc[0]['MAX("EXTRACTEDDATE")'].strftime('%Y-%m-%d') == str(current_date.strftime('%Y-%m-%d')):
#     print('Already Updated')
# else:
#     cs.execute(squery + name + query)
#     print('{} updated'.format(name))



cs.execute(squery + name + aquery + query)
print('{} updated'.format(name))