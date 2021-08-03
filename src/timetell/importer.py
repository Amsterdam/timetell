"""
csvimporter
"""
import argparse
import asyncio
import collections.abc
import csv
from copy import deepcopy
from datetime import datetime
import logging
import pathlib
import pkg_resources
import secrets
import signal
import typing as T

import asyncpg.pool
import uvloop

_TABLESETTINGS = collections.OrderedDict(
    ACT={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    act_id integer PRIMARY KEY,
    parent_id integer,
    fromdate date,
    todate date,
    name character varying(50) NOT NULL,
    nr character varying(20),
    code character varying(20),
    "group" integer,
    account character varying(20),
    calcid integer,
    authmode integer,
    ratelevel integer,
    ratetype integer,
    inherit_id integer,
    inherit integer,
    prj_id integer,
    prj_default integer,
    export integer,
    type integer,
    clock integer,
    balance integer,
    daytotal integer,
    nonegbal integer,
    overtime integer,
    fillinfo integer,
    info text,
    updatelocal timestamp,
    nobooking integer,
    act_path character varying(100),
    notmanual integer,
    pl_color integer,
    externkey character varying(50),
    regnumbers integer,
    expires integer,
    expiremonths integer,
    selectbalance integer,
    balance_min double precision,
    balance_max double precision,
    tag integer,
    tagtype integer,
    tagdate date,
    noselect integer
);
""",
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_parent_id FOREIGN KEY (parent_id) REFERENCES "{schemaname}"."ACT"(act_id);
        """
    },
    EMP={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    emp_id integer PRIMARY KEY,
    nonactive integer,
    empcat integer,
    name character varying(100),
    nationality character varying(50),
    startdate date
);
"""
    },
    ORG={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    org_id integer PRIMARY KEY,
    parent_id integer,
    fromdate date,
    todate date,
    name character varying(50),
    nr character varying(20),
    code character varying(20),
    "group" integer,
    account character varying(20),
    inherit_id integer,
    inherit integer,
    address character varying(100),
    zipcode character varying(10),
    place character varying(50),
    district character varying(50),
    country character varying(50),
    phone1 character varying(15),
    phone2 character varying(15),
    mobile1 character varying(15),
    mobile2 character varying(15),
    fax1 character varying(15),
    fax2 character varying(15),
    email1 character varying(50),
    email2 character varying(50),
    info text,
    updatelocal timestamp,
    org_path character varying(100),
    pl_color integer,
    externkey character varying(50),
    tag integer,
    tagtype integer,
    tagdate date
);
""",
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_parent_id FOREIGN KEY (parent_id) REFERENCES "{schemaname}"."ORG"(org_id);
        """
    },
    JOB={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    job_id integer PRIMARY KEY,
    name character varying(50),
    nr character varying(20),
    code character varying(20),
    nonactive integer,
    info text,
    tag integer,
    tagtype integer,
    tagdate date
);
"""
    },
    CUST={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    cust_id integer PRIMARY KEY,
    parent_id integer,
    fromdate date,
    todate date,
    name character varying(75) NOT NULL,
    nr character varying(20),
    code character varying(20),
    "group" integer,
    account character varying(20),
    authmode integer,
    ratelevel integer,
    inherit_id integer,
    inherit integer,
    address character varying(100),
    zipcode character varying(10),
    place character varying(50),
    district character varying(50),
    country character varying(50),
    phone1 character varying(15),
    phone2 character varying(15),
    mobile1 character varying(15),
    mobile2 character varying(15),
    fax1 character varying(15),
    fax2 character varying(15),
    email1 character varying(15),
    email2 character varying(15),
    info text,
    updatelocal timestamp,
    nobooking integer,
    cust_path character varying(100),
    pl_color integer,
    externkey character varying(50),
    tag integer,
    tagtype integer,
    tagdate date,
    distance text
);
        """,
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_parent_id FOREIGN KEY (parent_id) REFERENCES "{schemaname}"."CUST"(cust_id);
        """
    },
    CUST_CONTACT={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    cust_contact_id integer PRIMARY KEY,
    cust_id integer,
    contact character varying(100),
    department character varying(50),
    email character varying(50),
    phone character varying(50),
    item_id integer,
    tag integer,
    tagtype integer,
    tagdate date
);
"""
    },
    EMP_CONTRACT={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    emp_id integer,
    fromdate date,
    todate date,
    hours double precision,
    hoursmethod integer,
    fte double precision,
    scale integer,
    step integer,
    salary double precision,
    type integer,
    job_id integer,
    actprof_id integer,
    rate_id integer,
    rate double precision,
    internalrate double precision,
    overtimerate double precision,
    calcrule_id integer,
    perdate date,
    addleaveyear double precision,
    addleavehour double precision,
    percnotplan double precision,
    calcid integer,
    holidaygroup integer,
    bookhours integer,
    min_hours double precision,
    calctype_id integer,
    tag integer,
    tagtype integer,
    tagdate date,
    leavecalc_id integer
);
""",
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_emp_id FOREIGN KEY (emp_id) REFERENCES "{schemaname}"."EMP"(emp_id);
        """
    },
    EMP_ORG={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    emp_id integer,
    org_id integer,
    fromdate date,
    todate date,
    type integer,
    book integer,
    auth integer,
    tag integer,
    tagtype integer,
    tagdate date
);
""",
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_emp_id FOREIGN KEY (emp_id) REFERENCES "{schemaname}"."EMP"(emp_id),
    ADD CONSTRAINT fk_org_id FOREIGN KEY (org_id) REFERENCES "{schemaname}"."ORG"(org_id);
        """
    },
    PRJ={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    prj_id integer PRIMARY KEY,
    parent_id integer,
    fromdate date,
    todate date,
    name character varying(50),
    nr character varying(20),
    code character varying(20),
    "group" integer,
    account character varying(20),
    authmode integer,
    nobooking integer,
    ratelevel integer,
    inherit_id integer,
    inherit integer,
    act_id integer,
    act_default integer,
    cust_id integer,
    cust_default integer,
    cust_contact_id integer,
    info text,
    export integer,
    status integer,
    approve integer,
    limitact integer,
    limitcust integer,
    limitcosts integer,
    updatelocal timestamp,
    prj_path character varying(100),
    pl_color integer,
    externkey character varying(50),
    calc_hours double precision,
    calc_costs double precision,
    tag integer,
    tagtype integer,
    tagdate date,
    allowplan integer,
    exp_approve integer,
    prjcat integer
);
""",
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_parent_id FOREIGN KEY (parent_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    ADD CONSTRAINT fk_act_id FOREIGN KEY (act_id) REFERENCES "{schemaname}"."ACT"(act_id),
    ADD CONSTRAINT fk_cust_id FOREIGN KEY (cust_id) REFERENCES "{schemaname}"."CUST"(cust_id);
        """
    },
    PRJ_LINK={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    prj_id integer,
    emp_id integer,
    org_id integer,
    auth integer,
    book integer,
    prjleader integer,
    tag integer,
    tagtype integer,
    tagdate date
);
""",
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_prj_id FOREIGN KEY (prj_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    ADD CONSTRAINT fk_emp_id FOREIGN KEY (emp_id) REFERENCES "{schemaname}"."EMP"(emp_id),
    ADD CONSTRAINT fk_org_id FOREIGN KEY (org_id) REFERENCES "{schemaname}"."ORG"(org_id);
        """
    },
    SYS_PRJ_NIV={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    niv integer,
    niv_id integer,
    niv_name character varying(50),
    niv0_id integer,
    niv0_name character varying(50),
    niv1_id integer,
    niv1_name character varying(50),
    niv2_id integer,
    niv2_name character varying(50),
    niv3_id integer,
    niv3_name character varying(50),
    niv4_id integer,
    niv4_name character varying(50),
    niv5_id integer,
    niv5_name character varying(50),
    niv6_id integer,
    niv6_name character varying(50),
    niv7_id integer,
    niv7_name character varying(50),
    niv8_id integer,
    niv8_name character varying(50),
    niv9_id integer,
    niv9_name character varying(50),
    fullpath character varying(200)
);
""",
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_niv_id FOREIGN KEY (niv_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    ADD CONSTRAINT fk_niv0_id FOREIGN KEY (niv0_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    ADD CONSTRAINT fk_niv1_id FOREIGN KEY (niv1_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    ADD CONSTRAINT fk_niv2_id FOREIGN KEY (niv2_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    ADD CONSTRAINT fk_niv3_id FOREIGN KEY (niv3_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    ADD CONSTRAINT fk_niv4_id FOREIGN KEY (niv4_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    ADD CONSTRAINT fk_niv5_id FOREIGN KEY (niv5_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    ADD CONSTRAINT fk_niv6_id FOREIGN KEY (niv6_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    ADD CONSTRAINT fk_niv7_id FOREIGN KEY (niv7_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    ADD CONSTRAINT fk_niv8_id FOREIGN KEY (niv8_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    ADD CONSTRAINT fk_niv9_id FOREIGN KEY (niv9_id) REFERENCES "{schemaname}"."PRJ"(prj_id);
        """
    },
    HRS={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    hrs_id integer PRIMARY KEY,
    wk_id integer,
    emp_id integer,
    act_id integer,
    prj_id integer,
    cust_id integer,
    org_id integer,
    year integer,
    month integer,
    date date,
    fromtime date,
    totime date,
    hours double precision,
    break double precision,
    overtime double precision,
    internalrate double precision,
    rate double precision,
    hoursrate double precision,
    hoursinternalrate double precision,
    chargeable integer,
    chargehours double precision,
    charged integer,
    booktype integer,
    info text,
    exportflag integer,
    exportdate date,
    emp_book_id integer,
    status integer,
    linenr integer,
    tag integer,
    tagtype integer,
    tagdate date,
    balance_id integer,
    plan_week_id integer,
    plan_alloc_id integer,
    calendar_id integer,
    prjapproved_by integer,
    prjapproved_on date
);
""",
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_emp_id FOREIGN KEY (emp_id) REFERENCES "{schemaname}"."EMP"(emp_id),
    ADD CONSTRAINT fk_act_id FOREIGN KEY (act_id) REFERENCES "{schemaname}"."ACT"(act_id),
    ADD CONSTRAINT fk_prj_id FOREIGN KEY (prj_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    ADD CONSTRAINT fk_cust_id FOREIGN KEY (cust_id) REFERENCES "{schemaname}"."CUST"(cust_id),
    ADD CONSTRAINT fk_org_id FOREIGN KEY (org_id) REFERENCES "{schemaname}"."ORG"(org_id);
        """
    },
    SYS_OPT={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    opt_id integer PRIMARY KEY,
    name character varying(50)
);
        """
    },
    SYS_OPT_ITM={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    item_id integer,
    opt_id integer,
    item character varying(50) COLLATE pg_catalog."default",
    code character varying(20),
    updatelocal timestamp,
    calcid integer
);
        """,
        # 'constraints': """
# ALTER TABLE "{schemaname}"."{tablename}"
#     ADD CONSTRAINT fk_opt_id FOREIGN KEY (opt_id) REFERENCES "{schemaname}"."SYS_OPT"(opt_id);
#         """
    },
    VW_LABEL_PRJ={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (   
    dim_label_id integer not null,
    dim_id integer,
    "type" integer,
    item_id integer,
    "value" numeric,
    "date" date,
    "desc" character varying(50) collate pg_catalog."default",
    tag integer,
    tagtype integer,
    tagdate date,
    todate date
);
        """
    },
    CALENDAR={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    calendar_id integer PRIMARY KEY,
    plan_week_id integer,
    parentid integer,
    "type" integer,
    start date,
    finish date,
    options integer,
    caption character varying(50),
    recurrenceindex integer,
    emp_id integer,
    location character varying(20),
    message character varying(50),
    reminderdate date,
    reminderminutes integer,
    state integer,
    labelcolor integer,
    actualstart date,
    actualfinish date,
    syncidfield character varying(20),
    act_id integer,
    prj_id integer,
    cust_id integer,
    org_id integer,
    hours double precision,
    remaining double precision,
    emp_book_id integer,
    status integer,
    percent double precision,
    sharedid integer,
    owner_id integer,
    changedate date,
    syncdate date,
    private integer,
    method integer,
    planshift_id integer,
    tag integer,
    tagtype integer,
    tagdate date,
    plan_alloc_id integer,
    dayplan integer,
    tentative integer,
    meeting integer,
    reminderchangedate integer,
    changed_by integer,
    rate double precision,
    internalrate double precision
);
        """,
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_emp_id FOREIGN KEY (emp_id) REFERENCES "{schemaname}"."EMP"(emp_id);
        """
    },
    PLAN_WEEK={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    plan_week_id integer PRIMARY KEY,
    plan_alloc_id integer,
    emp_id integer,
    act_id integer,
    prj_id integer,
    cust_id integer,
    org_id integer,
    "desc" character varying(50) collate pg_catalog."default",
    info text,
    fromdate date,
    todate date,
    hours double precision,
    owner_id integer,
    changedate date,
    tag integer,
    tagtype integer,
    tagdate date,
    tentative integer,
    rate double precision,
    internalrate double precision
);
        """,
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_plan_alloc_id FOREIGN KEY (plan_alloc_id) REFERENCES "{schemaname}"."PLAN_ALLOC"(plan_alloc_id),
    ADD CONSTRAINT fk_emp_id FOREIGN KEY (emp_id) REFERENCES "{schemaname}"."EMP"(emp_id);
    -- ADD CONSTRAINT fk_act_id FOREIGN KEY (act_id) REFERENCES "{schemaname}"."ACT"(act_id),
    -- ADD CONSTRAINT fk_prj_id FOREIGN KEY (prj_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    -- ADD CONSTRAINT fk_cust_id FOREIGN KEY (cust_id) REFERENCES "{schemaname}"."CUST"(cust_id),
    -- ADD CONSTRAINT fk_org_id FOREIGN KEY (org_id) REFERENCES "{schemaname}"."ORG"(org_id);
        """
    },
    PLAN_TASK={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    plan_task_id integer PRIMARY KEY,
    act_id integer,
    prj_id integer,
    cust_id integer,
    org_id integer,
    fromdate date,
    todate date,
    "desc" character varying(50) collate pg_catalog."default",
    info text,
    label character varying(20),
    plan_prj_id integer,
    plantype integer,
    nr integer,
    type integer,
    start date,
    finish date,
    duration integer,
    options integer,
    taskindex integer,
    tasklinks character varying(50),
    base_idx integer,
    base_origin_id integer,
    base_desc character varying(50) collate pg_catalog."default",
    base_savedate date,
    hours double precision,
    costs integer,
    completed integer,
    owner_id integer,
    changedate date,
    tag integer,
    tagtype integer,
    tagdate date,
    group_id integer,
    editrequest integer,
    fixedperiod integer,
    status_date date,
    changed_by integer,
    pl_color integer,
    calcduration integer,
    category integer,
    status_alloc integer,
    status_allocinfo character varying(50),
    can_allocate integer,
    task_complete integer,
    status integer
)
        """,
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    -- ADD CONSTRAINT fk_act_id FOREIGN KEY (act_id) REFERENCES "{schemaname}"."ACT"(act_id),
    -- ADD CONSTRAINT fk_prj_id FOREIGN KEY (prj_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    -- ADD CONSTRAINT fk_cust_id FOREIGN KEY (cust_id) REFERENCES "{schemaname}"."CUST"(cust_id),
    ADD CONSTRAINT fk_org_id FOREIGN KEY (org_id) REFERENCES "{schemaname}"."ORG"(org_id),
    ADD CONSTRAINT fk_plan_prj_id FOREIGN KEY (plan_prj_id) REFERENCES "{schemaname}"."PLAN_PRJ"(plan_prj_id);
        """
    },
    PLAN_ALLOC={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    plan_alloc_id integer PRIMARY KEY,
    plan_request_id integer,
    "desc" character varying(50) collate pg_catalog."default",
    info text,
    fromdate date,
    todate date,
    act_id integer,
    prj_id integer,
    cust_id integer,
    emp_id integer,
    hours double precision,
    status integer,
    completed integer,
    extra_hours double precision,
    owner_id integer,
    changedate date,
    tag integer,
    tagtype integer,
    tagdate date,
    plan_task_id integer,
    skill_id integer,
    status_date date,
    changed_by integer,
    autoupdate integer,
    rate double precision,
    internalrate double precision,
    complete integer
);
        """,
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_emp_id FOREIGN KEY (emp_id) REFERENCES "{schemaname}"."EMP"(emp_id),
    ADD CONSTRAINT fk_act_id FOREIGN KEY (act_id) REFERENCES "{schemaname}"."ACT"(act_id),
    ADD CONSTRAINT fk_prj_id FOREIGN KEY (prj_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    -- ADD CONSTRAINT fk_cust_id FOREIGN KEY (cust_id) REFERENCES "{schemaname}"."CUST"(cust_id),
    ADD CONSTRAINT fk_plan_request_id FOREIGN KEY (plan_request_id) REFERENCES "{schemaname}"."PLAN_REQUEST"(plan_request_id),
    ADD CONSTRAINT fk_plan_task_id FOREIGN KEY (plan_task_id) REFERENCES "{schemaname}"."PLAN_TASK"(plan_task_id);
        """
    },
    PLAN_REQUEST={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    plan_request_id integer PRIMARY KEY,
    emp_id integer,
    org_id integer,
    skill_id integer,
    fromdate date,
    todate date,
    "desc" character varying(50) collate pg_catalog."default",
    info text,
    hours double precision,
    status integer,
    owner_id integer,
    changedate date,
    status_alloc integer,
    tag integer,
    tagtype integer,
    tagdate date,
    plan_task_id integer,
    changed_by integer,
    status_date date,
    days integer,
    weekhours double precision,
    status_allocinfo character varying(50),
    rate double precision,
    internalrate double precision
)
        """,
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_emp_id FOREIGN KEY (emp_id) REFERENCES "{schemaname}"."EMP"(emp_id),
    ADD CONSTRAINT fk_org_id FOREIGN KEY (org_id) REFERENCES "{schemaname}"."ORG"(org_id),
    ADD CONSTRAINT fk_plan_task_id FOREIGN KEY (plan_task_id) REFERENCES "{schemaname}"."PLAN_TASK"(plan_task_id);
        """
    },
    PLAN_PRJ={
        'create': """
    CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
        plan_prj_id integer PRIMARY KEY,
        prj_id integer,
        fromdate date,
        todate date,
        calcduration integer,
        version integer,
        tag integer,
        tagtype integer,
        tagdate date
    )
        """,
        },
    EMP_SKILLS={
        'create':   """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    emp_skills_id integer PRIMARY KEY,
    emp_id integer,
    skill_id integer,
    "order" integer,
    "desc" character varying(50) collate pg_catalog."default",
    perc double precision,
    tag integer,
    tagtype integer,
    tagdate date
)
        """,
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_emp_id FOREIGN KEY (emp_id) REFERENCES "{schemaname}"."EMP"(emp_id);
            """
        },
    PLAN_CELLS={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    plan_cells_id integer,
    plan_id integer,
    emp_id integer,
    act_id integer,
    prj_id integer,
    org_id integer,
    cust_id integer,
    fromdate timestamp,
    todate timestamp,
    type integer,
    dim_id integer,
    status integer,
    hours float,
    costs float,
    items float,
    completed integer,
    remaining float,
    info text,
    perc float,
    exportdate timestamp,
    exportvlag integer,
    tag integer,
    tagtype integer,
    tagdate timestamp
)
        """,
        'constrains': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_plan_cells_id FOREIGN KEY (plan_cells_id) REFERENCES "{schemaname}"."PLAN_CELLS"(plan_cells_id),
    ADD CONSTRAINT fk_emp_id FOREIGN KEY (emp_id) REFERENCES "{schemaname}"."EMP"(emp_id),
    ADD CONSTRAINT fk_act_id FOREIGN KEY (act_id) REFERENCES "{schemaname}"."ACT"(act_id),
    ADD CONSTRAINT fk_prj_id FOREIGN KEY (prj_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
    ADD CONSTRAINT fk_org_id FOREIGN KEY (org_id) REFERENCES "{schemaname}"."ORG"(org_id);
        """
    },
    PLAN_TASK_COST={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    PLAN_TASK_COST_ID integer,
    PLAN_TASK_ID integer,
    ACCOUNT character varying(20),
    "DESC" character varying(50),
    PLAN_COST float,
    COST float,
    DATE timestamp,
    STATUS integer,
    TAG integer,
    TAGTYPE integer,
    TAGDATE timestamp
)
        """,
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_plan_task_id FOREIGN KEY (plan_task_id) REFERENCES "{schemaname}"."PLAN_TASK"(plan_task_id);
        """
    },
    PLAN_TASK_LIST={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    PLAN_TASK_LIST_ID integer,
    PLAN_TASK_ID integer,
    "DESC" character varying(50),
    INFO text,
    COMPLETED integer,
    COMPLETED_DATE timestamp,
    TAG integer,
    TAGTYPE integer,
    TAGDATE timestamp
)
        """,
        'constraints': """
ALTER TABLE "{schemaname}"."{tablename}"
    ADD CONSTRAINT fk_plan_task_id FOREIGN KEY (plan_task_id) REFERENCES "{schemaname}"."PLAN_TASK"(plan_task_id);
        """
    },
    VW_PLAN={
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    plan_id integer,
    copy integer, 
    type integer,
    dim_id integer,
    plan_cells_id integer PRIMARY KEY,
    act_id integer,
    prj_id integer,
    emp_id integer,
    org_id integer,
    fromdate date,
    todate date,
    status integer,
    hours double precision,
    costs double precision
)
        """
    }
)

_DATAPUNT_TABLESETTINGS = _TABLESETTINGS

# Dienstverlening uses the same table definitions as de default _TABLESETTINGS that we use for datapunt.
# However, for datapunt the EMP table (employees) has been minimized. Currently we are unable in
# estimate impact for dienstverlening, so for now we let the dienstverlening import as it was.
_DIENSTVERLENING_TABLESETTINGS = deepcopy(_TABLESETTINGS)

_DIENSTVERLENING_TABLESETTINGS['EMP'] = {
        'create': """
CREATE TABLE IF NOT EXISTS "{schemaname}"."{tablename}" (
    emp_id integer PRIMARY KEY,
    nr character varying(20),
    code character varying(20),
    lastname character varying(50),
    middlename character varying(10),
    firstname character varying(30),
    plastname character varying(50),
    pmiddlename character varying(10),
    displayname integer,
    sex character varying(1),
    initials character varying(10),
    nonactive integer,
    activeforclk integer,
    birthdate date,
    loginname character varying(50),
    password character varying(150),
    askpassword integer,
    auth_id integer,
    fromgroup integer,
    togroup integer,
    taskreg date,
    empcat integer,
    export integer,
    address character varying(100),
    zipcode character varying(10),
    place character varying(50),
    district character varying(50),
    country character varying(50),
    phone1 character varying(15),
    phone2 character varying(15),
    mobile1 character varying(15),
    mobile2 character varying(15),
    fax1 character varying(15),
    fax2 character varying(15),
    email1 character varying(50),
    email2 character varying(50),
    floor integer,
    location character varying(20),
    room character varying(10),
    extension character varying(15),
    bank character varying(25),
    leasecar character varying(20),
    traveldist double precision,
    travelcosts double precision,
    config_id integer,
    refreshlocal date,
    name character varying(100),
    firstnames character varying(50),
    leadtitle character varying(20),
    trailtitle character varying(20),
    addressno character varying(20),
    birthplace character varying(75),
    birthcountry character varying(75),
    nationality character varying(50),
    bsn character varying(20),
    idnumber character varying(40),
    mstatus integer,
    mstatusdate date,
    warninfo text,
    medicalinfo text,
    psex character varying(1),
    pinitials character varying(10),
    pfirstnames character varying(50),
    pfirstname character varying(30),
    pleadtitle character varying(20),
    ptrailtitle character varying(20),
    pbirthdate date,
    pphone character varying(15),
    startdate date,
    enddate date,
    jubdate date,
    funcdate date,
    assesdate date,
    svcyears integer,
    noldap integer,
    pl_color integer,
    idexpiredate date,
    externkey character varying(50),
    recipient character varying(100),
    audit integer,
    syncprof_id integer,
    aduser integer,
    changepassword integer,
    dayhours double precision,
    savedpassword character varying(150),
    tag integer,
    tagtype integer,
    tagdate date
);
"""
}

ACTIVE_TABLESETTINGS = None

_CUSTOMER_SETTINGS = dict(
    datapunt=dict(
        table_settings=_DATAPUNT_TABLESETTINGS,
        tables=(
           'ACT', 'EMP', 'ORG',  'JOB', 'CUST', 'CUST_CONTACT', 'EMP_CONTRACT',
           'EMP_ORG', 'PRJ', 'PRJ_LINK', 'SYS_PRJ_NIV', 'HRS', 'SYS_OPT_ITM',
           'VW_LABEL_PRJ', 'VW_PLAN'
        ),
        sql=pkg_resources.resource_string(
            __name__, 'datapunt.sql'
        ).decode('utf-8'),
        tables_to_check=["v_timetell_projectenoverzicht_7",
                         "v_timetell_project_team_maand_ab_7"]
    ),
    dienstverlening=dict(
        table_settings=_DIENSTVERLENING_TABLESETTINGS,
        tables=(
            'ACT', 'EMP', 'ORG', 'JOB', 'CUST', 'EMP_CONTRACT', 'EMP_ORG', 'EMP_SKILLS', 'PRJ', 'PRJ_LINK',
            'SYS_PRJ_NIV', 'HRS', 'SYS_OPT', 'SYS_OPT_ITM', 'CALENDAR', 'PLAN_WEEK', 'PLAN_ALLOC', 'PLAN_REQUEST',
            'PLAN_TASK', 'PLAN_PRJ'
        ),
        sql=pkg_resources.resource_string(
            __name__, 'dienstverlening.sql'
        ).decode('utf-8'),
        tables_to_check=["viw_tableau_datum", "viw_tableau_emp", "viw_tableau_hrs_union_all_normuren",
                         "viw_tableau_normuren_per_werkdag", "viw_timetell_ivdi", "vw_tableau_act", "vw_tableau_cust",
                         "vw_tableau_emp", "vw_tableau_hrs", "vw_tableau_hulp_week", "vw_tableau_org",
                         "vw_tableau_prj", "vw_tableau_sys_prj_niv", "vw_timetell_ivdi",
                         "viwx_01_emp_skill", "viwx_01_zondagen", "viwx_02_contract_per_week",
                         "viwx_02_plan_alloc_verdeeld", "viwx_02_plan_request_verdeeld", "viwx_02_plan_task_verdeeld",
                         "viwx_03_plan_union", "viwx_04_union_all", "viwx_timetell_ivdi"]
    )
)

_pool: asyncpg.pool.Pool = None
_importschema: str = None
_logger: logging.Logger = logging.getLogger(__name__)
_view_query = None


async def initialize(
        dsn: str,
        max_conn_retry: int=5,
        conn_retry_interval_secs: int=2
):
    # language=rst
    """ Initialize the plugin.

    This function validates the configuration and creates a connection pool.
    The pool is stored as a module-scoped singleton in _pool.

    """
    global _pool, _importschema

    if _pool is not None:
        return

    eventloop = asyncio.get_event_loop()

    # create asyncpg engine
    _logger.debug("Connecting to database")
    connect_attempt_tries_left = max_conn_retry - 1
    while connect_attempt_tries_left >= 0:
        try:
            _pool = await asyncpg.create_pool(dsn, loop=eventloop)
        except ConnectionRefusedError:
            if connect_attempt_tries_left > 0:
                _logger.warning(
                    "Database not accepting connections. Retrying %d more times.",
                    connect_attempt_tries_left)
                connect_attempt_tries_left -= 1
                await asyncio.sleep(conn_retry_interval_secs)
            else:
                _logger.error("Could not connect to the database. Aborting.")
                raise
        else:
            break
    _logger.debug("Connected to database.")

    # register shutdown hook
    def shutdown_signal_handler():
        eventloop.run_until_complete(close())
    for s in (signal.SIGINT, signal.SIGQUIT, signal.SIGTERM, signal.SIGHUP):
        eventloop.add_signal_handler(s, shutdown_signal_handler)

    # create import schemaname
    _importschema = secrets.token_hex(4)
    try:
        # initialize schema
        await _pool.execute('CREATE SCHEMA "{}";'.format(_importschema))
    except:
        await close()
        raise
    _logger.debug("Temp schema created: %s", _importschema)


async def close():
    global _pool
    if _pool is not None:
        logging.debug("Killing db connection...")
        await _pool.close()
        _pool = None
        logging.debug("Done")


async def _fieldconverters(tablename: str, headers: T.Tuple[str, ...]) -> T.Tuple[T.Callable, ...]:

    def _float(s: str) -> float:
        return float(s.replace(',', '.') or '0')

    def _int(s: str) -> int:
        return int(s or '0')

    def _date(s: str) -> T.Optional[datetime]:
        if s == '':
            return None
        try:
            return datetime.strptime(s, '%d-%m-%Y')
        except ValueError:
            return datetime.strptime(s, '%d-%m-%Y %H:%M')

    def _timestamp(s: str) -> T.Optional[datetime]:
        if s == '':
            return None
        return datetime.strptime(s, '%d-%m-%Y %H:%M')

    def _time(s: str) -> T.Optional[datetime.time]:
        if s == '':
            return None
        return datetime.strptime(s, '%H:%M').time()

    def _id_field(s: str) -> T.Optional[int]:
        if s == '' or s == '0':
            return None
        return _int(s)

    q = """
SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_name='{tablename}'
   AND table_schema='{schemaname}';
""".format(tablename=tablename, schemaname=_importschema)

    coldefs = await _pool.fetch(q)
    columns = {}
    for column in coldefs:
        dt = column['data_type']
        if column['column_name'].endswith('_id'):
            columns[column['column_name']] = _id_field
        elif dt == 'integer':
            columns[column['column_name']] = _int
        elif dt == 'numeric':
            columns[column['column_name']] = _int
        elif dt == 'double precision':
            columns[column['column_name']] = _float
        elif dt == 'date':
            columns[column['column_name']] = _date
        elif dt == 'timestamp without time zone':
            columns[column['column_name']] = _timestamp
        elif dt == 'time without time zone':
            columns[column['column_name']] = _time
        elif dt == 'character varying':
            columns[column['column_name']] = str
        elif dt == 'text':
            columns[column['column_name']] = str
        else:
            raise Exception("Unknown column datatype: ", dt)

    # make sure we have all headers we expect
    if set(headers) != set(columns.keys()):
        raise Exception(
            "Expected columns {}, got {}".format(columns.keys(), headers)
        )
    _logger.debug("Headers found and matching table definition: %s", headers)

    # order the type converters
    return tuple(columns[c] for c in headers)


async def populate(tablename: str, reader: T.AsyncGenerator) -> int:
    _logger.debug("Importing data into %s", tablename)

    # first create the table
    settings = ACTIVE_TABLESETTINGS[tablename]
    await _pool.execute(settings['create'].format(schemaname=_importschema, tablename=tablename))
    _logger.debug("Table created: %s", tablename)

    # get the column names from the reader
    # here we strip the TT_ prefix and lowercase everything
    try:
        headers = tuple(map(lambda c: c.lower()[3:], await reader.__anext__()))
    except FileNotFoundError:
        _logger.warning("File for table '{}' does not exist".format(tablename))
        return -1

    # get the field converters for this table
    convs = await _fieldconverters(tablename, headers)

    # create the insert query
    q = 'INSERT INTO "{}"."{}" ('.format(_importschema, tablename) + \
        ', '.join('"{}"'.format(c) for c in headers) + \
        ') VALUES (' + \
        ','.join('${}'.format(i) for i in range(1, len(headers) + 1)) + \
        ');'

    # insert all rows
    numrows = 0
    async with _pool.acquire() as con:
        stmt = await con.prepare(q)
        async for row in reader:
            # convert the values
            values = [convs[i](row[i]) for i in range(len(convs))]
            # insert the row
            try:
                await stmt.fetch(*values)
            except:
                _logger.critical("Error at %s: %s, %s", tablename, values, q)
                raise
            numrows += 1
    _logger.debug("Inserted %d rows into %s", numrows, tablename)
    return numrows


async def set_constraints(tablename: str):
    _logger.debug("Creating constraints on table %s", tablename)
    if tablename not in ACTIVE_TABLESETTINGS:
        raise Exception("Don't know table {}".format(tablename))
    if 'constraints' not in ACTIVE_TABLESETTINGS[tablename]:
        _logger.debug("No constraints defined for table {}".format(tablename))
    else:
        constraints = ACTIVE_TABLESETTINGS[tablename]['constraints']
        await _pool.execute(
            constraints.format(schemaname=_importschema, tablename=tablename))
        _logger.debug("Constraints created for table {}".format(tablename))


async def publish():
    q = 'DROP SCHEMA IF EXISTS "public" CASCADE; ' \
        'ALTER SCHEMA "{}" RENAME TO "public";'.format(_importschema)
    await _pool.execute(q)
    _logger.debug("Published imported data in the public schema (existing tables are removed)")


async def assert_has_data(table_name):
    q = 'SELECT COUNT (*) FROM "%s";' % (table_name, )
    count_result = await _pool.fetch(q)
    count = count_result[0]['count']
    if count < 0:
        raise(Exception("Empty view for %s" % (table_name,)))


async def run_sql(sql: str):
    """ Run arbitrary sql. {schemaname} placeholders will be replaced by the
    temporary schemaname.

    :param sql:
    """
    await _pool.execute(sql.format(schemaname=_importschema))


def cli():
    """ Simple CLI to run an import from files on the filesystem. """

    async def csv_generator(tablename) -> T.AsyncGenerator:
        f = (csv_dir / ('TT_' + tablename + '.csv')).open(newline='', encoding='WINDOWS-1252')
        try:
            for row in csv.reader(f, delimiter=';'):
                yield row
        finally:
            f.close()

    # create eventloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    eventloop = asyncio.get_event_loop()

    parser = argparse.ArgumentParser(description='Timetell CSV importer')
    parser.add_argument(
        'dsn', metavar='DSN', help='postgresql connection string')
    parser.add_argument('customer', choices=list(_CUSTOMER_SETTINGS.keys()), help='cutomer name')
    parser.add_argument(
        '-d', '--csvdir', action='store', default='.', metavar='PATH',
        help='dir containing csv files')
    parser.add_argument(
        '-v', '--verbose', dest='loglevel', action='store_const',
        const=logging.DEBUG, default=logging.INFO,
        help='set debug loglevel (default INFO)')
    args = parser.parse_args()

    # init logging
    logging.basicConfig(level=args.loglevel)
    # make sure CSV dir exists
    csv_dir = pathlib.Path(args.csvdir)
    if not csv_dir.exists():
        _logger.fatal("directory {} doesn't exist".format(csv_dir))
    dsn = args.dsn
    # initialize database
    eventloop.run_until_complete(initialize(dsn))
    # get customer settings
    customer = _CUSTOMER_SETTINGS[args.customer]

    global ACTIVE_TABLESETTINGS
    ACTIVE_TABLESETTINGS = customer['table_settings']

    # run import
    try:
        # run import
        coros = []
        for table_name in customer['tables']:
            coros.append(populate(table_name, csv_generator(table_name)))
        eventloop.run_until_complete(asyncio.gather(*coros))
        # create constraints
        for table_name in customer['tables']:
            # we need to do this one-by-one: may result in deadlocks otherwise
            eventloop.run_until_complete(set_constraints(table_name))
        # after all is done we run extra sql
        if 'sql' in customer:
            eventloop.run_until_complete(run_sql(customer['sql']))
        # publish data
        eventloop.run_until_complete(publish())

        # make sure that the views have data, otherwise raise
        coros = []
        for table_name in customer['tables_to_check']:
            coros.append(assert_has_data(table_name))
        eventloop.run_until_complete(asyncio.gather(*coros))

    finally:
        eventloop.run_until_complete(close())


if __name__ == '__main__':
    cli()
