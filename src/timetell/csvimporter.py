import argparse
import asyncio
import collections
import csv
from datetime import datetime
import functools
import logging
import pathlib
import secrets
import signal
import typing as T

import asyncpg.pool
import uvloop

CONNECT_ATTEMPT_MAX_TRIES = 5
CONNECT_ATTEMPT_INTERVAL_SECS = 2

_QUERY_COLUMNDEF = """
SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_name='{tablename}'
   AND table_schema='{schemaname}';
"""
_QUERY_CREATE_SCHEMA = 'CREATE SCHEMA "{schemaname}";'
_QUERY_PUBLISH_SCHEMA = """
DROP SCHEMA "public" CASCADE;
ALTER SCHEMA "{schemaname}" RENAME TO "public";
"""

_pool: asyncpg.pool.Pool = None
_importschema: str = None
_logger: logging.Logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=1)
def _tablesettings():
    return collections.OrderedDict(
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
            'contraints': """
    ALTER TABLE "{schemaname}"."{tablename}"
        ADD CONSTRAINT fk_parent_id FOREIGN KEY (parent_id) REFERENCES "{schemaname}"."ACT"(act_id);
            """
        },
        EMP={
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
            'contraints': """
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
        name character varying(50) NOT NULL,
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
            'contraints': """
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
    """,
            'contraints': """
    ALTER TABLE "{schemaname}"."{tablename}"
        ADD CONSTRAINT fk_cust_id FOREIGN KEY (cust_id) REFERENCES "{schemaname}"."CUST"(cust_id);
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
            'contraints': """
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
            'contraints': """
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
            'contraints': """
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
            'contraints': """
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
            'contraints': """
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
            'contraints': """
    ALTER TABLE "{schemaname}"."{tablename}"
        ADD CONSTRAINT fk_emp_id FOREIGN KEY (emp_id) REFERENCES "{schemaname}"."EMP"(emp_id),
        ADD CONSTRAINT fk_act_id FOREIGN KEY (act_id) REFERENCES "{schemaname}"."ACT"(act_id),
        ADD CONSTRAINT fk_prj_id FOREIGN KEY (prj_id) REFERENCES "{schemaname}"."PRJ"(prj_id),
        ADD CONSTRAINT fk_cust_id FOREIGN KEY (cust_id) REFERENCES "{schemaname}"."CUST"(cust_id),
        ADD CONSTRAINT fk_org_id FOREIGN KEY (org_id) REFERENCES "{schemaname}"."ORG"(org_id),
        ADD CONSTRAINT fk_prjapproved_by FOREIGN KEY (prjapproved_by) REFERENCES "{schemaname}"."EMP"(emp_id);
            """
        }
    )

async def initialize(eventloop: asyncio.AbstractEventLoop, dsn: str):
    # language=rst
    """ Initialize the plugin.

    This function validates the configuration and creates a connection pool.
    The pool is stored as a module-scoped singleton in _pool.

    """
    global _pool, _importschema

    if _pool is not None:
        return

    # create asyncpg engine
    _logger.debug(
        "Connecting to database: %s", dsn
    )
    connect_attempt_tries_left = CONNECT_ATTEMPT_MAX_TRIES - 1
    while connect_attempt_tries_left >= 0:
        try:
            _pool = await asyncpg.create_pool(dsn, loop=eventloop)
        except ConnectionRefusedError:
            if connect_attempt_tries_left > 0:
                _logger.warning(
                    "Database not accepting connections. Retrying %d more times.",
                    connect_attempt_tries_left)
                connect_attempt_tries_left -= 1
                await asyncio.sleep(CONNECT_ATTEMPT_INTERVAL_SECS)
            else:
                _logger.error("Could not connect to the database. Aborting.")
                raise
        else:
            break

    # register shutdown hook
    def shutdown_signel_handler():
        eventloop.run_until_complete(_shutdown())
    for s in (signal.SIGINT, signal.SIGQUIT, signal.SIGTERM, signal.SIGHUP):
        eventloop.add_signal_handler(s, shutdown_signel_handler)

    # create import schemaname
    _importschema = secrets.token_hex(4)

    try:
        # initialize schema
        await _pool.execute(_QUERY_CREATE_SCHEMA.format(schemaname=_importschema))

        # initialize tables
        tablesettings = _tablesettings()
        for tablename, settings in tablesettings.items():
            await _pool.execute(settings['create'].format(schemaname=_importschema, tablename=tablename))
    except:
        await _shutdown()
        raise


async def _shutdown():
    logging.info("Killing db connection...")
    await _pool.close()
    logging.info("Done")


def _float(s: str) -> float:
    return float(s.replace(',', '.') or '0')


def _int(s: str) -> int:
    return int(s or '0')


def _date(s: str) -> datetime:
    if s == '':
        return None
    return datetime.strptime(s, '%d-%m-%Y')


def _timestamp(s: str) -> datetime:
    if s == '':
        return None
    return datetime.strptime(s, '%d-%m-%Y %H:%M')


def _parent_id(s: str) -> T.Optional[int]:
    if s == '' or s == '0':
        return None
    return _int(s)


async def import_csv(reader: T.AsyncGenerator, tablename: str):
    # first get data type converters based on column data types
    coldefs = await _pool.fetch(
        _QUERY_COLUMNDEF.format(tablename=tablename, schemaname=_importschema)
    )
    columns = {}
    for column in coldefs:
        dt = column['data_type']
        if column['column_name'] == 'parent_id':
            columns[column['column_name']] = _parent_id
        elif dt == 'integer':
            columns[column['column_name']] = _int
        elif dt == 'double precision':
            columns[column['column_name']] = _float
        elif dt == 'date':
            columns[column['column_name']] = _date
        elif dt == 'timestamp without time zone':
            columns[column['column_name']] = _timestamp
        elif dt == 'character varying':
            columns[column['column_name']] = str
        elif dt == 'text':
            columns[column['column_name']] = str
        else:
            raise Exception("Unknown column datatype: ", dt)

    # now get the column names from the reader
    # here we strip the TT_ prefix and lowercase everything
    headers = list(map(lambda c: c.lower()[3:], await reader.__anext__()))

    # make sure we have all headers we expect
    if set(headers) != set(columns.keys()):
        raise Exception(
            "Expected columns {}, got {}".format(columns.keys(), headers)
        )

    # order the type converters
    convs = [columns[c] for c in headers]

    # create the insert query
    q = 'INSERT INTO "{}"."{}" ('.format(_importschema, tablename) + \
        ', '.join('"{}"'.format(c) for c in headers) + \
        ') VALUES (' + \
        ','.join('${}'.format(i) for i in range(1, len(headers) + 1)) + \
        ');'

    # insert all rows
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


async def publish():
    await _pool.execute(_QUERY_PUBLISH_SCHEMA.format(schemaname=_importschema))


def cli():
    # init logging
    logging.basicConfig()
    # configuration
    parser = argparse.ArgumentParser(description='Timetell CSV importer')
    parser.add_argument('csvdir', metavar='PATH', help='path to parent dir of csv files')
    parser.add_argument('dsn', metavar='DSN', help='postgresql connection string')
    args = parser.parse_args()
    csv_dir = pathlib.Path(args.csvdir)
    if not csv_dir.exists():
        _logger.fatal("directory {} doesn't exist".format(csv_dir))
    dsn = args.dsn
    # create eventloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    eventloop = asyncio.get_event_loop()
    # initialize database
    eventloop.run_until_complete(initialize(eventloop, dsn))
    # turn a csv.reader into an async generator
    async def csv_generator(tablename) -> T.AsyncGenerator:
        f = (csv_dir / ('TT_' + tablename + '.csv')).open(newline='', encoding='WINDOWS-1252')
        try:
            for row in csv.reader(f, delimiter=';'):
                yield row
        finally:
            f.close()
    # run import
    tablesettings = _tablesettings()
    coros = [import_csv(csv_generator(tablename), tablename) for tablename in tablesettings]
    eventloop.run_until_complete(asyncio.gather(*coros))
    eventloop.run_until_complete(publish())

if __name__ == '__main__':
    cli()
