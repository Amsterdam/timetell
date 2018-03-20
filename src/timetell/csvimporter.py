import argparse
import asyncio
import collections.abc
import csv
from datetime import datetime
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
""",
        'constraints': """
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
    }
)

_pool: asyncpg.pool.Pool = None
_importschema: str = None
_logger: logging.Logger = logging.getLogger(__name__)


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
    _logger.debug("Connecting to database")
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
    _logger.debug("Connected to database.")

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
        _logger.debug("Temp schema created: %s", _importschema)

        # initialize tables
        for tablename, settings in _TABLESETTINGS.items():
            await _pool.execute(settings['create'].format(schemaname=_importschema, tablename=tablename))
            _logger.debug("Table created: %s", tablename)
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


def _id_field(s: str) -> T.Optional[int]:
    if s == '' or s == '0':
        return None
    return _int(s)


async def import_csv(reader: T.AsyncGenerator, tablename: str) -> int:
    _logger.debug("Importing data into %s", tablename)
    # first get data type converters based on column data types
    coldefs = await _pool.fetch(
        _QUERY_COLUMNDEF.format(tablename=tablename, schemaname=_importschema)
    )
    columns = {}
    for column in coldefs:
        dt = column['data_type']
        if column['column_name'].endswith('_id'):
            columns[column['column_name']] = _id_field
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
    _logger.debug("Headers found and matching table definition: %s", headers)

    # order the type converters
    convs = [columns[c] for c in headers]

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


class _ProjectTree(collections.abc.Iterable):

    def __init__(self):
        self._projects = dict()
        self._roots = []
        self._frozen = False

    def add(self, project_id: int, budget: int, hours: int, parent_id: T.Optional[int]):
        assert not self._frozen, "Project tree is frozen"
        project = {
            'id': project_id,
            'parent_id': parent_id,
            'budget': budget,
            'hours': hours,
            'costs': 0
        }
        if parent_id is None:
            self._roots.append(project)
        self._projects[project_id] = project

    def _freeze(self):
        if self._frozen:
            return
        self._frozen = True
        budgets = [(pid, p['budget']) for pid, p in self._projects.items() if p['budget'] > 0]
        hours = [(pid, p['hours']) for pid, p in self._projects.items() if p['hours'] > 0]
        for pid, budget in budgets:
            self._propagate_value(pid, budget, 'budget')
        for pid, h in hours:
            self._propagate_value(pid, h, 'hours')

    def _propagate_value(self, project_id: int, value: int, field: str):
        if not self._frozen:
            self._freeze()
        if project_id not in self._projects:
            _logger.critical("Projectid %s not found while %d %s!", project_id, value, field)
            raise KeyError
        self._projects[project_id][field] += value
        parent_id = self._projects[project_id]['parent_id']
        while parent_id is not None and parent_id in self._projects:
            self._projects[parent_id][field] += value
            parent_id = self._projects[parent_id]['parent_id']

    def setcosts(self, project_id: int, costs: int):
        self._propagate_value(project_id, costs, 'costs')

    def printtree(self):
        _logger.debug('Printing tree...')
        sep = '|-- '
        def doprint(project_id, level=0):
            costs = self._projects[project_id]['costs']
            budget = self._projects[project_id]['budget']
            hours = self._projects[project_id]['hours']
            _logger.debug((sep * level) + "{}: budget={}, hours={}, costs={}".format(project_id, budget, hours, costs))
            children = [pid for (pid, p) in self._projects.items() if p['parent_id'] == project_id]
            for pid in children:
                doprint(pid, level+1)
        for root in self._roots:
            _logger.debug('===================')
            doprint(root['id'])
        _logger.debug('===================')

    def __iter__(self):
        for k, v in self._projects.items():
            yield k, v


def prj_aggregator(reader: T.AsyncGenerator) -> T.Tuple[_ProjectTree, T.AsyncGenerator]:
    """This function generates the project tree while yielding prj table rows"""
    projects = _ProjectTree()
    async def aggregator():
        # first yield the headers
        headers = await reader.__anext__()
        for i in range(len(headers)):
            if headers[i] == 'TT_PRJ_ID':
                project_id_idx = i
            elif headers[i] == 'TT_PARENT_ID':
                parent_id_idx = i
            elif headers[i] == 'TT_CALC_COSTS':
                budget_idx = i
            elif headers[i] == 'TT_CALC_HOURS':
                hours_idx = i
        yield headers
        # now add project relations to the tree while yielding
        async for row in reader:
            project_id = _id_field(row[project_id_idx])
            budget = _float(row[budget_idx])
            hours = _float(row[hours_idx])
            parent_id = _id_field(row[parent_id_idx])
            projects.add(project_id, budget, hours, parent_id)
            yield row
    return projects, aggregator()


def hrs_aggregator(reader: T.AsyncGenerator) -> T.Tuple[dict, T.AsyncGenerator]:
    """This function creates an aggregator function for the TT_HRS table"""
    aggregations = collections.defaultdict(int)
    async def aggregator():
        # first yield the headers
        headers = await reader.__anext__()
        for i in range(len(headers)):
            if headers[i] == 'TT_PRJ_ID':
                prj_id_idx = i
            elif headers[i] == 'TT_HOURSINTERNALRATE':
                rate_idx = i
        yield headers
        # now aggregate costs per project
        async for row in reader:
            costs = _float(row[rate_idx])
            if costs > 0:
                pid = _id_field(row[prj_id_idx])
                if pid is None:
                    _logger.warn("Costs (%d) added without project", costs)
                else:
                    aggregations[pid] += costs
            yield row
    return aggregations, aggregator()


async def insert_prj_aggregations(projects: _ProjectTree):
    _logger.debug("Updating project budget and costs in PRJ table")
    # add column
    q = 'ALTER TABLE "{}"."PRJ" ADD COLUMN hoursinternalrate integer;'.format(_importschema)
    await _pool.execute(q)
    # prepare update query
    async with _pool.acquire() as con:
        stmt = await con.prepare(
                'UPDATE "{}"."PRJ" SET hoursinternalrate = $1, calc_costs = $2, '
                'calc_hours = $3 WHERE prj_id = $4;'.format(_importschema))
        for pid, p in projects:
            # insert the row
            await stmt.fetch(p['costs'], p['budget'], p['hours'], pid)
    _logger.debug("Done updating project budget and costs in PRJ table")


async def create_constraints(tablename: str):
    _logger.debug("Creating constraints on table %s", tablename)
    if tablename not in _TABLESETTINGS:
        raise Exception("Don't know table {}".format(tablename))
    if 'constraints' not in _TABLESETTINGS[tablename]:
        _logger.debug("No constraints defined for table {}".format(tablename))
    else:
        constraints = _TABLESETTINGS[tablename]['constraints']
        await _pool.execute(
            constraints.format(schemaname=_importschema, tablename=tablename))
        _logger.debug("Constraints created for table {}".format(tablename))


async def publish():
    await _pool.execute(_QUERY_PUBLISH_SCHEMA.format(schemaname=_importschema))
    _logger.debug("Published imported data in the public schema (existing tables are removed)")


def cli():
    """ Simple CLI to run an import from files on the filesystem. """

    def parseargs():
        parser = argparse.ArgumentParser(description='Timetell CSV importer')
        parser.add_argument(
            'csvdir', metavar='PATH', help='path to parent dir of csv files')
        parser.add_argument(
            'dsn', metavar='DSN', help='postgresql connection string')
        parser.add_argument(
            '-v', '--verbose', dest='loglevel', action='store_const',
            const=logging.DEBUG, default=logging.INFO,
            help='set debug loglevel (default INFO)')
        return parser.parse_args()

    async def csv_generator(tablename) -> T.AsyncGenerator:
        f = (csv_dir / ('TT_' + tablename + '.csv')).open(newline='', encoding='WINDOWS-1252')
        try:
            for row in csv.reader(f, delimiter=';'):
                yield row
        finally:
            f.close()

    def run_import(eventloop):
        cost_aggregations, projects = None, None
        try:
            # run import
            coros = []
            for tablename in _TABLESETTINGS:
                if tablename == 'HRS':
                    cost_aggregations, agg = hrs_aggregator(csv_generator(tablename))
                    coros.append(import_csv(agg, tablename))
                elif tablename == 'PRJ':
                    projects, agg = prj_aggregator(csv_generator(tablename))
                    coros.append(import_csv(agg, tablename))
                else:
                    coros.append(import_csv(csv_generator(tablename), tablename))
            eventloop.run_until_complete(asyncio.gather(*coros))
            # add cost aggregations
            for project_id, costs in cost_aggregations.items():
                projects.setcosts(project_id, costs)
            eventloop.run_until_complete(insert_prj_aggregations(projects))
            # create constraints
            for tablename in _TABLESETTINGS:
                # we need to do this one-by-one: may result in deadlocks otherwise
                eventloop.run_until_complete(create_constraints(tablename))
            # publish data
            eventloop.run_until_complete(publish())
        finally:
            eventloop.run_until_complete(_shutdown())

    args = parseargs()
    # init logging
    logging.basicConfig(level=args.loglevel)
    # make sure CSV dir exists
    csv_dir = pathlib.Path(args.csvdir)
    if not csv_dir.exists():
        _logger.fatal("directory {} doesn't exist".format(csv_dir))
    dsn = args.dsn
    # create eventloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    eventloop = asyncio.get_event_loop()
    # initialize database
    eventloop.run_until_complete(initialize(eventloop, dsn))
    # run import
    run_import(eventloop)


if __name__ == '__main__':
    cli()
