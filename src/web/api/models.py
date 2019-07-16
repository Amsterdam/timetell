from django.db import models

# This is an auto-generated Django model module using python manage.py inspectdb.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey has `on_delete` set to the desired behavior.
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.


class Act(models.Model):
    act_id = models.IntegerField(primary_key=True)
    parent = models.ForeignKey('self', on_delete=models.DO_NOTHING, blank=True, null=True)
    fromdate = models.DateField(blank=True, null=True)
    todate = models.DateField(blank=True, null=True)
    name = models.CharField(max_length=50)
    nr = models.CharField(max_length=20, blank=True, null=True)
    code = models.CharField(max_length=20, blank=True, null=True)
    group = models.IntegerField(blank=True, null=True)
    account = models.CharField(max_length=20, blank=True, null=True)
    calcid = models.IntegerField(blank=True, null=True)
    authmode = models.IntegerField(blank=True, null=True)
    ratelevel = models.IntegerField(blank=True, null=True)
    ratetype = models.IntegerField(blank=True, null=True)
    inherit_id = models.IntegerField(blank=True, null=True)
    inherit = models.IntegerField(blank=True, null=True)
    prj_id = models.IntegerField(blank=True, null=True)
    prj_default = models.IntegerField(blank=True, null=True)
    export = models.IntegerField(blank=True, null=True)
    type = models.IntegerField(blank=True, null=True)
    clock = models.IntegerField(blank=True, null=True)
    balance = models.IntegerField(blank=True, null=True)
    daytotal = models.IntegerField(blank=True, null=True)
    nonegbal = models.IntegerField(blank=True, null=True)
    overtime = models.IntegerField(blank=True, null=True)
    fillinfo = models.IntegerField(blank=True, null=True)
    info = models.TextField(blank=True, null=True)
    updatelocal = models.DateTimeField(blank=True, null=True)
    nobooking = models.IntegerField(blank=True, null=True)
    act_path = models.CharField(max_length=100, blank=True, null=True)
    notmanual = models.IntegerField(blank=True, null=True)
    pl_color = models.IntegerField(blank=True, null=True)
    externkey = models.CharField(max_length=50, blank=True, null=True)
    regnumbers = models.IntegerField(blank=True, null=True)
    expires = models.IntegerField(blank=True, null=True)
    expiremonths = models.IntegerField(blank=True, null=True)
    selectbalance = models.IntegerField(blank=True, null=True)
    balance_min = models.FloatField(blank=True, null=True)
    balance_max = models.FloatField(blank=True, null=True)
    tag = models.IntegerField(blank=True, null=True)
    tagtype = models.IntegerField(blank=True, null=True)
    tagdate = models.DateField(blank=True, null=True)
    noselect = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'ACT'


class Cust(models.Model):
    cust_id = models.IntegerField(primary_key=True)
    parent = models.ForeignKey('self', on_delete=models.DO_NOTHING, blank=True, null=True)
    fromdate = models.DateField(blank=True, null=True)
    todate = models.DateField(blank=True, null=True)
    name = models.CharField(max_length=75)
    nr = models.CharField(max_length=20, blank=True, null=True)
    code = models.CharField(max_length=20, blank=True, null=True)
    group = models.IntegerField(blank=True, null=True)
    account = models.CharField(max_length=20, blank=True, null=True)
    authmode = models.IntegerField(blank=True, null=True)
    ratelevel = models.IntegerField(blank=True, null=True)
    inherit_id = models.IntegerField(blank=True, null=True)
    inherit = models.IntegerField(blank=True, null=True)
    address = models.CharField(max_length=100, blank=True, null=True)
    zipcode = models.CharField(max_length=10, blank=True, null=True)
    place = models.CharField(max_length=50, blank=True, null=True)
    district = models.CharField(max_length=50, blank=True, null=True)
    country = models.CharField(max_length=50, blank=True, null=True)
    phone1 = models.CharField(max_length=15, blank=True, null=True)
    phone2 = models.CharField(max_length=15, blank=True, null=True)
    mobile1 = models.CharField(max_length=15, blank=True, null=True)
    mobile2 = models.CharField(max_length=15, blank=True, null=True)
    fax1 = models.CharField(max_length=15, blank=True, null=True)
    fax2 = models.CharField(max_length=15, blank=True, null=True)
    email1 = models.CharField(max_length=15, blank=True, null=True)
    email2 = models.CharField(max_length=15, blank=True, null=True)
    info = models.TextField(blank=True, null=True)
    updatelocal = models.DateTimeField(blank=True, null=True)
    nobooking = models.IntegerField(blank=True, null=True)
    cust_path = models.CharField(max_length=100, blank=True, null=True)
    pl_color = models.IntegerField(blank=True, null=True)
    externkey = models.CharField(max_length=50, blank=True, null=True)
    tag = models.IntegerField(blank=True, null=True)
    tagtype = models.IntegerField(blank=True, null=True)
    tagdate = models.DateField(blank=True, null=True)
    distance = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'CUST'


class Org(models.Model):
    org_id = models.IntegerField(primary_key=True)
    parent = models.ForeignKey('self', on_delete=models.DO_NOTHING, blank=True, null=True)
    fromdate = models.DateField(blank=True, null=True)
    todate = models.DateField(blank=True, null=True)
    name = models.CharField(max_length=50, blank=True, null=True)
    nr = models.CharField(max_length=20, blank=True, null=True)
    code = models.CharField(max_length=20, blank=True, null=True)
    group = models.IntegerField(blank=True, null=True)
    account = models.CharField(max_length=20, blank=True, null=True)
    inherit_id = models.IntegerField(blank=True, null=True)
    inherit = models.IntegerField(blank=True, null=True)
    address = models.CharField(max_length=100, blank=True, null=True)
    zipcode = models.CharField(max_length=10, blank=True, null=True)
    place = models.CharField(max_length=50, blank=True, null=True)
    district = models.CharField(max_length=50, blank=True, null=True)
    country = models.CharField(max_length=50, blank=True, null=True)
    phone1 = models.CharField(max_length=15, blank=True, null=True)
    phone2 = models.CharField(max_length=15, blank=True, null=True)
    mobile1 = models.CharField(max_length=15, blank=True, null=True)
    mobile2 = models.CharField(max_length=15, blank=True, null=True)
    fax1 = models.CharField(max_length=15, blank=True, null=True)
    fax2 = models.CharField(max_length=15, blank=True, null=True)
    email1 = models.CharField(max_length=50, blank=True, null=True)
    email2 = models.CharField(max_length=50, blank=True, null=True)
    info = models.TextField(blank=True, null=True)
    updatelocal = models.DateTimeField(blank=True, null=True)
    org_path = models.CharField(max_length=100, blank=True, null=True)
    pl_color = models.IntegerField(blank=True, null=True)
    externkey = models.CharField(max_length=50, blank=True, null=True)
    tag = models.IntegerField(blank=True, null=True)
    tagtype = models.IntegerField(blank=True, null=True)
    tagdate = models.DateField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'ORG'


class Prj(models.Model):
    prj_id = models.IntegerField(primary_key=True)
    parent = models.ForeignKey('self', on_delete=models.DO_NOTHING, blank=True, null=True)
    fromdate = models.DateField(blank=True, null=True)
    todate = models.DateField(blank=True, null=True)
    name = models.CharField(max_length=50, blank=True, null=True)
    nr = models.CharField(max_length=20, blank=True, null=True)
    code = models.CharField(max_length=20, blank=True, null=True)
    group = models.IntegerField(blank=True, null=True)
    account = models.CharField(max_length=20, blank=True, null=True)
    authmode = models.IntegerField(blank=True, null=True)
    nobooking = models.IntegerField(blank=True, null=True)
    ratelevel = models.IntegerField(blank=True, null=True)
    inherit_id = models.IntegerField(blank=True, null=True)
    inherit = models.IntegerField(blank=True, null=True)
    act = models.ForeignKey(Act, on_delete=models.DO_NOTHING, blank=True, null=True)
    act_default = models.IntegerField(blank=True, null=True)
    cust = models.ForeignKey(Cust, on_delete=models.DO_NOTHING, blank=True, null=True)
    cust_default = models.IntegerField(blank=True, null=True)
    cust_contact_id = models.IntegerField(blank=True, null=True)
    info = models.TextField(blank=True, null=True)
    export = models.IntegerField(blank=True, null=True)
    status = models.IntegerField(blank=True, null=True)
    approve = models.IntegerField(blank=True, null=True)
    limitact = models.IntegerField(blank=True, null=True)
    limitcust = models.IntegerField(blank=True, null=True)
    limitcosts = models.IntegerField(blank=True, null=True)
    updatelocal = models.DateTimeField(blank=True, null=True)
    prj_path = models.CharField(max_length=100, blank=True, null=True)
    pl_color = models.IntegerField(blank=True, null=True)
    externkey = models.CharField(max_length=50, blank=True, null=True)
    calc_hours = models.FloatField(blank=True, null=True)
    calc_costs = models.FloatField(blank=True, null=True)
    tag = models.IntegerField(blank=True, null=True)
    tagtype = models.IntegerField(blank=True, null=True)
    tagdate = models.DateField(blank=True, null=True)
    allowplan = models.IntegerField(blank=True, null=True)
    exp_approve = models.IntegerField(blank=True, null=True)
    prjcat = models.IntegerField(blank=True, null=True)

    def __str__(self):
        return "{} - {}".format(self.nr, self.name)

    class Meta:
        managed = False
        db_table = 'PRJ'


class Emp(models.Model):
    emp_id = models.IntegerField(primary_key=True)
    nr = models.CharField(max_length=20, blank=True, null=True)
    code = models.CharField(max_length=20, blank=True, null=True)
    lastname = models.CharField(max_length=50, blank=True, null=True)
    middlename = models.CharField(max_length=10, blank=True, null=True)
    firstname = models.CharField(max_length=30, blank=True, null=True)
    plastname = models.CharField(max_length=50, blank=True, null=True)
    pmiddlename = models.CharField(max_length=10, blank=True, null=True)
    displayname = models.IntegerField(blank=True, null=True)
    sex = models.CharField(max_length=1, blank=True, null=True)
    initials = models.CharField(max_length=10, blank=True, null=True)
    nonactive = models.BooleanField(default=1)
    activeforclk = models.BooleanField(default=1)
    birthdate = models.DateField(blank=True, null=True)
    loginname = models.CharField(max_length=50, blank=True, null=True)
    password = models.CharField(max_length=150, blank=True, null=True)
    askpassword = models.IntegerField(blank=True, null=True)
    auth_id = models.IntegerField(blank=True, null=True)
    fromgroup = models.IntegerField(blank=True, null=True)
    togroup = models.IntegerField(blank=True, null=True)
    taskreg = models.DateField(blank=True, null=True)
    empcat = models.IntegerField(blank=True, null=True)
    export = models.IntegerField(blank=True, null=True)
    address = models.CharField(max_length=100, blank=True, null=True)
    zipcode = models.CharField(max_length=10, blank=True, null=True)
    place = models.CharField(max_length=50, blank=True, null=True)
    district = models.CharField(max_length=50, blank=True, null=True)
    country = models.CharField(max_length=50, blank=True, null=True)
    phone1 = models.CharField(max_length=15, blank=True, null=True)
    phone2 = models.CharField(max_length=15, blank=True, null=True)
    mobile1 = models.CharField(max_length=15, blank=True, null=True)
    mobile2 = models.CharField(max_length=15, blank=True, null=True)
    fax1 = models.CharField(max_length=15, blank=True, null=True)
    fax2 = models.CharField(max_length=15, blank=True, null=True)
    email1 = models.CharField(max_length=50, blank=True, null=True)
    email2 = models.CharField(max_length=50, blank=True, null=True)
    floor = models.IntegerField(blank=True, null=True)
    location = models.CharField(max_length=20, blank=True, null=True)
    room = models.CharField(max_length=10, blank=True, null=True)
    extension = models.CharField(max_length=15, blank=True, null=True)
    bank = models.CharField(max_length=25, blank=True, null=True)
    leasecar = models.CharField(max_length=20, blank=True, null=True)
    traveldist = models.FloatField(blank=True, null=True)
    travelcosts = models.FloatField(blank=True, null=True)
    config_id = models.IntegerField(blank=True, null=True)
    refreshlocal = models.DateField(blank=True, null=True)
    name = models.CharField(max_length=100, blank=True, null=True)
    firstnames = models.CharField(max_length=50, blank=True, null=True)
    leadtitle = models.CharField(max_length=20, blank=True, null=True)
    trailtitle = models.CharField(max_length=20, blank=True, null=True)
    addressno = models.CharField(max_length=20, blank=True, null=True)
    birthplace = models.CharField(max_length=75, blank=True, null=True)
    birthcountry = models.CharField(max_length=75, blank=True, null=True)
    nationality = models.CharField(max_length=50, blank=True, null=True)
    bsn = models.CharField(max_length=20, blank=True, null=True)
    idnumber = models.CharField(max_length=40, blank=True, null=True)
    mstatus = models.IntegerField(blank=True, null=True)
    mstatusdate = models.DateField(blank=True, null=True)
    warninfo = models.TextField(blank=True, null=True)
    medicalinfo = models.TextField(blank=True, null=True)
    psex = models.CharField(max_length=1, blank=True, null=True)
    pinitials = models.CharField(max_length=10, blank=True, null=True)
    pfirstnames = models.CharField(max_length=50, blank=True, null=True)
    pfirstname = models.CharField(max_length=30, blank=True, null=True)
    pleadtitle = models.CharField(max_length=20, blank=True, null=True)
    ptrailtitle = models.CharField(max_length=20, blank=True, null=True)
    pbirthdate = models.DateField(blank=True, null=True)
    pphone = models.CharField(max_length=15, blank=True, null=True)
    startdate = models.DateField(blank=True, null=True)
    enddate = models.DateField(blank=True, null=True)
    jubdate = models.DateField(blank=True, null=True)
    funcdate = models.DateField(blank=True, null=True)
    assesdate = models.DateField(blank=True, null=True)
    svcyears = models.IntegerField(blank=True, null=True)
    noldap = models.IntegerField(blank=True, null=True)
    pl_color = models.IntegerField(blank=True, null=True)
    idexpiredate = models.DateField(blank=True, null=True)
    externkey = models.CharField(max_length=50, blank=True, null=True)
    recipient = models.CharField(max_length=100, blank=True, null=True)
    audit = models.IntegerField(blank=True, null=True)
    syncprof_id = models.IntegerField(blank=True, null=True)
    aduser = models.IntegerField(blank=True, null=True)
    changepassword = models.IntegerField(blank=True, null=True)
    dayhours = models.FloatField(blank=True, null=True)
    savedpassword = models.CharField(max_length=150, blank=True, null=True)
    tag = models.IntegerField(blank=True, null=True)
    tagtype = models.IntegerField(blank=True, null=True)
    tagdate = models.DateField(blank=True, null=True)

    projects = models.ManyToManyField(Prj, through='PrjLink')

    #def get_employees_set(self):
    #    return PrjLink.objects.filter(prjlink_prj_id_set__prj__prj_id=self.prj_id)

    def __str__(self):
        return "{} {} {}".format(self.firstname, self.middlename, self.lastname)

    class Meta:
        managed = False
        db_table = 'EMP'


class PrjLink(models.Model):
    prj = models.ForeignKey(Prj, on_delete=models.DO_NOTHING, blank=True, null=True)
    emp = models.ForeignKey(Emp, on_delete=models.DO_NOTHING, blank=True, null=True)
    org = models.ForeignKey(Org, on_delete=models.DO_NOTHING, blank=True, null=True)
    auth = models.BooleanField(default=0)
    book = models.BooleanField(default=0)
    prjleader = models.BooleanField(default=0)
    tag = models.IntegerField(blank=True, null=True)
    tagtype = models.IntegerField(blank=True, null=True)
    tagdate = models.DateField(blank=True, null=True)
    prj_link_id = models.IntegerField(primary_key=True)

    #employee = models.ManyToManyField(Emp)

    class Meta:
        managed = False
        db_table = 'PRJ_LINK'


class CustContact(models.Model):
    cust_contact_id = models.IntegerField(primary_key=True)
    cust_id = models.IntegerField(blank=True, null=True)
    contact = models.CharField(max_length=100, blank=True, null=True)
    department = models.CharField(max_length=50, blank=True, null=True)
    email = models.CharField(max_length=50, blank=True, null=True)
    phone = models.CharField(max_length=50, blank=True, null=True)
    item_id = models.IntegerField(blank=True, null=True)
    tag = models.IntegerField(blank=True, null=True)
    tagtype = models.IntegerField(blank=True, null=True)
    tagdate = models.DateField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'CUST_CONTACT'


class EmpContract(models.Model):
    emp = models.ForeignKey(Emp, on_delete=models.DO_NOTHING, blank=True, null=True)
    fromdate = models.DateField(blank=True, null=True)
    todate = models.DateField(blank=True, null=True)
    hours = models.FloatField(blank=True, null=True)
    hoursmethod = models.IntegerField(blank=True, null=True)
    fte = models.FloatField(blank=True, null=True)
    scale = models.IntegerField(blank=True, null=True)
    step = models.IntegerField(blank=True, null=True)
    salary = models.FloatField(blank=True, null=True)
    type = models.IntegerField(blank=True, null=True)
    job_id = models.IntegerField(blank=True, null=True)
    actprof_id = models.IntegerField(blank=True, null=True)
    rate_id = models.IntegerField(blank=True, null=True)
    rate = models.FloatField(blank=True, null=True)
    internalrate = models.FloatField(blank=True, null=True)
    overtimerate = models.FloatField(blank=True, null=True)
    calcrule_id = models.IntegerField(blank=True, null=True)
    perdate = models.DateField(blank=True, null=True)
    addleaveyear = models.FloatField(blank=True, null=True)
    addleavehour = models.FloatField(blank=True, null=True)
    percnotplan = models.FloatField(blank=True, null=True)
    calcid = models.IntegerField(blank=True, null=True)
    holidaygroup = models.IntegerField(blank=True, null=True)
    bookhours = models.IntegerField(blank=True, null=True)
    min_hours = models.FloatField(blank=True, null=True)
    calctype_id = models.IntegerField(blank=True, null=True)
    tag = models.IntegerField(blank=True, null=True)
    tagtype = models.IntegerField(blank=True, null=True)
    tagdate = models.DateField(blank=True, null=True)
    leavecalc_id = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'EMP_CONTRACT'


class EmpOrg(models.Model):
    emp = models.ForeignKey(Emp, on_delete=models.DO_NOTHING, blank=True, null=True)
    org = models.ForeignKey('Org', on_delete=models.DO_NOTHING, blank=True, null=True)
    fromdate = models.DateField(blank=True, null=True)
    todate = models.DateField(blank=True, null=True)
    type = models.IntegerField(blank=True, null=True)
    book = models.IntegerField(blank=True, null=True)
    auth = models.IntegerField(blank=True, null=True)
    tag = models.IntegerField(blank=True, null=True)
    tagtype = models.IntegerField(blank=True, null=True)
    tagdate = models.DateField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'EMP_ORG'


class Hrs(models.Model):
    hrs_id = models.IntegerField(primary_key=True)
    wk_id = models.IntegerField(blank=True, null=True)
    emp = models.ForeignKey(Emp, on_delete=models.DO_NOTHING, blank=True, null=True)
    act = models.ForeignKey(Act, on_delete=models.DO_NOTHING, blank=True, null=True)
    prj = models.ForeignKey('Prj', on_delete=models.DO_NOTHING, blank=True, null=True)
    cust = models.ForeignKey(Cust, on_delete=models.DO_NOTHING, blank=True, null=True)
    org = models.ForeignKey('Org', on_delete=models.DO_NOTHING, blank=True, null=True)
    year = models.IntegerField(blank=True, null=True)
    month = models.IntegerField(blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    fromtime = models.DateField(blank=True, null=True)
    totime = models.DateField(blank=True, null=True)
    hours = models.FloatField(blank=True, null=True)
    break_field = models.FloatField(db_column='break', blank=True, null=True)  # Field renamed because it was a Python reserved word.
    overtime = models.FloatField(blank=True, null=True)
    internalrate = models.FloatField(blank=True, null=True)
    rate = models.FloatField(blank=True, null=True)
    hoursrate = models.FloatField(blank=True, null=True)
    hoursinternalrate = models.FloatField(blank=True, null=True)
    chargeable = models.IntegerField(blank=True, null=True)
    chargehours = models.FloatField(blank=True, null=True)
    charged = models.IntegerField(blank=True, null=True)
    booktype = models.IntegerField(blank=True, null=True)
    info = models.TextField(blank=True, null=True)
    exportflag = models.IntegerField(blank=True, null=True)
    exportdate = models.DateField(blank=True, null=True)
    emp_book_id = models.IntegerField(blank=True, null=True)
    status = models.IntegerField(blank=True, null=True)
    linenr = models.IntegerField(blank=True, null=True)
    tag = models.IntegerField(blank=True, null=True)
    tagtype = models.IntegerField(blank=True, null=True)
    tagdate = models.DateField(blank=True, null=True)
    balance_id = models.IntegerField(blank=True, null=True)
    plan_week_id = models.IntegerField(blank=True, null=True)
    plan_alloc_id = models.IntegerField(blank=True, null=True)
    calendar_id = models.IntegerField(blank=True, null=True)
    prjapproved_by = models.IntegerField(blank=True, null=True)
    prjapproved_on = models.DateField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'HRS'


class Job(models.Model):
    job_id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=50, blank=True, null=True)
    nr = models.CharField(max_length=20, blank=True, null=True)
    code = models.CharField(max_length=20, blank=True, null=True)
    nonactive = models.IntegerField(blank=True, null=True)
    info = models.TextField(blank=True, null=True)
    tag = models.IntegerField(blank=True, null=True)
    tagtype = models.IntegerField(blank=True, null=True)
    tagdate = models.DateField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'JOB'


class SysOptItm(models.Model):
    item_id = models.IntegerField(blank=True, null=True)
    opt_id = models.IntegerField(blank=True, null=True)
    item = models.CharField(max_length=50, blank=True, null=True)
    code = models.CharField(max_length=10, blank=True, null=True)
    updatelocal = models.DateTimeField(blank=True, null=True)
    calcid = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'SYS_OPT_ITM'


class SysPrjNiv(models.Model):
    niv = models.IntegerField(primary_key=True)
    niv_0 = models.ForeignKey(Prj, on_delete=models.DO_NOTHING, related_name='prj_niv_0', db_column='niv_id', blank=True, null=True)  # Field renamed because of name conflict.
    niv_name = models.CharField(max_length=50, blank=True, null=True)
    niv0 = models.ForeignKey(Prj, on_delete=models.DO_NOTHING, blank=True, null=True, related_name='prj_niv0')
    niv0_name = models.CharField(max_length=50, blank=True, null=True)
    niv1 = models.ForeignKey(Prj, on_delete=models.DO_NOTHING, blank=True, null=True, related_name='prj_niv1')
    niv1_name = models.CharField(max_length=50, blank=True, null=True)
    niv2 = models.ForeignKey(Prj, on_delete=models.DO_NOTHING, blank=True, null=True, related_name='prj_niv2')
    niv2_name = models.CharField(max_length=50, blank=True, null=True)
    niv3 = models.ForeignKey(Prj, on_delete=models.DO_NOTHING, blank=True, null=True, related_name='prj_niv3')
    niv3_name = models.CharField(max_length=50, blank=True, null=True)
    niv4 = models.ForeignKey(Prj, on_delete=models.DO_NOTHING, blank=True, null=True, related_name='prj_niv4')
    niv4_name = models.CharField(max_length=50, blank=True, null=True)
    niv5 = models.ForeignKey(Prj, on_delete=models.DO_NOTHING, blank=True, null=True, related_name='prj_niv5')
    niv5_name = models.CharField(max_length=50, blank=True, null=True)
    niv6 = models.ForeignKey(Prj, on_delete=models.DO_NOTHING, blank=True, null=True, related_name='prj_niv6')
    niv6_name = models.CharField(max_length=50, blank=True, null=True)
    niv7 = models.ForeignKey(Prj, on_delete=models.DO_NOTHING, blank=True, null=True, related_name='prj_niv7')
    niv7_name = models.CharField(max_length=50, blank=True, null=True)
    niv8 = models.ForeignKey(Prj, on_delete=models.DO_NOTHING, blank=True, null=True, related_name='prj_niv8')
    niv8_name = models.CharField(max_length=50, blank=True, null=True)
    niv9 = models.ForeignKey(Prj, on_delete=models.DO_NOTHING, blank=True, null=True, related_name='prj_niv9')
    niv9_name = models.CharField(max_length=50, blank=True, null=True)
    fullpath = models.CharField(max_length=200, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'SYS_PRJ_NIV'


class VwLabelPrj(models.Model):
    dim_label_id = models.IntegerField()
    dim_id = models.IntegerField(blank=True, null=True)
    type = models.IntegerField(blank=True, null=True)
    item_id = models.IntegerField(blank=True, null=True)
    value = models.DecimalField(max_digits=65535, decimal_places=65535, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    desc = models.CharField(max_length=50, blank=True, null=True)
    tag = models.IntegerField(blank=True, null=True)
    tagtype = models.IntegerField(blank=True, null=True)
    tagdate = models.DateField(blank=True, null=True)
    todate = models.DateField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'VW_LABEL_PRJ'
