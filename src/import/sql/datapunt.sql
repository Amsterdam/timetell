CREATE MATERIALIZED VIEW "{schemaname}"."v_timetell_projectenoverzicht" AS
  SELECT
    a.hrs_date,
    a.hrs_hours,
    a.hrs_internalrate,
    a.hrs_rate,
    a.hrs_hoursrate,
    a.hrs_hoursinternalrate,
    a.cust_name,
    a.org_name,
    a.emp_empcat,
    a.emp_name,
    a."Medew. Verantw.",
    a.act_name,
    a.prj_niv,
    a.prj_niv_name,
    a.prj_niv0_name,
    a.prj_niv1_name,
    a.prj_niv2_name,
    a."Project Verantw.",
    a."Project Leader",
    NULL :: INTEGER           AS prj_niv_id,
    NULL :: INTEGER           AS prj_niv0_id,
    NULL :: INTEGER           AS prj_niv1_id,
    NULL :: INTEGER           AS prj_niv2_id,
    NULL :: INTEGER           AS prj_prj_id,
    NULL :: INTEGER           AS prj_parent_id,
    NULL :: DATE              AS fromdate,
    NULL :: DATE              AS todate,
    NULL :: CHARACTER VARYING AS prj_name,
    NULL :: DOUBLE PRECISION  AS calc_hours,
    NULL :: DOUBLE PRECISION  AS calc_costs,
    NULL :: CHARACTER VARYING AS "Project Nummer"
  FROM (SELECT
          a_1.hrs_date,
          a_1.hrs_hours,
          a_1.hrs_internalrate,
          a_1.hrs_rate,
          a_1.hrs_hoursrate,
          a_1.hrs_hoursinternalrate,
          a_1.cust_name,
          a_1.org_name,
          a_1.emp_empcat,
          a_1.emp_name,
          a_1."Medew. Verantw.",
          a_1.act_name,
          a_1.prj_niv,
          a_1.prj_niv_name,
          a_1.prj_niv0_name,
          a_1.prj_niv1_name,
          a_1."prj_niv2_ name" AS prj_niv2_name,
          a_1."Project Verantw.",
          a_1."Project Leader"
        FROM (SELECT
                "Left".hrs_prj_id,
                "Left".hrs_date,
                "Left".hrs_hours,
                "Left".hrs_internalrate,
                "Left".hrs_rate,
                "Left".hrs_hoursrate,
                "Left".hrs_hoursinternalrate,
                "Left".cust_name,
                "Left".org_name,
                "Left".emp_empcat,
                "Left".emp_name,
                "Left"."Medew. Verantw.",
                "Left".act_name,
                "Right".prj_niv,
                "Right".prj_niv_id,
                "Right".prj_niv_name,
                "Right".prj_niv0_id,
                "Right".prj_niv0_name,
                "Right".prj_niv1_id,
                "Right".prj_niv1_name,
                "Right".prj_niv2_id,
                "Right"."prj_niv2_ name",
                "Right".prj_prj_id,
                "Right"."Project Verantw.",
                "Right"."Project Leader",
                "Right".prj_id
              FROM (SELECT
                      a_2.hrs_prj_id,
                      a_2.hrs_date,
                      a_2.hrs_hours,
                      a_2.hrs_internalrate,
                      a_2.hrs_rate,
                      a_2.hrs_hoursrate,
                      a_2.hrs_hoursinternalrate,
                      a_2.cust_name,
                      a_2.org_name,
                      a_2.emp_empcat,
                      a_2.emp_name,
                      a_2."Medew. Verantw.",
                      a_2.act_name
                    FROM (SELECT
                            "Left_1".hrs_hrs_id,
                            "Left_1".hrs_act_id,
                            "Left_1".hrs_prj_id,
                            "Left_1".hrs_date,
                            "Left_1".hrs_hours,
                            "Left_1".hrs_internalrate,
                            "Left_1".hrs_rate,
                            "Left_1".hrs_hoursrate,
                            "Left_1".hrs_hoursinternalrate,
                            "Left_1".cust_name,
                            "Left_1".org_name,
                            "Left_1".emp_empcat,
                            "Left_1".emp_name,
                            "Left_1"."Medew. Verantw.",
                            "Right_1".act_act_id,
                            "Right_1".act_name
                          FROM (SELECT
                                  a_3.hrs_hrs_id,
                                  a_3.hrs_act_id,
                                  a_3.hrs_prj_id,
                                  a_3.hrs_date,
                                  a_3.hrs_hours,
                                  a_3.hrs_internalrate,
                                  a_3.hrs_rate,
                                  a_3.hrs_hoursrate,
                                  a_3.hrs_hoursinternalrate,
                                  a_3.cust_name,
                                  a_3.org_name,
                                  a_3.emp_empcat,
                                  a_3.emp_name,
                                  a_3."Medew. Verantw."
                                FROM (SELECT
                                        "Left_2".hrs_hrs_id,
                                        "Left_2".hrs_emp_id,
                                        "Left_2".hrs_act_id,
                                        "Left_2".hrs_prj_id,
                                        "Left_2".hrs_date,
                                        "Left_2".hrs_hours,
                                        "Left_2".hrs_internalrate,
                                        "Left_2".hrs_rate,
                                        "Left_2".hrs_hoursrate,
                                        "Left_2".hrs_hoursinternalrate,
                                        "Left_2".cust_name,
                                        "Left_2".org_name,
                                        "Right_2".emp_emp_id,
                                        "Right_2".emp_empcat,
                                        "Right_2".emp_name,
                                        "Right_2"."Medew. Verantw."
                                      FROM (SELECT
                                              a_4.hrs_hrs_id,
                                              a_4.hrs_emp_id,
                                              a_4.hrs_act_id,
                                              a_4.hrs_prj_id,
                                              a_4.hrs_date,
                                              a_4.hrs_hours,
                                              a_4.hrs_internalrate,
                                              a_4.hrs_rate,
                                              a_4.hrs_hoursrate,
                                              a_4.hrs_hoursinternalrate,
                                              a_4.cust_name,
                                              a_4.org_name
                                            FROM (SELECT
                                                    "Left_3".hrs_hrs_id,
                                                    "Left_3".hrs_emp_id,
                                                    "Left_3".hrs_act_id,
                                                    "Left_3".hrs_prj_id,
                                                    "Left_3".hrs_org_id,
                                                    "Left_3".hrs_date,
                                                    "Left_3".hrs_hours,
                                                    "Left_3".hrs_internalrate,
                                                    "Left_3".hrs_rate,
                                                    "Left_3".hrs_hoursrate,
                                                    "Left_3".hrs_hoursinternalrate,
                                                    "Left_3".cust_name,
                                                    "Right_3".org_org_id,
                                                    "Right_3".org_name
                                                  FROM (SELECT
                                                          a_5.hrs_hrs_id,
                                                          a_5.hrs_emp_id,
                                                          a_5.hrs_act_id,
                                                          a_5.hrs_prj_id,
                                                          a_5.hrs_org_id,
                                                          a_5.hrs_date,
                                                          a_5.hrs_hours,
                                                          a_5.hrs_internalrate,
                                                          a_5.hrs_rate,
                                                          a_5.hrs_hoursrate,
                                                          a_5.hrs_hoursinternalrate,
                                                          a_5.cust_name
                                                        FROM (SELECT
                                                                "Left_4".hrs_hrs_id,
                                                                "Left_4".hrs_emp_id,
                                                                "Left_4".hrs_act_id,
                                                                "Left_4".hrs_prj_id,
                                                                "Left_4".hrs_cust_id,
                                                                "Left_4".hrs_org_id,
                                                                "Left_4".hrs_date,
                                                                "Left_4".hrs_hours,
                                                                "Left_4".hrs_internalrate,
                                                                "Left_4".hrs_rate,
                                                                "Left_4".hrs_hoursrate,
                                                                "Left_4".hrs_hoursinternalrate,
                                                                "Right_4".cust_cust_id,
                                                                "Right_4".cust_name
                                                              FROM (SELECT
                                                                      a_6.hrs_id            AS hrs_hrs_id,
                                                                      a_6.emp_id            AS hrs_emp_id,
                                                                      a_6.act_id            AS hrs_act_id,
                                                                      a_6.prj_id            AS hrs_prj_id,
                                                                      a_6.cust_id           AS hrs_cust_id,
                                                                      a_6.org_id            AS hrs_org_id,
                                                                      a_6.date              AS hrs_date,
                                                                      a_6.hours             AS hrs_hours,
                                                                      a_6.internalrate      AS hrs_internalrate,
                                                                      a_6.rate              AS hrs_rate,
                                                                      a_6.hoursrate         AS hrs_hoursrate,
                                                                      a_6.hoursinternalrate AS hrs_hoursinternalrate
                                                                    FROM (SELECT
                                                                            "{schemaname}"."HRS".hrs_id,
                                                                            "{schemaname}"."HRS".wk_id,
                                                                            "{schemaname}"."HRS".emp_id,
                                                                            "{schemaname}"."HRS".act_id,
                                                                            "{schemaname}"."HRS".prj_id,
                                                                            "{schemaname}"."HRS".cust_id,
                                                                            "{schemaname}"."HRS".org_id,
                                                                            "{schemaname}"."HRS".year,
                                                                            "{schemaname}"."HRS".month,
                                                                            "{schemaname}"."HRS".date,
                                                                            "{schemaname}"."HRS".fromtime,
                                                                            "{schemaname}"."HRS".totime,
                                                                            "{schemaname}"."HRS".hours,
                                                                            "{schemaname}"."HRS".break,
                                                                            "{schemaname}"."HRS".overtime,
                                                                            "{schemaname}"."HRS".internalrate,
                                                                            "{schemaname}"."HRS".rate,
                                                                            "{schemaname}"."HRS".hoursrate,
                                                                            "{schemaname}"."HRS".hoursinternalrate,
                                                                            "{schemaname}"."HRS".chargeable,
                                                                            "{schemaname}"."HRS".chargehours,
                                                                            "{schemaname}"."HRS".charged,
                                                                            "{schemaname}"."HRS".booktype,
                                                                            "{schemaname}"."HRS".info,
                                                                            "{schemaname}"."HRS".exportflag,
                                                                            "{schemaname}"."HRS".exportdate,
                                                                            "{schemaname}"."HRS".emp_book_id,
                                                                            "{schemaname}"."HRS".status,
                                                                            "{schemaname}"."HRS".linenr,
                                                                            "{schemaname}"."HRS".tag,
                                                                            "{schemaname}"."HRS".tagtype,
                                                                            "{schemaname}"."HRS".tagdate,
                                                                            "{schemaname}"."HRS".balance_id,
                                                                            "{schemaname}"."HRS".plan_week_id,
                                                                            "{schemaname}"."HRS".plan_alloc_id,
                                                                            "{schemaname}"."HRS".calendar_id,
                                                                            "{schemaname}"."HRS".prjapproved_by,
                                                                            "{schemaname}"."HRS".prjapproved_on
                                                                          FROM "{schemaname}"."HRS") a_6) "Left_4"
                                                                LEFT JOIN (SELECT
                                                                             a_6.cust_id AS cust_cust_id,
                                                                             a_6.name    AS cust_name
                                                                           FROM (SELECT
                                                                                   "{schemaname}"."CUST".cust_id,
                                                                                   "{schemaname}"."CUST".parent_id,
                                                                                   "{schemaname}"."CUST".fromdate,
                                                                                   "{schemaname}"."CUST".todate,
                                                                                   "{schemaname}"."CUST".name,
                                                                                   "{schemaname}"."CUST".nr,
                                                                                   "{schemaname}"."CUST".code,
                                                                                   "{schemaname}"."CUST"."group",
                                                                                   "{schemaname}"."CUST".account,
                                                                                   "{schemaname}"."CUST".authmode,
                                                                                   "{schemaname}"."CUST".ratelevel,
                                                                                   "{schemaname}"."CUST".inherit_id,
                                                                                   "{schemaname}"."CUST".inherit,
                                                                                   "{schemaname}"."CUST".address,
                                                                                   "{schemaname}"."CUST".zipcode,
                                                                                   "{schemaname}"."CUST".place,
                                                                                   "{schemaname}"."CUST".district,
                                                                                   "{schemaname}"."CUST".country,
                                                                                   "{schemaname}"."CUST".phone1,
                                                                                   "{schemaname}"."CUST".phone2,
                                                                                   "{schemaname}"."CUST".mobile1,
                                                                                   "{schemaname}"."CUST".mobile2,
                                                                                   "{schemaname}"."CUST".fax1,
                                                                                   "{schemaname}"."CUST".fax2,
                                                                                   "{schemaname}"."CUST".email1,
                                                                                   "{schemaname}"."CUST".email2,
                                                                                   "{schemaname}"."CUST".info,
                                                                                   "{schemaname}"."CUST".updatelocal,
                                                                                   "{schemaname}"."CUST".nobooking,
                                                                                   "{schemaname}"."CUST".cust_path,
                                                                                   "{schemaname}"."CUST".pl_color,
                                                                                   "{schemaname}"."CUST".externkey,
                                                                                   "{schemaname}"."CUST".tag,
                                                                                   "{schemaname}"."CUST".tagtype,
                                                                                   "{schemaname}"."CUST".tagdate,
                                                                                   "{schemaname}"."CUST".distance
                                                                                 FROM "{schemaname}"."CUST") a_6) "Right_4"
                                                                  ON "Left_4".hrs_cust_id =
                                                                     "Right_4".cust_cust_id) a_5) "Left_3"
                                                    LEFT JOIN (SELECT
                                                                 a_5.org_id AS org_org_id,
                                                                 a_5.name   AS org_name
                                                               FROM (SELECT
                                                                       "{schemaname}"."ORG".org_id,
                                                                       "{schemaname}"."ORG".parent_id,
                                                                       "{schemaname}"."ORG".fromdate,
                                                                       "{schemaname}"."ORG".todate,
                                                                       "{schemaname}"."ORG".name,
                                                                       "{schemaname}"."ORG".nr,
                                                                       "{schemaname}"."ORG".code,
                                                                       "{schemaname}"."ORG"."group",
                                                                       "{schemaname}"."ORG".account,
                                                                       "{schemaname}"."ORG".inherit_id,
                                                                       "{schemaname}"."ORG".inherit,
                                                                       "{schemaname}"."ORG".address,
                                                                       "{schemaname}"."ORG".zipcode,
                                                                       "{schemaname}"."ORG".place,
                                                                       "{schemaname}"."ORG".district,
                                                                       "{schemaname}"."ORG".country,
                                                                       "{schemaname}"."ORG".phone1,
                                                                       "{schemaname}"."ORG".phone2,
                                                                       "{schemaname}"."ORG".mobile1,
                                                                       "{schemaname}"."ORG".mobile2,
                                                                       "{schemaname}"."ORG".fax1,
                                                                       "{schemaname}"."ORG".fax2,
                                                                       "{schemaname}"."ORG".email1,
                                                                       "{schemaname}"."ORG".email2,
                                                                       "{schemaname}"."ORG".info,
                                                                       "{schemaname}"."ORG".updatelocal,
                                                                       "{schemaname}"."ORG".org_path,
                                                                       "{schemaname}"."ORG".pl_color,
                                                                       "{schemaname}"."ORG".externkey,
                                                                       "{schemaname}"."ORG".tag,
                                                                       "{schemaname}"."ORG".tagtype,
                                                                       "{schemaname}"."ORG".tagdate
                                                                     FROM "{schemaname}"."ORG") a_5) "Right_3"
                                                      ON "Left_3".hrs_org_id = "Right_3".org_org_id) a_4) "Left_2"
                                        LEFT JOIN (SELECT
                                                     a_4.emp_emp_id,
                                                     a_4.emp_empcat,
                                                     a_4.emp_name,
                                                     a_4."Medew. Verantw."
                                                   FROM (SELECT
                                                           "Left_3".emp_emp_id,
                                                           "Left_3".emp_empcat,
                                                           "Left_3".emp_name,
                                                           "Right_3".emp_id,
                                                           "Right_3"."Medew. Verantw."
                                                         FROM (SELECT
                                                                 a_5.emp_id AS emp_emp_id,
                                                                 a_5.empcat AS emp_empcat,
                                                                 a_5.name   AS emp_name
                                                               FROM (SELECT
                                                                       "{schemaname}"."EMP".emp_id,
                                                                       "{schemaname}"."EMP".nr,
                                                                       "{schemaname}"."EMP".code,
                                                                       "{schemaname}"."EMP".lastname,
                                                                       "{schemaname}"."EMP".middlename,
                                                                       "{schemaname}"."EMP".firstname,
                                                                       "{schemaname}"."EMP".plastname,
                                                                       "{schemaname}"."EMP".pmiddlename,
                                                                       "{schemaname}"."EMP".displayname,
                                                                       "{schemaname}"."EMP".sex,
                                                                       "{schemaname}"."EMP".initials,
                                                                       "{schemaname}"."EMP".nonactive,
                                                                       "{schemaname}"."EMP".activeforclk,
                                                                       "{schemaname}"."EMP".birthdate,
                                                                       "{schemaname}"."EMP".loginname,
                                                                       "{schemaname}"."EMP".password,
                                                                       "{schemaname}"."EMP".askpassword,
                                                                       "{schemaname}"."EMP".auth_id,
                                                                       "{schemaname}"."EMP".fromgroup,
                                                                       "{schemaname}"."EMP".togroup,
                                                                       "{schemaname}"."EMP".taskreg,
                                                                       "{schemaname}"."EMP".empcat,
                                                                       "{schemaname}"."EMP".export,
                                                                       "{schemaname}"."EMP".address,
                                                                       "{schemaname}"."EMP".zipcode,
                                                                       "{schemaname}"."EMP".place,
                                                                       "{schemaname}"."EMP".district,
                                                                       "{schemaname}"."EMP".country,
                                                                       "{schemaname}"."EMP".phone1,
                                                                       "{schemaname}"."EMP".phone2,
                                                                       "{schemaname}"."EMP".mobile1,
                                                                       "{schemaname}"."EMP".mobile2,
                                                                       "{schemaname}"."EMP".fax1,
                                                                       "{schemaname}"."EMP".fax2,
                                                                       "{schemaname}"."EMP".email1,
                                                                       "{schemaname}"."EMP".email2,
                                                                       "{schemaname}"."EMP".floor,
                                                                       "{schemaname}"."EMP".location,
                                                                       "{schemaname}"."EMP".room,
                                                                       "{schemaname}"."EMP".extension,
                                                                       "{schemaname}"."EMP".bank,
                                                                       "{schemaname}"."EMP".leasecar,
                                                                       "{schemaname}"."EMP".traveldist,
                                                                       "{schemaname}"."EMP".travelcosts,
                                                                       "{schemaname}"."EMP".config_id,
                                                                       "{schemaname}"."EMP".refreshlocal,
                                                                       "{schemaname}"."EMP".name,
                                                                       "{schemaname}"."EMP".firstnames,
                                                                       "{schemaname}"."EMP".leadtitle,
                                                                       "{schemaname}"."EMP".trailtitle,
                                                                       "{schemaname}"."EMP".addressno,
                                                                       "{schemaname}"."EMP".birthplace,
                                                                       "{schemaname}"."EMP".birthcountry,
                                                                       "{schemaname}"."EMP".nationality,
                                                                       "{schemaname}"."EMP".bsn,
                                                                       "{schemaname}"."EMP".idnumber,
                                                                       "{schemaname}"."EMP".mstatus,
                                                                       "{schemaname}"."EMP".mstatusdate,
                                                                       "{schemaname}"."EMP".warninfo,
                                                                       "{schemaname}"."EMP".medicalinfo,
                                                                       "{schemaname}"."EMP".psex,
                                                                       "{schemaname}"."EMP".pinitials,
                                                                       "{schemaname}"."EMP".pfirstnames,
                                                                       "{schemaname}"."EMP".pfirstname,
                                                                       "{schemaname}"."EMP".pleadtitle,
                                                                       "{schemaname}"."EMP".ptrailtitle,
                                                                       "{schemaname}"."EMP".pbirthdate,
                                                                       "{schemaname}"."EMP".pphone,
                                                                       "{schemaname}"."EMP".startdate,
                                                                       "{schemaname}"."EMP".enddate,
                                                                       "{schemaname}"."EMP".jubdate,
                                                                       "{schemaname}"."EMP".funcdate,
                                                                       "{schemaname}"."EMP".assesdate,
                                                                       "{schemaname}"."EMP".svcyears,
                                                                       "{schemaname}"."EMP".noldap,
                                                                       "{schemaname}"."EMP".pl_color,
                                                                       "{schemaname}"."EMP".idexpiredate,
                                                                       "{schemaname}"."EMP".externkey,
                                                                       "{schemaname}"."EMP".recipient,
                                                                       "{schemaname}"."EMP".audit,
                                                                       "{schemaname}"."EMP".syncprof_id,
                                                                       "{schemaname}"."EMP".aduser,
                                                                       "{schemaname}"."EMP".changepassword,
                                                                       "{schemaname}"."EMP".dayhours,
                                                                       "{schemaname}"."EMP".savedpassword,
                                                                       "{schemaname}"."EMP".tag,
                                                                       "{schemaname}"."EMP".tagtype,
                                                                       "{schemaname}"."EMP".tagdate
                                                                     FROM "{schemaname}"."EMP") a_5) "Left_3"
                                                           LEFT JOIN (SELECT
                                                                        a_5.emp_id,
                                                                        min(
                                                                            a_5."Medew. Verantw." :: TEXT) AS "Medew. Verantw."
                                                                      FROM (SELECT
                                                                              a_6.emp_id,
                                                                              a_6.org_name AS "Medew. Verantw."
                                                                            FROM (SELECT
                                                                                    "Left_4".emp_id,
                                                                                    "Left_4".org_id,
                                                                                    "Left_4".type,
                                                                                    "Right_4".org_org_id,
                                                                                    "Right_4".org_name
                                                                                  FROM (SELECT
                                                                                          a_7.emp_id,
                                                                                          a_7.org_id,
                                                                                          a_7.type
                                                                                        FROM (SELECT
                                                                                                a_8.emp_id,
                                                                                                a_8.org_id,
                                                                                                a_8.type
                                                                                              FROM (SELECT
                                                                                                      "{schemaname}"."EMP_ORG".emp_id,
                                                                                                      "{schemaname}"."EMP_ORG".org_id,
                                                                                                      "{schemaname}"."EMP_ORG".fromdate,
                                                                                                      "{schemaname}"."EMP_ORG".todate,
                                                                                                      "{schemaname}"."EMP_ORG".type,
                                                                                                      "{schemaname}"."EMP_ORG".book,
                                                                                                      "{schemaname}"."EMP_ORG".auth,
                                                                                                      "{schemaname}"."EMP_ORG".tag,
                                                                                                      "{schemaname}"."EMP_ORG".tagtype,
                                                                                                      "{schemaname}"."EMP_ORG".tagdate
                                                                                                    FROM
                                                                                                      "{schemaname}"."EMP_ORG") a_8) a_7
                                                                                        WHERE a_7.type = 1) "Left_4"
                                                                                    JOIN (SELECT
                                                                                            a_7.org_org_id,
                                                                                            a_7.org_name
                                                                                          FROM (SELECT
                                                                                                  a_8.org_id AS org_org_id,
                                                                                                  a_8.name   AS org_name
                                                                                                FROM (SELECT
                                                                                                        "{schemaname}"."ORG".org_id,
                                                                                                        "{schemaname}"."ORG".parent_id,
                                                                                                        "{schemaname}"."ORG".fromdate,
                                                                                                        "{schemaname}"."ORG".todate,
                                                                                                        "{schemaname}"."ORG".name,
                                                                                                        "{schemaname}"."ORG".nr,
                                                                                                        "{schemaname}"."ORG".code,
                                                                                                        "{schemaname}"."ORG"."group",
                                                                                                        "{schemaname}"."ORG".account,
                                                                                                        "{schemaname}"."ORG".inherit_id,
                                                                                                        "{schemaname}"."ORG".inherit,
                                                                                                        "{schemaname}"."ORG".address,
                                                                                                        "{schemaname}"."ORG".zipcode,
                                                                                                        "{schemaname}"."ORG".place,
                                                                                                        "{schemaname}"."ORG".district,
                                                                                                        "{schemaname}"."ORG".country,
                                                                                                        "{schemaname}"."ORG".phone1,
                                                                                                        "{schemaname}"."ORG".phone2,
                                                                                                        "{schemaname}"."ORG".mobile1,
                                                                                                        "{schemaname}"."ORG".mobile2,
                                                                                                        "{schemaname}"."ORG".fax1,
                                                                                                        "{schemaname}"."ORG".fax2,
                                                                                                        "{schemaname}"."ORG".email1,
                                                                                                        "{schemaname}"."ORG".email2,
                                                                                                        "{schemaname}"."ORG".info,
                                                                                                        "{schemaname}"."ORG".updatelocal,
                                                                                                        "{schemaname}"."ORG".org_path,
                                                                                                        "{schemaname}"."ORG".pl_color,
                                                                                                        "{schemaname}"."ORG".externkey,
                                                                                                        "{schemaname}"."ORG".tag,
                                                                                                        "{schemaname}"."ORG".tagtype,
                                                                                                        "{schemaname}"."ORG".tagdate
                                                                                                      FROM
                                                                                                        "{schemaname}"."ORG") a_8) a_7
                                                                                          WHERE a_7.org_org_id > 12 AND
                                                                                                a_7.org_org_id <
                                                                                                21) "Right_4"
                                                                                      ON "Left_4".org_id =
                                                                                         "Right_4".org_org_id) a_6) a_5
                                                                      GROUP BY a_5.emp_id) "Right_3"
                                                             ON "Left_3".emp_emp_id = "Right_3".emp_id) a_4) "Right_2"
                                          ON "Left_2".hrs_emp_id = "Right_2".emp_emp_id) a_3) "Left_1"
                            LEFT JOIN (SELECT
                                         a_3.act_id AS act_act_id,
                                         a_3.name   AS act_name
                                       FROM (SELECT
                                               "{schemaname}"."ACT".act_id,
                                               "{schemaname}"."ACT".parent_id,
                                               "{schemaname}"."ACT".fromdate,
                                               "{schemaname}"."ACT".todate,
                                               "{schemaname}"."ACT".name,
                                               "{schemaname}"."ACT".nr,
                                               "{schemaname}"."ACT".code,
                                               "{schemaname}"."ACT"."group",
                                               "{schemaname}"."ACT".account,
                                               "{schemaname}"."ACT".calcid,
                                               "{schemaname}"."ACT".authmode,
                                               "{schemaname}"."ACT".ratelevel,
                                               "{schemaname}"."ACT".ratetype,
                                               "{schemaname}"."ACT".inherit_id,
                                               "{schemaname}"."ACT".inherit,
                                               "{schemaname}"."ACT".prj_id,
                                               "{schemaname}"."ACT".prj_default,
                                               "{schemaname}"."ACT".export,
                                               "{schemaname}"."ACT".type,
                                               "{schemaname}"."ACT".clock,
                                               "{schemaname}"."ACT".balance,
                                               "{schemaname}"."ACT".daytotal,
                                               "{schemaname}"."ACT".nonegbal,
                                               "{schemaname}"."ACT".overtime,
                                               "{schemaname}"."ACT".fillinfo,
                                               "{schemaname}"."ACT".info,
                                               "{schemaname}"."ACT".updatelocal,
                                               "{schemaname}"."ACT".nobooking,
                                               "{schemaname}"."ACT".act_path,
                                               "{schemaname}"."ACT".notmanual,
                                               "{schemaname}"."ACT".pl_color,
                                               "{schemaname}"."ACT".externkey,
                                               "{schemaname}"."ACT".regnumbers,
                                               "{schemaname}"."ACT".expires,
                                               "{schemaname}"."ACT".expiremonths,
                                               "{schemaname}"."ACT".selectbalance,
                                               "{schemaname}"."ACT".balance_min,
                                               "{schemaname}"."ACT".balance_max,
                                               "{schemaname}"."ACT".tag,
                                               "{schemaname}"."ACT".tagtype,
                                               "{schemaname}"."ACT".tagdate,
                                               "{schemaname}"."ACT".noselect
                                             FROM "{schemaname}"."ACT") a_3) "Right_1"
                              ON "Left_1".hrs_act_id = "Right_1".act_act_id) a_2) "Left"
                JOIN (SELECT
                        "Left_1".prj_niv,
                        "Left_1".prj_niv_id,
                        "Left_1".prj_niv_name,
                        "Left_1".prj_niv0_id,
                        "Left_1".prj_niv0_name,
                        "Left_1".prj_niv1_id,
                        "Left_1".prj_niv1_name,
                        "Left_1".prj_niv2_id,
                        "Left_1"."prj_niv2_ name",
                        "Left_1".prj_prj_id,
                        "Left_1"."Project Verantw.",
                        "Right_1"."Project Leader",
                        "Right_1".prj_id
                      FROM (SELECT
                              a_2.prj_niv,
                              a_2.prj_niv_id,
                              a_2.prj_niv_name,
                              a_2.prj_niv0_id,
                              a_2.prj_niv0_name,
                              a_2.prj_niv1_id,
                              a_2.prj_niv1_name,
                              a_2.prj_niv2_id,
                              a_2."prj_niv2_ name",
                              a_2.prj_prj_id,
                              a_2."Project Verantw."
                            FROM (SELECT
                                    "Left_2".prj_niv,
                                    "Left_2".prj_niv_id,
                                    "Left_2".prj_niv_name,
                                    "Left_2".prj_niv0_id,
                                    "Left_2".prj_niv0_name,
                                    "Left_2".prj_niv1_id,
                                    "Left_2".prj_niv1_name,
                                    "Left_2".prj_niv2_id,
                                    "Left_2".prj_prj_id,
                                    "Left_2".prj_parent_id,
                                    "Left_2".fromdate,
                                    "Left_2".todate,
                                    "Left_2".prj_name,
                                    "Left_2".calc_hours,
                                    "Left_2".calc_costs,
                                    "Left_2"."Project Nummer",
                                    "Left_2"."prj_niv2_ name",
                                    "Right_2".prj_id,
                                    "Right_2"."Project Verantw."
                                  FROM (SELECT
                                          a_3.prj_niv,
                                          a_3.prj_niv_id,
                                          a_3.prj_niv_name,
                                          a_3.prj_niv0_id,
                                          a_3.prj_niv0_name,
                                          a_3.prj_niv1_id,
                                          a_3.prj_niv1_name,
                                          a_3.prj_niv2_id,
                                          a_3.prj_prj_id,
                                          a_3.prj_parent_id,
                                          a_3.prj_fromdate   AS fromdate,
                                          a_3.prj_todate     AS todate,
                                          a_3.prj_name,
                                          a_3.prj_calc_hours AS calc_hours,
                                          a_3.prj_calc_costs AS calc_costs,
                                          a_3."Project Nummer",
                                          a_3."prj_niv2_ name"
                                        FROM (SELECT
                                                "Left_3".prj_niv,
                                                "Left_3".prj_niv_id,
                                                "Left_3".prj_niv_name,
                                                "Left_3".prj_niv0_id,
                                                "Left_3".prj_niv0_name,
                                                "Left_3".prj_niv1_id,
                                                "Left_3".prj_niv1_name,
                                                "Left_3".prj_niv2_id,
                                                "Left_3".prj_prj_id,
                                                "Left_3".prj_parent_id,
                                                "Left_3".prj_fromdate,
                                                "Left_3".prj_todate,
                                                "Left_3".prj_name,
                                                "Left_3".prj_calc_hours,
                                                "Left_3".prj_calc_costs,
                                                "Left_3"."Project Nummer",
                                                "Right_3".prj_niv2_id AS "R_prj_niv2_id",
                                                "Right_3"."prj_niv2_ name"
                                              FROM (SELECT
                                                      a_4.prj_niv,
                                                      a_4.prj_niv_id,
                                                      a_4.prj_niv_name,
                                                      a_4.prj_niv0_id,
                                                      a_4.prj_niv0_name,
                                                      a_4.prj_niv1_id,
                                                      a_4.prj_niv1_name,
                                                      a_4.prj_niv2_id,
                                                      a_4.prj_prj_id,
                                                      a_4.prj_parent_id,
                                                      a_4.prj_fromdate,
                                                      a_4.prj_todate,
                                                      a_4.prj_name,
                                                      a_4.prj_calc_hours,
                                                      a_4.prj_calc_costs,
                                                      a_4."Project Nummer"
                                                    FROM (SELECT
                                                            a_5.prj_niv,
                                                            a_5.prj_niv_id,
                                                            a_5.prj_niv_name,
                                                            a_5.prj_niv0_id,
                                                            a_5.prj_niv0_name,
                                                            a_5.prj_niv1_id,
                                                            a_5.prj_niv1_name_new AS prj_niv1_name,
                                                            a_5.prj_niv2_id,
                                                            a_5.prj_niv2_name,
                                                            a_5.prj_prj_id,
                                                            a_5.prj_parent_id,
                                                            a_5.prj_fromdate,
                                                            a_5.prj_todate,
                                                            a_5.prj_name,
                                                            a_5.prj_calc_hours,
                                                            a_5.prj_calc_costs,
                                                            a_5."Project Nummer"
                                                          FROM (SELECT
                                                                  "Left_4".prj_niv,
                                                                  "Left_4".prj_niv_id,
                                                                  "Left_4".prj_niv_name,
                                                                  "Left_4".prj_niv0_id,
                                                                  "Left_4".prj_niv0_name,
                                                                  "Left_4".prj_niv1_id,
                                                                  "Left_4".prj_niv2_id,
                                                                  "Left_4".prj_niv2_name,
                                                                  "Left_4".prj_prj_id,
                                                                  "Left_4".prj_parent_id,
                                                                  "Left_4".prj_fromdate,
                                                                  "Left_4".prj_todate,
                                                                  "Left_4".prj_name,
                                                                  "Left_4"."Project Nummer",
                                                                  "Left_4".prj_calc_hours,
                                                                  "Left_4".prj_calc_costs,
                                                                  "Right_4".prj_prj_id  AS "R_prj_prj_id",
                                                                  "Right_4".prj_niv1_id AS "R_prj_niv1_id",
                                                                  "Right_4".prj_niv1_name_new
                                                                FROM (SELECT
                                                                        a_6.prj_niv,
                                                                        a_6.prj_niv_id,
                                                                        a_6.prj_niv_name,
                                                                        a_6.prj_niv0_id,
                                                                        a_6.prj_niv0_name,
                                                                        a_6.prj_niv1_id,
                                                                        a_6.prj_niv2_id,
                                                                        a_6.prj_niv2_name,
                                                                        a_6.prj_prj_id,
                                                                        a_6.prj_parent_id,
                                                                        a_6.prj_fromdate,
                                                                        a_6.prj_todate,
                                                                        a_6.prj_name,
                                                                        a_6."prj_Project Nummer" AS "Project Nummer",
                                                                        a_6.prj_calc_hours,
                                                                        a_6.prj_calc_costs
                                                                      FROM (SELECT
                                                                              "Left_5".prj_niv,
                                                                              "Left_5".prj_niv_id,
                                                                              "Left_5".prj_niv_name,
                                                                              "Left_5".prj_niv0_id,
                                                                              "Left_5".prj_niv0_name,
                                                                              "Left_5".prj_niv1_id,
                                                                              "Left_5".prj_niv1_name,
                                                                              "Left_5".prj_niv2_id,
                                                                              "Left_5".prj_niv2_name,
                                                                              "Right_5".prj_prj_id,
                                                                              "Right_5".prj_parent_id,
                                                                              "Right_5".prj_fromdate,
                                                                              "Right_5".prj_todate,
                                                                              "Right_5".prj_name,
                                                                              "Right_5"."prj_Project Nummer",
                                                                              "Right_5".prj_calc_hours,
                                                                              "Right_5".prj_calc_costs
                                                                            FROM (SELECT
                                                                                    a_7.niv       AS prj_niv,
                                                                                    a_7.niv_id    AS prj_niv_id,
                                                                                    a_7.niv_name  AS prj_niv_name,
                                                                                    a_7.niv0_id   AS prj_niv0_id,
                                                                                    a_7.niv0_name AS prj_niv0_name,
                                                                                    a_7.niv1_id   AS prj_niv1_id,
                                                                                    a_7.niv1_name AS prj_niv1_name,
                                                                                    a_7.niv2_id   AS prj_niv2_id,
                                                                                    a_7.niv2_name AS prj_niv2_name
                                                                                  FROM (SELECT
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv_id,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv_name,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv0_id,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv0_name,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv1_id,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv1_name,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv2_id,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv2_name,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv3_id,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv3_name,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv4_id,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv4_name,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv5_id,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv5_name,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv6_id,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv6_name,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv7_id,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv7_name,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv8_id,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv8_name,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv9_id,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".niv9_name,
                                                                                          "{schemaname}"."SYS_PRJ_NIV".fullpath
                                                                                        FROM
                                                                                          "{schemaname}"."SYS_PRJ_NIV") a_7) "Left_5"
                                                                              JOIN (SELECT
                                                                                      a_7.prj_id     AS prj_prj_id,
                                                                                      a_7.parent_id  AS prj_parent_id,
                                                                                      a_7.fromdate   AS prj_fromdate,
                                                                                      a_7.todate     AS prj_todate,
                                                                                      a_7.name       AS prj_name,
                                                                                      a_7.nr         AS "prj_Project Nummer",
                                                                                      a_7.calc_hours AS prj_calc_hours,
                                                                                      a_7.calc_costs AS prj_calc_costs
                                                                                    FROM (SELECT
                                                                                            "{schemaname}"."PRJ".prj_id,
                                                                                            "{schemaname}"."PRJ".parent_id,
                                                                                            "{schemaname}"."PRJ".fromdate,
                                                                                            "{schemaname}"."PRJ".todate,
                                                                                            "{schemaname}"."PRJ".name,
                                                                                            "{schemaname}"."PRJ".nr,
                                                                                            "{schemaname}"."PRJ".code,
                                                                                            "{schemaname}"."PRJ"."group",
                                                                                            "{schemaname}"."PRJ".account,
                                                                                            "{schemaname}"."PRJ".authmode,
                                                                                            "{schemaname}"."PRJ".nobooking,
                                                                                            "{schemaname}"."PRJ".ratelevel,
                                                                                            "{schemaname}"."PRJ".inherit_id,
                                                                                            "{schemaname}"."PRJ".inherit,
                                                                                            "{schemaname}"."PRJ".act_id,
                                                                                            "{schemaname}"."PRJ".act_default,
                                                                                            "{schemaname}"."PRJ".cust_id,
                                                                                            "{schemaname}"."PRJ".cust_default,
                                                                                            "{schemaname}"."PRJ".cust_contact_id,
                                                                                            "{schemaname}"."PRJ".info,
                                                                                            "{schemaname}"."PRJ".export,
                                                                                            "{schemaname}"."PRJ".status,
                                                                                            "{schemaname}"."PRJ".approve,
                                                                                            "{schemaname}"."PRJ".limitact,
                                                                                            "{schemaname}"."PRJ".limitcust,
                                                                                            "{schemaname}"."PRJ".limitcosts,
                                                                                            "{schemaname}"."PRJ".updatelocal,
                                                                                            "{schemaname}"."PRJ".prj_path,
                                                                                            "{schemaname}"."PRJ".pl_color,
                                                                                            "{schemaname}"."PRJ".externkey,
                                                                                            "{schemaname}"."PRJ".calc_hours,
                                                                                            "{schemaname}"."PRJ".calc_costs,
                                                                                            "{schemaname}"."PRJ".tag,
                                                                                            "{schemaname}"."PRJ".tagtype,
                                                                                            "{schemaname}"."PRJ".tagdate,
                                                                                            "{schemaname}"."PRJ".allowplan,
                                                                                            "{schemaname}"."PRJ".exp_approve,
                                                                                            "{schemaname}"."PRJ".prjcat
                                                                                          FROM "{schemaname}"."PRJ") a_7) "Right_5"
                                                                                ON "Left_5".prj_niv_id =
                                                                                   "Right_5".prj_prj_id) a_6) "Left_4"
                                                                  LEFT JOIN (SELECT
                                                                               a_6.prj_prj_id,
                                                                               a_6.prj_niv1_id,
                                                                               a_6.prj_niv1_name_new
                                                                             FROM (SELECT
                                                                                     a_7.prj_prj_id,
                                                                                     a_7.prj_niv1_id,
                                                                                     a_7.prj_niv1_name,
                                                                                     a_7.prj_niv2_name,
                                                                                     a_7."Project Nummer",
                                                                                     a_7.prj_niv1_name_new
                                                                                   FROM (SELECT
                                                                                           a_8.prj_prj_id,
                                                                                           a_8.prj_niv1_id,
                                                                                           a_8.prj_niv1_name,
                                                                                           a_8.prj_niv2_name,
                                                                                           a_8."Project Nummer",
                                                                                           ltrim(
                                                                                               a_8.prj_niv1_name_new :: TEXT,
                                                                                               ' - ' :: TEXT) AS prj_niv1_name_new
                                                                                         FROM (SELECT
                                                                                                 a_9.prj_prj_id,
                                                                                                 a_9.prj_niv1_id,
                                                                                                 a_9.prj_niv1_name,
                                                                                                 a_9.prj_niv2_name,
                                                                                                 a_9."Project Nummer",
                                                                                                 CASE
                                                                                                 WHEN
                                                                                                   a_9.prj_niv2_name :: TEXT
                                                                                                   = ' ' :: TEXT AND
                                                                                                   a_9."Project Nummer" :: TEXT
                                                                                                   <> ' ' :: TEXT
                                                                                                   THEN ((
                                                                                                           a_9."Project Nummer" :: TEXT
                                                                                                           ||
                                                                                                           ' - ' :: TEXT)
                                                                                                         ||
                                                                                                         a_9.prj_niv1_name :: TEXT) :: CHARACTER VARYING
                                                                                                 ELSE a_9.prj_niv1_name
                                                                                                 END AS prj_niv1_name_new
                                                                                               FROM (SELECT
                                                                                                       a_10.prj_prj_id,
                                                                                                       a_10.prj_niv1_id,
                                                                                                       a_10.prj_niv1_name,
                                                                                                       a_10.prj_niv2_name,
                                                                                                       a_10."Project Nummer"
                                                                                                     FROM (SELECT
                                                                                                             a_11.prj_niv,
                                                                                                             a_11.prj_niv_id,
                                                                                                             a_11.prj_niv_name,
                                                                                                             a_11.prj_niv0_id,
                                                                                                             a_11.prj_niv0_name,
                                                                                                             a_11.prj_niv1_id,
                                                                                                             a_11.prj_niv1_name,
                                                                                                             a_11.prj_niv2_id,
                                                                                                             a_11.prj_niv2_name,
                                                                                                             a_11.prj_prj_id,
                                                                                                             a_11.prj_parent_id,
                                                                                                             a_11.prj_fromdate,
                                                                                                             a_11.prj_todate,
                                                                                                             a_11.prj_name,
                                                                                                             a_11."Project Nummer",
                                                                                                             a_11.prj_calc_hours,
                                                                                                             a_11.prj_calc_costs
                                                                                                           FROM (SELECT
                                                                                                                   a_12.prj_niv,
                                                                                                                   a_12.prj_niv_id,
                                                                                                                   a_12.prj_niv_name,
                                                                                                                   a_12.prj_niv0_id,
                                                                                                                   a_12.prj_niv0_name,
                                                                                                                   a_12.prj_niv1_id,
                                                                                                                   a_12.prj_niv1_name,
                                                                                                                   a_12.prj_niv2_id,
                                                                                                                   a_12.prj_niv2_name,
                                                                                                                   a_12.prj_prj_id,
                                                                                                                   a_12.prj_parent_id,
                                                                                                                   a_12.prj_fromdate,
                                                                                                                   a_12.prj_todate,
                                                                                                                   a_12.prj_name,
                                                                                                                   a_12."prj_Project Nummer" AS "Project Nummer",
                                                                                                                   a_12.prj_calc_hours,
                                                                                                                   a_12.prj_calc_costs
                                                                                                                 FROM
                                                                                                                   (SELECT
                                                                                                                      "Left_5".prj_niv,
                                                                                                                      "Left_5".prj_niv_id,
                                                                                                                      "Left_5".prj_niv_name,
                                                                                                                      "Left_5".prj_niv0_id,
                                                                                                                      "Left_5".prj_niv0_name,
                                                                                                                      "Left_5".prj_niv1_id,
                                                                                                                      "Left_5".prj_niv1_name,
                                                                                                                      "Left_5".prj_niv2_id,
                                                                                                                      "Left_5".prj_niv2_name,
                                                                                                                      "Right_5".prj_prj_id,
                                                                                                                      "Right_5".prj_parent_id,
                                                                                                                      "Right_5".prj_fromdate,
                                                                                                                      "Right_5".prj_todate,
                                                                                                                      "Right_5".prj_name,
                                                                                                                      "Right_5"."prj_Project Nummer",
                                                                                                                      "Right_5".prj_calc_hours,
                                                                                                                      "Right_5".prj_calc_costs
                                                                                                                    FROM
                                                                                                                      (SELECT
                                                                                                                         a_13.niv       AS prj_niv,
                                                                                                                         a_13.niv_id    AS prj_niv_id,
                                                                                                                         a_13.niv_name  AS prj_niv_name,
                                                                                                                         a_13.niv0_id   AS prj_niv0_id,
                                                                                                                         a_13.niv0_name AS prj_niv0_name,
                                                                                                                         a_13.niv1_id   AS prj_niv1_id,
                                                                                                                         a_13.niv1_name AS prj_niv1_name,
                                                                                                                         a_13.niv2_id   AS prj_niv2_id,
                                                                                                                         a_13.niv2_name AS prj_niv2_name
                                                                                                                       FROM
                                                                                                                         (SELECT
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv_id,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv_name,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv0_id,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv0_name,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv1_id,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv1_name,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv2_id,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv2_name,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv3_id,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv3_name,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv4_id,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv4_name,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv5_id,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv5_name,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv6_id,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv6_name,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv7_id,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv7_name,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv8_id,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv8_name,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv9_id,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".niv9_name,
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV".fullpath
                                                                                                                          FROM
                                                                                                                            "{schemaname}"."SYS_PRJ_NIV") a_13) "Left_5"
                                                                                                                      JOIN
                                                                                                                      (SELECT
                                                                                                                         a_13.prj_id     AS prj_prj_id,
                                                                                                                         a_13.parent_id  AS prj_parent_id,
                                                                                                                         a_13.fromdate   AS prj_fromdate,
                                                                                                                         a_13.todate     AS prj_todate,
                                                                                                                         a_13.name       AS prj_name,
                                                                                                                         a_13.nr         AS "prj_Project Nummer",
                                                                                                                         a_13.calc_hours AS prj_calc_hours,
                                                                                                                         a_13.calc_costs AS prj_calc_costs
                                                                                                                       FROM
                                                                                                                         (SELECT
                                                                                                                            "{schemaname}"."PRJ".prj_id,
                                                                                                                            "{schemaname}"."PRJ".parent_id,
                                                                                                                            "{schemaname}"."PRJ".fromdate,
                                                                                                                            "{schemaname}"."PRJ".todate,
                                                                                                                            "{schemaname}"."PRJ".name,
                                                                                                                            "{schemaname}"."PRJ".nr,
                                                                                                                            "{schemaname}"."PRJ".code,
                                                                                                                            "{schemaname}"."PRJ"."group",
                                                                                                                            "{schemaname}"."PRJ".account,
                                                                                                                            "{schemaname}"."PRJ".authmode,
                                                                                                                            "{schemaname}"."PRJ".nobooking,
                                                                                                                            "{schemaname}"."PRJ".ratelevel,
                                                                                                                            "{schemaname}"."PRJ".inherit_id,
                                                                                                                            "{schemaname}"."PRJ".inherit,
                                                                                                                            "{schemaname}"."PRJ".act_id,
                                                                                                                            "{schemaname}"."PRJ".act_default,
                                                                                                                            "{schemaname}"."PRJ".cust_id,
                                                                                                                            "{schemaname}"."PRJ".cust_default,
                                                                                                                            "{schemaname}"."PRJ".cust_contact_id,
                                                                                                                            "{schemaname}"."PRJ".info,
                                                                                                                            "{schemaname}"."PRJ".export,
                                                                                                                            "{schemaname}"."PRJ".status,
                                                                                                                            "{schemaname}"."PRJ".approve,
                                                                                                                            "{schemaname}"."PRJ".limitact,
                                                                                                                            "{schemaname}"."PRJ".limitcust,
                                                                                                                            "{schemaname}"."PRJ".limitcosts,
                                                                                                                            "{schemaname}"."PRJ".updatelocal,
                                                                                                                            "{schemaname}"."PRJ".prj_path,
                                                                                                                            "{schemaname}"."PRJ".pl_color,
                                                                                                                            "{schemaname}"."PRJ".externkey,
                                                                                                                            "{schemaname}"."PRJ".calc_hours,
                                                                                                                            "{schemaname}"."PRJ".calc_costs,
                                                                                                                            "{schemaname}"."PRJ".tag,
                                                                                                                            "{schemaname}"."PRJ".tagtype,
                                                                                                                            "{schemaname}"."PRJ".tagdate,
                                                                                                                            "{schemaname}"."PRJ".allowplan,
                                                                                                                            "{schemaname}"."PRJ".exp_approve,
                                                                                                                            "{schemaname}"."PRJ".prjcat
                                                                                                                          FROM
                                                                                                                            "{schemaname}"."PRJ") a_13) "Right_5"
                                                                                                                        ON
                                                                                                                          "Left_5".prj_niv_id
                                                                                                                          =
                                                                                                                          "Right_5".prj_prj_id) a_12) a_11
                                                                                                           WHERE
                                                                                                             a_11.prj_prj_id
                                                                                                             <> 402 AND
                                                                                                             a_11.prj_prj_id
                                                                                                             <> 403 AND
                                                                                                             a_11.prj_prj_id
                                                                                                             <> 404 AND
                                                                                                             a_11.prj_prj_id
                                                                                                             <>
                                                                                                             1012) a_10
                                                                                                     GROUP BY
                                                                                                       a_10.prj_prj_id,
                                                                                                       a_10.prj_niv1_id,
                                                                                                       a_10.prj_niv1_name,
                                                                                                       a_10.prj_niv2_name,
                                                                                                       a_10."Project Nummer") a_9) a_8) a_7
                                                                                   WHERE a_7.prj_niv2_name :: TEXT =
                                                                                         ' ' :: TEXT) a_6) "Right_4"
                                                                    ON "Left_4".prj_niv1_id =
                                                                       "Right_4".prj_niv1_id) a_5) a_4) "Left_3"
                                                LEFT JOIN (SELECT
                                                             a_4.prj_niv2_id,
                                                             a_4.prj_niv2_name_new AS "prj_niv2_ name"
                                                           FROM (SELECT
                                                                   a_5.prj_prj_id,
                                                                   a_5.prj_niv1_id,
                                                                   a_5.prj_niv1_name,
                                                                   a_5.prj_niv2_name,
                                                                   a_5."Project Nummer",
                                                                   a_5.prj_niv2_id,
                                                                   a_5.prj_niv2_name_new
                                                                 FROM (SELECT
                                                                         a_6.prj_prj_id,
                                                                         a_6.prj_niv1_id,
                                                                         a_6.prj_niv1_name,
                                                                         a_6.prj_niv2_name,
                                                                         a_6."Project Nummer",
                                                                         a_6.prj_niv2_id,
                                                                         CASE
                                                                         WHEN a_6.prj_niv2_name :: TEXT <> ' ' :: TEXT
                                                                           THEN ((a_6."Project Nummer" :: TEXT ||
                                                                                  ' - ' :: TEXT) ||
                                                                                 a_6.prj_niv2_name :: TEXT) :: CHARACTER VARYING
                                                                         ELSE a_6.prj_niv2_name
                                                                         END AS prj_niv2_name_new
                                                                       FROM (SELECT
                                                                               a_7.prj_prj_id,
                                                                               a_7.prj_niv1_id,
                                                                               a_7.prj_niv1_name,
                                                                               a_7.prj_niv2_name,
                                                                               a_7."Project Nummer",
                                                                               a_7.prj_niv2_id
                                                                             FROM (SELECT
                                                                                     a_8.prj_niv,
                                                                                     a_8.prj_niv_id,
                                                                                     a_8.prj_niv_name,
                                                                                     a_8.prj_niv0_id,
                                                                                     a_8.prj_niv0_name,
                                                                                     a_8.prj_niv1_id,
                                                                                     a_8.prj_niv1_name,
                                                                                     a_8.prj_niv2_id,
                                                                                     a_8.prj_niv2_name,
                                                                                     a_8.prj_prj_id,
                                                                                     a_8.prj_parent_id,
                                                                                     a_8.prj_fromdate,
                                                                                     a_8.prj_todate,
                                                                                     a_8.prj_name,
                                                                                     a_8."Project Nummer",
                                                                                     a_8.prj_calc_hours,
                                                                                     a_8.prj_calc_costs
                                                                                   FROM (SELECT
                                                                                           a_9.prj_niv,
                                                                                           a_9.prj_niv_id,
                                                                                           a_9.prj_niv_name,
                                                                                           a_9.prj_niv0_id,
                                                                                           a_9.prj_niv0_name,
                                                                                           a_9.prj_niv1_id,
                                                                                           a_9.prj_niv1_name,
                                                                                           a_9.prj_niv2_id,
                                                                                           a_9.prj_niv2_name,
                                                                                           a_9.prj_prj_id,
                                                                                           a_9.prj_parent_id,
                                                                                           a_9.prj_fromdate,
                                                                                           a_9.prj_todate,
                                                                                           a_9.prj_name,
                                                                                           a_9."prj_Project Nummer" AS "Project Nummer",
                                                                                           a_9.prj_calc_hours,
                                                                                           a_9.prj_calc_costs
                                                                                         FROM (SELECT
                                                                                                 "Left_4".prj_niv,
                                                                                                 "Left_4".prj_niv_id,
                                                                                                 "Left_4".prj_niv_name,
                                                                                                 "Left_4".prj_niv0_id,
                                                                                                 "Left_4".prj_niv0_name,
                                                                                                 "Left_4".prj_niv1_id,
                                                                                                 "Left_4".prj_niv1_name,
                                                                                                 "Left_4".prj_niv2_id,
                                                                                                 "Left_4".prj_niv2_name,
                                                                                                 "Right_4".prj_prj_id,
                                                                                                 "Right_4".prj_parent_id,
                                                                                                 "Right_4".prj_fromdate,
                                                                                                 "Right_4".prj_todate,
                                                                                                 "Right_4".prj_name,
                                                                                                 "Right_4"."prj_Project Nummer",
                                                                                                 "Right_4".prj_calc_hours,
                                                                                                 "Right_4".prj_calc_costs
                                                                                               FROM (SELECT
                                                                                                       a_10.niv       AS prj_niv,
                                                                                                       a_10.niv_id    AS prj_niv_id,
                                                                                                       a_10.niv_name  AS prj_niv_name,
                                                                                                       a_10.niv0_id   AS prj_niv0_id,
                                                                                                       a_10.niv0_name AS prj_niv0_name,
                                                                                                       a_10.niv1_id   AS prj_niv1_id,
                                                                                                       a_10.niv1_name AS prj_niv1_name,
                                                                                                       a_10.niv2_id   AS prj_niv2_id,
                                                                                                       a_10.niv2_name AS prj_niv2_name
                                                                                                     FROM (SELECT
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv_id,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv_name,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv0_id,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv0_name,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv1_id,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv1_name,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv2_id,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv2_name,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv3_id,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv3_name,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv4_id,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv4_name,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv5_id,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv5_name,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv6_id,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv6_name,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv7_id,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv7_name,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv8_id,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv8_name,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv9_id,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".niv9_name,
                                                                                                             "{schemaname}"."SYS_PRJ_NIV".fullpath
                                                                                                           FROM
                                                                                                             "{schemaname}"."SYS_PRJ_NIV") a_10) "Left_4"
                                                                                                 JOIN (SELECT
                                                                                                         a_10.prj_id     AS prj_prj_id,
                                                                                                         a_10.parent_id  AS prj_parent_id,
                                                                                                         a_10.fromdate   AS prj_fromdate,
                                                                                                         a_10.todate     AS prj_todate,
                                                                                                         a_10.name       AS prj_name,
                                                                                                         a_10.nr         AS "prj_Project Nummer",
                                                                                                         a_10.calc_hours AS prj_calc_hours,
                                                                                                         a_10.calc_costs AS prj_calc_costs
                                                                                                       FROM (SELECT
                                                                                                               "{schemaname}"."PRJ".prj_id,
                                                                                                               "{schemaname}"."PRJ".parent_id,
                                                                                                               "{schemaname}"."PRJ".fromdate,
                                                                                                               "{schemaname}"."PRJ".todate,
                                                                                                               "{schemaname}"."PRJ".name,
                                                                                                               "{schemaname}"."PRJ".nr,
                                                                                                               "{schemaname}"."PRJ".code,
                                                                                                               "{schemaname}"."PRJ"."group",
                                                                                                               "{schemaname}"."PRJ".account,
                                                                                                               "{schemaname}"."PRJ".authmode,
                                                                                                               "{schemaname}"."PRJ".nobooking,
                                                                                                               "{schemaname}"."PRJ".ratelevel,
                                                                                                               "{schemaname}"."PRJ".inherit_id,
                                                                                                               "{schemaname}"."PRJ".inherit,
                                                                                                               "{schemaname}"."PRJ".act_id,
                                                                                                               "{schemaname}"."PRJ".act_default,
                                                                                                               "{schemaname}"."PRJ".cust_id,
                                                                                                               "{schemaname}"."PRJ".cust_default,
                                                                                                               "{schemaname}"."PRJ".cust_contact_id,
                                                                                                               "{schemaname}"."PRJ".info,
                                                                                                               "{schemaname}"."PRJ".export,
                                                                                                               "{schemaname}"."PRJ".status,
                                                                                                               "{schemaname}"."PRJ".approve,
                                                                                                               "{schemaname}"."PRJ".limitact,
                                                                                                               "{schemaname}"."PRJ".limitcust,
                                                                                                               "{schemaname}"."PRJ".limitcosts,
                                                                                                               "{schemaname}"."PRJ".updatelocal,
                                                                                                               "{schemaname}"."PRJ".prj_path,
                                                                                                               "{schemaname}"."PRJ".pl_color,
                                                                                                               "{schemaname}"."PRJ".externkey,
                                                                                                               "{schemaname}"."PRJ".calc_hours,
                                                                                                               "{schemaname}"."PRJ".calc_costs,
                                                                                                               "{schemaname}"."PRJ".tag,
                                                                                                               "{schemaname}"."PRJ".tagtype,
                                                                                                               "{schemaname}"."PRJ".tagdate,
                                                                                                               "{schemaname}"."PRJ".allowplan,
                                                                                                               "{schemaname}"."PRJ".exp_approve,
                                                                                                               "{schemaname}"."PRJ".prjcat
                                                                                                             FROM
                                                                                                               "{schemaname}"."PRJ") a_10) "Right_4"
                                                                                                   ON
                                                                                                     "Left_4".prj_niv_id
                                                                                                     =
                                                                                                     "Right_4".prj_prj_id) a_9) a_8
                                                                                   WHERE a_8.prj_prj_id <> 402 AND
                                                                                         a_8.prj_prj_id <> 403 AND
                                                                                         a_8.prj_prj_id <> 404 AND
                                                                                         a_8.prj_prj_id <> 1012) a_7
                                                                             GROUP BY a_7.prj_prj_id, a_7.prj_niv1_id,
                                                                               a_7.prj_niv1_name, a_7.prj_niv2_name,
                                                                               a_7."Project Nummer",
                                                                               a_7.prj_niv2_id) a_6) a_5
                                                                 WHERE a_5.prj_niv2_name :: TEXT <>
                                                                       ' ' :: TEXT) a_4) "Right_3"
                                                  ON "Left_3".prj_niv2_id = "Right_3".prj_niv2_id) a_3) "Left_2"
                                    LEFT JOIN (SELECT
                                                 a_3.prj_id,
                                                 a_3."Project Verantw."
                                               FROM (SELECT
                                                       a_4.org_name AS "Project Verantw.",
                                                       a_4.prj_id
                                                     FROM (SELECT
                                                             "Left_3".org_org_id,
                                                             "Left_3".org_name,
                                                             "Right_3".prj_id,
                                                             "Right_3".org_id
                                                           FROM (SELECT
                                                                   a_5.org_org_id,
                                                                   a_5.org_name
                                                                 FROM (SELECT
                                                                         a_6.org_id AS org_org_id,
                                                                         a_6.name   AS org_name
                                                                       FROM (SELECT
                                                                               "{schemaname}"."ORG".org_id,
                                                                               "{schemaname}"."ORG".parent_id,
                                                                               "{schemaname}"."ORG".fromdate,
                                                                               "{schemaname}"."ORG".todate,
                                                                               "{schemaname}"."ORG".name,
                                                                               "{schemaname}"."ORG".nr,
                                                                               "{schemaname}"."ORG".code,
                                                                               "{schemaname}"."ORG"."group",
                                                                               "{schemaname}"."ORG".account,
                                                                               "{schemaname}"."ORG".inherit_id,
                                                                               "{schemaname}"."ORG".inherit,
                                                                               "{schemaname}"."ORG".address,
                                                                               "{schemaname}"."ORG".zipcode,
                                                                               "{schemaname}"."ORG".place,
                                                                               "{schemaname}"."ORG".district,
                                                                               "{schemaname}"."ORG".country,
                                                                               "{schemaname}"."ORG".phone1,
                                                                               "{schemaname}"."ORG".phone2,
                                                                               "{schemaname}"."ORG".mobile1,
                                                                               "{schemaname}"."ORG".mobile2,
                                                                               "{schemaname}"."ORG".fax1,
                                                                               "{schemaname}"."ORG".fax2,
                                                                               "{schemaname}"."ORG".email1,
                                                                               "{schemaname}"."ORG".email2,
                                                                               "{schemaname}"."ORG".info,
                                                                               "{schemaname}"."ORG".updatelocal,
                                                                               "{schemaname}"."ORG".org_path,
                                                                               "{schemaname}"."ORG".pl_color,
                                                                               "{schemaname}"."ORG".externkey,
                                                                               "{schemaname}"."ORG".tag,
                                                                               "{schemaname}"."ORG".tagtype,
                                                                               "{schemaname}"."ORG".tagdate
                                                                             FROM "{schemaname}"."ORG") a_6) a_5
                                                                 WHERE
                                                                   a_5.org_org_id > 12 AND a_5.org_org_id < 21) "Left_3"
                                                             JOIN (SELECT
                                                                     a_5.prj_id,
                                                                     a_5.org_id
                                                                   FROM (SELECT
                                                                           a_6.prj_id,
                                                                           a_6.org_id
                                                                         FROM (SELECT
                                                                                 "{schemaname}"."PRJ_LINK".prj_id,
                                                                                 "{schemaname}"."PRJ_LINK".emp_id,
                                                                                 "{schemaname}"."PRJ_LINK".org_id,
                                                                                 "{schemaname}"."PRJ_LINK".auth,
                                                                                 "{schemaname}"."PRJ_LINK".book,
                                                                                 "{schemaname}"."PRJ_LINK".prjleader,
                                                                                 "{schemaname}"."PRJ_LINK".tag,
                                                                                 "{schemaname}"."PRJ_LINK".tagtype,
                                                                                 "{schemaname}"."PRJ_LINK".tagdate
                                                                               FROM "{schemaname}"."PRJ_LINK") a_6
                                                                         GROUP BY a_6.prj_id, a_6.org_id) a_5
                                                                   WHERE a_5.org_id IS NOT NULL) "Right_3"
                                                               ON "Left_3".org_org_id = "Right_3".org_id) a_4) a_3
                                               GROUP BY a_3.prj_id, a_3."Project Verantw.") "Right_2"
                                      ON "Left_2".prj_niv1_id = "Right_2".prj_id) a_2) "Left_1"
                        LEFT JOIN (SELECT
                                     a_2.emp_name AS "Project Leader",
                                     a_2.prj_id
                                   FROM (SELECT
                                           "Left_2".emp_emp_id,
                                           "Left_2".emp_empcat,
                                           "Left_2".emp_name,
                                           "Right_2".prj_id,
                                           "Right_2".emp_id
                                         FROM (SELECT
                                                 a_3.emp_id AS emp_emp_id,
                                                 a_3.empcat AS emp_empcat,
                                                 a_3.name   AS emp_name
                                               FROM (SELECT
                                                       "{schemaname}"."EMP".emp_id,
                                                       "{schemaname}"."EMP".nr,
                                                       "{schemaname}"."EMP".code,
                                                       "{schemaname}"."EMP".lastname,
                                                       "{schemaname}"."EMP".middlename,
                                                       "{schemaname}"."EMP".firstname,
                                                       "{schemaname}"."EMP".plastname,
                                                       "{schemaname}"."EMP".pmiddlename,
                                                       "{schemaname}"."EMP".displayname,
                                                       "{schemaname}"."EMP".sex,
                                                       "{schemaname}"."EMP".initials,
                                                       "{schemaname}"."EMP".nonactive,
                                                       "{schemaname}"."EMP".activeforclk,
                                                       "{schemaname}"."EMP".birthdate,
                                                       "{schemaname}"."EMP".loginname,
                                                       "{schemaname}"."EMP".password,
                                                       "{schemaname}"."EMP".askpassword,
                                                       "{schemaname}"."EMP".auth_id,
                                                       "{schemaname}"."EMP".fromgroup,
                                                       "{schemaname}"."EMP".togroup,
                                                       "{schemaname}"."EMP".taskreg,
                                                       "{schemaname}"."EMP".empcat,
                                                       "{schemaname}"."EMP".export,
                                                       "{schemaname}"."EMP".address,
                                                       "{schemaname}"."EMP".zipcode,
                                                       "{schemaname}"."EMP".place,
                                                       "{schemaname}"."EMP".district,
                                                       "{schemaname}"."EMP".country,
                                                       "{schemaname}"."EMP".phone1,
                                                       "{schemaname}"."EMP".phone2,
                                                       "{schemaname}"."EMP".mobile1,
                                                       "{schemaname}"."EMP".mobile2,
                                                       "{schemaname}"."EMP".fax1,
                                                       "{schemaname}"."EMP".fax2,
                                                       "{schemaname}"."EMP".email1,
                                                       "{schemaname}"."EMP".email2,
                                                       "{schemaname}"."EMP".floor,
                                                       "{schemaname}"."EMP".location,
                                                       "{schemaname}"."EMP".room,
                                                       "{schemaname}"."EMP".extension,
                                                       "{schemaname}"."EMP".bank,
                                                       "{schemaname}"."EMP".leasecar,
                                                       "{schemaname}"."EMP".traveldist,
                                                       "{schemaname}"."EMP".travelcosts,
                                                       "{schemaname}"."EMP".config_id,
                                                       "{schemaname}"."EMP".refreshlocal,
                                                       "{schemaname}"."EMP".name,
                                                       "{schemaname}"."EMP".firstnames,
                                                       "{schemaname}"."EMP".leadtitle,
                                                       "{schemaname}"."EMP".trailtitle,
                                                       "{schemaname}"."EMP".addressno,
                                                       "{schemaname}"."EMP".birthplace,
                                                       "{schemaname}"."EMP".birthcountry,
                                                       "{schemaname}"."EMP".nationality,
                                                       "{schemaname}"."EMP".bsn,
                                                       "{schemaname}"."EMP".idnumber,
                                                       "{schemaname}"."EMP".mstatus,
                                                       "{schemaname}"."EMP".mstatusdate,
                                                       "{schemaname}"."EMP".warninfo,
                                                       "{schemaname}"."EMP".medicalinfo,
                                                       "{schemaname}"."EMP".psex,
                                                       "{schemaname}"."EMP".pinitials,
                                                       "{schemaname}"."EMP".pfirstnames,
                                                       "{schemaname}"."EMP".pfirstname,
                                                       "{schemaname}"."EMP".pleadtitle,
                                                       "{schemaname}"."EMP".ptrailtitle,
                                                       "{schemaname}"."EMP".pbirthdate,
                                                       "{schemaname}"."EMP".pphone,
                                                       "{schemaname}"."EMP".startdate,
                                                       "{schemaname}"."EMP".enddate,
                                                       "{schemaname}"."EMP".jubdate,
                                                       "{schemaname}"."EMP".funcdate,
                                                       "{schemaname}"."EMP".assesdate,
                                                       "{schemaname}"."EMP".svcyears,
                                                       "{schemaname}"."EMP".noldap,
                                                       "{schemaname}"."EMP".pl_color,
                                                       "{schemaname}"."EMP".idexpiredate,
                                                       "{schemaname}"."EMP".externkey,
                                                       "{schemaname}"."EMP".recipient,
                                                       "{schemaname}"."EMP".audit,
                                                       "{schemaname}"."EMP".syncprof_id,
                                                       "{schemaname}"."EMP".aduser,
                                                       "{schemaname}"."EMP".changepassword,
                                                       "{schemaname}"."EMP".dayhours,
                                                       "{schemaname}"."EMP".savedpassword,
                                                       "{schemaname}"."EMP".tag,
                                                       "{schemaname}"."EMP".tagtype,
                                                       "{schemaname}"."EMP".tagdate
                                                     FROM "{schemaname}"."EMP") a_3) "Left_2"
                                           JOIN (SELECT
                                                   a_3.prj_id,
                                                   min(a_3.emp_id) AS emp_id
                                                 FROM (SELECT
                                                         a_4.prj_id,
                                                         a_4.emp_id,
                                                         a_4.org_id,
                                                         a_4.auth,
                                                         a_4.book,
                                                         a_4.prjleader,
                                                         a_4.tag,
                                                         a_4.tagtype,
                                                         a_4.tagdate
                                                       FROM (SELECT
                                                               "{schemaname}"."PRJ_LINK".prj_id,
                                                               "{schemaname}"."PRJ_LINK".emp_id,
                                                               "{schemaname}"."PRJ_LINK".org_id,
                                                               "{schemaname}"."PRJ_LINK".auth,
                                                               "{schemaname}"."PRJ_LINK".book,
                                                               "{schemaname}"."PRJ_LINK".prjleader,
                                                               "{schemaname}"."PRJ_LINK".tag,
                                                               "{schemaname}"."PRJ_LINK".tagtype,
                                                               "{schemaname}"."PRJ_LINK".tagdate
                                                             FROM "{schemaname}"."PRJ_LINK") a_4
                                                       WHERE a_4.prjleader = 1) a_3
                                                 GROUP BY a_3.prj_id) "Right_2"
                                             ON "Left_2".emp_emp_id = "Right_2".emp_id) a_2) "Right_1"
                          ON "Left_1".prj_niv1_id = "Right_1".prj_id) "Right"
                  ON "Left".hrs_prj_id = "Right".prj_prj_id) a_1) a
  UNION ALL
  SELECT
    NULL :: DATE              AS hrs_date,
    NULL :: DOUBLE PRECISION  AS hrs_hours,
    NULL :: DOUBLE PRECISION  AS hrs_internalrate,
    NULL :: DOUBLE PRECISION  AS hrs_rate,
    NULL :: DOUBLE PRECISION  AS hrs_hoursrate,
    NULL :: DOUBLE PRECISION  AS hrs_hoursinternalrate,
    NULL :: CHARACTER VARYING AS cust_name,
    NULL :: CHARACTER VARYING AS org_name,
    NULL :: INTEGER           AS emp_empcat,
    NULL :: CHARACTER VARYING AS emp_name,
    NULL :: TEXT              AS "Medew. Verantw.",
    NULL :: CHARACTER VARYING AS act_name,
    a.prj_niv,
    a.prj_niv_name,
    a.prj_niv0_name,
    a.prj_niv1_name,
    a.prj_niv2_name,
    a."Project Verantw.",
    NULL :: CHARACTER VARYING AS "Project Leader",
    a.prj_niv_id,
    a.prj_niv0_id,
    a.prj_niv1_id,
    a.prj_niv2_id,
    a.prj_prj_id,
    a.prj_parent_id,
    a.fromdate,
    a.todate,
    a.prj_name,
    a.calc_hours,
    a.calc_costs,
    a."Project Nummer"
  FROM (SELECT
          a_1.prj_niv,
          a_1.prj_niv_id,
          a_1.prj_niv_name,
          a_1.prj_niv0_id,
          a_1.prj_niv0_name,
          a_1.prj_niv1_id,
          a_1.prj_niv1_name,
          a_1.prj_niv2_id,
          a_1.prj_prj_id,
          a_1.prj_parent_id,
          a_1.fromdate,
          a_1.todate,
          a_1.prj_name,
          a_1.calc_hours,
          a_1.calc_costs,
          a_1."Project Nummer",
          a_1.prj_niv2_name,
          a_1."Project Verantw."
        FROM (SELECT
                "Left".prj_niv,
                "Left".prj_niv_id,
                "Left".prj_niv_name,
                "Left".prj_niv0_id,
                "Left".prj_niv0_name,
                "Left".prj_niv1_id,
                "Left".prj_niv1_name,
                "Left".prj_niv2_id,
                "Left".prj_prj_id,
                "Left".prj_parent_id,
                "Left".fromdate,
                "Left".todate,
                "Left".prj_name,
                "Left".calc_hours,
                "Left".calc_costs,
                "Left"."Project Nummer",
                "Left".prj_niv2_name,
                "Right".prj_id,
                "Right"."Project Verantw."
              FROM (SELECT
                      a_2.prj_niv,
                      a_2.prj_niv_id,
                      a_2.prj_niv_name,
                      a_2.prj_niv0_id,
                      a_2.prj_niv0_name,
                      a_2.prj_niv1_id,
                      a_2.prj_niv1_name,
                      a_2.prj_niv2_id,
                      a_2.prj_prj_id,
                      a_2.prj_parent_id,
                      a_2.fromdate,
                      a_2.todate,
                      a_2.prj_name,
                      a_2.calc_hours,
                      a_2.calc_costs,
                      a_2."Project Nummer",
                      a_2."prj_niv2_ name" AS prj_niv2_name
                    FROM (SELECT
                            a_3.prj_niv,
                            a_3.prj_niv_id,
                            a_3.prj_niv_name,
                            a_3.prj_niv0_id,
                            a_3.prj_niv0_name,
                            a_3.prj_niv1_id,
                            a_3.prj_niv1_name,
                            a_3.prj_niv2_id,
                            a_3.prj_prj_id,
                            a_3.prj_parent_id,
                            a_3.prj_fromdate   AS fromdate,
                            a_3.prj_todate     AS todate,
                            a_3.prj_name,
                            a_3.prj_calc_hours AS calc_hours,
                            a_3.prj_calc_costs AS calc_costs,
                            a_3."Project Nummer",
                            a_3."prj_niv2_ name"
                          FROM (SELECT
                                  "Left_1".prj_niv,
                                  "Left_1".prj_niv_id,
                                  "Left_1".prj_niv_name,
                                  "Left_1".prj_niv0_id,
                                  "Left_1".prj_niv0_name,
                                  "Left_1".prj_niv1_id,
                                  "Left_1".prj_niv1_name,
                                  "Left_1".prj_niv2_id,
                                  "Left_1".prj_prj_id,
                                  "Left_1".prj_parent_id,
                                  "Left_1".prj_fromdate,
                                  "Left_1".prj_todate,
                                  "Left_1".prj_name,
                                  "Left_1".prj_calc_hours,
                                  "Left_1".prj_calc_costs,
                                  "Left_1"."Project Nummer",
                                  "Right_1".prj_niv2_id AS "R_prj_niv2_id",
                                  "Right_1"."prj_niv2_ name"
                                FROM (SELECT
                                        a_4.prj_niv,
                                        a_4.prj_niv_id,
                                        a_4.prj_niv_name,
                                        a_4.prj_niv0_id,
                                        a_4.prj_niv0_name,
                                        a_4.prj_niv1_id,
                                        a_4.prj_niv1_name,
                                        a_4.prj_niv2_id,
                                        a_4.prj_prj_id,
                                        a_4.prj_parent_id,
                                        a_4.prj_fromdate,
                                        a_4.prj_todate,
                                        a_4.prj_name,
                                        a_4.prj_calc_hours,
                                        a_4.prj_calc_costs,
                                        a_4."Project Nummer"
                                      FROM (SELECT
                                              a_5.prj_niv,
                                              a_5.prj_niv_id,
                                              a_5.prj_niv_name,
                                              a_5.prj_niv0_id,
                                              a_5.prj_niv0_name,
                                              a_5.prj_niv1_id,
                                              a_5.prj_niv1_name_new AS prj_niv1_name,
                                              a_5.prj_niv2_id,
                                              a_5.prj_niv2_name,
                                              a_5.prj_prj_id,
                                              a_5.prj_parent_id,
                                              a_5.prj_fromdate,
                                              a_5.prj_todate,
                                              a_5.prj_name,
                                              a_5.prj_calc_hours,
                                              a_5.prj_calc_costs,
                                              a_5."Project Nummer"
                                            FROM (SELECT
                                                    "Left_2".prj_niv,
                                                    "Left_2".prj_niv_id,
                                                    "Left_2".prj_niv_name,
                                                    "Left_2".prj_niv0_id,
                                                    "Left_2".prj_niv0_name,
                                                    "Left_2".prj_niv1_id,
                                                    "Left_2".prj_niv2_id,
                                                    "Left_2".prj_niv2_name,
                                                    "Left_2".prj_prj_id,
                                                    "Left_2".prj_parent_id,
                                                    "Left_2".prj_fromdate,
                                                    "Left_2".prj_todate,
                                                    "Left_2".prj_name,
                                                    "Left_2"."Project Nummer",
                                                    "Left_2".prj_calc_hours,
                                                    "Left_2".prj_calc_costs,
                                                    "Right_2".prj_prj_id  AS "R_prj_prj_id",
                                                    "Right_2".prj_niv1_id AS "R_prj_niv1_id",
                                                    "Right_2".prj_niv1_name_new
                                                  FROM (SELECT
                                                          a_6.prj_niv,
                                                          a_6.prj_niv_id,
                                                          a_6.prj_niv_name,
                                                          a_6.prj_niv0_id,
                                                          a_6.prj_niv0_name,
                                                          a_6.prj_niv1_id,
                                                          a_6.prj_niv2_id,
                                                          a_6.prj_niv2_name,
                                                          a_6.prj_prj_id,
                                                          a_6.prj_parent_id,
                                                          a_6.prj_fromdate,
                                                          a_6.prj_todate,
                                                          a_6.prj_name,
                                                          a_6."prj_Project Nummer" AS "Project Nummer",
                                                          a_6.prj_calc_hours,
                                                          a_6.prj_calc_costs
                                                        FROM (SELECT
                                                                "Left_3".prj_niv,
                                                                "Left_3".prj_niv_id,
                                                                "Left_3".prj_niv_name,
                                                                "Left_3".prj_niv0_id,
                                                                "Left_3".prj_niv0_name,
                                                                "Left_3".prj_niv1_id,
                                                                "Left_3".prj_niv1_name,
                                                                "Left_3".prj_niv2_id,
                                                                "Left_3".prj_niv2_name,
                                                                "Right_3".prj_prj_id,
                                                                "Right_3".prj_parent_id,
                                                                "Right_3".prj_fromdate,
                                                                "Right_3".prj_todate,
                                                                "Right_3".prj_name,
                                                                "Right_3"."prj_Project Nummer",
                                                                "Right_3".prj_calc_hours,
                                                                "Right_3".prj_calc_costs
                                                              FROM (SELECT
                                                                      a_7.niv       AS prj_niv,
                                                                      a_7.niv_id    AS prj_niv_id,
                                                                      a_7.niv_name  AS prj_niv_name,
                                                                      a_7.niv0_id   AS prj_niv0_id,
                                                                      a_7.niv0_name AS prj_niv0_name,
                                                                      a_7.niv1_id   AS prj_niv1_id,
                                                                      a_7.niv1_name AS prj_niv1_name,
                                                                      a_7.niv2_id   AS prj_niv2_id,
                                                                      a_7.niv2_name AS prj_niv2_name
                                                                    FROM (SELECT
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv_id,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv_name,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv0_id,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv0_name,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv1_id,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv1_name,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv2_id,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv2_name,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv3_id,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv3_name,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv4_id,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv4_name,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv5_id,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv5_name,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv6_id,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv6_name,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv7_id,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv7_name,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv8_id,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv8_name,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv9_id,
                                                                            "{schemaname}"."SYS_PRJ_NIV".niv9_name,
                                                                            "{schemaname}"."SYS_PRJ_NIV".fullpath
                                                                          FROM "{schemaname}"."SYS_PRJ_NIV") a_7) "Left_3"
                                                                JOIN (SELECT
                                                                        a_7.prj_id     AS prj_prj_id,
                                                                        a_7.parent_id  AS prj_parent_id,
                                                                        a_7.fromdate   AS prj_fromdate,
                                                                        a_7.todate     AS prj_todate,
                                                                        a_7.name       AS prj_name,
                                                                        a_7.nr         AS "prj_Project Nummer",
                                                                        a_7.calc_hours AS prj_calc_hours,
                                                                        a_7.calc_costs AS prj_calc_costs
                                                                      FROM (SELECT
                                                                              "{schemaname}"."PRJ".prj_id,
                                                                              "{schemaname}"."PRJ".parent_id,
                                                                              "{schemaname}"."PRJ".fromdate,
                                                                              "{schemaname}"."PRJ".todate,
                                                                              "{schemaname}"."PRJ".name,
                                                                              "{schemaname}"."PRJ".nr,
                                                                              "{schemaname}"."PRJ".code,
                                                                              "{schemaname}"."PRJ"."group",
                                                                              "{schemaname}"."PRJ".account,
                                                                              "{schemaname}"."PRJ".authmode,
                                                                              "{schemaname}"."PRJ".nobooking,
                                                                              "{schemaname}"."PRJ".ratelevel,
                                                                              "{schemaname}"."PRJ".inherit_id,
                                                                              "{schemaname}"."PRJ".inherit,
                                                                              "{schemaname}"."PRJ".act_id,
                                                                              "{schemaname}"."PRJ".act_default,
                                                                              "{schemaname}"."PRJ".cust_id,
                                                                              "{schemaname}"."PRJ".cust_default,
                                                                              "{schemaname}"."PRJ".cust_contact_id,
                                                                              "{schemaname}"."PRJ".info,
                                                                              "{schemaname}"."PRJ".export,
                                                                              "{schemaname}"."PRJ".status,
                                                                              "{schemaname}"."PRJ".approve,
                                                                              "{schemaname}"."PRJ".limitact,
                                                                              "{schemaname}"."PRJ".limitcust,
                                                                              "{schemaname}"."PRJ".limitcosts,
                                                                              "{schemaname}"."PRJ".updatelocal,
                                                                              "{schemaname}"."PRJ".prj_path,
                                                                              "{schemaname}"."PRJ".pl_color,
                                                                              "{schemaname}"."PRJ".externkey,
                                                                              "{schemaname}"."PRJ".calc_hours,
                                                                              "{schemaname}"."PRJ".calc_costs,
                                                                              "{schemaname}"."PRJ".tag,
                                                                              "{schemaname}"."PRJ".tagtype,
                                                                              "{schemaname}"."PRJ".tagdate,
                                                                              "{schemaname}"."PRJ".allowplan,
                                                                              "{schemaname}"."PRJ".exp_approve,
                                                                              "{schemaname}"."PRJ".prjcat
                                                                            FROM "{schemaname}"."PRJ") a_7) "Right_3"
                                                                  ON "Left_3".prj_niv_id =
                                                                     "Right_3".prj_prj_id) a_6) "Left_2"
                                                    LEFT JOIN (SELECT
                                                                 a_6.prj_prj_id,
                                                                 a_6.prj_niv1_id,
                                                                 a_6.prj_niv1_name_new
                                                               FROM (SELECT
                                                                       a_7.prj_prj_id,
                                                                       a_7.prj_niv1_id,
                                                                       a_7.prj_niv1_name,
                                                                       a_7.prj_niv2_name,
                                                                       a_7."Project Nummer",
                                                                       a_7.prj_niv1_name_new
                                                                     FROM (SELECT
                                                                             a_8.prj_prj_id,
                                                                             a_8.prj_niv1_id,
                                                                             a_8.prj_niv1_name,
                                                                             a_8.prj_niv2_name,
                                                                             a_8."Project Nummer",
                                                                             ltrim(a_8.prj_niv1_name_new :: TEXT,
                                                                                   ' - ' :: TEXT) AS prj_niv1_name_new
                                                                           FROM (SELECT
                                                                                   a_9.prj_prj_id,
                                                                                   a_9.prj_niv1_id,
                                                                                   a_9.prj_niv1_name,
                                                                                   a_9.prj_niv2_name,
                                                                                   a_9."Project Nummer",
                                                                                   CASE
                                                                                   WHEN a_9.prj_niv2_name :: TEXT =
                                                                                        ' ' :: TEXT AND
                                                                                        a_9."Project Nummer" :: TEXT <>
                                                                                        ' ' :: TEXT
                                                                                     THEN (
                                                                                       (a_9."Project Nummer" :: TEXT ||
                                                                                        ' - ' :: TEXT) ||
                                                                                       a_9.prj_niv1_name :: TEXT) :: CHARACTER VARYING
                                                                                   ELSE a_9.prj_niv1_name
                                                                                   END AS prj_niv1_name_new
                                                                                 FROM (SELECT
                                                                                         a_10.prj_prj_id,
                                                                                         a_10.prj_niv1_id,
                                                                                         a_10.prj_niv1_name,
                                                                                         a_10.prj_niv2_name,
                                                                                         a_10."Project Nummer"
                                                                                       FROM (SELECT
                                                                                               a_11.prj_niv,
                                                                                               a_11.prj_niv_id,
                                                                                               a_11.prj_niv_name,
                                                                                               a_11.prj_niv0_id,
                                                                                               a_11.prj_niv0_name,
                                                                                               a_11.prj_niv1_id,
                                                                                               a_11.prj_niv1_name,
                                                                                               a_11.prj_niv2_id,
                                                                                               a_11.prj_niv2_name,
                                                                                               a_11.prj_prj_id,
                                                                                               a_11.prj_parent_id,
                                                                                               a_11.prj_fromdate,
                                                                                               a_11.prj_todate,
                                                                                               a_11.prj_name,
                                                                                               a_11."Project Nummer",
                                                                                               a_11.prj_calc_hours,
                                                                                               a_11.prj_calc_costs
                                                                                             FROM (SELECT
                                                                                                     a_12.prj_niv,
                                                                                                     a_12.prj_niv_id,
                                                                                                     a_12.prj_niv_name,
                                                                                                     a_12.prj_niv0_id,
                                                                                                     a_12.prj_niv0_name,
                                                                                                     a_12.prj_niv1_id,
                                                                                                     a_12.prj_niv1_name,
                                                                                                     a_12.prj_niv2_id,
                                                                                                     a_12.prj_niv2_name,
                                                                                                     a_12.prj_prj_id,
                                                                                                     a_12.prj_parent_id,
                                                                                                     a_12.prj_fromdate,
                                                                                                     a_12.prj_todate,
                                                                                                     a_12.prj_name,
                                                                                                     a_12."prj_Project Nummer" AS "Project Nummer",
                                                                                                     a_12.prj_calc_hours,
                                                                                                     a_12.prj_calc_costs
                                                                                                   FROM (SELECT
                                                                                                           "Left_3".prj_niv,
                                                                                                           "Left_3".prj_niv_id,
                                                                                                           "Left_3".prj_niv_name,
                                                                                                           "Left_3".prj_niv0_id,
                                                                                                           "Left_3".prj_niv0_name,
                                                                                                           "Left_3".prj_niv1_id,
                                                                                                           "Left_3".prj_niv1_name,
                                                                                                           "Left_3".prj_niv2_id,
                                                                                                           "Left_3".prj_niv2_name,
                                                                                                           "Right_3".prj_prj_id,
                                                                                                           "Right_3".prj_parent_id,
                                                                                                           "Right_3".prj_fromdate,
                                                                                                           "Right_3".prj_todate,
                                                                                                           "Right_3".prj_name,
                                                                                                           "Right_3"."prj_Project Nummer",
                                                                                                           "Right_3".prj_calc_hours,
                                                                                                           "Right_3".prj_calc_costs
                                                                                                         FROM (SELECT
                                                                                                                 a_13.niv       AS prj_niv,
                                                                                                                 a_13.niv_id    AS prj_niv_id,
                                                                                                                 a_13.niv_name  AS prj_niv_name,
                                                                                                                 a_13.niv0_id   AS prj_niv0_id,
                                                                                                                 a_13.niv0_name AS prj_niv0_name,
                                                                                                                 a_13.niv1_id   AS prj_niv1_id,
                                                                                                                 a_13.niv1_name AS prj_niv1_name,
                                                                                                                 a_13.niv2_id   AS prj_niv2_id,
                                                                                                                 a_13.niv2_name AS prj_niv2_name
                                                                                                               FROM
                                                                                                                 (SELECT
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv_id,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv_name,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv0_id,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv0_name,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv1_id,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv1_name,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv2_id,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv2_name,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv3_id,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv3_name,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv4_id,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv4_name,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv5_id,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv5_name,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv6_id,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv6_name,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv7_id,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv7_name,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv8_id,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv8_name,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv9_id,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".niv9_name,
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV".fullpath
                                                                                                                  FROM
                                                                                                                    "{schemaname}"."SYS_PRJ_NIV") a_13) "Left_3"
                                                                                                           JOIN (SELECT
                                                                                                                   a_13.prj_id     AS prj_prj_id,
                                                                                                                   a_13.parent_id  AS prj_parent_id,
                                                                                                                   a_13.fromdate   AS prj_fromdate,
                                                                                                                   a_13.todate     AS prj_todate,
                                                                                                                   a_13.name       AS prj_name,
                                                                                                                   a_13.nr         AS "prj_Project Nummer",
                                                                                                                   a_13.calc_hours AS prj_calc_hours,
                                                                                                                   a_13.calc_costs AS prj_calc_costs
                                                                                                                 FROM
                                                                                                                   (SELECT
                                                                                                                      "{schemaname}"."PRJ".prj_id,
                                                                                                                      "{schemaname}"."PRJ".parent_id,
                                                                                                                      "{schemaname}"."PRJ".fromdate,
                                                                                                                      "{schemaname}"."PRJ".todate,
                                                                                                                      "{schemaname}"."PRJ".name,
                                                                                                                      "{schemaname}"."PRJ".nr,
                                                                                                                      "{schemaname}"."PRJ".code,
                                                                                                                      "{schemaname}"."PRJ"."group",
                                                                                                                      "{schemaname}"."PRJ".account,
                                                                                                                      "{schemaname}"."PRJ".authmode,
                                                                                                                      "{schemaname}"."PRJ".nobooking,
                                                                                                                      "{schemaname}"."PRJ".ratelevel,
                                                                                                                      "{schemaname}"."PRJ".inherit_id,
                                                                                                                      "{schemaname}"."PRJ".inherit,
                                                                                                                      "{schemaname}"."PRJ".act_id,
                                                                                                                      "{schemaname}"."PRJ".act_default,
                                                                                                                      "{schemaname}"."PRJ".cust_id,
                                                                                                                      "{schemaname}"."PRJ".cust_default,
                                                                                                                      "{schemaname}"."PRJ".cust_contact_id,
                                                                                                                      "{schemaname}"."PRJ".info,
                                                                                                                      "{schemaname}"."PRJ".export,
                                                                                                                      "{schemaname}"."PRJ".status,
                                                                                                                      "{schemaname}"."PRJ".approve,
                                                                                                                      "{schemaname}"."PRJ".limitact,
                                                                                                                      "{schemaname}"."PRJ".limitcust,
                                                                                                                      "{schemaname}"."PRJ".limitcosts,
                                                                                                                      "{schemaname}"."PRJ".updatelocal,
                                                                                                                      "{schemaname}"."PRJ".prj_path,
                                                                                                                      "{schemaname}"."PRJ".pl_color,
                                                                                                                      "{schemaname}"."PRJ".externkey,
                                                                                                                      "{schemaname}"."PRJ".calc_hours,
                                                                                                                      "{schemaname}"."PRJ".calc_costs,
                                                                                                                      "{schemaname}"."PRJ".tag,
                                                                                                                      "{schemaname}"."PRJ".tagtype,
                                                                                                                      "{schemaname}"."PRJ".tagdate,
                                                                                                                      "{schemaname}"."PRJ".allowplan,
                                                                                                                      "{schemaname}"."PRJ".exp_approve,
                                                                                                                      "{schemaname}"."PRJ".prjcat
                                                                                                                    FROM
                                                                                                                      "{schemaname}"."PRJ") a_13) "Right_3"
                                                                                                             ON
                                                                                                               "Left_3".prj_niv_id
                                                                                                               =
                                                                                                               "Right_3".prj_prj_id) a_12) a_11
                                                                                             WHERE
                                                                                               a_11.prj_prj_id <> 402
                                                                                               AND
                                                                                               a_11.prj_prj_id <> 403
                                                                                               AND
                                                                                               a_11.prj_prj_id <> 404
                                                                                               AND
                                                                                               a_11.prj_prj_id <>
                                                                                               1012) a_10
                                                                                       GROUP BY a_10.prj_prj_id,
                                                                                         a_10.prj_niv1_id,
                                                                                         a_10.prj_niv1_name,
                                                                                         a_10.prj_niv2_name,
                                                                                         a_10."Project Nummer") a_9) a_8) a_7
                                                                     WHERE a_7.prj_niv2_name :: TEXT =
                                                                           ' ' :: TEXT) a_6) "Right_2"
                                                      ON "Left_2".prj_niv1_id =
                                                         "Right_2".prj_niv1_id) a_5) a_4) "Left_1"
                                  LEFT JOIN (SELECT
                                               a_4.prj_niv2_id,
                                               a_4.prj_niv2_name_new AS "prj_niv2_ name"
                                             FROM (SELECT
                                                     a_5.prj_prj_id,
                                                     a_5.prj_niv1_id,
                                                     a_5.prj_niv1_name,
                                                     a_5.prj_niv2_name,
                                                     a_5."Project Nummer",
                                                     a_5.prj_niv2_id,
                                                     a_5.prj_niv2_name_new
                                                   FROM (SELECT
                                                           a_6.prj_prj_id,
                                                           a_6.prj_niv1_id,
                                                           a_6.prj_niv1_name,
                                                           a_6.prj_niv2_name,
                                                           a_6."Project Nummer",
                                                           a_6.prj_niv2_id,
                                                           CASE
                                                           WHEN a_6.prj_niv2_name :: TEXT <> ' ' :: TEXT
                                                             THEN ((a_6."Project Nummer" :: TEXT || ' - ' :: TEXT) ||
                                                                   a_6.prj_niv2_name :: TEXT) :: CHARACTER VARYING
                                                           ELSE a_6.prj_niv2_name
                                                           END AS prj_niv2_name_new
                                                         FROM (SELECT
                                                                 a_7.prj_prj_id,
                                                                 a_7.prj_niv1_id,
                                                                 a_7.prj_niv1_name,
                                                                 a_7.prj_niv2_name,
                                                                 a_7."Project Nummer",
                                                                 a_7.prj_niv2_id
                                                               FROM (SELECT
                                                                       a_8.prj_niv,
                                                                       a_8.prj_niv_id,
                                                                       a_8.prj_niv_name,
                                                                       a_8.prj_niv0_id,
                                                                       a_8.prj_niv0_name,
                                                                       a_8.prj_niv1_id,
                                                                       a_8.prj_niv1_name,
                                                                       a_8.prj_niv2_id,
                                                                       a_8.prj_niv2_name,
                                                                       a_8.prj_prj_id,
                                                                       a_8.prj_parent_id,
                                                                       a_8.prj_fromdate,
                                                                       a_8.prj_todate,
                                                                       a_8.prj_name,
                                                                       a_8."Project Nummer",
                                                                       a_8.prj_calc_hours,
                                                                       a_8.prj_calc_costs
                                                                     FROM (SELECT
                                                                             a_9.prj_niv,
                                                                             a_9.prj_niv_id,
                                                                             a_9.prj_niv_name,
                                                                             a_9.prj_niv0_id,
                                                                             a_9.prj_niv0_name,
                                                                             a_9.prj_niv1_id,
                                                                             a_9.prj_niv1_name,
                                                                             a_9.prj_niv2_id,
                                                                             a_9.prj_niv2_name,
                                                                             a_9.prj_prj_id,
                                                                             a_9.prj_parent_id,
                                                                             a_9.prj_fromdate,
                                                                             a_9.prj_todate,
                                                                             a_9.prj_name,
                                                                             a_9."prj_Project Nummer" AS "Project Nummer",
                                                                             a_9.prj_calc_hours,
                                                                             a_9.prj_calc_costs
                                                                           FROM (SELECT
                                                                                   "Left_2".prj_niv,
                                                                                   "Left_2".prj_niv_id,
                                                                                   "Left_2".prj_niv_name,
                                                                                   "Left_2".prj_niv0_id,
                                                                                   "Left_2".prj_niv0_name,
                                                                                   "Left_2".prj_niv1_id,
                                                                                   "Left_2".prj_niv1_name,
                                                                                   "Left_2".prj_niv2_id,
                                                                                   "Left_2".prj_niv2_name,
                                                                                   "Right_2".prj_prj_id,
                                                                                   "Right_2".prj_parent_id,
                                                                                   "Right_2".prj_fromdate,
                                                                                   "Right_2".prj_todate,
                                                                                   "Right_2".prj_name,
                                                                                   "Right_2"."prj_Project Nummer",
                                                                                   "Right_2".prj_calc_hours,
                                                                                   "Right_2".prj_calc_costs
                                                                                 FROM (SELECT
                                                                                         a_10.niv       AS prj_niv,
                                                                                         a_10.niv_id    AS prj_niv_id,
                                                                                         a_10.niv_name  AS prj_niv_name,
                                                                                         a_10.niv0_id   AS prj_niv0_id,
                                                                                         a_10.niv0_name AS prj_niv0_name,
                                                                                         a_10.niv1_id   AS prj_niv1_id,
                                                                                         a_10.niv1_name AS prj_niv1_name,
                                                                                         a_10.niv2_id   AS prj_niv2_id,
                                                                                         a_10.niv2_name AS prj_niv2_name
                                                                                       FROM (SELECT
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv_id,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv_name,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv0_id,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv0_name,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv1_id,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv1_name,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv2_id,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv2_name,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv3_id,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv3_name,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv4_id,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv4_name,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv5_id,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv5_name,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv6_id,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv6_name,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv7_id,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv7_name,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv8_id,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv8_name,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv9_id,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".niv9_name,
                                                                                               "{schemaname}"."SYS_PRJ_NIV".fullpath
                                                                                             FROM
                                                                                               "{schemaname}"."SYS_PRJ_NIV") a_10) "Left_2"
                                                                                   JOIN (SELECT
                                                                                           a_10.prj_id     AS prj_prj_id,
                                                                                           a_10.parent_id  AS prj_parent_id,
                                                                                           a_10.fromdate   AS prj_fromdate,
                                                                                           a_10.todate     AS prj_todate,
                                                                                           a_10.name       AS prj_name,
                                                                                           a_10.nr         AS "prj_Project Nummer",
                                                                                           a_10.calc_hours AS prj_calc_hours,
                                                                                           a_10.calc_costs AS prj_calc_costs
                                                                                         FROM (SELECT
                                                                                                 "{schemaname}"."PRJ".prj_id,
                                                                                                 "{schemaname}"."PRJ".parent_id,
                                                                                                 "{schemaname}"."PRJ".fromdate,
                                                                                                 "{schemaname}"."PRJ".todate,
                                                                                                 "{schemaname}"."PRJ".name,
                                                                                                 "{schemaname}"."PRJ".nr,
                                                                                                 "{schemaname}"."PRJ".code,
                                                                                                 "{schemaname}"."PRJ"."group",
                                                                                                 "{schemaname}"."PRJ".account,
                                                                                                 "{schemaname}"."PRJ".authmode,
                                                                                                 "{schemaname}"."PRJ".nobooking,
                                                                                                 "{schemaname}"."PRJ".ratelevel,
                                                                                                 "{schemaname}"."PRJ".inherit_id,
                                                                                                 "{schemaname}"."PRJ".inherit,
                                                                                                 "{schemaname}"."PRJ".act_id,
                                                                                                 "{schemaname}"."PRJ".act_default,
                                                                                                 "{schemaname}"."PRJ".cust_id,
                                                                                                 "{schemaname}"."PRJ".cust_default,
                                                                                                 "{schemaname}"."PRJ".cust_contact_id,
                                                                                                 "{schemaname}"."PRJ".info,
                                                                                                 "{schemaname}"."PRJ".export,
                                                                                                 "{schemaname}"."PRJ".status,
                                                                                                 "{schemaname}"."PRJ".approve,
                                                                                                 "{schemaname}"."PRJ".limitact,
                                                                                                 "{schemaname}"."PRJ".limitcust,
                                                                                                 "{schemaname}"."PRJ".limitcosts,
                                                                                                 "{schemaname}"."PRJ".updatelocal,
                                                                                                 "{schemaname}"."PRJ".prj_path,
                                                                                                 "{schemaname}"."PRJ".pl_color,
                                                                                                 "{schemaname}"."PRJ".externkey,
                                                                                                 "{schemaname}"."PRJ".calc_hours,
                                                                                                 "{schemaname}"."PRJ".calc_costs,
                                                                                                 "{schemaname}"."PRJ".tag,
                                                                                                 "{schemaname}"."PRJ".tagtype,
                                                                                                 "{schemaname}"."PRJ".tagdate,
                                                                                                 "{schemaname}"."PRJ".allowplan,
                                                                                                 "{schemaname}"."PRJ".exp_approve,
                                                                                                 "{schemaname}"."PRJ".prjcat
                                                                                               FROM
                                                                                                 "{schemaname}"."PRJ") a_10) "Right_2"
                                                                                     ON "Left_2".prj_niv_id =
                                                                                        "Right_2".prj_prj_id) a_9) a_8
                                                                     WHERE
                                                                       a_8.prj_prj_id <> 402 AND a_8.prj_prj_id <> 403
                                                                       AND
                                                                       a_8.prj_prj_id <> 404 AND
                                                                       a_8.prj_prj_id <> 1012) a_7
                                                               GROUP BY a_7.prj_prj_id, a_7.prj_niv1_id,
                                                                 a_7.prj_niv1_name, a_7.prj_niv2_name,
                                                                 a_7."Project Nummer", a_7.prj_niv2_id) a_6) a_5
                                                   WHERE a_5.prj_niv2_name :: TEXT <> ' ' :: TEXT) a_4) "Right_1"
                                    ON "Left_1".prj_niv2_id = "Right_1".prj_niv2_id) a_3) a_2) "Left"
                LEFT JOIN (SELECT
                             a_2.prj_id,
                             a_2."Project Verantw."
                           FROM (SELECT
                                   a_3.org_name AS "Project Verantw.",
                                   a_3.prj_id
                                 FROM (SELECT
                                         "Left_1".org_org_id,
                                         "Left_1".org_name,
                                         "Right_1".prj_id,
                                         "Right_1".org_id
                                       FROM (SELECT
                                               a_4.org_org_id,
                                               a_4.org_name
                                             FROM (SELECT
                                                     a_5.org_id AS org_org_id,
                                                     a_5.name   AS org_name
                                                   FROM (SELECT
                                                           "{schemaname}"."ORG".org_id,
                                                           "{schemaname}"."ORG".parent_id,
                                                           "{schemaname}"."ORG".fromdate,
                                                           "{schemaname}"."ORG".todate,
                                                           "{schemaname}"."ORG".name,
                                                           "{schemaname}"."ORG".nr,
                                                           "{schemaname}"."ORG".code,
                                                           "{schemaname}"."ORG"."group",
                                                           "{schemaname}"."ORG".account,
                                                           "{schemaname}"."ORG".inherit_id,
                                                           "{schemaname}"."ORG".inherit,
                                                           "{schemaname}"."ORG".address,
                                                           "{schemaname}"."ORG".zipcode,
                                                           "{schemaname}"."ORG".place,
                                                           "{schemaname}"."ORG".district,
                                                           "{schemaname}"."ORG".country,
                                                           "{schemaname}"."ORG".phone1,
                                                           "{schemaname}"."ORG".phone2,
                                                           "{schemaname}"."ORG".mobile1,
                                                           "{schemaname}"."ORG".mobile2,
                                                           "{schemaname}"."ORG".fax1,
                                                           "{schemaname}"."ORG".fax2,
                                                           "{schemaname}"."ORG".email1,
                                                           "{schemaname}"."ORG".email2,
                                                           "{schemaname}"."ORG".info,
                                                           "{schemaname}"."ORG".updatelocal,
                                                           "{schemaname}"."ORG".org_path,
                                                           "{schemaname}"."ORG".pl_color,
                                                           "{schemaname}"."ORG".externkey,
                                                           "{schemaname}"."ORG".tag,
                                                           "{schemaname}"."ORG".tagtype,
                                                           "{schemaname}"."ORG".tagdate
                                                         FROM "{schemaname}"."ORG") a_5) a_4
                                             WHERE a_4.org_org_id > 12 AND a_4.org_org_id < 21) "Left_1"
                                         JOIN (SELECT
                                                 a_4.prj_id,
                                                 a_4.org_id
                                               FROM (SELECT
                                                       a_5.prj_id,
                                                       a_5.org_id
                                                     FROM (SELECT
                                                             "{schemaname}"."PRJ_LINK".prj_id,
                                                             "{schemaname}"."PRJ_LINK".emp_id,
                                                             "{schemaname}"."PRJ_LINK".org_id,
                                                             "{schemaname}"."PRJ_LINK".auth,
                                                             "{schemaname}"."PRJ_LINK".book,
                                                             "{schemaname}"."PRJ_LINK".prjleader,
                                                             "{schemaname}"."PRJ_LINK".tag,
                                                             "{schemaname}"."PRJ_LINK".tagtype,
                                                             "{schemaname}"."PRJ_LINK".tagdate
                                                           FROM "{schemaname}"."PRJ_LINK") a_5
                                                     GROUP BY a_5.prj_id, a_5.org_id) a_4
                                               WHERE a_4.org_id IS NOT NULL) "Right_1"
                                           ON "Left_1".org_org_id = "Right_1".org_id) a_3) a_2
                           GROUP BY a_2.prj_id, a_2."Project Verantw.") "Right"
                  ON "Left".prj_niv1_id = "Right".prj_id) a_1) a;


/*
V3 implementation from Hans Kleijn
*/
CREATE MATERIALIZED VIEW "{schemaname}"."v_timetell_projectenoverzicht_3" AS
select
uren.hrs_date,
uren.hrs_hours,
uren.hrs_internalrate,
uren.hrs_rate,
uren.hrs_hoursrate,
uren.hrs_hoursinternalrate,
project.cust_name,
uren.org_name,
uren.emp_empcat,
uren.emp_name,
uren."Medew. Verantw.",
uren.act_name,
project.prj_niv,
project."prj_niv_name" ,
project."prj_niv0_name",
project."prj_niv1_name",
project."prj_niv2_name",
project."Project Verantw.",
project."Project Leader",
project."prj_niv_id",
project."prj_niv0_id" ,
project."prj_niv1_id",
project."prj_niv2_id"       ,
project."prj_prj_id",
project."prj_parent_id" ,
project.fromdate,
project.todate,
project.prj_name,
project.calc_hours,
project.calc_costs,
project."Project Nummer"
FROM (
               select 0 :: INTEGER as prj_niv,
               a0.name as "prj_niv_name" ,
               a0.name as "prj_niv0_name",
               NULL ::TEXT as "prj_niv1_name",
               NULL ::character varying as "prj_niv2_name",
               NULL :: character varying(50) as "Project Verantw.",
               NULL :: CHARACTER VARYING AS "Project Leader",
               NULL :: CHARACTER VARYING AS cust_name,
               a0.prj_id "prj_niv_id",
               a0.prj_id as "prj_niv0_id" ,
               NULL :: INTEGER as "prj_niv1_id"               ,
               NULL :: INTEGER as "prj_niv2_id"               ,
               a0.prj_id as "prj_prj_id"  ,
               NULL :: INTEGER as "prj_parent_id" ,
               a0.fromdate      AS fromdate,
               a0.todate        AS todate,
               a0.name as prj_name,
              a0.calc_hours  AS calc_hours,
               a0.calc_costs AS calc_costs,
               a0.nr AS "Project Nummer"
               from "{schemaname}"."PRJ" a0
               where  a0.parent_id is null
UNION
               select 1 :: INTEGER as prj_niv,
               b1.name  as "prj_niv_name" ,
               a1.name as "prj_niv0_name",
               (b1.nr || ' - ' || b1.name) :: character varying as "prj_niv1_name" ,
               NULL ::TEXT as "prj_niv2_name",
               vo1.basis_team_name ::character varying(50) as  "Project Verantw.",
               e1.name AS "Project Leader" ,
               cust1.name AS cust_name,
               b1.prj_id as "prj_niv_id" ,
               a1.prj_id as "prj_niv0_id" ,
               b1.prj_id as "prj_niv1_id"               ,
               NULL :: INTEGER as "prj_niv2_id"               ,
               b1.prj_id as "prj_prj_id"  ,
               b1.parent_id as "prj_parent_id" ,
               b1.fromdate      AS fromdate,
               b1.todate        AS todate,
               b1.name as prj_name,
               b1.calc_hours  AS calc_hours,
               b1.calc_costs AS calc_costs,
               b1.nr AS "Project Nummer"
               from "{schemaname}"."PRJ" a1  JOIN "{schemaname}"."PRJ" b1 ON a1.prj_id=b1.parent_id and  a1.parent_id is null
               LEFT JOIN (Select prj_id,emp_id from "{schemaname}"."PRJ_LINK" where prjleader=1 group by prj_id, emp_id ) prjpl ON  b1.prj_id=prjpl.prj_id
               JOIN "{schemaname}"."EMP" e1 on e1.emp_id=prjpl.emp_id
               LEFT JOIN (select lp.dim_id as prj_id, max(soi.item) as basis_team_name
                                                                                                from  "{schemaname}"."VW_LABEL_PRJ" lp
                                                                                                LEFT JOIN  "{schemaname}"."SYS_OPT_ITM" as soi on lp.item_id= soi.item_id
                                                                                                where to_char(lp.todate, 'YYYYMMDD')='29991231'
                                                                                                group by lp.dim_id) vo1 on vo1.prj_id=b1.prj_id
               LEFT JOIN "{schemaname}"."CUST" cust1 ON b1.cust_id=cust1.cust_id
UNION
               select 2 :: INTEGER as prj_niv,
               c2.name as "prj_niv_name",
               a2.name as " prj_niv0_name",
               (b2.nr || ' - ' || b2.name)  as "prj_niv1_name" ,
               (c2.nr || ' - ' || c2.name) :: character varying as "prj_niv2_name",
               vo2.basis_team_name ::character varying(50) as "Project Verantw.",
               e2.name AS "Project Leader"  ,
               cust2.name AS cust_name,
               c2.prj_id as "prj_niv_id" ,
               a2.prj_id as "prj_niv0_id" ,
               b2.prj_id as "prj_niv1_id"               ,
               c2.prj_id as "prj_niv2_id"               ,
               c2.prj_id as "prj_prj_id"   ,
               c2.parent_id as "prj_parent_id" ,
               c2.fromdate      AS fromdate,
               c2.todate        AS todate,
               c2.name as prj_name,
               c2.calc_hours  AS calc_hours,
               c2.calc_costs AS calc_costs,
               c2.nr AS "Project Nummer"
               from "{schemaname}"."PRJ" a2  JOIN "{schemaname}"."PRJ" b2 ON a2.prj_id=b2.parent_id and a2.parent_id is null
     JOIN "{schemaname}"."PRJ" c2 ON b2.prj_id=c2.parent_id
               LEFT JOIN (Select prj_id,emp_id from "{schemaname}"."PRJ_LINK" where prjleader=1 group by prj_id, emp_id ) prjpl2 ON  b2.prj_id=prjpl2.prj_id
               JOIN "{schemaname}"."EMP" e2 on e2.emp_id=prjpl2.emp_id
               LEFT JOIN (select lp.dim_id as prj_id, max(soi.item) as basis_team_name
                                                                                                                from  "{schemaname}"."VW_LABEL_PRJ" lp
                                                                                                                LEFT JOIN  "{schemaname}"."SYS_OPT_ITM" as soi on lp.item_id= soi.item_id
                                                                                                    where to_char(lp.todate, 'YYYYMMDD')='29991231'
                                                                                                                group by lp.dim_id) vo2 on vo2.prj_id=b2.prj_id
                            LEFT JOIN "{schemaname}"."CUST" cust2 ON c2.cust_id=cust2.cust_id) project
LEFT JOIN
               (select h.prj_id,
               date as hrs_date,
               hours as hrs_hours,
               internalrate as hrs_internalrate,
               rate as hrs_rate,
               hoursrate as hrs_hoursrate,
               hoursinternalrate as hrs_hoursinternalrate,
               emp.name as emp_name,
               vafd.name as org_name,
               emp.empcat as emp_empcat,
               vafd.name ::TEXT as "Medew. Verantw.",
               act.name as act_name
               FROM "{schemaname}"."HRS" h
               JOIN "{schemaname}"."EMP" emp ON h.emp_id=emp.emp_id
               JOIN (Select eo.emp_id, o.org_id, o.name from "{schemaname}"."EMP_ORG" eo JOIN "{schemaname}"."ORG" o ON eo.org_id=o.org_id
                              and o.parent_id is not null
                              where o.todate=to_date('2999-12-31', 'YYYY-MM-DD') and eo.type=0
                              group by eo.emp_id, o.org_id, o.name ) vafd
                on vafd.emp_id=h.emp_id
               JOIN "{schemaname}"."ACT" act on h.act_id=act.act_id
               ) uren
               ON project.prj_prj_id=uren.prj_id;


/*
V4 Implementation from Hans Kleijn
 */
CREATE MATERIALIZED VIEW "{schemaname}".v_timetell_projectenoverzicht_4
AS
 SELECT uren.hrs_date,
    uren.hrs_hours,
    uren.hrs_internalrate,
    uren.hrs_rate,
    uren.hrs_hoursrate,
    uren.hrs_hoursinternalrate,
    project.cust_name,
    uren.org_name,
    uren.emp_empcat,
    uren.emp_name,
    uren."Medew. Verantw.",
    uren.act_name,
    project.prj_niv,
    project.prj_niv_name,
    project.prj_niv0_name,
    project.prj_niv1_name,
    project.prj_niv2_name,
    project."Project Verantw.",
    project."Project Leader",
    project.prj_niv_id,
    project.prj_niv0_id,
    project.prj_niv1_id,
    project.prj_niv2_id,
    project.prj_prj_id,
    project.prj_parent_id,
    project.fromdate,
    project.todate,
    project.prj_name,
    project.calc_hours,
    project.calc_costs,
    project."Project Nummer"
   FROM ( SELECT 0 AS prj_niv,
            a0.name AS prj_niv_name,
            a0.name AS prj_niv0_name,
            NULL::text AS prj_niv1_name,
            NULL::character varying AS prj_niv2_name,
            NULL::character varying(50) AS "Project Verantw.",
            NULL::character varying AS "Project Leader",
            NULL::character varying AS cust_name,
            a0.prj_id AS prj_niv_id,
            a0.prj_id AS prj_niv0_id,
            NULL::integer AS prj_niv1_id,
            NULL::integer AS prj_niv2_id,
            a0.prj_id AS prj_prj_id,
            NULL::integer AS prj_parent_id,
            a0.fromdate,
            a0.todate,
            a0.name AS prj_name,
            a0.calc_hours,
            a0.calc_costs,
            a0.nr AS "Project Nummer"
           FROM "{schemaname}"."PRJ" a0
          WHERE a0.parent_id IS NULL
        UNION
         SELECT 1 AS prj_niv,
            b1.name AS prj_niv_name,
            a1.name AS prj_niv0_name,
            ((b1.nr::text || ' - '::text) || b1.name::text)::character varying AS prj_niv1_name,
            NULL::text AS prj_niv2_name,
            vo1.basis_team_name::character varying(50) AS "Project Verantw.",
            e1.name AS "Project Leader",
            cust1.name AS cust_name,
            b1.prj_id AS prj_niv_id,
            a1.prj_id AS prj_niv0_id,
            b1.prj_id AS prj_niv1_id,
            NULL::integer AS prj_niv2_id,
            b1.prj_id AS prj_prj_id,
            b1.parent_id AS prj_parent_id,
            b1.fromdate,
            b1.todate,
            b1.name AS prj_name,
            b1.calc_hours,
            b1.calc_costs,
            b1.nr AS "Project Nummer"
           FROM "{schemaname}"."PRJ" a1
             JOIN "{schemaname}"."PRJ" b1 ON a1.prj_id = b1.parent_id AND a1.parent_id IS NULL
             LEFT JOIN ( SELECT "PRJ_LINK".prj_id,
                    "PRJ_LINK".emp_id
                   FROM "{schemaname}"."PRJ_LINK"
                  WHERE "PRJ_LINK".prjleader = 1
                  GROUP BY "PRJ_LINK".prj_id, "PRJ_LINK".emp_id) prjpl ON b1.prj_id = prjpl.prj_id
             JOIN "{schemaname}"."EMP" e1 ON e1.emp_id = prjpl.emp_id
             LEFT JOIN ( SELECT lp.dim_id AS prj_id,
                    max(soi.item::text) AS basis_team_name
                   FROM "{schemaname}"."VW_LABEL_PRJ" lp
                     LEFT JOIN "{schemaname}"."SYS_OPT_ITM" soi ON lp.item_id = soi.item_id
                  WHERE to_char(lp.todate, 'YYYYMMDD') = '29991231'
                  GROUP BY lp.dim_id) vo1 ON vo1.prj_id = b1.prj_id
             LEFT JOIN "{schemaname}"."CUST" cust1 ON b1.cust_id = cust1.cust_id
        UNION
         SELECT 2 AS prj_niv,
            c2.name AS prj_niv_name,
            a2.name AS " prj_niv0_name",
            (b2.nr::text || ' - '::text) || b2.name::text AS prj_niv1_name,
            ((c2.nr::text || ' - '::text) || c2.name::text)::character varying AS prj_niv2_name,
            vo2.basis_team_name::character varying(50) AS "Project Verantw.",
            e2.name AS "Project Leader",
            cust2.name AS cust_name,
            c2.prj_id AS prj_niv_id,
            a2.prj_id AS prj_niv0_id,
            b2.prj_id AS prj_niv1_id,
            c2.prj_id AS prj_niv2_id,
            c2.prj_id AS prj_prj_id,
            c2.parent_id AS prj_parent_id,
            c2.fromdate,
            c2.todate,
            c2.name AS prj_name,
            c2.calc_hours,
            c2.calc_costs,
            c2.nr AS "Project Nummer"
           FROM "{schemaname}"."PRJ" a2
             JOIN "{schemaname}"."PRJ" b2 ON a2.prj_id = b2.parent_id AND a2.parent_id IS NULL
             JOIN "{schemaname}"."PRJ" c2 ON b2.prj_id = c2.parent_id
             LEFT JOIN ( SELECT "PRJ_LINK".prj_id,
                    "PRJ_LINK".emp_id
                   FROM "{schemaname}"."PRJ_LINK"
                  WHERE "{schemaname}"."PRJ_LINK".prjleader = 1
                  GROUP BY "PRJ_LINK".prj_id, "PRJ_LINK".emp_id) prjpl2 ON b2.prj_id = prjpl2.prj_id
             JOIN "{schemaname}"."EMP" e2 ON e2.emp_id = prjpl2.emp_id
             LEFT JOIN ( SELECT lp.dim_id AS prj_id,
                    max(soi.item::text) AS basis_team_name
                   FROM "{schemaname}"."VW_LABEL_PRJ" lp
                     LEFT JOIN "{schemaname}"."SYS_OPT_ITM" soi ON lp.item_id = soi.item_id
                  WHERE to_char(lp.todate, 'YYYYMMDD') = '29991231'
                  GROUP BY lp.dim_id) vo2 ON vo2.prj_id = b2.prj_id
             LEFT JOIN "{schemaname}"."CUST" cust2 ON c2.cust_id = cust2.cust_id) project
     LEFT JOIN ( SELECT h.prj_id,
            h.date AS hrs_date,
            h.hours AS hrs_hours,
            h.internalrate AS hrs_internalrate,
            h.rate AS hrs_rate,
            h.hoursrate AS hrs_hoursrate,
            h.hoursinternalrate AS hrs_hoursinternalrate,
            emp.name AS emp_name,
            vafd.name AS org_name,
            emp.empcat AS emp_empcat,
            vafd.name::text AS "Medew. Verantw.",
            act.name AS act_name
           FROM "{schemaname}"."HRS" h
             JOIN "{schemaname}"."EMP" emp ON h.emp_id = emp.emp_id
             JOIN (
				 select distinct teo.emp_id, top.org_id, top.name from (
				select org_id, name from "{schemaname}"."ORG"
				 where todate=to_date('2999-12-31','YYYY-MM-DD') and parent_id is null) top
				JOIN (select emp_id, org_id from "{schemaname}"."EMP_ORG") teo ON top.org_id=teo.org_id
				where teo.emp_id not in
					(Select xeo.emp_id from "{schemaname}"."EMP_ORG" xeo JOIN "{schemaname}"."ORG" xo ON xeo.org_id=xo.org_id
                              and xo.parent_id is not null
                              where xo.todate=to_date('2999-12-31', 'YYYY-MM-DD') and xeo.type=0)
				UNION
					Select eo.emp_id, o.org_id, o.name from "{schemaname}"."EMP_ORG" eo
				 		JOIN "{schemaname}"."ORG" o ON eo.org_id=o.org_id
                        and o.parent_id is not null
                              where o.todate=to_date('2999-12-31','YYYY-MM-DD') AND eo.type = 0
                              group by eo.emp_id, o.org_id, o.name

			 ) vafd ON vafd.emp_id = h.emp_id
             JOIN "{schemaname}"."ACT" act ON h.act_id = act.act_id) uren ON project.prj_prj_id = uren.prj_id;
