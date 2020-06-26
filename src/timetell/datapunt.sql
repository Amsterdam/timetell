
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


CREATE MATERIALIZED VIEW "{schemaname}".v_timetell_projectenoverzicht_5
AS
SELECT uren.hrs_date,
    uren.hrs_hours,
    uren.hrs_internalrate,
    uren.hrs_rate,
    uren.hrs_hoursrate,
    uren.hrs_hoursinternalrate,
    uren.hrs_hours_status,
    project.cust_name,
    uren.org_name,
    uren.org_name_toen,
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
            (((b1.nr::text || ' - '::text) || b1.name::text))::character varying(50) AS prj_niv1_name,
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
             LEFT JOIN "{schemaname}"."PRJ" b1 ON a1.prj_id = b1.parent_id AND a1.parent_id IS NULL
             LEFT JOIN ( SELECT "PRJ_LINK".prj_id,
                    "PRJ_LINK".emp_id
                   FROM "{schemaname}"."PRJ_LINK"
                  WHERE "PRJ_LINK".prjleader = 1
                  GROUP BY "PRJ_LINK".prj_id, "PRJ_LINK".emp_id) prjpl ON b1.prj_id = prjpl.prj_id
             LEFT JOIN "{schemaname}"."EMP" e1 ON e1.emp_id = prjpl.emp_id
             LEFT JOIN ( SELECT lp.dim_id AS prj_id,
                    max(soi.item::text) AS basis_team_name
                   FROM "{schemaname}"."VW_LABEL_PRJ" lp
                     LEFT JOIN "{schemaname}"."SYS_OPT_ITM" soi ON lp.item_id = soi.item_id
                  WHERE to_char(lp.todate::timestamp with time zone, 'YYYYMMDD'::text) = '29991231'::text
                  GROUP BY lp.dim_id) vo1 ON vo1.prj_id = b1.prj_id
             LEFT JOIN "{schemaname}"."CUST" cust1 ON b1.cust_id = cust1.cust_id
        UNION
         SELECT 2 AS prj_niv,
            c2.name AS prj_niv_name,
            a2.name AS " prj_niv0_name",
            (b2.nr::text || ' - '::text) || b2.name::text AS prj_niv1_name,
            (c2.nr::text || ' - '::text) || c2.name::text AS prj_niv2_name,
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
                  WHERE "PRJ_LINK".prjleader = 1
                  GROUP BY "PRJ_LINK".prj_id, "PRJ_LINK".emp_id) prjpl2 ON b2.prj_id = prjpl2.prj_id
             JOIN "{schemaname}"."EMP" e2 ON e2.emp_id = prjpl2.emp_id
             LEFT JOIN ( SELECT lp.dim_id AS prj_id,
                    max(soi.item::text) AS basis_team_name
                   FROM "{schemaname}"."VW_LABEL_PRJ" lp
                     LEFT JOIN "{schemaname}"."SYS_OPT_ITM" soi ON lp.item_id = soi.item_id
                  WHERE to_char(lp.todate::timestamp with time zone, 'YYYYMMDD'::text) = '29991231'::text
                  GROUP BY lp.dim_id) vo2 ON vo2.prj_id = b2.prj_id
             LEFT JOIN "{schemaname}"."CUST" cust2 ON c2.cust_id = cust2.cust_id) project
     LEFT JOIN ( SELECT h.prj_id,
            h.date AS hrs_date,
            h.hours AS hrs_hours,
            h.internalrate AS hrs_internalrate,
            h.rate AS hrs_rate,
            h.hoursrate AS hrs_hoursrate,
            h.hoursinternalrate AS hrs_hoursinternalrate,
            h.status AS hrs_hours_status,
            org2.name AS org_name_toen,
            emp.name AS emp_name,
            vafd.name AS org_name,
            emp.empcat AS emp_empcat,
            vafd.name::text AS "Medew. Verantw.",
            act.name AS act_name
           FROM "{schemaname}"."HRS" h
             JOIN "{schemaname}"."EMP" emp ON h.emp_id = emp.emp_id
             JOIN ( SELECT "ORG".org_id,
                    "ORG".name
                   FROM "{schemaname}"."ORG") org2 ON h.org_id = org2.org_id
             JOIN ( SELECT DISTINCT teo.emp_id,
                    top.org_id,
                    top.name
                   FROM ( SELECT "ORG".org_id,
                            "ORG".name
                           FROM "{schemaname}"."ORG"
                          WHERE "ORG".todate = to_date('2999-12-31'::text, 'YYYY-MM-DD'::text) AND "ORG".parent_id IS NULL) top
                     JOIN ( SELECT "EMP_ORG".emp_id,
                            "EMP_ORG".org_id
                           FROM "{schemaname}"."EMP_ORG") teo ON top.org_id = teo.org_id
                  WHERE NOT (teo.emp_id IN ( SELECT xeo.emp_id
                           FROM "{schemaname}"."EMP_ORG" xeo
                             JOIN "{schemaname}"."ORG" xo ON xeo.org_id = xo.org_id AND xo.parent_id IS NOT NULL
                          WHERE xo.todate = to_date('2999-12-31'::text, 'YYYY-MM-DD'::text) AND xeo.type = 0))
                UNION
                 SELECT eo.emp_id,
                    o.org_id,
                    o.name
                   FROM "{schemaname}"."EMP_ORG" eo
                     JOIN "{schemaname}"."ORG" o ON eo.org_id = o.org_id AND o.parent_id IS NOT NULL
                  WHERE o.todate = to_date('2999-12-31'::text, 'YYYY-MM-DD'::text) AND (eo.todate = to_date('2999-12-31'::text, 'YYYY-MM-DD'::text) OR eo.todate = to_date('2099-12-31'::text, 'YYYY-MM-DD'::text)) AND eo.type = 0
                  GROUP BY eo.emp_id, o.org_id, o.name
                UNION
                 SELECT eo.emp_id,
                    o.org_id,
                    o.name
                   FROM ( SELECT laatste.emp_id,
                            laatste.maxtodate
                           FROM ( SELECT eo_1.emp_id,
                                    max(eo_1.todate) AS maxtodate
                                   FROM "{schemaname}"."EMP_ORG" eo_1
                                  GROUP BY eo_1.emp_id) laatste
                          WHERE NOT (laatste.maxtodate = to_date('2999-12-31'::text, 'YYYY-MM-DD'::text) OR laatste.maxtodate = to_date('2099-12-31'::text, 'YYYY-MM-DD'::text))) sel
                     JOIN "{schemaname}"."EMP_ORG" eo ON sel.emp_id = eo.emp_id AND eo.todate = sel.maxtodate
                     JOIN "{schemaname}"."ORG" o ON eo.org_id = o.org_id AND o.parent_id IS NOT NULL
                  WHERE eo.type = 0) vafd ON vafd.emp_id = h.emp_id
             JOIN "{schemaname}"."ACT" act ON h.act_id = act.act_id) uren ON project.prj_prj_id = uren.prj_id;

CREATE MATERIALIZED VIEW "{schemaname}".v_timetell_projectenoverzicht_6
AS
 SELECT uren.hrs_date,
    uren.hrs_hours,
    uren.hrs_internalrate,
    uren.hrs_rate,
    uren.hrs_hoursrate,
    uren.hrs_hoursinternalrate,
    uren.hrs_hours_status,
    project.cust_name,
    uren.org_id,
	uren.org_name,
    uren.org_id_toen,
	uren.org_name_toen,
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
            (((b1.nr::text || ' - '::text) || b1.name::text))::character varying(50) AS prj_niv1_name,
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
             LEFT JOIN "{schemaname}"."PRJ" b1 ON a1.prj_id = b1.parent_id AND a1.parent_id IS NULL
             LEFT JOIN ( SELECT "PRJ_LINK".prj_id,
                    "PRJ_LINK".emp_id
                   FROM "{schemaname}"."PRJ_LINK"
                  WHERE "PRJ_LINK".prjleader = 1
                  GROUP BY "PRJ_LINK".prj_id, "PRJ_LINK".emp_id) prjpl ON b1.prj_id = prjpl.prj_id
             LEFT JOIN "{schemaname}"."EMP" e1 ON e1.emp_id = prjpl.emp_id
             LEFT JOIN ( SELECT lp.dim_id AS prj_id,
                    max(soi.item::text) AS basis_team_name
                   FROM "{schemaname}"."VW_LABEL_PRJ" lp
                     LEFT JOIN "{schemaname}"."SYS_OPT_ITM" soi ON lp.item_id = soi.item_id
                  WHERE to_char(lp.todate::timestamp with time zone, 'YYYYMMDD'::text) = '29991231'::text
                  GROUP BY lp.dim_id) vo1 ON vo1.prj_id = b1.prj_id
             LEFT JOIN "{schemaname}"."CUST" cust1 ON b1.cust_id = cust1.cust_id
        UNION
         SELECT 2 AS prj_niv,
            c2.name AS prj_niv_name,
            a2.name AS " prj_niv0_name",
            (b2.nr::text || ' - '::text) || b2.name::text AS prj_niv1_name,
            (c2.nr::text || ' - '::text) || c2.name::text AS prj_niv2_name,
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
                  WHERE "PRJ_LINK".prjleader = 1
                  GROUP BY "PRJ_LINK".prj_id, "PRJ_LINK".emp_id) prjpl2 ON b2.prj_id = prjpl2.prj_id
             JOIN "{schemaname}"."EMP" e2 ON e2.emp_id = prjpl2.emp_id
             LEFT JOIN ( SELECT lp.dim_id AS prj_id,
                    max(soi.item::text) AS basis_team_name
                   FROM "{schemaname}"."VW_LABEL_PRJ" lp
                     LEFT JOIN "{schemaname}"."SYS_OPT_ITM" soi ON lp.item_id = soi.item_id
                  WHERE to_char(lp.todate::timestamp with time zone, 'YYYYMMDD'::text) = '29991231'::text
                  GROUP BY lp.dim_id) vo2 ON vo2.prj_id = b2.prj_id
             LEFT JOIN "{schemaname}"."CUST" cust2 ON c2.cust_id = cust2.cust_id) project
     LEFT JOIN ( SELECT h.prj_id,
            h.date AS hrs_date,
            h.hours AS hrs_hours,
            h.internalrate AS hrs_internalrate,
            h.rate AS hrs_rate,
            h.hoursrate AS hrs_hoursrate,
            h.hoursinternalrate AS hrs_hoursinternalrate,
            h.status AS hrs_hours_status,
			org2.org_id AS org_id_toen,
            org2.name AS org_name_toen,
            emp.name AS emp_name,
			vafd.org_id as org_id,
            vafd.name AS org_name,
            emp.empcat AS emp_empcat,
            vafd.name::text AS "Medew. Verantw.",
            act.name AS act_name
           FROM "{schemaname}"."HRS" h
             JOIN "{schemaname}"."EMP" emp ON h.emp_id = emp.emp_id
             JOIN ( SELECT "ORG".org_id,
                    "ORG".name
                   FROM "{schemaname}"."ORG") org2 ON h.org_id = org2.org_id
                   JOIN ( SELECT DISTINCT teo.emp_id, top.org_id, top.name
                   		FROM ( SELECT "ORG".org_id,"ORG".name
                           		FROM "{schemaname}"."ORG"
                         	 	WHERE "ORG".todate = to_date('2999-12-31'::text, 'YYYY-MM-DD'::text) AND "ORG".parent_id IS NULL) top
                     	JOIN ( SELECT "EMP_ORG".emp_id,"EMP_ORG".org_id
                           	FROM "{schemaname}"."EMP_ORG") teo ON top.org_id = teo.org_id
                 			 WHERE NOT (teo.emp_id IN ( SELECT xeo.emp_id
                           								FROM "{schemaname}"."EMP_ORG" xeo
                             							JOIN "{schemaname}"."ORG" xo ON xeo.org_id = xo.org_id AND xo.parent_id IS NOT NULL
                          								WHERE xo.todate = to_date('2999-12-31'::text, 'YYYY-MM-DD'::text) AND xeo.type = 0))
                UNION
                 SELECT eo.emp_id, o.org_id, o.name
                   FROM "{schemaname}"."EMP_ORG" eo
                     JOIN "{schemaname}"."ORG" o ON eo.org_id = o.org_id AND o.parent_id IS NOT NULL
                  	WHERE o.todate = to_date('2999-12-31'::text, 'YYYY-MM-DD'::text)
				   	AND (eo.todate = to_date('2999-12-31'::text, 'YYYY-MM-DD'::text)
						OR eo.todate = to_date('2099-12-31'::text, 'YYYY-MM-DD'::text)) AND eo.type = 0
                  GROUP BY eo.emp_id, o.org_id, o.name
                UNION
                 SELECT eo.emp_id,
                    o.org_id,
                    o.name
                   FROM ( SELECT laatste.emp_id,
                            laatste.maxtodate
                           FROM ( SELECT eo_1.emp_id,
                                    max(eo_1.todate) AS maxtodate
                                   FROM "{schemaname}"."EMP_ORG" eo_1
                                  GROUP BY eo_1.emp_id) laatste
                          WHERE NOT (laatste.maxtodate = to_date('2999-12-31'::text, 'YYYY-MM-DD'::text) OR laatste.maxtodate = to_date('2099-12-31'::text, 'YYYY-MM-DD'::text))) sel
                     JOIN "{schemaname}"."EMP_ORG" eo ON sel.emp_id = eo.emp_id AND eo.todate = sel.maxtodate
                     JOIN "{schemaname}"."ORG" o ON eo.org_id = o.org_id AND o.parent_id IS NOT NULL
                  WHERE eo.type = 0) vafd ON vafd.emp_id = h.emp_id
             		JOIN "{schemaname}"."ACT" act ON h.act_id = act.act_id) uren ON project.prj_prj_id = uren.prj_id
WITH DATA;

CREATE OR REPLACE VIEW "{schemaname}".v_timetell_project_team_maand_ab_6 AS
SELECT
    c.prj_id,
    c.project_nummer,
    c.prj_name,
    c.prj_niv1_name,
    c.org_id,
    c.org_name,
    c.project_verantw,
    c.project_leader,
    c.cust_name,
    c.act_name,
    c.jaar,
    c.maand,
    c.eerstedagvandemaand,
    sum(c.hours) AS hours,
    sum(c.costs) AS costs,
    max(c.budget) AS budget
FROM (
    WITH tijdreeks AS (
        SELECT
            date_part('year'::text, a.datum) AS jaar,
            date_part('month'::text, a.datum) AS maand,
            a.datum AS eerste_dag
        FROM (
            SELECT generate_series('2019-01-01 00:00:00'::timestamp without time zone, '2027-12-31 23:59:00'::timestamp without time zone, '1 mon'::interval) AS datum
        ) a
    ),

    budget AS (
        SELECT
            "VW_PLAN".prj_id,
            "VW_PLAN".org_id,
            date_part('year'::text, "VW_PLAN".fromdate) AS jaar,
            sum("VW_PLAN".costs) AS budget
        FROM
            "{schemaname}"."VW_PLAN"
        WHERE
            "VW_PLAN".fromdate >= '2019-01-01'::date AND
            "VW_PLAN".prj_id IS NOT NULL AND
            "VW_PLAN".org_id <> 0
        GROUP BY
            "VW_PLAN".prj_id,
            "VW_PLAN".org_id,
            (date_part('year'::text, "VW_PLAN".fromdate))
    ),

    project_jaar AS (
        SELECT
            d.prj_id,
            d.jaar,
            max(d.project_nummer::text) AS project_nummer,
            max(d.prj_name::text) AS prj_name,
            max(d.prj_niv1_name) AS prj_niv1_name,
            max(d.project_leader::text) AS project_leader,
            max(d.project_verantw::text) AS project_verantw,
            max(d.cust_name::text) AS cust_name,
            max(d.act_name::text) AS act_name
        FROM (
            SELECT
                v_timetell_projectenoverzicht_6.prj_prj_id AS prj_id,
                v_timetell_projectenoverzicht_6."Project Nummer" AS project_nummer,
                v_timetell_projectenoverzicht_6.prj_name,
                v_timetell_projectenoverzicht_6.prj_niv1_name,
                v_timetell_projectenoverzicht_6."Project Verantw." AS project_verantw,
                v_timetell_projectenoverzicht_6."Project Leader" AS project_leader,
                v_timetell_projectenoverzicht_6.cust_name,
                v_timetell_projectenoverzicht_6.act_name,
                date_part('year'::text, v_timetell_projectenoverzicht_6.hrs_date) AS jaar
            FROM
                "{schemaname}".v_timetell_projectenoverzicht_6
            WHERE
                v_timetell_projectenoverzicht_6."Project Nummer" IS NOT NULL AND
                v_timetell_projectenoverzicht_6.hrs_date IS NOT NULL
            GROUP BY
                v_timetell_projectenoverzicht_6.prj_prj_id,
                v_timetell_projectenoverzicht_6."Project Nummer",
                v_timetell_projectenoverzicht_6.prj_name,
                v_timetell_projectenoverzicht_6.prj_niv1_name,
                v_timetell_projectenoverzicht_6."Project Verantw.",
                v_timetell_projectenoverzicht_6."Project Leader",
                v_timetell_projectenoverzicht_6.cust_name,
                v_timetell_projectenoverzicht_6.act_name,
                (date_part('year'::text, v_timetell_projectenoverzicht_6.hrs_date))
        ) d
        GROUP BY
            d.prj_id,
            d.jaar
    ),

    budget_maand AS (
        WITH budget AS (
            SELECT
                "VW_PLAN".prj_id,
                "VW_PLAN".org_id,
                date_part('year'::text, "VW_PLAN".fromdate) AS jaar,
                sum("VW_PLAN".costs) AS budget
            FROM
                "{schemaname}"."VW_PLAN"
            WHERE
                "VW_PLAN".fromdate >= '2019-01-01'::date AND
                "VW_PLAN".prj_id IS NOT NULL
            GROUP BY
                "VW_PLAN".prj_id,
                "VW_PLAN".org_id,
                (date_part('year'::text, "VW_PLAN".fromdate))
        )
        SELECT
            budget.prj_id,
            budget.org_id,
            tijdreeks.jaar,
            tijdreeks.maand,
            budget.budget
        FROM
            budget
        LEFT JOIN
            tijdreeks ON
                budget.jaar = tijdreeks.jaar
        ORDER BY
            budget.prj_id,
            budget.org_id,
            budget.jaar,
            tijdreeks.maand
    ),

    actuals AS (
        SELECT
            sum(v_timetell_projectenoverzicht_6.hrs_hours) AS hours,
            sum(v_timetell_projectenoverzicht_6.hrs_hoursrate) AS costs,
            v_timetell_projectenoverzicht_6.prj_prj_id AS prj_id,
            v_timetell_projectenoverzicht_6."Project Nummer" AS project_nummer,
            v_timetell_projectenoverzicht_6.prj_name,
            v_timetell_projectenoverzicht_6.prj_niv1_name,
            v_timetell_projectenoverzicht_6.org_id,
            v_timetell_projectenoverzicht_6.org_name,
            v_timetell_projectenoverzicht_6."Project Verantw." AS project_verantw,
            v_timetell_projectenoverzicht_6."Project Leader" AS project_leader,
            v_timetell_projectenoverzicht_6.cust_name,
            v_timetell_projectenoverzicht_6.act_name,
            date_part('year'::text, v_timetell_projectenoverzicht_6.hrs_date) AS jaar,
            date_part('month'::text, v_timetell_projectenoverzicht_6.hrs_date) AS maand,
            date_trunc('month'::text, v_timetell_projectenoverzicht_6.hrs_date::timestamp with time zone)::date AS eerstedagvandemaand
        FROM
            "{schemaname}".v_timetell_projectenoverzicht_6
        WHERE
            v_timetell_projectenoverzicht_6.hrs_date IS NOT NULL AND
            v_timetell_projectenoverzicht_6.hrs_hours_status <> 0
        GROUP BY
            v_timetell_projectenoverzicht_6.prj_prj_id,
            v_timetell_projectenoverzicht_6."Project Nummer",
            v_timetell_projectenoverzicht_6.prj_name,
            v_timetell_projectenoverzicht_6.prj_niv1_name,
            v_timetell_projectenoverzicht_6.org_id,
            v_timetell_projectenoverzicht_6.org_name,
            v_timetell_projectenoverzicht_6."Project Verantw.",
            v_timetell_projectenoverzicht_6."Project Leader",
            v_timetell_projectenoverzicht_6.cust_name,
            v_timetell_projectenoverzicht_6.act_name,
            (date_part('year'::text, v_timetell_projectenoverzicht_6.hrs_date)),
            (date_part('month'::text, v_timetell_projectenoverzicht_6.hrs_date)),
            (date_trunc('month'::text, v_timetell_projectenoverzicht_6.hrs_date::timestamp with time zone)::date)
    )

    SELECT
        budget.prj_id,
        pj.project_nummer,
        pj.prj_name,
        pj.prj_niv1_name,
        budget.org_id,
        o.name AS org_name,
        pj.project_verantw,
        pj.project_leader,
        pj.cust_name,
        pj.act_name,
        tijdreeks.jaar,
        tijdreeks.maand,
        tijdreeks.eerste_dag AS eerstedagvandemaand,
        0::double precision AS hours,
        0::double precision AS costs,
        budget.budget
    FROM
        budget
    LEFT JOIN tijdreeks ON
        budget.jaar = tijdreeks.jaar
    LEFT JOIN project_jaar pj ON
        pj.prj_id = budget.prj_id AND
        budget.jaar = pj.jaar
    JOIN "{schemaname}"."ORG" o ON
        budget.org_id = o.org_id

    UNION

    SELECT
        a.prj_id,
        a.project_nummer,
        a.prj_name,
        a.prj_niv1_name,
        a.org_id,
        a.org_name,
        a.project_verantw,
        a.project_leader,
        a.cust_name,
        a.act_name,
        a.jaar,
        a.maand,
        a.eerstedagvandemaand,
        COALESCE(a.hours, 0::double precision) AS hours,
        COALESCE(a.costs, 0::double precision) AS costs,
        COALESCE(bm.budget, 0::numeric) AS budget
    FROM
        actuals a
    LEFT JOIN budget_maand bm ON
        a.prj_id = bm.prj_id AND
        a.org_id = bm.org_id AND
        a.jaar = bm.jaar AND
        a.maand = bm.maand
) c
GROUP BY
    c.prj_id,
    c.project_nummer,
    c.prj_name,
    c.prj_niv1_name,
    c.org_id,
    c.org_name,
    c.project_verantw,
    c.project_leader,
    c.cust_name,
    c.act_name,
    c.jaar,
    c.maand,
    c.eerstedagvandemaand
ORDER BY
    c.project_nummer,
    c.eerstedagvandemaand,
    c.org_id;
