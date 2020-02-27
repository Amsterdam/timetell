
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
