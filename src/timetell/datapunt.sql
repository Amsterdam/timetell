-- View: "{schemaname}".v_timetell_projectenoverzicht_7

CREATE MATERIALIZED VIEW "{schemaname}".v_timetell_projectenoverzicht_7
AS
WITH w_project_adviseur AS (
    SELECT
        a.prj_id,
        b.item_id as project_adviseur_emp_id,
        b.item as project_adviseur_name
    FROM "{schemaname}"."PRJ" a
    JOIN "{schemaname}"."SYS_OPT_ITM" b ON b.item_id = a.prjcat
    WHERE  b.opt_id = 32
),
w_basis_team_naam AS (
    SELECT
        lp.dim_id AS prj_id,
        max(soi.item::text) AS basis_team_name
    FROM "{schemaname}"."VW_LABEL_PRJ" lp
    LEFT JOIN "{schemaname}"."SYS_OPT_ITM" soi ON lp.item_id = soi.item_id
    WHERE to_char(lp.todate::timestamp with time zone, 'YYYYMMDD'::text) = '29991231'::text
    GROUP BY lp.dim_id
),
w_laatste_projectleider AS (
    WITH a AS (
        SELECT prjpl.prj_id,
        prjpl.emp_id,
        emp.name AS prjl_name
        FROM "{schemaname}"."PRJ_LINK" prjpl
        LEFT JOIN "{schemaname}"."EMP" emp ON emp.emp_id = prjpl.emp_id
        WHERE prjpl.prjleader = 1
    ),
    prj_pl AS (
        SELECT a.prj_id, count(*) AS aantal
        FROM a
        GROUP BY a.prj_id
    ),
    b AS (
        SELECT
            h.prj_id,
            a.emp_id,
            max(h.date) AS max
        FROM a
        JOIN prj_pl pl2 ON a.prj_id = pl2.prj_id
        LEFT JOIN "{schemaname}"."HRS" h ON a.prj_id = h.prj_id AND a.emp_id = h.emp_id
        WHERE pl2.aantal > 1
        GROUP BY h.prj_id, a.emp_id
    ),
    c AS (
        SELECT
            DISTINCT ON (b.prj_id) b.prj_id,
            b.emp_id
        FROM b
        ORDER BY b.prj_id, b.emp_id
    )

    SELECT
        a.prj_id,
        a.emp_id,
        a.prjl_name
    FROM a
    JOIN c ON a.prj_id = c.prj_id AND a.emp_id = c.emp_id
    UNION
    SELECT
        a.prj_id,
        a.emp_id,
        a.prjl_name
    FROM a
    JOIN prj_pl ON a.prj_id = prj_pl.prj_id
    WHERE prj_pl.aantal = 1
),
w_emp_basis_team  as(
    with emp_laatste_basis_team as (
        with emp_laatste_basis_team_all as (
            SELECT laatste.emp_id, laatste.maxtodate, eo2.org_id as laatste_org_id
            FROM (
                SELECT
                    eo.emp_id,
                    max(eo.todate) AS maxtodate
                FROM "{schemaname}"."EMP_ORG" eo
                JOIN "{schemaname}"."ORG" o ON eo.org_id=o.org_id
                WHERE eo.type = 0
                GROUP BY eo.emp_id
            ) laatste
            JOIN "{schemaname}"."EMP_ORG" eo2 on laatste.emp_id=eo2.emp_id and laatste.maxtodate=eo2.todate -- bij 2099 en 2999
            where eo2.type = 0
        )

        SELECT
            emp_id,
            maxtodate,
            laatste_org_id,
            count(*) AS aantal
        FROM emp_laatste_basis_team_all
        GROUP BY
            emp_id,
            maxtodate,
            laatste_org_id
        ORDER BY emp_id, maxtodate
    )
    SELECT
        sel.emp_id,
        o.org_id,
        o.name as oname_toen,
        CASE WHEN
            o.todate>=to_date('2099-12-31', 'YYYY-MM-DD')
            THEN o.name
            ELSE null END
        as oname_nu
    FROM emp_laatste_basis_team sel
    JOIN "{schemaname}"."ORG" o ON sel.laatste_org_id = o.org_id
    order by sel.emp_id
)

SELECT
    uren.hrs_date,
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
    project."Project Adviseur",
    project.prj_niv_id,
    project.prj_niv0_id,
    project.prj_niv1_id,
    project.prj_niv2_id,
    project.prj_prj_id,
    project.prj_parent_id,
    project.fromdate,
    project.todate,
    project.prj_name,
    project.code,
    project.calc_hours,
    project.calc_costs,
    project."Project Nummer",
    project.status_project
   FROM ( SELECT 0 AS prj_niv,
            a0.name AS prj_niv_name,
            a0.name AS prj_niv0_name,
            NULL::text AS prj_niv1_name,
            NULL::character varying AS prj_niv2_name,
            NULL::character varying(50) AS "Project Verantw.",
            NULL::character varying AS "Project Leader",
            pa0.project_adviseur_name AS "Project Adviseur",
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
            a0.code,
            a0.calc_hours,
            a0.calc_costs,
            a0.nr AS "Project Nummer",
            soi0.item AS status_project
           FROM "{schemaname}"."PRJ" a0
		 	LEFT JOIN w_project_adviseur pa0 ON pa0.prj_id = a0.prj_id
            LEFT JOIN "{schemaname}"."SYS_OPT_ITM" soi0 ON soi0.item_id = a0.status
          WHERE a0.parent_id IS NULL
        UNION
         SELECT 1 AS prj_niv,
            b1.name AS prj_niv_name,
            a1.name AS prj_niv0_name,
            (((b1.nr::text || ' - '::text) || b1.name::text))::character varying(50) AS prj_niv1_name,
            NULL::text AS prj_niv2_name,
            btn1.basis_team_name::character varying(50) AS "Project Verantw.",
            prjpl.prjl_name AS "Project Leader",
            pa1.project_adviseur_name AS "Project Adviseur",
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
            b1.code,
            b1.calc_hours,
            b1.calc_costs,
            b1.nr AS "Project Nummer",
            soi1.item AS status_project
           FROM "{schemaname}"."PRJ" a1
             LEFT JOIN "{schemaname}"."PRJ" b1 ON a1.prj_id = b1.parent_id
             LEFT JOIN w_laatste_projectleider prjpl ON b1.prj_id = prjpl.prj_id
             LEFT JOIN "{schemaname}"."EMP" e1 ON e1.emp_id = prjpl.emp_id
             LEFT JOIN w_basis_team_naam btn1 ON btn1.prj_id = b1.prj_id
             LEFT JOIN w_project_adviseur pa1 ON pa1.prj_id = b1.prj_id
             LEFT JOIN "{schemaname}"."CUST" cust1 ON b1.cust_id = cust1.cust_id
             LEFT JOIN "{schemaname}"."SYS_OPT_ITM" soi1 ON soi1.item_id = b1.status
		 	WHERE a1.parent_id IS NULL
        UNION
         SELECT 2 AS prj_niv,
            c2.name AS prj_niv_name,
            a2.name AS " prj_niv0_name",
            (b2.nr::text || ' - '::text) || b2.name::text AS prj_niv1_name,
            (c2.nr::text || ' - '::text) || c2.name::text AS prj_niv2_name,
            btn2.basis_team_name::character varying(50) AS "Project Verantw.",
            prjpl2.prjl_name AS "Project Leader",
            pa2.project_adviseur_name AS "Project Adviseur",
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
            c2.code,
            c2.calc_hours,
            c2.calc_costs,
            c2.nr AS "Project Nummer",
            soi2.item AS status_project
           FROM "{schemaname}"."PRJ" a2
             JOIN "{schemaname}"."PRJ" b2 ON a2.prj_id = b2.parent_id
             JOIN "{schemaname}"."PRJ" c2 ON b2.prj_id = c2.parent_id
             LEFT JOIN w_laatste_projectleider prjpl2 ON b2.prj_id = prjpl2.prj_id
             LEFT JOIN w_basis_team_naam btn2 ON btn2.prj_id = b2.prj_id
             LEFT JOIN w_project_adviseur pa2 ON pa2.prj_id = b2.prj_id
             LEFT JOIN "{schemaname}"."CUST" cust2 ON c2.cust_id = cust2.cust_id
             LEFT JOIN "{schemaname}"."SYS_OPT_ITM" soi2 ON soi2.item_id = c2.status
			 where a2.parent_id IS NULL) project
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
            ebt.org_id,
            ebt.oname_nu AS org_name,
            emp.empcat AS emp_empcat,
            ebt.oname_nu AS "Medew. Verantw.",
            act.name AS act_name
           FROM "{schemaname}"."HRS" h
             JOIN "{schemaname}"."EMP" emp ON h.emp_id = emp.emp_id
             JOIN "{schemaname}"."ORG" org2 ON h.org_id = org2.org_id
			JOIN w_emp_basis_team ebt on ebt.emp_id=h.emp_id
             JOIN "{schemaname}"."ACT" act ON h.act_id = act.act_id) uren ON project.prj_prj_id = uren.prj_id
WITH DATA;


CREATE OR REPLACE VIEW "{schemaname}".v_timetell_project_team_maand_ab_7 AS

WITH w_laatste_projectleider AS (
    WITH a AS (
        SELECT
            prjpl.prj_id,
            prjpl.emp_id,
            emp.name AS prjl_name
        FROM "{schemaname}"."PRJ_LINK" prjpl
        LEFT JOIN "{schemaname}"."EMP" emp ON emp.emp_id = prjpl.emp_id
        WHERE prjpl.prjleader = 1
    ),
    prj_pl AS (
        SELECT
            a.prj_id,
            count(*) AS aantal
        FROM a
        GROUP BY a.prj_id
    ),
    b AS (
        SELECT
            h.prj_id,
            a.emp_id,
            max(h.date) AS max
        FROM a
        JOIN prj_pl pl2 ON a.prj_id = pl2.prj_id
        LEFT JOIN "{schemaname}"."HRS" h ON a.prj_id = h.prj_id AND a.emp_id = h.emp_id
        WHERE pl2.aantal > 1
        GROUP BY
            h.prj_id,
            a.emp_id
    ),
    c AS (
        SELECT
            DISTINCT ON (b.prj_id) b.prj_id,
            b.emp_id
        FROM b
        ORDER BY
            b.prj_id,
            b.emp_id
    )

    SELECT
        a.prj_id,
        a.emp_id,
        a.prjl_name
    FROM a
    JOIN c c_1 ON a.prj_id = c_1.prj_id AND a.emp_id = c_1.emp_id

    UNION

    SELECT
        a.prj_id,
        a.emp_id,
        a.prjl_name
    FROM a
    JOIN prj_pl ON a.prj_id = prj_pl.prj_id
    WHERE prj_pl.aantal = 1
),
w_project_adviseur AS (
    SELECT
        a.prj_id,
        b.item AS project_adviseur
    FROM "{schemaname}"."PRJ" a
    JOIN "{schemaname}"."SYS_OPT_ITM" b ON b.item_id = a.prjcat
    WHERE b.opt_id = 32
),
tijdreeks AS (
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
    FROM "{schemaname}"."VW_PLAN"
    WHERE
        "VW_PLAN".fromdate >= '2019-01-01'::date AND
        "VW_PLAN".prj_id IS NOT NULL AND
        "VW_PLAN".org_id <> 0
    GROUP BY
        "VW_PLAN".prj_id,
        "VW_PLAN".org_id,
        date_part('year'::text, "VW_PLAN".fromdate)
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
        max(d.project_adviseur::text) AS project_adviseur,
        max(d.cust_name::text) AS cust_name,
        max(d.act_name::text) AS act_name
    FROM (
        SELECT
            po.prj_prj_id AS prj_id,
            po."Project Nummer" AS project_nummer,
            po.prj_name,
            po.prj_niv1_name,
            po."Project Verantw." AS project_verantw,
            po."Project Leader" AS project_leader,
            po."Project Adviseur" AS project_adviseur,
            po.cust_name,
            po.act_name,
            date_part('year'::text, po.hrs_date) AS jaar
        FROM "{schemaname}".v_timetell_projectenoverzicht_7 po
        WHERE po."Project Nummer" IS NOT NULL
        GROUP BY
            po.prj_prj_id,
            po."Project Nummer",
            po.prj_name,
            po.prj_niv1_name,
            po."Project Verantw.",
            po."Project Leader",
            po."Project Adviseur",
            po.cust_name,
            po.act_name,
            date_part('year'::text, po.hrs_date)
    ) d
    GROUP BY
        d.prj_id,
        d.jaar
),
budget_maand AS (
    WITH budget AS (
        SELECT
            b.prj_id,
            b.org_id,
            date_part('year'::text, b.fromdate) AS jaar,
            sum(b.costs) AS budget
        FROM "{schemaname}"."VW_PLAN" b
        WHERE b.fromdate >= '2019-01-01'::date AND NOT (b.prj_id IS NULL OR b.prj_id <= 0)
        GROUP BY
            b.prj_id,
            b.org_id,
            date_part('year'::text, b.fromdate)
    )
    SELECT
        budget.prj_id,
        budget.org_id,
        tijdreeks.jaar,
        tijdreeks.maand,
        budget.budget
    FROM budget
    LEFT JOIN tijdreeks ON budget.jaar = tijdreeks.jaar
    ORDER BY
        budget.prj_id,
        budget.org_id,
        budget.jaar,
        tijdreeks.maand
),
actuals AS (
    SELECT
        sum(po.hrs_hours) AS hours,
        sum(po.hrs_hoursrate) AS costs,
        po.prj_prj_id AS prj_id,
        po."Project Nummer" AS project_nummer,
        po.prj_name,
        po.prj_niv1_name,
        p.code,
        po.org_id,
        po.org_name,
        po."Project Verantw." AS project_verantw,
        po."Project Leader" AS project_leader,
        po."Project Adviseur" AS project_adviseur,
        p.status AS status_project,
        po.cust_name,
        act.name AS act_name,
        date_part('year'::text, po.hrs_date) AS jaar,
        date_part('month'::text, po.hrs_date) AS maand,
        date_trunc('month'::text, po.hrs_date::timestamp with time zone)::date AS eerstedagvandemaand
    FROM "{schemaname}".v_timetell_projectenoverzicht_7 po
    JOIN "{schemaname}"."PRJ" p ON po.prj_prj_id = p.prj_id
    LEFT JOIN "{schemaname}"."ACT" act ON act.act_id = p.act_id
    WHERE
        po.hrs_date IS NOT NULL AND
        po.hrs_hours_status <> 0
    GROUP BY
        po.prj_prj_id,
        po."Project Nummer",
        po.prj_name,
        po.prj_niv1_name,
        p.code,
        po.org_id,
        po.org_name,
        po."Project Verantw.",
        po."Project Leader",
        po."Project Adviseur",
        p.status,
        po.cust_name,
        act.name,
        date_part('year'::text, po.hrs_date),
        date_part('month'::text, po.hrs_date),
        date_trunc('month'::text, po.hrs_date::timestamp with time zone)::date
)

SELECT
    c.prj_id,
    c.project_nummer,
    c.prj_name,
    c.prj_niv1_name,
    c.code,
    c.org_id,
    c.org_name,
    c.project_verantw,
    c.project_leader,
    c.project_adviseur,
    c.status_project,
    c.cust_name,
    c.act_name,
    c.jaar,
    c.maand,
    c.eerstedagvandemaand,
    sum(c.hours) AS hours,
    sum(c.costs) AS costs,
    max(c.budget) AS budget
FROM (
    SELECT
        budget.prj_id,
        p.nr AS project_nummer,
        p.name AS prj_name,
        pj.prj_niv1_name,
        p.code,
        budget.org_id,
        o.name AS org_name,
        pj.project_verantw,
        pl.prjl_name AS project_leader,
        pa.project_adviseur,
        soi.item AS status_project,
        c_1.name AS cust_name,
        act.name AS act_name,
        tijdreeks.jaar,
        tijdreeks.maand,
        tijdreeks.eerste_dag AS eerstedagvandemaand,
        0::double precision AS hours,
        0::double precision AS costs,
        budget.budget
    FROM budget
    LEFT JOIN tijdreeks ON budget.jaar = tijdreeks.jaar
    LEFT JOIN project_jaar pj ON pj.prj_id = budget.prj_id
    JOIN "{schemaname}"."ORG" o ON budget.org_id = o.org_id
    JOIN "{schemaname}"."PRJ" p ON p.prj_id = budget.prj_id
    LEFT JOIN "{schemaname}"."CUST" c_1 ON p.cust_id = c_1.cust_id
    LEFT JOIN "{schemaname}"."ACT" act ON p.act_id = act.act_id
    LEFT JOIN w_laatste_projectleider pl ON pl.prj_id = budget.prj_id
    LEFT JOIN w_project_adviseur pa ON pa.prj_id = budget.prj_id
    LEFT JOIN "{schemaname}"."SYS_OPT_ITM" soi ON soi.item_id = p.status

    UNION ALL

    SELECT
        a.prj_id,
        a.project_nummer,
        a.prj_name,
        a.prj_niv1_name,
        a.code,
        a.org_id,
        a.org_name,
        a.project_verantw,
        a.project_leader,
        a.project_adviseur,
        soi.item AS status_project,
        a.cust_name,
        a.act_name,
        a.jaar,
        a.maand,
        a.eerstedagvandemaand,
        COALESCE(a.hours, 0::double precision) AS hours,
        COALESCE(a.costs, 0::double precision) AS costs,
        COALESCE(bm.budget, 0::numeric)::double precision AS budget
    FROM actuals a
    LEFT JOIN budget_maand bm ON a.prj_id = bm.prj_id AND a.org_id = bm.org_id AND a.jaar = bm.jaar AND a.maand = bm.maand
    LEFT JOIN "{schemaname}"."SYS_OPT_ITM" soi ON soi.item_id = a.status_project
) c
GROUP BY
    c.prj_id,
    c.project_nummer,
    c.prj_name,
    c.prj_niv1_name,
    c.code,
    c.org_id,
    c.org_name,
    c.project_verantw,
    c.project_leader,
    c.project_adviseur,
    c.status_project,
    c.cust_name,
    c.act_name,
    c.jaar,
    c.maand,
    c.eerstedagvandemaand
ORDER BY
    c.prj_id,
    c.eerstedagvandemaand,
    c.org_id;


----------------------------------


CREATE OR REPLACE VIEW "{schemaname}".v_timetell_emp_contracts_month AS

-- add information to the list of employees.
WITH emp_contracts AS (
    SELECT
     	emp.emp_id,
     	emp.name,
	 	case when emp_contract.internalrate <>0 then 'Extern'
        else 'Intern' end AS contractsoort_name,
	    emp_contract.fromdate AS contract_from_date,
	    emp_contract.todate AS contract_to_date,
	    job.name AS job_name,
	    ambition_group_opt.item AS ambitiegroep_name,
        ambition_group_opt.code AS ambitiemanager_name
    FROM
	    "{schemaname}"."EMP" as emp
	    -- join all contracts
	    LEFT JOIN "{schemaname}"."EMP_CONTRACT" AS emp_contract ON emp_contract.emp_id = emp.emp_id
	    -- join the job to get its name
	    LEFT JOIN "{schemaname}"."JOB" AS job ON emp_contract.job_id = job.job_id
	    -- join sys_opt_itm to get ambitiegroep
	    LEFT JOIN "{schemaname}"."SYS_OPT_ITM" AS ambition_group_opt ON ambition_group_opt.item_id = emp_contract.type AND ambition_group_opt.opt_id = 3
	    )

-- make a record per month for every employee that falls within the contract period (per contract).
,emp_contracts_per_month AS (
    SELECT
	    emp_contracts.*,
	    date_trunc('month', contract_month)::date as contract_month,
		contract_month as contract_month_date,
		emp_team.org_id,
	    org.name AS team,
	    emp_team.fromdate AS team_from_date,
	    emp_team.todate AS team_to_date
    FROM emp_contracts
    CROSS JOIN GENERATE_SERIES(emp_contracts.contract_from_date, emp_contracts.contract_to_date, '1 month') AS contract_month

	-- join the current main team (type = 0)
	LEFT JOIN (
			SELECT emp_id, emp_org.org_id, fromdate, todate
			FROM "{schemaname}"."EMP_ORG" AS emp_org
			WHERE emp_org.type = 0  -- join main team (type = 0)
		) emp_team ON emp_team.emp_id = emp_contracts.emp_id

	-- join the organisation to get team name
	LEFT JOIN "{schemaname}"."ORG" AS org ON org.org_id = emp_team.org_id
	WHERE emp_team.fromdate <= date_trunc('month', contract_month)  -- aansluiting bij team moet voor of op peildatum zijn begonnen.
	AND emp_team.todate > date_trunc('month', contract_month)	-- team verlaten juist NIET voor of op peildatum.
	AND contract_from_date <= date_trunc('month', contract_month) -- contract_from_date moet voor of op peildatum zijn begonnen.
	AND contract_to_date > date_trunc('month', contract_month)  -- contract_to_date (einde contract) alleen na peildatum.
)

--Select statement shows per employee, per month their contract type, team etc.
SELECT *
FROM emp_contracts_per_month
WHERE emp_contracts_per_month.contract_month <= date_trunc('month', NOW()) AND emp_contracts_per_month.name != '[admin]'; --no admin.

