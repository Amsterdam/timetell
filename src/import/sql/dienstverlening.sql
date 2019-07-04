--------------------------------------------------------------
/*
20180709: Fred Koenders / Infotopics

Purpose:
1. Create a view for each source table.
2. Combine these views into 1 single view for Tableau

*/

/*
remove the views first, in the right order
*/
DROP VIEW IF EXISTS "{schemaname}"."vw_timetell_ivdi";
DROP VIEW IF EXISTS "{schemaname}"."vw_tableau_act";
DROP VIEW IF EXISTS "{schemaname}"."vw_tableau_cust";
DROP VIEW IF EXISTS "{schemaname}"."vw_tableau_emp";
DROP VIEW IF EXISTS "{schemaname}"."vw_tableau_hrs";
DROP VIEW IF EXISTS "{schemaname}"."vw_tableau_org";
DROP VIEW IF EXISTS "{schemaname}"."vw_tableau_prj";
DROP VIEW IF EXISTS "{schemaname}"."vw_tableau_sys_prj_niv";
DROP VIEW IF EXISTS "{schemaname}"."vw_tableau_hulp_week";
--------------------------------------------------------------
-- 1/x: vw_tableau_act
--------------------------------------------------------------
CREATE OR REPLACE VIEW "{schemaname}"."vw_tableau_act" AS
SELECT  act_id
,       name AS activiteit
FROM    "{schemaname}"."ACT";

--------------------------------------------------------------
-- 2/x: vw_tableau_cust
--------------------------------------------------------------
CREATE OR REPLACE VIEW "{schemaname}"."vw_tableau_cust" AS
SELECT  cust_id
,       name AS opdrachtgever
FROM    "{schemaname}"."CUST";

--------------------------------------------------------------
-- 3/x: vw_tableau_emp
--------------------------------------------------------------
CREATE OR REPLACE VIEW "{schemaname}"."vw_tableau_emp" AS
SELECT  emp_id
,       lastname AS mdw_achternaam
,       middlename AS mdw_tussenvoegsels
,       firstname AS mdw_voornaam
FROM    "{schemaname}"."EMP";

--------------------------------------------------------------
-- 4/x: vw_tableau_hrs
--------------------------------------------------------------
CREATE OR REPLACE VIEW "{schemaname}"."vw_tableau_hrs" AS
SELECT  act_id
,       cust_id
,       emp_id
,       org_id
,       prj_id
,       date AS datum
,       hours AS uren
FROM    "{schemaname}"."HRS";

--------------------------------------------------------------
-- 5/x: vw_tableau_org
--------------------------------------------------------------
CREATE OR REPLACE VIEW "{schemaname}"."vw_tableau_org" AS
SELECT  org_id
,       name AS organisatie
FROM    "{schemaname}"."ORG";

--------------------------------------------------------------
-- 6/x: vw_tableau_prj
--------------------------------------------------------------
CREATE OR REPLACE VIEW "{schemaname}"."vw_tableau_prj" AS
SELECT  prj_id
,       nr AS project_nummer
,       name AS project
,       prj_id::VARCHAR(20) AS project_id
,       parent_id::VARCHAR(20) AS project_id_bovenliggend
,       CASE
        WHEN LEFT(nr,1) = 'A' AND SUBSTRING(nr, 2, 1) IN ('0','1','2','3','4','5','6','7','8','9') THEN 'A'
        WHEN LEFT(nr,1) = 'P' AND SUBSTRING(nr, 2, 1) IN ('0','1','2','3','4','5','6','7','8','9') THEN 'P'
        WHEN LEFT(nr,1) = 'I' AND SUBSTRING(nr, 2, 1) IN ('0','1','2','3','4','5','6','7','8','9') THEN 'I'
        WHEN LEFT(nr,1) = 'M' AND SUBSTRING(nr, 2, 1) IN ('0','1','2','3','4','5','6','7','8','9') THEN 'M'
        WHEN LEFT(nr,2) = 'DM' AND SUBSTRING(nr, 3, 1) IN ('0','1','2','3','4','5','6','7','8','9') THEN 'DM'
        WHEN LEFT(nr,2) = 'In' AND SUBSTRING(nr, 3, 1) IN ('0','1','2','3','4','5','6','7','8','9') THEN 'In'
        ELSE '?' END project_type
,       parent_id -- is used in vw_timetell_ivdi
FROM    "{schemaname}"."PRJ";

--------------------------------------------------------------
-- 7/x: vw_tableau_sys_prj_niv
--------------------------------------------------------------
--DROP VIEW vw_tableau_sys_prj_niv;
CREATE VIEW "{schemaname}"."vw_tableau_sys_prj_niv" AS
SELECT  niv_id AS prj_id
,       niv::VARCHAR(20) AS project_niveau
,       niv0_name AS project_op_niveau_0
,       niv1_name AS project_op_niveau_1
,       niv0_id::VARCHAR(20) AS project_id_op_niveau_0
,       niv1_id::VARCHAR(20) AS project_id_op_niveau_1
FROM    "{schemaname}"."SYS_PRJ_NIV";

--------------------------------------------------------------
-- 8/x: vw_tableau_sys_prj_niv
--------------------------------------------------------------
CREATE OR REPLACE VIEW "{schemaname}"."vw_tableau_hulp_week" AS
SELECT extract(year FROM date) jaar, extract(week FROM date) weeknummer, Max(date) datum
FROM "{schemaname}"."HRS"
GROUP BY extract(year FROM date), extract(week FROM date);

--------------------------------------------------------------
-- Combine x views into just one
--------------------------------------------------------------

CREATE OR REPLACE VIEW "{schemaname}"."vw_timetell_ivdi" AS
-- part 1: hrs records
SELECT  'hrs' AS bron
,       hrs.datum
,       hrs.uren
,       act.activiteit
,       cust.opdrachtgever
,       emp.mdw_achternaam
,       emp.mdw_voornaam
,       emp.mdw_tussenvoegsels
,       org.organisatie

,       prj.project_nummer
,       prj.project
,       prj.project_type
,       prj.project_id

,       parent.project_nummer AS project_nummer_bovenliggend
,       parent.project AS project_bovenliggend
,       parent.project_type AS project_type_bovenliggend
,       parent.project_id AS project_id_bovenliggend

,       prj_niv.project_niveau
,       prj_niv.project_op_niveau_0
,       prj_niv.project_op_niveau_1
,       prj_niv.project_id_op_niveau_0
,       prj_niv.project_id_op_niveau_1

,       CASE
        WHEN prj.project_type = 'A' AND RIGHT(prj.project_nummer,1) = '1' THEN 'Regietaken'
        WHEN prj.project_type = 'A' AND RIGHT(prj.project_nummer,1) = '2' THEN 'Gepland in stand houden'
        WHEN prj.project_type = 'A' AND RIGHT(prj.project_nummer,1) = '3' THEN 'Onvoorzien in stand houden'
        WHEN prj.project_type = 'A' AND RIGHT(prj.project_nummer,1) = '4' THEN 'Changes'
        WHEN prj.project_type = 'A' AND RIGHT(prj.project_nummer,1) = '5' THEN 'LC'
        WHEN prj.project_type = 'P' AND RIGHT(prj.project_nummer,1) = '1' THEN 'Intake'
        WHEN prj.project_type = 'P' AND RIGHT(prj.project_nummer,1) = '2' THEN 'Verkenning'
        WHEN prj.project_type = 'P' AND RIGHT(prj.project_nummer,1) = '3' THEN 'Definitie'
        WHEN prj.project_type = 'P' AND RIGHT(prj.project_nummer,1) = '4' THEN 'Realisatie/Implementatie/Nazorg'
        WHEN prj.project_type = 'P' AND RIGHT(prj.project_nummer,1) = '5' THEN 'Agile'
        ELSE '?'
        END app_prj_detail
,       NULL::float norm_uren
,       current_timestamp AS datum_tijd_verversen_extract
FROM    "{schemaname}"."vw_tableau_hrs" AS hrs       -- we start with the facts
LEFT JOIN "{schemaname}"."vw_tableau_act" AS act     -- and then, all dimensions
ON      hrs.act_id = act.act_id
LEFT JOIN "{schemaname}"."vw_tableau_cust" AS cust
ON      hrs.cust_id = cust.cust_id
LEFT JOIN "{schemaname}"."vw_tableau_emp" AS emp
ON      hrs.emp_id = emp.emp_id
LEFT JOIN "{schemaname}"."vw_tableau_org" AS org
ON      hrs.org_id = org.org_id
LEFT JOIN "{schemaname}"."vw_tableau_prj" AS prj
ON      hrs.prj_id = prj.prj_id
LEFT JOIN "{schemaname}"."vw_tableau_sys_prj_niv" prj_niv
ON      hrs.prj_id = prj_niv.prj_id
LEFT JOIN "{schemaname}"."vw_tableau_prj" AS parent
ON      prj.parent_id = parent.prj_id
WHERE   uren <> 0
UNION ALL
-- part 2: norm_uren
SELECT  'emp_contract'
,       datum
,       NULL AS uren
,       NULL AS activiteit
,       NULL AS opdrachtgever
,       mdw_achternaam
,       mdw_voornaam
,       mdw_tussenvoegsels
,       NULL AS organisatie
,       NULL AS project_nummer
,       NULL AS project
,       NULL AS project_type
,       NULL AS project_id
,       NULL AS project_nummer_bovenliggend
,       NULL AS project_bovenliggend
,       NULL AS project_type_bovenliggend
,       NULL AS project_id_bovenliggend
,       NULL AS project_niveau
,       NULL AS project_op_niveau_0
,       NULL AS project_op_niveau_1
,       NULL AS project_id_op_niveau_0
,       NULL AS project_id_op_niveau_1
,       NULL AS app_prj_detail
,       hours
,       current_timestamp AS datum_tijd_verversen_extract
FROM    "{schemaname}"."EMP_CONTRACT" a
JOIN    "{schemaname}"."vw_tableau_hulp_week" b
ON      b.datum >= fromdate
AND     b.datum <= todate
LEFT JOIN "{schemaname}"."vw_tableau_emp" c
ON      a.emp_id = c.emp_id;

---------------------------------------
-- View: public.viw_tableau_datum
--
-- DROP VIEW public.viw_tableau_datum;
-- views tbv timetell_dienstverlening
-- 20-11-2018
---------------------------------------

CREATE OR REPLACE VIEW "{schemaname}"."viw_tableau_datum" AS
 WITH cte1 AS (
         SELECT row_number() OVER (ORDER BY columns.column_name) - 1 AS dagnummer
           FROM information_schema.columns
        ), cte2 AS (
         SELECT ('2018-01-01'::date + ((cte1.dagnummer || ' day'::text)::interval))::date AS datum
           FROM cte1
        ), cte3 AS (
         SELECT cte2.datum,
            date_part('dow'::text, cte2.datum) AS dow_datum,
            date_part('dow'::text, '2018-10-29'::date) AS dow_ma
           FROM cte2
        ), cte4 AS (
         SELECT cte3_1.datum AS ingdtm_week,
            lead(cte3_1.datum) OVER (ORDER BY cte3_1.datum) AS enddtm_week
           FROM cte3 cte3_1
          WHERE cte3_1.dow_datum = cte3_1.dow_ma
        )
 SELECT cte3.datum,
    cte3.dow_datum,
    cte3.dow_ma,
    cte4.ingdtm_week,
    cte4.enddtm_week
   FROM cte3,
    cte4
  WHERE cte3.datum >= cte4.ingdtm_week AND cte3.datum < cte4.enddtm_week AND date_part('year'::text, cte3.datum)::integer <= (date_part('year'::text, CURRENT_DATE)::integer + 1);

--------------------------------------------
-- View: "{schemaname}".viw_tableau_emp
-- DROP VIEW "{schemaname}".viw_tableau_emp;
---------------------------------------------

CREATE OR REPLACE VIEW "{schemaname}"."viw_tableau_emp" AS
 SELECT "EMP".emp_id,
    "EMP".nr AS mdw_nummer,
    "EMP".lastname AS mdw_achternaam,
    "EMP".middlename AS mdw_tussenvoegsels,
    "EMP".firstname AS mdw_voornaam,
    "EMP".empcat AS mdw_categorie
   FROM "{schemaname}"."EMP";

----------------------------------------------------------
-- View: "{schemaname}".viw_tableau_normuren_per_werkdag
-- DROP VIEW "{schemaname}".viw_tableau_normuren_per_werkdag;
----------------------------------------------------------

CREATE OR REPLACE VIEW "{schemaname}"."viw_tableau_normuren_per_werkdag" AS
 WITH cte AS (
         SELECT date_part('year'::text, viw_tableau_datum.datum) AS jaar,
            count(*) AS aantal_werkdagen_in_jaar
          FROM "{schemaname}"."viw_tableau_datum" as viw_tableau_datum
          WHERE viw_tableau_datum.dow_datum >= viw_tableau_datum.dow_ma AND viw_tableau_datum.dow_datum <= (viw_tableau_datum.dow_ma + 4::double precision)
          GROUP BY (date_part('year'::text, viw_tableau_datum.datum))
        )
 SELECT 'emp_contract'::text AS bron,
    b.datum,
    a.emp_id,
    a.fte,
    a.hours,
    cte.aantal_werkdagen_in_jaar,
    a.fte / cte.aantal_werkdagen_in_jaar::double precision AS fte_per_werkdag,
    a.hours / cte.aantal_werkdagen_in_jaar::double precision AS hours_per_werkdag
   FROM "{schemaname}"."EMP_CONTRACT" a
     JOIN "{schemaname}".viw_tableau_datum b ON b.datum >= a.fromdate AND b.datum <= a.todate
     JOIN cte ON date_part('year'::text, b.datum) = cte.jaar
  WHERE b.dow_datum >= b.dow_ma AND b.dow_datum <= (b.dow_ma + 4::double precision);
  
-------------------------------------------------------------
-- View: "{schemaname}".viw_tableau_hrs_union_all_normuren
-- DROP VIEW "{schemaname}".viw_tableau_hrs_union_all_normuren;
--------------------------------------------------------------

CREATE OR REPLACE VIEW "{schemaname}"."viw_tableau_hrs_union_all_normuren" AS
 SELECT 'hrs'::text AS bron,
    vw_tableau_hrs.act_id,
    vw_tableau_hrs.cust_id,
    vw_tableau_hrs.emp_id,
    vw_tableau_hrs.org_id,
    vw_tableau_hrs.prj_id,
    vw_tableau_hrs.datum,
    vw_tableau_hrs.uren,
    NULL::double precision AS hours,
    NULL::double precision AS fte,
    NULL::integer AS aantal_werkdagen_in_jaar,
    NULL::double precision AS hours_per_werkdag,
    NULL::double precision AS fte_per_werkdag
   FROM "{schemaname}".vw_tableau_hrs
  WHERE vw_tableau_hrs.uren <> 0::double precision
UNION ALL
 SELECT 'emp_contract'::text AS bron,
    NULL::integer AS act_id,
    NULL::integer AS cust_id,
    a.emp_id,
    b.org_id,
    NULL::integer AS prj_id,
    a.datum,
    NULL::double precision AS uren,
    a.hours,
    a.fte,
    a.aantal_werkdagen_in_jaar,
    a.hours_per_werkdag,
    a.fte_per_werkdag
   FROM "{schemaname}"."viw_tableau_normuren_per_werkdag" a,
    ( SELECT "{schemaname}".vw_tableau_hrs.emp_id,
            min(vw_tableau_hrs.org_id) AS org_id
           FROM "{schemaname}".vw_tableau_hrs
          GROUP BY vw_tableau_hrs.emp_id) b
  WHERE a.emp_id = b.emp_id;

----------------------------------------
-- View: "{schemaname}".viw_timetell_ivdi

-- DROP VIEW "{schemaname}".viw_timetell_ivdi;

CREATE OR REPLACE VIEW "{schemaname}"."viw_timetell_ivdi" AS
 SELECT hrs.bron,
    hrs.datum,
    hrs.uren,
    act.activiteit,
    cust.opdrachtgever,
    emp.mdw_achternaam,
    emp.mdw_voornaam,
    emp.mdw_tussenvoegsels,
    emp.mdw_nummer,
    emp.mdw_categorie,
    org.organisatie,
    prj.project_nummer,
        CASE
            WHEN hrs.bron = 'emp_contract'::text THEN '_Normuren'::character varying
            ELSE prj.project
        END AS project,
    prj.project_type,
    prj.project_id,
    parent.project_nummer AS project_nummer_bovenliggend,
    parent.project AS project_bovenliggend,
    parent.project_type AS project_type_bovenliggend,
    parent.project_id AS project_id_bovenliggend,
    prj_niv.project_niveau,
    prj_niv.project_op_niveau_0,
    prj_niv.project_op_niveau_1,
    prj_niv.project_id_op_niveau_0,
    prj_niv.project_id_op_niveau_1,
        CASE
            WHEN prj.project_type = 'A'::text AND "right"(prj.project_nummer::text, 1) = '1'::text THEN 'Regietaken'::text
            WHEN prj.project_type = 'A'::text AND "right"(prj.project_nummer::text, 1) = '2'::text THEN 'Gepland in stand houden'::text
            WHEN prj.project_type = 'A'::text AND "right"(prj.project_nummer::text, 1) = '3'::text THEN 'Onvoorzien in stand houden'::text
            WHEN prj.project_type = 'A'::text AND "right"(prj.project_nummer::text, 1) = '4'::text THEN 'Changes'::text
            WHEN prj.project_type = 'A'::text AND "right"(prj.project_nummer::text, 1) = '5'::text THEN 'LC'::text
            WHEN prj.project_type = 'P'::text AND "right"(prj.project_nummer::text, 1) = '1'::text THEN 'Intake'::text
            WHEN prj.project_type = 'P'::text AND "right"(prj.project_nummer::text, 1) = '2'::text THEN 'Verkenning'::text
            WHEN prj.project_type = 'P'::text AND "right"(prj.project_nummer::text, 1) = '3'::text THEN 'Definitie'::text
            WHEN prj.project_type = 'P'::text AND "right"(prj.project_nummer::text, 1) = '4'::text THEN 'Realisatie/Implementatie/Nazorg'::text
            WHEN prj.project_type = 'P'::text AND "right"(prj.project_nummer::text, 1) = '5'::text THEN 'Agile'::text
            WHEN hrs.bron = 'emp_contract'::text THEN 'Normuren'::text
            ELSE '?'::text
        END AS app_prj_detail,
    hrs.fte AS contract_fte,
    hrs.hours AS contract_uren,
    hrs.aantal_werkdagen_in_jaar,
    hrs.fte_per_werkdag AS contract_fte_per_werkdag,
    hrs.hours_per_werkdag AS contract_uren_per_werkdag,
    hrs.fte_per_werkdag AS norm_uren,
    CURRENT_TIMESTAMP AS datum_tijd_verversen_extract
   FROM "{schemaname}"."viw_tableau_hrs_union_all_normuren" hrs
     LEFT JOIN "{schemaname}".vw_tableau_act act ON hrs.act_id = act.act_id
     LEFT JOIN "{schemaname}".vw_tableau_cust cust ON hrs.cust_id = cust.cust_id
     LEFT JOIN "{schemaname}".viw_tableau_emp emp ON hrs.emp_id = emp.emp_id
     LEFT JOIN "{schemaname}".vw_tableau_org org ON hrs.org_id = org.org_id
     LEFT JOIN "{schemaname}".vw_tableau_prj prj ON hrs.prj_id = prj.prj_id
     LEFT JOIN "{schemaname}".vw_tableau_sys_prj_niv prj_niv ON hrs.prj_id = prj_niv.prj_id
     LEFT JOIN "{schemaname}".vw_tableau_prj parent ON prj.parent_id = parent.prj_id;


CREATE OR REPLACE VIEW "{schemaname}"."viwx_01_emp_skill"
AS SELECT cte.emp_id,
    min(b.item::text) AS skill
   FROM "{schemaname}"."PLAN_REQUEST" cte
     LEFT JOIN "{schemaname}"."SYS_OPT_ITM" b ON cte.skill_id = b.item_id
  GROUP BY cte.emp_id
 HAVING count(DISTINCT cte.skill_id) = 1;

CREATE OR REPLACE VIEW "{schemaname}"."viwx_01_zondagen"
AS WITH cte1 AS (
         SELECT row_number() OVER (ORDER BY columns.column_name) - 1 AS weeknummer
           FROM information_schema.columns
        ), cte2 AS (
         SELECT ('2018-01-07'::date + ((cte1.weeknummer || ' week'::text)::interval))::date AS datum
           FROM cte1
        )
 SELECT cte2.datum
   FROM cte2
  WHERE date_part('year'::text, cte2.datum)::integer <= (date_part('year'::text, CURRENT_DATE)::integer + 1);

CREATE OR REPLACE VIEW "{schemaname}"."viwx_02_contract_per_week"
AS SELECT 'emp_contract'::text AS bron,
    b.datum,
    a.emp_id,
    a.fte,
    a.hours,
    a.fromdate,
    a.todate
   FROM "{schemaname}"."EMP_CONTRACT" a
     JOIN "{schemaname}"."viwx_01_zondagen" b ON b.datum >= a.fromdate AND b.datum <= a.todate;

CREATE OR REPLACE VIEW "{schemaname}"."viwx_02_plan_alloc_verdeeld"
AS WITH cte1 AS (
         SELECT a.plan_alloc_id,
            b.datum,
            a.fromdate,
            a.todate,
            a.emp_id,
            a.hours,
            a.skill_id,
            a.plan_request_id,
            a.status,
            a.cust_id,
            a.prj_id,
            a.act_id
           FROM "{schemaname}"."PLAN_ALLOC" a,
            "{schemaname}"."viwx_01_zondagen" b
          WHERE (a.todate - a.fromdate) >= 7 AND a.fromdate <= b.datum AND b.datum <= a.todate AND b.datum >= (CURRENT_DATE - 7)
        ), cte2 AS (
         SELECT cte1.plan_alloc_id,
            count(*) AS deler
           FROM cte1
          GROUP BY cte1.plan_alloc_id
        )
 SELECT cte1.datum,
    cte1.emp_id,
    cte1.hours,
    cte1.skill_id,
    cte1.plan_alloc_id,
    cte1.status,
    cte2.deler,
    cte1.hours / cte2.deler::double precision AS hours_verdeeld,
    cte1.plan_request_id,
    cte1.fromdate,
    cte1.todate,
    cte1.cust_id,
    cte1.prj_id,
    cte1.act_id,
    101 AS status_alloc
   FROM cte1,
    cte2
  WHERE cte1.plan_alloc_id = cte2.plan_alloc_id AND (EXISTS ( SELECT 1
           FROM "{schemaname}"."PLAN_WEEK" x
          WHERE x.plan_alloc_id = cte1.plan_alloc_id))
UNION ALL
 SELECT cte1.datum,
    cte1.emp_id,
    cte1.hours,
    cte1.skill_id,
    cte1.plan_alloc_id,
    cte1.status,
    cte2.deler,
    cte1.hours / cte2.deler::double precision AS hours_verdeeld,
    cte1.plan_request_id,
    cte1.fromdate,
    cte1.todate,
    cte1.cust_id,
    cte1.prj_id,
    cte1.act_id,
    102 AS status_alloc
   FROM cte1,
    cte2
  WHERE cte1.plan_alloc_id = cte2.plan_alloc_id AND NOT (EXISTS ( SELECT 1
           FROM "{schemaname}"."PLAN_WEEK" x
          WHERE x.plan_alloc_id = cte1.plan_alloc_id))
UNION ALL
 SELECT a.todate AS datum,
    a.emp_id,
    a.hours,
    a.skill_id,
    a.plan_alloc_id,
    a.status,
    1 AS deler,
    a.hours AS hours_verdeeld,
    a.plan_request_id,
    a.fromdate,
    a.todate,
    a.cust_id,
    a.prj_id,
    a.act_id,
    101 AS status_alloc
   FROM "{schemaname}"."PLAN_ALLOC" a
  WHERE (a.todate - a.fromdate) < 7 AND a.todate >= (CURRENT_DATE - 7) AND (EXISTS ( SELECT 1
           FROM "{schemaname}"."PLAN_WEEK" x
          WHERE x.plan_alloc_id = a.plan_alloc_id))
UNION ALL
 SELECT a.todate AS datum,
    a.emp_id,
    a.hours,
    a.skill_id,
    a.plan_alloc_id,
    a.status,
    1 AS deler,
    a.hours AS hours_verdeeld,
    a.plan_request_id,
    a.fromdate,
    a.todate,
    a.cust_id,
    a.prj_id,
    a.act_id,
    102 AS status_alloc
   FROM "{schemaname}"."PLAN_ALLOC" a
  WHERE (a.todate - a.fromdate) < 7 AND a.todate >= (CURRENT_DATE - 7) AND (EXISTS ( SELECT 1
           FROM "{schemaname}"."PLAN_WEEK" x
          WHERE x.plan_alloc_id = a.plan_alloc_id));

CREATE OR REPLACE VIEW "{schemaname}"."viwx_02_plan_request_verdeeld"
AS WITH cte1 AS (
         SELECT a.plan_request_id,
            b.datum,
            a.fromdate,
            a.todate,
            a.emp_id,
            a.org_id,
            a.hours,
            a.skill_id,
            a.plan_task_id,
            a.status,
            a.status_alloc
           FROM "{schemaname}"."PLAN_REQUEST" a,
            "{schemaname}"."viwx_01_zondagen" b
          WHERE (a.todate - a.fromdate) >= 7 AND a.fromdate <= b.datum AND b.datum <= a.todate AND b.datum >= (CURRENT_DATE - 7)
        ), cte2 AS (
         SELECT cte1.plan_request_id,
            count(*) AS deler
           FROM cte1
          GROUP BY cte1.plan_request_id
        )
 SELECT cte1.datum,
    cte1.emp_id,
    cte1.hours,
    cte1.skill_id,
    cte1.plan_task_id,
    cte1.status,
    cte2.deler,
    cte1.hours / cte2.deler::double precision AS hours_verdeeld,
    cte1.plan_request_id,
    cte1.fromdate,
    cte1.todate,
    cte1.org_id,
    cte1.status_alloc
   FROM cte1,
    cte2
  WHERE cte1.plan_request_id = cte2.plan_request_id
UNION ALL
 SELECT a.todate AS datum,
    a.emp_id,
    a.hours,
    a.skill_id,
    a.plan_task_id,
    a.status,
    1 AS deler,
    a.hours AS hours_verdeeld,
    a.plan_request_id,
    a.fromdate,
    a.todate,
    a.org_id,
    a.status_alloc
   FROM "{schemaname}"."PLAN_REQUEST" a
  WHERE (a.todate - a.fromdate) < 7 AND a.todate >= (CURRENT_DATE - 7);

CREATE OR REPLACE VIEW "{schemaname}"."viwx_02_plan_task_verdeeld"
AS WITH cte1 AS (
         SELECT a.plan_task_id,
            b.datum,
            a.fromdate,
            a.todate,
            a.cust_id,
            a.hours,
            a.org_id,
            a.act_id,
            a.prj_id,
            a.status,
            a.can_allocate
           FROM "{schemaname}"."PLAN_TASK" a,
            "{schemaname}"."viwx_01_zondagen" b
          WHERE (a.todate - a.fromdate) >= 7 AND a.fromdate <= b.datum AND b.datum <= a.todate AND b.datum >= (CURRENT_DATE - 7)
        ), cte2 AS (
         SELECT cte1.plan_task_id,
            count(*) AS deler
           FROM cte1
          GROUP BY cte1.plan_task_id
        )
 SELECT a.datum,
    a.cust_id,
    a.hours,
    a.org_id,
    a.plan_task_id,
    a.status,
    cte2.deler,
    a.hours / cte2.deler::double precision AS hours_verdeeld,
    a.act_id,
    a.prj_id,
    a.fromdate,
    a.todate,
    a.can_allocate
   FROM cte1 a,
    cte2
  WHERE a.plan_task_id = cte2.plan_task_id
UNION ALL
 SELECT a.fromdate AS datum,
    a.cust_id,
    a.hours,
    a.org_id,
    a.plan_task_id,
    a.status,
    1 AS deler,
    a.hours AS hours_verdeeld,
    a.act_id,
    a.prj_id,
    a.fromdate,
    a.todate,
    a.can_allocate
   FROM "{schemaname}"."PLAN_TASK" a
  WHERE (a.todate - a.fromdate) < 7 AND a.todate >= (CURRENT_DATE - 7);

CREATE OR REPLACE VIEW "{schemaname}"."viwx_03_plan_union"
AS WITH cte AS (
         SELECT 'plan_task'::character varying(20) AS bron,
            viwx_02_plan_task_verdeeld.plan_task_id AS pk_id,
            viwx_02_plan_task_verdeeld.datum,
            viwx_02_plan_task_verdeeld.act_id,
            viwx_02_plan_task_verdeeld.prj_id,
            viwx_02_plan_task_verdeeld.cust_id,
            viwx_02_plan_task_verdeeld.fromdate,
            viwx_02_plan_task_verdeeld.todate,
            NULL::integer AS emp_id,
            viwx_02_plan_task_verdeeld.hours_verdeeld AS uren_taakplan,
            NULL::double precision AS uren_aangevraagd,
            NULL::double precision AS uren_gealloceerd,
            NULL::double precision AS uren_ingepland,
            NULL::integer AS skill_id,
            viwx_02_plan_task_verdeeld.can_allocate,
            viwx_02_plan_task_verdeeld.status,
            viwx_02_plan_task_verdeeld.org_id,
            NULL::integer AS status_alloc
           FROM "{schemaname}"."viwx_02_plan_task_verdeeld"
        UNION ALL
         SELECT 'plan_request'::character varying(20) AS bron,
            a.plan_request_id,
            a.datum,
            b_1.act_id,
            b_1.prj_id,
            b_1.cust_id,
            a.fromdate,
            a.todate,
            a.emp_id,
            NULL::double precision AS uren_taakplan,
            a.hours_verdeeld AS uren_aangevraagd,
            NULL::double precision AS uren_gealloceerd,
            NULL::double precision AS uren_ingepland,
            a.skill_id,
            NULL::integer AS can_allocate,
            a.status,
            a.org_id,
            a.status_alloc
           FROM "{schemaname}"."viwx_02_plan_request_verdeeld" a
             LEFT JOIN "{schemaname}"."PLAN_TASK" b_1 ON a.plan_task_id = b_1.plan_task_id
        UNION ALL
         SELECT 'plan_alloc'::character varying(20) AS bron,
            a.plan_alloc_id,
            a.datum,
            a.act_id,
            a.prj_id,
            a.cust_id,
            a.fromdate,
            a.todate,
            a.emp_id,
            NULL::double precision AS float8,
            NULL::double precision AS float8,
            a.hours_verdeeld,
            NULL::double precision AS float8,
            b_1.skill_id,
            NULL::integer AS can_allocate,
            a.status,
            b_1.org_id,
            a.status_alloc
           FROM "{schemaname}"."viwx_02_plan_alloc_verdeeld" a
             LEFT JOIN "{schemaname}"."PLAN_REQUEST" b_1 ON a.plan_request_id = b_1.plan_request_id
        UNION ALL
         SELECT 'plan_week'::character varying(20) AS bron,
            a.plan_week_id,
            a.fromdate AS datum,
            a.act_id,
            a.prj_id,
            a.cust_id,
            a.fromdate,
            a.todate,
            a.emp_id,
            NULL::double precision AS float8,
            NULL::double precision AS float8,
            NULL::double precision AS float8,
            a.hours,
            c.skill_id,
            NULL::integer AS can_allocate,
            NULL::integer AS status,
            a.org_id,
            NULL::integer AS status_alloc
           FROM "{schemaname}"."PLAN_WEEK" a
             LEFT JOIN "{schemaname}"."PLAN_ALLOC" b_1 ON a.plan_alloc_id = b_1.plan_alloc_id
             LEFT JOIN "{schemaname}"."PLAN_REQUEST" c ON b_1.plan_request_id = c.plan_request_id
        )
 SELECT cte.bron,
    cte.datum,
    cte.act_id,
    cte.prj_id,
    cte.cust_id,
    cte.fromdate,
    cte.todate,
    cte.emp_id,
    cte.uren_taakplan,
    cte.uren_aangevraagd,
    cte.uren_gealloceerd,
    cte.uren_ingepland,
    b.item AS skill,
    cte.can_allocate,
    cte.status,
    cte.org_id,
    cte.pk_id,
    cte.status_alloc
   FROM cte
     LEFT JOIN "{schemaname}"."SYS_OPT_ITM" b ON cte.skill_id = b.item_id;

CREATE OR REPLACE VIEW "{schemaname}"."viwx_04_union_all"
AS SELECT 'hrs'::text AS bron,
    a.act_id,
    a.cust_id,
    a.emp_id,
    a.org_id,
    a.prj_id,
    a.datum,
    a.uren,
    NULL::double precision AS hours,
    NULL::double precision AS fte,
    NULL::double precision AS uren_taakplan,
    NULL::double precision AS uren_aangevraagd,
    NULL::double precision AS uren_gealloceerd,
    NULL::double precision AS uren_ingepland,
    b.skill::character varying(50) AS skill,
    a.datum AS fromdate,
    a.datum AS todate,
    NULL::integer AS can_allocate,
    NULL::integer AS status,
    NULL::integer AS status_alloc
   FROM "{schemaname}"."vw_tableau_hrs" a
     LEFT JOIN "{schemaname}"."viwx_01_emp_skill" b ON a.emp_id = b.emp_id
  WHERE a.uren <> 0::double precision
UNION ALL
 SELECT 'emp_contract'::text AS bron,
    NULL::integer AS act_id,
    NULL::integer AS cust_id,
    a.emp_id,
    b.org_id,
    NULL::integer AS prj_id,
    a.datum,
    NULL::double precision AS uren,
    a.hours,
    a.fte,
    NULL::double precision AS uren_taakplan,
    NULL::double precision AS uren_aangevraagd,
    NULL::double precision AS uren_gealloceerd,
    NULL::double precision AS uren_ingepland,
    c.skill::character varying(50) AS skill,
    a.datum AS fromdate,
    a.datum AS todate,
    NULL::integer AS can_allocate,
    NULL::integer AS status,
    NULL::integer AS status_alloc
   FROM "{schemaname}"."viwx_02_contract_per_week" a
     JOIN ( SELECT "{schemaname}"."vw_tableau_hrs".emp_id,
            min("{schemaname}"."vw_tableau_hrs".org_id) AS org_id
           FROM "{schemaname}"."vw_tableau_hrs"
          GROUP BY "{schemaname}"."vw_tableau_hrs".emp_id) b ON a.emp_id = b.emp_id
     LEFT JOIN "{schemaname}"."viwx_01_emp_skill" c ON a.emp_id = c.emp_id
UNION ALL
 SELECT a.bron,
    a.act_id,
    a.cust_id,
    a.emp_id,
        CASE
            WHEN a.org_id IS NULL THEN b.org_id
            ELSE a.org_id
        END AS org_id,
    a.prj_id,
    a.datum,
    NULL::double precision AS uren,
    NULL::double precision AS hours,
    NULL::double precision AS fte,
    a.uren_taakplan,
    a.uren_aangevraagd,
    a.uren_gealloceerd,
    a.uren_ingepland,
    a.skill,
    a.fromdate,
    a.todate,
    a.can_allocate,
    a.status,
    a.status_alloc
   FROM "{schemaname}"."viwx_03_plan_union" a
     LEFT JOIN ( SELECT "{schemaname}"."vw_tableau_hrs".emp_id,
            min("{schemaname}"."vw_tableau_hrs".org_id) AS org_id
           FROM "{schemaname}"."vw_tableau_hrs"
          GROUP BY "{schemaname}"."vw_tableau_hrs".emp_id) b ON a.emp_id = b.emp_id;

CREATE OR REPLACE VIEW "{schemaname}"."viwx_timetell_ivdi"
AS SELECT hrs.bron,
    hrs.datum,
        CASE
            WHEN hrs.datum > CURRENT_DATE THEN 'Toekomst'::text
            ELSE 'Historie'::text
        END AS toekomst_of_historie,
    hrs.uren,
    act.activiteit,
    cust.opdrachtgever,
    emp.mdw_achternaam,
    emp.mdw_voornaam,
    emp.mdw_tussenvoegsels,
    emp.mdw_nummer,
    emp.mdw_categorie,
    org.organisatie,
    prj.project_nummer,
        CASE
            WHEN hrs.bron = 'emp_contract'::text THEN '_Normuren'::character varying
            ELSE prj.project
        END AS project,
    prj.project_type,
    prj.project_id,
    parent.project_nummer AS project_nummer_bovenliggend,
    parent.project AS project_bovenliggend,
    parent.project_type AS project_type_bovenliggend,
    parent.project_id AS project_id_bovenliggend,
    prj_niv.project_niveau,
    prj_niv.project_op_niveau_0,
    prj_niv.project_op_niveau_1,
    prj_niv.project_id_op_niveau_0,
    prj_niv.project_id_op_niveau_1,
        CASE
            WHEN prj.project_type = 'A'::text AND "right"(prj.project_nummer::text, 1) = '1'::text THEN 'Regietaken'::text
            WHEN prj.project_type = 'A'::text AND "right"(prj.project_nummer::text, 1) = '2'::text THEN 'Gepland in stand houden'::text
            WHEN prj.project_type = 'A'::text AND "right"(prj.project_nummer::text, 1) = '3'::text THEN 'Onvoorzien in stand houden'::text
            WHEN prj.project_type = 'A'::text AND "right"(prj.project_nummer::text, 1) = '4'::text THEN 'Changes'::text
            WHEN prj.project_type = 'A'::text AND "right"(prj.project_nummer::text, 1) = '5'::text THEN 'LC'::text
            WHEN prj.project_type = 'P'::text AND "right"(prj.project_nummer::text, 1) = '1'::text THEN 'Intake'::text
            WHEN prj.project_type = 'P'::text AND "right"(prj.project_nummer::text, 1) = '2'::text THEN 'Verkenning'::text
            WHEN prj.project_type = 'P'::text AND "right"(prj.project_nummer::text, 1) = '3'::text THEN 'Definitie'::text
            WHEN prj.project_type = 'P'::text AND "right"(prj.project_nummer::text, 1) = '4'::text THEN 'Realisatie/Implementatie/Nazorg'::text
            WHEN prj.project_type = 'P'::text AND "right"(prj.project_nummer::text, 1) = '5'::text THEN 'Agile'::text
            WHEN hrs.bron = 'emp_contract'::text THEN 'Normuren'::text
            ELSE '?'::text
        END AS app_prj_detail,
    hrs.fte AS contract_fte,
    hrs.hours AS contract_uren,
    CURRENT_TIMESTAMP AS datum_tijd_verversen_extract,
    hrs.uren_taakplan,
    hrs.uren_aangevraagd,
    hrs.uren_gealloceerd,
    hrs.uren_ingepland,
    hrs.skill,
    hrs.fromdate,
    hrs.todate,
    hrs.can_allocate,
    hrs.status,
    hrs.status_alloc
   FROM "{schemaname}"."viwx_04_union_all" hrs
     LEFT JOIN "{schemaname}"."vw_tableau_act" act ON hrs.act_id = act.act_id
     LEFT JOIN "{schemaname}"."vw_tableau_cust" cust ON hrs.cust_id = cust.cust_id
     LEFT JOIN "{schemaname}"."viw_tableau_emp" emp ON hrs.emp_id = emp.emp_id
     LEFT JOIN "{schemaname}"."vw_tableau_org" org ON hrs.org_id = org.org_id
     LEFT JOIN "{schemaname}"."vw_tableau_prj" prj ON hrs.prj_id = prj.prj_id
     LEFT JOIN "{schemaname}"."vw_tableau_sys_prj_niv" prj_niv ON hrs.prj_id = prj_niv.prj_id
     LEFT JOIN "{schemaname}"."vw_tableau_prj" parent ON prj.parent_id = parent.prj_id;