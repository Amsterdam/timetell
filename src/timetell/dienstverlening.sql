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
   FROM "EMP";

----------------------------------------------------------
-- View: "{schemaname}".viw_tableau_normuren_per_werkdag
-- DROP VIEW "{schemaname}".viw_tableau_normuren_per_werkdag;
----------------------------------------------------------

CREATE OR REPLACE VIEW "{schemaname}"."viw_tableau_normuren_per_werkdag" AS
 WITH cte AS (
         SELECT date_part('year'::text, viw_tableau_datum.datum) AS jaar,
            count(*) AS aantal_werkdagen_in_jaar
           FROM viw_tableau_datum
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
   FROM "EMP_CONTRACT" a
     JOIN viw_tableau_datum b ON b.datum >= a.fromdate AND b.datum <= a.todate
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
   FROM vw_tableau_hrs
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
   FROM viw_tableau_normuren_per_werkdag a,
    ( SELECT vw_tableau_hrs.emp_id,
            min(vw_tableau_hrs.org_id) AS org_id
           FROM vw_tableau_hrs
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
   FROM viw_tableau_hrs_union_all_normuren hrs
     LEFT JOIN vw_tableau_act act ON hrs.act_id = act.act_id
     LEFT JOIN vw_tableau_cust cust ON hrs.cust_id = cust.cust_id
     LEFT JOIN viw_tableau_emp emp ON hrs.emp_id = emp.emp_id
     LEFT JOIN vw_tableau_org org ON hrs.org_id = org.org_id
     LEFT JOIN vw_tableau_prj prj ON hrs.prj_id = prj.prj_id
     LEFT JOIN vw_tableau_sys_prj_niv prj_niv ON hrs.prj_id = prj_niv.prj_id
     LEFT JOIN vw_tableau_prj parent ON prj.parent_id = parent.prj_id;
