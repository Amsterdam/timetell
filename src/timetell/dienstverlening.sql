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
DROP VIEW IF EXISTS vw_timetell_ivdi;

DROP VIEW IF EXISTS vw_tableau_act;
DROP VIEW IF EXISTS vw_tableau_cust;
DROP VIEW IF EXISTS vw_tableau_emp;
DROP VIEW IF EXISTS vw_tableau_hrs;
DROP VIEW IF EXISTS vw_tableau_org;
DROP VIEW IF EXISTS vw_tableau_prj;
DROP VIEW IF EXISTS vw_tableau_sys_prj_niv;
DROP VIEW IF EXISTS vw_tableau_hulp_week;
--------------------------------------------------------------
-- 1/x: vw_tableau_act
--------------------------------------------------------------
CREATE OR REPLACE VIEW vw_tableau_act AS
SELECT  act_id
,       name AS activiteit
FROM    "ACT";

--------------------------------------------------------------
-- 2/x: vw_tableau_cust
--------------------------------------------------------------
CREATE OR REPLACE VIEW vw_tableau_cust AS
SELECT  cust_id
,       name AS opdrachtgever
FROM    "CUST";

--------------------------------------------------------------
-- 3/x: vw_tableau_emp
--------------------------------------------------------------
CREATE OR REPLACE VIEW vw_tableau_emp AS
SELECT  emp_id
,       lastname AS mdw_achternaam
,       middlename AS mdw_tussenvoegsels
,       firstname AS mdw_voornaam
FROM    "EMP";

--------------------------------------------------------------
-- 4/x: vw_tableau_hrs
--------------------------------------------------------------
CREATE OR REPLACE VIEW vw_tableau_hrs AS
SELECT  act_id
,       cust_id
,       emp_id
,       org_id
,       prj_id
,       date AS datum
,       hours AS uren
FROM    "HRS";

--------------------------------------------------------------
-- 5/x: vw_tableau_org
--------------------------------------------------------------
CREATE OR REPLACE VIEW vw_tableau_org AS
SELECT  org_id
,       name AS organisatie
FROM    "ORG";

--------------------------------------------------------------
-- 6/x: vw_tableau_prj
--------------------------------------------------------------
CREATE OR REPLACE VIEW vw_tableau_prj AS
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
FROM    "PRJ";

--------------------------------------------------------------
-- 7/x: vw_tableau_sys_prj_niv
--------------------------------------------------------------
--DROP VIEW vw_tableau_sys_prj_niv;
CREATE VIEW vw_tableau_sys_prj_niv AS
SELECT  niv_id AS prj_id
,       niv::VARCHAR(20) AS project_niveau
,       niv0_name AS project_op_niveau_0
,       niv1_name AS project_op_niveau_1
,       niv0_id::VARCHAR(20) AS project_id_op_niveau_0
,       niv1_id::VARCHAR(20) AS project_id_op_niveau_1
FROM    "SYS_PRJ_NIV";

--------------------------------------------------------------
-- 8/x: vw_tableau_sys_prj_niv
--------------------------------------------------------------
CREATE OR REPLACE VIEW vw_tableau_hulp_week AS
SELECT extract(year FROM date) jaar, extract(week FROM date) weeknummer, Max(date) datum
FROM "HRS"
GROUP BY extract(year FROM date), extract(week FROM date);

--------------------------------------------------------------
-- Combine x views into just one
--------------------------------------------------------------

CREATE OR REPLACE VIEW vw_timetell_ivdi AS
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
FROM    vw_tableau_hrs AS hrs       -- we start with the facts
LEFT JOIN vw_tableau_act AS act     -- and then, all dimensions
ON      hrs.act_id = act.act_id
LEFT JOIN vw_tableau_cust AS cust
ON      hrs.cust_id = cust.cust_id
LEFT JOIN vw_tableau_emp AS emp
ON      hrs.emp_id = emp.emp_id
LEFT JOIN vw_tableau_org AS org
ON      hrs.org_id = org.org_id
LEFT JOIN vw_tableau_prj AS prj
ON      hrs.prj_id = prj.prj_id
LEFT JOIN vw_tableau_sys_prj_niv prj_niv
ON      hrs.prj_id = prj_niv.prj_id
LEFT JOIN vw_tableau_prj AS parent
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
FROM    "EMP_CONTRACT" a
JOIN    vw_tableau_hulp_week b
ON      b.datum >= fromdate
AND     b.datum <= todate
LEFT JOIN vw_tableau_emp c
ON      a.emp_id = c.emp_id
