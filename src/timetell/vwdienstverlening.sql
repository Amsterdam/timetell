---------------------------------------
-- View: public.viw_tableau_datum
--
-- DROP VIEW public.viw_tableau_datum;
-- views tbv timetell_dienstverlening
---------------------------------------

CREATE OR REPLACE VIEW public.viw_tableau_datum AS
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
-- View: public.viw_tableau_emp
-- DROP VIEW public.viw_tableau_emp;
---------------------------------------------

CREATE OR REPLACE VIEW public.viw_tableau_emp AS
 SELECT "EMP".emp_id,
    "EMP".nr AS mdw_nummer,
    "EMP".lastname AS mdw_achternaam,
    "EMP".middlename AS mdw_tussenvoegsels,
    "EMP".firstname AS mdw_voornaam,
    "EMP".empcat AS mdw_categorie
   FROM "EMP";

----------------------------------------------------------
-- View: public.viw_tableau_normuren_per_werkdag
-- DROP VIEW public.viw_tableau_normuren_per_werkdag;
----------------------------------------------------------

CREATE OR REPLACE VIEW public.viw_tableau_normuren_per_werkdag AS
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
-- View: public.viw_tableau_hrs_union_all_normuren
-- DROP VIEW public.viw_tableau_hrs_union_all_normuren;
--------------------------------------------------------------

CREATE OR REPLACE VIEW public.viw_tableau_hrs_union_all_normuren AS
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
-- View: public.viw_timetell_ivdi

-- DROP VIEW public.viw_timetell_ivdi;

CREATE OR REPLACE VIEW public.viw_timetell_ivdi AS
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
