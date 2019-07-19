
create_app_public_views = """
DROP VIEW IF EXISTS "app_public"."employees";
CREATE VIEW "app_public"."employees" AS
SELECT
       emp_id,
       bool(nonactive) as nonactive,
       (regexp_matches(firstname, '\((.*)\)', 'g'))[1] as initials,
       TRIM(LEFT(firstname, position('(' in firstname) - 1)) as firstname,
       middlename, lastname,
       sex,
       loginname,
       -- remove duplicate phone numbers and clean
       REPLACE(
       CASE
         WHEN mobile1 != '' AND mobile1!='06 5117 8424' THEN REPLACE(mobile1, ' ', '')
         ELSE REPLACE(mobile2, ' ','')
       END, '-', '') AS phone,
       CASE
         WHEN email1 = email2 AND RIGHT(email1, 12) != 'amsterdam.nl'
         THEN NULL
         WHEN email2 is null AND RIGHT(email1, 12) != 'amsterdam.nl' THEN null
         ELSE email1
       END AS email1,
       CASE
         WHEN email1 != '' AND RIGHT(email1, 12) != 'amsterdam.nl' AND email2 = ''
         THEN email1
         WHEN RIGHT(email1, 12) != 'amsterdam.nl'
         THEN null
         ELSE email2
       END AS email2,
       nr, code
  FROM public."EMP"
  """
