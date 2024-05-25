-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ## Overview
-- MAGIC 
-- MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
-- MAGIC 
-- MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

-- COMMAND ----------

CREATE EXTERNAL TABLE clinicaltrial_2021 
(Id STRING , 
Sponsor string,
Status string, 
Start_date string,
Completion string,
Type string,
Submission string,
Conditions string,
Interventions string) 
USING CSV OPTIONS(path "/FileStore/tables/clinicaltrial_2021.csv", delimiter "|", header "true");

-- COMMAND ----------

CREATE EXTERNAL TABLE mesh
(term string,
tree string)
 USING CSV OPTIONS (path "/FileStore/tables/mesh.csv", delimiter ",", header "true");

-- COMMAND ----------

CREATE EXTERNAL TABLE pharma
(Company string,
Parent_Company string,
Penalty_Amount string,
Subtraction_From_Penalty string,
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting string,
Penalty_Year string ,
Penalty_Date string,
Offense_Group string,
Primary_Offense string,
Secondary_Offense string,
Description string,
Level_of_Government string ,
Action_Type string ,
Agency string,
Civil_Criminal string ,
Prosecution_Agreement string,
Court string,
Case_ID string ,
Private_Litigation_Case_Title string,
Lawsuit_Resolution string,
Facility_State string ,
City string,
Address string,
Zip string,
NAICS_Code string ,
NAICS_Translation string ,
HQ_Country_of_Parent string ,
HQ_State_of_Parent string ,
Ownership_Structure string,
Parent_Company_Stock_Ticker string ,
Major_Industry_of_Parent string ,
Specific_Industry_of_Parent string ,
Info_Source string ,
Notes string) USING CSV OPTIONS (path "/FileStore/tables/pharma.csv", delimiter ",", header "true")

-- COMMAND ----------

select * from `clinicaltrial_2021`

-- COMMAND ----------

select * from mesh

-- COMMAND ----------

select * from pharma

-- COMMAND ----------

--Question1
select distinct count(*) from `clinicaltrial_2021`

-- COMMAND ----------

--Question2
select Type,count(Type) as frequency
from clinicaltrial_2021
group by Type
order by count(Type) desc

-- COMMAND ----------

create table exploded_conditions as select explode(split(Conditions, ","))
as condition from clinicaltrial_2021

-- COMMAND ----------

select * from exploded_conditions

-- COMMAND ----------

--Question 3
select condition, count(*) from exploded_conditions group by condition
order by count(*) desc limit 5

-- COMMAND ----------

--Question4
create table mesh_conditions as select * from mesh inner join exploded_conditions on mesh.term = exploded_conditions.condition

-- COMMAND ----------

select left(tree,3) as root, count(*) as frequency
from mesh_conditions
group by root order by count(*) desc limit 5;

-- COMMAND ----------

--Question 5

-- COMMAND ----------

create table pharma_sponsor as select * from clinicaltrial_2021
left join pharma on clinicaltrial_2021.Sponsor = pharma.Parent_Company

-- COMMAND ----------

select Sponsor, count(*) as trials_offered from pharma_sponsor
where Parent_Company is null
group by Sponsor 
order by count(*) desc limit 10

-- COMMAND ----------

--Question 6

-- COMMAND ----------

create table trials as select Completion, Status, left(Completion, 3) as month, substring(Completion, 5, 8) as year
from clinicaltrial_2021
where Completion is not null and Status = "Completed" and substring(Completion, 5, 8) == "2021"

-- COMMAND ----------

create table monthly_completion_status as select from_unixtime(unix_timestamp(concat('01-', month, '-', year), 'dd-MMM-yyyy'), 'dd-MM-yyyy') as date, month, count(*) as Total_Completed
from trials 
group by month,year
order by substring(date, 4, 5)

-- COMMAND ----------

select * from monthly_completion_status

-- COMMAND ----------

select month, Total_Completed
from monthly_completion_status

-- COMMAND ----------


