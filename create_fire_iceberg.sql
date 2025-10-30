-- Configure a local Hadoop catalog rooted at a repo-local path (relative)
SET spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog;
SET spark.sql.catalog.local.type=hadoop;
SET spark.sql.catalog.local.warehouse=warehouse/iceberg;

DROP TABLE IF EXISTS local.fire;

CREATE TABLE local.fire (
  employee_id INT,
  full_name STRING,
  department STRING,
  job_title STRING,
  salary DECIMAL(9,2),
  date_of_joining DATE,
  is_active BOOLEAN
)
USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.format.default'='avro');

INSERT INTO local.fire VALUES
(1,'Ava Johnson','Engineering','Software Engineer',80321.25,DATE_SUB(CURRENT_DATE, 1200), true),
(2,'Liam Smith','HR','Recruiter',61200.00,DATE_SUB(CURRENT_DATE, 2300), true),
(3,'Olivia Williams','Finance','Financial Analyst',74210.50,DATE_SUB(CURRENT_DATE, 600), true),
(4,'Noah Brown','Sales','Account Executive',55890.75,DATE_SUB(CURRENT_DATE, 3200), false),
(5,'Emma Jones','Engineering','QA Engineer',70550.00,DATE_SUB(CURRENT_DATE, 1700), true),
(6,'Elijah Garcia','Engineering','DevOps Engineer',99800.40,DATE_SUB(CURRENT_DATE, 900), true),
(7,'Sophia Miller','Finance','Senior Accountant',88210.10,DATE_SUB(CURRENT_DATE, 2500), true),
(8,'Lucas Davis','Sales','Sales Representative',49770.00,DATE_SUB(CURRENT_DATE, 450), true),
(9,'Mia Rodriguez','HR','HR Generalist',65330.33,DATE_SUB(CURRENT_DATE, 2800), true),
(10,'Mason Martinez','Engineering','Data Engineer',112330.00,DATE_SUB(CURRENT_DATE, 1500), true),
(11,'Isabella Hernandez','Finance','Controller',134500.00,DATE_SUB(CURRENT_DATE, 3300), false),
(12,'Ethan Lopez','Sales','Sales Manager',120000.00,DATE_SUB(CURRENT_DATE, 3650), true),
(13,'Amelia Gonzalez','HR','HR Manager',91500.00,DATE_SUB(CURRENT_DATE, 2900), true),
(14,'Logan Wilson','Engineering','Senior Software Engineer',127500.00,DATE_SUB(CURRENT_DATE, 2100), true),
(15,'Harper Anderson','Sales','Customer Success Manager',77320.00,DATE_SUB(CURRENT_DATE, 750), true),
(16,'James Thomas','Finance','Payroll Specialist',61500.00,DATE_SUB(CURRENT_DATE, 1400), true),
(17,'Evelyn Taylor','Engineering','Software Engineer',84500.00,DATE_SUB(CURRENT_DATE, 2000), true),
(18,'Benjamin Moore','Sales','Account Executive',69000.00,DATE_SUB(CURRENT_DATE, 120), true),
(19,'Charlotte Jackson','HR','Compensation Analyst',80250.00,DATE_SUB(CURRENT_DATE, 2700), true),
(20,'Henry Martin','Engineering','Engineering Manager',142000.00,DATE_SUB(CURRENT_DATE, 3100), true),
(21,'Ava Johnson','Engineering','Software Engineer',92310.00,DATE_SUB(CURRENT_DATE, 600), true),
(22,'Liam Smith','HR','Recruiter',60210.00,DATE_SUB(CURRENT_DATE, 2100), false),
(23,'Olivia Williams','Finance','Financial Analyst',73850.00,DATE_SUB(CURRENT_DATE, 1880), true),
(24,'Noah Brown','Sales','Account Executive',58210.00,DATE_SUB(CURRENT_DATE, 1100), true),
(25,'Emma Jones','Engineering','QA Engineer',70500.00,DATE_SUB(CURRENT_DATE, 2600), true),
(26,'Elijah Garcia','Engineering','DevOps Engineer',100200.00,DATE_SUB(CURRENT_DATE, 2200), true),
(27,'Sophia Miller','Finance','Senior Accountant',88000.00,DATE_SUB(CURRENT_DATE, 3000), true),
(28,'Lucas Davis','Sales','Sales Representative',50500.00,DATE_SUB(CURRENT_DATE, 1700), true),
(29,'Mia Rodriguez','HR','HR Generalist',66200.00,DATE_SUB(CURRENT_DATE, 1300), true),
(30,'Mason Martinez','Engineering','Data Engineer',113500.00,DATE_SUB(CURRENT_DATE, 500), true),
(31,'Isabella Hernandez','Finance','Controller',133000.00,DATE_SUB(CURRENT_DATE, 3400), true),
(32,'Ethan Lopez','Sales','Sales Manager',119500.00,DATE_SUB(CURRENT_DATE, 2200), true),
(33,'Amelia Gonzalez','HR','HR Manager',93000.00,DATE_SUB(CURRENT_DATE, 2100), true),
(34,'Logan Wilson','Engineering','Senior Software Engineer',128750.00,DATE_SUB(CURRENT_DATE, 1900), true),
(35,'Harper Anderson','Sales','Customer Success Manager',78500.00,DATE_SUB(CURRENT_DATE, 800), true),
(36,'James Thomas','Finance','Payroll Specialist',62500.00,DATE_SUB(CURRENT_DATE, 1500), true),
(37,'Evelyn Taylor','Engineering','Software Engineer',85200.00,DATE_SUB(CURRENT_DATE, 2300), true),
(38,'Benjamin Moore','Sales','Account Executive',70500.00,DATE_SUB(CURRENT_DATE, 900), true),
(39,'Charlotte Jackson','HR','Compensation Analyst',81500.00,DATE_SUB(CURRENT_DATE, 2400), true),
(40,'Henry Martin','Engineering','Engineering Manager',145000.00,DATE_SUB(CURRENT_DATE, 3500), false),
(41,'Ava Johnson','Engineering','Software Engineer',80321.25,DATE_SUB(CURRENT_DATE, 1700), true),
(42,'Liam Smith','HR','Recruiter',61200.00,DATE_SUB(CURRENT_DATE, 1650), true),
(43,'Olivia Williams','Finance','Financial Analyst',74210.50,DATE_SUB(CURRENT_DATE, 1620), true),
(44,'Noah Brown','Sales','Account Executive',55890.75,DATE_SUB(CURRENT_DATE, 1600), false),
(45,'Emma Jones','Engineering','QA Engineer',70550.00,DATE_SUB(CURRENT_DATE, 1580), true),
(46,'Elijah Garcia','Engineering','DevOps Engineer',99800.40,DATE_SUB(CURRENT_DATE, 1550), true),
(47,'Sophia Miller','Finance','Senior Accountant',88210.10,DATE_SUB(CURRENT_DATE, 1520), true),
(48,'Lucas Davis','Sales','Sales Representative',49770.00,DATE_SUB(CURRENT_DATE, 1500), true),
(49,'Mia Rodriguez','HR','HR Generalist',65330.33,DATE_SUB(CURRENT_DATE, 1480), true),
(50,'Mason Martinez','Engineering','Data Engineer',112330.00,DATE_SUB(CURRENT_DATE, 1450), true);

-- Basic checks
SELECT * FROM local.fire LIMIT 5;
SELECT * FROM local.fire ORDER BY employee_id DESC LIMIT 5;
