# 20191211-DB-update-for-SMTP-Alarm-management


This Delivery Consists :
•	Install_DbCemtoreUpdate.sh script that you should run in order to get the things installed. 
This script is very closed to the one used to install a fresh version of CemTore. 
I tried to keep it generic in order to use the same script for all CemTore future deliveries / updates.
This script will install all the SQL files delivered in this update


•	Alarm/Pkg/pg_Alarm_Mgt is some equivalent of Oracle package (contains a set of PostgreSQL Functions / Procedures) 
and is delivered because some fix was required for this version. Usually, you may find one folder per database schema 
that required changes (ex: Easyshare / Cemtore / AqsaTools).


•	UpdateData folder will always contain all the scripts that modify the configuration data and may ensure migration 
from one version to another version. In this specific case, there is only one script EnableSmtpAlarm.sql. 
In this script (which was used in Lab), you will see that R_MailAddress column of ALARM.R_AlarmForward table 
contains “name.name@xxx.com”. You can change this value to any valid email 
address (don’t know if support@xxx.com exists for example).


•	Version folder will always contain a script that builds the list of versions and dumps it to Package_Version.log.



