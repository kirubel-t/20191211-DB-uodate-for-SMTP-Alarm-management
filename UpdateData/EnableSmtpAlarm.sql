UPDATE ALARM.R_ALARM
SET R_SpecificProblem = 'CemTore Service Failure'
WHERE R_AlarmId = 100;

DELETE FROM alarm.R_ALARMFORWARD WHERE R_AlarmID = 100;

INSERT INTO ALARM.R_AlarmForward( R_AlarmID, R_SendMail, R_MailAddress, R_SendSnmp, R_snmpServer, R_SendSms, R_SmsAddress, R_SendSysLog, R_SysLogServer, R_DISPLAYINDASHBOARD )
VALUES (100, 'Yes', 'kirubel.teklemedhin@aexonis.com', 'No', NULL, 'No', NULL, 'No', NULL, 'No' );
