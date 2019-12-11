  /*****************************************************************************/
  /**  FUNCTION LIST                                                          **/
  /*===========================================================================*/
  /**  Adm_Alarm             :                                                **/
  /**  ALARM MANAGEMENT                                                       **/
  /*---------------------------------------------------------------------------*/
  /**  Audit_Alarm           :                                                **/
  /**  AUDIT ACTIVE ALARMS                                                    **/
  /*---------------------------------------------------------------------------*/
  /**  Backup_Log            :                                                **/
  /**  SAVE/ EXPORT ALARM LOG                                                 **/
  /*---------------------------------------------------------------------------*/
  /**  Deal_AlarmMsg         :                                                **/
  /**  ACTIVATION / DEACTIVATION OF ALARMS LINKED TO AN ALARM MESSAGE         **/
  /*---------------------------------------------------------------------------*/
  /**  Deal_PostAlarm        :                                                **/
  /**  EXECUTION OF POST-PROCEDURE RELATED TO ALARM MESSAGE                   **/
  /*---------------------------------------------------------------------------*/
  /**  Forward_Alarm         :                                                **/
  /**  FORWARD ALARM MESSAGE TO EXTERNAL ALARM MANAGEMENT TOOL                **/
  /*---------------------------------------------------------------------------*/
  /**  Get_LinkedObj         :                                                **/
  /**  GET OBJECTS LINKED TO THE PROVIDED ONE (PARENTS OR CHILDREN)           **/
  /*---------------------------------------------------------------------------*/
  /**  L_AlarmPurge          :                                                **/
  /**  ALARM LOG PURGE                                                        **/
  /*---------------------------------------------------------------------------*/
  /**  List_DestAlarm        :                                                **/
  /**  PREPARE THE ALARMS RECIPIENTS LIST                                     **/
  /*---------------------------------------------------------------------------*/
  /**  GetAlarmType          :                                                **/
  /**  RETURNS ALARM'S CODE RELATIVE TO ALARM'S CATEGORY                      **/
  /*---------------------------------------------------------------------------*/
  /**  Msg_RefreshAlarm      :                                                **/
  /**  CALLS MSG_REFRESH FUNCTION IF NOT IN HTTP/REST MODE                    **/
  /*----------------------------------------------+----------------------------*/
  /* PROGRAMMER: PFI - PPE                        | AQSACOM Copyrights 2019    */
  /*----------------------------------------------+----------------------------*/
  /* DATE: 28/11/2019                             | VERSION : 8.00.02          */
  /*----------------------------------------------+----------------------------*/
  /*                                                                           */
  /* DESCRIPTION : This package is used to management of Alarm.                */
  /*                                                                           */
  /* CREATION - 8.00.00 - PPE - 18/07/2018 - EGR232-01                         */
  /* This Package is based on the Oracle Alarm.pMgt package 8.06.10            */
  /*                                                                           */
  /* CHANGES PROVIDED BY VERSION 8.00.01 - PFI - 30/08/2019                    */
  /* CemTore IoT - EGR238-02                                                   */
  /* 1) Fix package by changing LENGTHB to LENGTH                              */
  /*                                                                           */
  /* CHANGES PROVIDED BY VERSION 8.00.02 - PFI - 28/11/2019                    */
  /* CemTore IoT - EGR238-02                                                   */
  /* 1) Fix package porting issues -> Remove .pString from several calls...    */
  /*                                                                           */
  /*****************************************************************************/

 SELECT  AQSATOOLS.StoreProcVersion ( 'ALARM', 'pg_Alarm_Mgt', '8.00.02' );
                           
/*
  ===========================================================================
  -- ALARM MANAGEMENT 
  ===========================================================================
*/

DROP FUNCTION IF EXISTS ALARM.Adm_Alarm;
CREATE FUNCTION ALARM.Adm_Alarm()
RETURNS INT4 AS $$

  DECLARE
    cn_admRefresh        int4 := 103;

    c_msgParam           VARCHAR(1000);
    c_userClass          VARCHAR(5);
    c_userId             VARCHAR(50);
    c_objClass           VARCHAR(5);
    c_objOwnerClass      VARCHAR(5);
    c_string             VARCHAR(1000);
    c_tag                VARCHAR(4);
    c_viewAllAlarmsClass VARCHAR(10);
    c_EventDate          VARCHAR(30);
    c_ErrAlarmSent       VARCHAR(128);
    c_msgAlarmLog        VARCHAR(1000);
    c_AlarmOperation     VARCHAR(30);
    c_postOperation      VARCHAR(512);
    c_pkgVersion         VARCHAR(10);
    c_alarmRemark        VARCHAR(512);
    c_optionGrouped      VARCHAR(10);
    c_dispInDashboard    VARCHAR(10);
    c_ParamMail          VARCHAR(255);
    c_ParamSNMP          VARCHAR(255);
    c_ParamSMS           VARCHAR(255);
    c_ParamSyslog        VARCHAR(255);
    c_probableCause      VARCHAR(255);
    c_sendMail           VARCHAR(10);
    c_sendSNMP           VARCHAR(10);
    c_sendSMS            VARCHAR(10);
    c_sendSyslog         VARCHAR(10);
    c_severity           VARCHAR(30);
    c_severityList       VARCHAR(255);
    c_specificProblem    VARCHAR(255);
    c_alarmTech          VARCHAR(60);
    c_AlarmObject        VARCHAR(255);
    c_AlarmDest          VARCHAR(255);
    c_exception          VARCHAR(1000);
    cc_class_Alarm       VARCHAR(4);
    cc_class_AlarmDef    VARCHAR(4);
    cc_separator         VARCHAR(4);
    n_alarmType          INT4;
    n_alarmRef           INT4;
    n_alarmGroupRef      INT4;
    n_alarmMsg           INT4;
    n_indexEnd           INT4;
    n_logRecId           INT4;
    n_logWindowId        INT4;
    n_msgActivity        INT4;
    n_msgAlarm           INT4;
    n_msgEventId         INT4;
    n_nbAdded            INT4;
    n_nbDeleted          INT4;
    n_nbUpdated          INT4;
    n_objRef             INT4;
    n_objOwnerRef        INT4;
    n_newSeverity        INT4;
    n_oldSeverity        INT4;
    n_perceivedSeverity  INT4;
    n_returnCode         INT4;
    n_rowcount           INT4;
    n_sendAck            INT4;
    n_userRef            INT4;
    n_temp               INT4;
    n_userGroupRef       INT4;
    n_userProfileFound   INT4;
    n_msgOperationCode   INT4;
    n_msgCompType        INT4;
    n_msgUserRef         INT4;
    n_comAlarmRef        INT4;
    n_alarmLatency       INT4;
    n_alarmThreshold     INT4;
    n_defaultSeverity    INT4;
    n_return             INT4;
    cn_admAlarmRef       INT4;
    cn_separLength       INT4;

    ts_begin             TIMESTAMP;
    ts_end               TIMESTAMP;
    --ids_duration         INTERVAL DAY TO SECOND;
    n_durationMs         INT8;
    AlarmMsg             RECORD;
    AlarmMsgResp         RECORD;
    Alarm                RECORD;
    UserProf             RECORD;
    UserGroup            RECORD;

 BEGIN
    c_pkgVersion := AQSATOOLS.GetPkgVersion ('ALARM', 'pg_Alarm_Mgt');
    ts_begin := clock_timestamp();
    n_temp := 0;

    -------------------------------------------------------------------
    -- Initialize some data
    -------------------------------------------------------------------
    cn_admAlarmRef := 121;
    cn_separLength := 2;
    cc_class_Alarm := '36';
    cc_class_AlarmDef := '38';
    cc_separator := '~|';
    n_logWindowID := 4;
    n_alarmMsg := 0;

  BEGIN
    -------------------------------------------------------------------
    -- Get the List of User Classes which should be able to get all alarms
    -------------------------------------------------------------------
    BEGIN
      SELECT Param_VALUE INTO STRICT c_viewAllAlarmsClass
      FROM EASYSHARE.D_ParamETER 
      WHERE OBJ_REF = cn_admAlarmRef
        AND Param_TAG = '35';
    EXCEPTION
       WHEN NO_DATA_FOUND THEN NULL;
    END;
    
    ------------------------------------------------------------------------
    -- Check Generic states of ADM_ALARM module
    ------------------------------------------------------------------------
  
    IF NOT EXISTS ( SELECT OBJ_REF 
                    FROM EASYSHARE.D_OBJECT 
                    WHERE OBJ_REF = cn_admAlarmRef
                      AND OBJ_STATES LIKE '22%'
                      AND OBJ_CLASS_TAG = '5A' )
    THEN
      RAISE NOTICE ' Module LOCK';
      RETURN -1;
    END IF;
    
    -------------------------------------------------------------------------------
    -- Backup Active Alarms for Refresh management at the end of the procedure
    -------------------------------------------------------------------------------
    CREATE TEMPORARY TABLE IF NOT EXISTS tt_Mgt_DAlarm
    (
      D_AlarmID           INT4         NOT NULL,
      D_RAlarmID          INT4         NOT NULL,
      D_PerceivedSeverity INT4         NOT NULL,
      D_ObjRef            INT4         NOT NULL,
      D_ObjID             VARCHAR(255) NOT NULL,
      D_ObjOwnerRef       INT4         NOT NULL,
      D_ObjClassTag       VARCHAR(5)   NOT NULL,
      D_SpecificProblem   VARCHAR(255) NOT NULL,
      L_LogRecId          INT4         NULL 
    );

    DELETE FROM tt_Mgt_DAlarm;
    INSERT INTO tt_Mgt_DAlarm( D_AlarmID, D_RAlarmID, D_PerceivedSeverity, D_ObjRef, D_ObjID, D_ObjOwnerRef, D_ObjClassTag, D_SpecificProblem )
      SELECT D_AlarmID, D_RAlarmID, D_PerceivedSeverity, D_ObjRef, D_ObjID, D_ObjOwnerRef, D_ObjClassTag, D_SpecificProblem 
      FROM ALARM.D_ALARM;

    -------------------------------------------------------------------------------
    -- Deal D_FLUX messages in destination of ADM_ALARM
    -------------------------------------------------------------------------------
    <<AuditSeverityLevel>>
    BEGIN
      CREATE TEMPORARY TABLE IF NOT EXISTS tt_Mgt_AdmAlarmMsg
      (
        LIKE EASYSHARE.D_FLUX
        INCLUDING DEFAULTS
      );
      DELETE FROM tt_Mgt_AdmAlarmMsg;

      INSERT INTO tt_Mgt_AdmAlarmMsg( AppFrom, AppTo, EventId, Status, TransactionType, ComponentType, OperationCode, InvokeId, Param, ExpirationDate )
        SELECT AppFrom, AppTo, EventId, Status, TransactionType, ComponentType, OperationCode, InvokeId, Param, ExpirationDate 
        FROM EASYSHARE.D_FLUX 
        WHERE AppTo = cn_admAlarmRef
          -- AND ComponentType = TO_NUMBER( '91', 'xx' )
          AND Status >= 0
          AND RoutingModule = 0
          AND ExpirationDate >= NOW()
        ORDER BY EventId;
          
      -- If no message is to be processed, just audit the already existing alarms
      IF ( NOT FOUND ) 
      THEN
        EXIT AuditSeverityLevel;
      END IF;
    
      -- Process alarms messages
      FOR AlarmMsg IN 
        ( SELECT AppFrom, EventId, OperationCode, Param
          FROM tt_Mgt_AdmAlarmMsg
          WHERE ComponentType = x'91'::int4
          ORDER BY EventId )
      LOOP
        BEGIN 
          n_sendAck := 0;
          n_msgUserRef := AlarmMsg.AppFrom;
          n_msgEventId := AlarmMsg.EventId;
          n_msgOperationCode := AlarmMsg.OperationCode;
          c_msgParam := AlarmMsg.Param;
      
          ----------------------------------------------------------------------
          -- Get Identifier and Class of the user originating the alarm message
          ----------------------------------------------------------------------
          SELECT OBJ_CLASS_TAG, OBJ_ID 
          INTO c_userClass, c_userId
          FROM EASYSHARE.D_OBJECT 
          WHERE OBJ_REF = n_msgUserRef
            AND OBJ_CLASS_TAG NOT LIKE '-%';
            
          ----------------------------------------------------------------------
          -- Degroup the Param field
          ----------------------------------------------------------------------
          n_alarmRef := 0;
          n_msgAlarm := 0;
          n_msgActivity := 0;
          c_objClass := EASYSHARE.GetTagValueFromList( '1C', c_msgParam );
          n_alarmRef := CAST( COALESCE(NULLIF(EASYSHARE.GetTagValueFromList( '1F', c_msgParam ), ''), '0') AS INT4 );
          n_msgAlarm := CAST( COALESCE(NULLIF(EASYSHARE.GetTagValueFromList( '3F', c_msgParam ), ''), '0') AS INT4 );
          n_msgActivity := CAST( COALESCE(NULLIF(EASYSHARE.GetTagValueFromList( '3G', c_msgParam ), ''), '0') AS INT4 );



          ----------------------------------------------
          -- Alarm Acknowledge Message
          ----------------------------------------------
          IF ( n_msgOperationCode = x'F7'::int4 ) THEN
            BEGIN
              n_alarmMsg := n_alarmMsg + 1;
              n_sendAck := 1;
            
              c_alarmRemark := EASYSHARE.GetTagValueFromList( '3X', c_msgParam );

              UPDATE ALARM.D_ALARM
              SET D_AcknowledgedDate = NOW(),
                  D_AcknowledgedBy = c_userId,
                  D_Remark = SUBSTRING( CASE 
                                          WHEN D_Remark IS NULL OR D_Remark = '' THEN c_alarmRemark
                                          WHEN c_alarmRemark IS NULL OR c_alarmRemark = '' THEN D_Remark
                                          ELSE D_Remark || CHR(10) || c_alarmRemark
                                        END, 1, 512 )
              WHERE D_AlarmID = n_alarmRef
                AND D_AcknowledgedDate IS NULL;
            
              GET DIAGNOSTICS n_rowcount = ROW_COUNT;
              IF n_rowcount <> 0 THEN
                RAISE NOTICE ' Alarm Acknowledged by User : % With Remark : %', c_userId, c_alarmRemark;
              END IF;
              
              -- Send Refresh message
              SELECT D_ObjOwnerRef INTO n_objOwnerRef
              FROM ALARM.D_ALARM 
              WHERE D_AlarmID = n_alarmRef;
              
              SELECT OBJ_CLASS_TAG INTO c_objOwnerClass
              FROM EASYSHARE.D_OBJECT 
              WHERE OBJ_REF = n_objOwnerRef
                AND OBJ_CLASS_TAG NOT LIKE '-%';
        
              PERFORM Alarm.Msg_RefreshAlarm( 0, 'FD', '36', n_alarmRef, 0, n_objOwnerRef, in_refreshOtherClass => 0 );
        
              -- Send a Refresh Alarm to Alarm parent as Color Update is possible
              SELECT T3.OBJ_REF INTO n_alarmGroupRef
              FROM ALARM.D_ALARM         T1,
                  EASYSHARE.D_OBJECT    T2,
                  EASYSHARE.D_ParamETER T3,
                  EASYSHARE.D_ParamETER T4
              WHERE T1.D_AlarmID = n_alarmRef
                AND T2.OBJ_CLASS_TAG = '35'
                AND T3.OBJ_REF = T2.OBJ_REF
                AND T3.Param_TAG = '20'
                AND T3.Param_VALUE = CAST( T1.D_PerceivedSeverity AS VARCHAR )
                AND T4.OBJ_REF = T3.OBJ_REF
                AND T4.Param_TAG = '21'
                AND STRPOS( T4.Param_VALUE, COALESCE( c_objOwnerClass, '5B' ) ) > 0;
                
              PERFORM Alarm.Msg_RefreshAlarm( 0, 'FD', '35', n_alarmGroupRef, 0, n_objOwnerRef, in_refreshOtherClass => 0 );
            END;  

          ----------------------------------------------
          -- Alarm Clear Message
          ----------------------------------------------
          ELSIF ( n_msgOperationCode = x'F8'::int4 ) THEN
            BEGIN
            
              n_alarmMsg := n_alarmMsg + 1;
              n_sendAck := 1;
            
              c_alarmRemark := EASYSHARE.GetTagValueFromList( '3X', c_msgParam );
              RAISE NOTICE ' Alarm Cleared by User : % With Remark : %', c_userId, c_alarmRemark;

              SELECT EASYSHARE.GetTagValueFromList( '21', Param_ValueDefault ) INTO c_postOperation
              FROM EASYSHARE.R_Class WHERE Class_Tag = '36' AND Param_TAG = 'F8';

              IF ( c_postOperation IS NOT NULL AND c_postOperation <> '' )
              THEN
                c_postOperation := 'SELECT ' || c_postOperation || ' ( ' || CAST(n_alarmRef AS VARCHAR) || ' )';
                RAISE NOTICE '%', c_postOperation;
                EXECUTE c_postOperation;
              END IF;

              UPDATE ALARM.D_ALARM
              SET D_ClearedDate = NOW(),
                  D_ClearedBy = c_userId,
                  D_Remark = SUBSTRING( CASE 
                                          WHEN D_Remark IS NULL THEN c_alarmRemark
                                          WHEN c_alarmRemark IS NULL THEN D_Remark
                                          ELSE D_Remark || CHR(10) || c_alarmRemark
                                        END, 1, 512 )
              WHERE D_AlarmID = n_alarmRef
                AND D_ClearedDate IS NULL;
              
            
              -- For Alarms of type 32, all the alarms of severity 0 related to this alarm have to be removed
              -- When it is an alarm that groups all the objects of the class, all the messages have to be removed
              DELETE FROM ALARM.D_Alarm
              WHERE D_PerceivedSeverity = 0
                AND EXISTS ( SELECT T1.D_AlarmID
                            FROM ALARM.D_ALARM T1,
                                  ALARM.R_ALARM T2
                            WHERE T1.D_AlarmID = n_alarmRef
                              AND T2.R_AlarmID = T1.D_RAlarmID
                              AND ( T2.R_AlarmType & 32 ) <> 0
                              AND T2.R_AlarmGrouped LIKE 'N%'
                              AND D_Alarm.D_RAlarmID = T1.D_RAlarmID
                              AND D_Alarm.D_ObjRef = T1.D_ObjRef
                              AND D_Alarm.D_PerceivedSeverity = 0 
                          );
        
              DELETE FROM ALARM.D_Alarm
              WHERE D_PerceivedSeverity = 0
                AND EXISTS ( SELECT T1.D_AlarmID
                            FROM ALARM.D_ALARM T1,
                                  ALARM.R_ALARM T2
                            WHERE T1.D_AlarmID = n_alarmRef
                              AND T2.R_AlarmID = T1.D_RAlarmID
                              AND ( T2.R_AlarmType & 32 ) <> 0
                              AND T2.R_AlarmGrouped NOT LIKE 'N%'
                              AND D_Alarm.D_RAlarmID = T1.D_RAlarmID
                              AND D_Alarm.D_PerceivedSeverity = 0 
                          );
            END;
      
          ----------------------------------------------
          -- Alarm Activation / Deactivation Message
          ----------------------------------------------
          ELSIF ( n_msgOperationCode = x'F6'::int4 ) THEN
            n_alarmMsg := n_alarmMsg + 1;
            n_return := ALARM.Deal_AlarmMsg( n_msgAlarm, n_msgActivity );

          ----------------------------------------------
          -- Alarm Activation / Deactivation Message
          ----------------------------------------------
          ELSIF ( n_msgOperationCode = x'F5'::int4 ) THEN
            n_alarmMsg := n_alarmMsg + 1;
            n_return := ALARM.Deal_AlarmMsg( n_msgAlarm, 0, c_msgParam );
      
          ----------------------------------------------
          -- Alarm Update (Remark)
          ----------------------------------------------
          ELSIF ( n_msgOperationCode = x'FD'::int4 AND c_objClass = cc_class_Alarm ) THEN
            BEGIN
              RAISE NOTICE 'Alarm Remark Update Msg : %', c_msgParam;

              c_alarmRemark := EASYSHARE.GetTagValueFromList( '3X', c_msgParam );

              UPDATE ALARM.D_ALARM
              SET D_Remark = c_alarmRemark
              WHERE D_AlarmId = n_alarmRef;

              -- Send Refresh message
              SELECT D_ObjOwnerRef INTO n_objOwnerRef
              FROM ALARM.D_ALARM 
              WHERE D_AlarmID = n_alarmRef;
              
              SELECT OBJ_CLASS_TAG INTO c_objOwnerClass
              FROM EASYSHARE.D_OBJECT 
              WHERE OBJ_REF = n_objOwnerRef
                AND OBJ_CLASS_TAG NOT LIKE '-%';
        
              n_return :=  ALARM.Msg_RefreshAlarm( 0, 'FD', '36', n_alarmRef, 0, n_objOwnerRef, in_refreshOtherClass => 0 );
              n_alarmMsg := n_alarmMsg + 1;
              n_sendAck := 1;
            END;

          ----------------------------------------------
          -- Alarm Definition Update
          ----------------------------------------------
          ELSIF ( n_msgOperationCode = x'FD'::int4 AND c_objClass = cc_class_AlarmDef ) THEN
            BEGIN
              RAISE NOTICE 'Alarm Definition Update Msg : %', c_msgParam;
      
              c_specificProblem := EASYSHARE.GetTagValueFromList( '1E', c_msgParam );
              c_probableCause := EASYSHARE.GetTagValueFromList( '28', c_msgParam );
              c_severity := EASYSHARE.GetTagValueFromList( '30', c_msgParam );
              
              n_alarmThreshold := CASE 
                                    WHEN LENGTH( EASYSHARE.GetTagValueFromList( '31', c_msgParam ) ) > 0 THEN EASYSHARE.GetTagValueFromList( '31', c_msgParam )
                                    ELSE NULL
                                  END;
              n_alarmLatency := CASE 
                                  WHEN LENGTH( EASYSHARE.GetTagValueFromList( '32', c_msgParam ) ) > 0 THEN EASYSHARE.GetTagValueFromList( '32', c_msgParam )
                                  ELSE NULL
                                END;
              c_optionGrouped := CASE 
                                  WHEN LENGTH( EASYSHARE.GetTagValueFromList( '35', c_msgParam ) ) > 0 THEN EASYSHARE.GetTagValueFromList( '35', c_msgParam )
                                  ELSE NULL
                                END;
              c_sendMail := CASE 
                              WHEN EASYSHARE.GetTagValueFromList( '50', c_msgParam ) <> '' THEN EASYSHARE.GetTagValueFromList( '50', c_msgParam )
                              WHEN STRPOS( c_msgParam, cc_separator || '50' ) > 0 THEN ' '
                              ELSE NULL
                            END;
              c_ParamMail := CASE 
                              WHEN EASYSHARE.GetTagValueFromList( '51', c_msgParam ) <> '' THEN EASYSHARE.GetTagValueFromList( '51', c_msgParam )
                              WHEN STRPOS( c_msgParam, cc_separator || '51' ) > 0 THEN ' '
                              ELSE NULL
                            END;
              c_sendSNMP := CASE 
                              WHEN EASYSHARE.GetTagValueFromList( '52', c_msgParam ) <> '' THEN EASYSHARE.GetTagValueFromList( '52', c_msgParam )
                              WHEN STRPOS( c_msgParam, cc_separator || '52' ) > 0 THEN ' '
                              ELSE NULL
                            END;
              c_ParamSNMP := CASE 
                              WHEN EASYSHARE.GetTagValueFromList( '53', c_msgParam ) <> '' THEN EASYSHARE.GetTagValueFromList( '53', c_msgParam )
                              WHEN STRPOS( c_msgParam, cc_separator || '53' ) > 0 THEN ' '
                              ELSE NULL
                            END;
              c_sendSMS := CASE 
                            WHEN EASYSHARE.GetTagValueFromList( '54', c_msgParam ) <> '' THEN EASYSHARE.GetTagValueFromList( '54', c_msgParam )
                            WHEN STRPOS( c_msgParam, cc_separator || '54' ) > 0 THEN ' '
                            ELSE NULL
                          END;
              c_ParamSMS := CASE 
                              WHEN EASYSHARE.GetTagValueFromList( '55', c_msgParam ) <> '' THEN EASYSHARE.GetTagValueFromList( '55', c_msgParam )
                              WHEN STRPOS( c_msgParam, cc_separator || '55' ) > 0 THEN ' '
                              ELSE NULL
                            END;
              c_sendSyslog := CASE 
                                WHEN EASYSHARE.GetTagValueFromList( '56', c_msgParam ) <> '' THEN EASYSHARE.GetTagValueFromList( '56', c_msgParam )
                                WHEN STRPOS( c_msgParam, cc_separator || '56' ) > 0 THEN ' '
                                ELSE NULL
                              END;
              c_ParamSyslog := CASE 
                                WHEN EASYSHARE.GetTagValueFromList( '57', c_msgParam ) <> '' THEN EASYSHARE.GetTagValueFromList( '57', c_msgParam )
                                WHEN STRPOS( c_msgParam, cc_separator || '57' ) > 0 THEN ' '
                                ELSE NULL
                              END;
              c_dispInDashboard := CASE 
                                    WHEN EASYSHARE.GetTagValueFromList( '58', c_msgParam ) <> '' THEN EASYSHARE.GetTagValueFromList( '58', c_msgParam )
                                    WHEN STRPOS( c_msgParam, cc_separator || '58' ) > 0 THEN ' '
                                    ELSE NULL
                                  END;
        
              SELECT Param_ValueList, 0 INTO c_severityList, n_defaultSeverity
              FROM EASYSHARE.R_Class 
              WHERE Class_Tag = '38'
                AND Param_Tag = '30';
        
              WHILE ( c_severityList IS NOT NULL AND LENGTH( c_severityList ) > 0 ) 
              LOOP 
                n_indexEnd := STRPOS( c_severityList, cc_separator ) - 1;
                IF ( STRPOS( c_severityList, c_severity ) = 1 ) THEN
                  EXIT;
                END IF;
                c_severityList := AQSATOOLS.STUFF( c_severityList, 1, n_indexEnd + cn_separLength, NULL );
                n_defaultSeverity := n_defaultSeverity + 1;
              END LOOP;
        
              RAISE NOTICE 'Alarm Definition Updated by User : %', c_userId;
              
              UPDATE ALARM.R_Alarm
              SET R_SpecificProblem = c_specificProblem,
                  R_ProbableCause = c_probableCause,
                  R_DefaultSeverity = n_defaultSeverity,
                  R_AlarmThreshold = COALESCE( n_alarmThreshold, R_AlarmThreshold ),
                  R_AlarmLatency = COALESCE( n_alarmLatency, R_AlarmLatency ),
                  R_AlarmGrouped = COALESCE( c_optionGrouped, R_AlarmGrouped ),
                  R_AlarmType = CASE 
                                  WHEN ( n_alarmThreshold > 1 ) THEN ( R_AlarmType | 32 )
                                  WHEN ( n_alarmThreshold = 1 ) THEN ( R_AlarmType # 32 )
                                  ELSE R_AlarmType
                                END
              WHERE R_AlarmID = n_alarmRef;
      
              UPDATE ALARM.R_AlarmForward
              SET R_SendMail = CASE 
                                WHEN ( c_sendMail IS NOT NULL AND c_sendMail <> '' ) THEN RTRIM( c_sendMail )
                                ELSE R_SendMail
                              END,
                  R_MailAddress = CASE 
                                    WHEN ( c_ParamMail IS NOT NULL AND c_ParamMail <> '' ) THEN RTRIM( c_ParamMail )
                                    ELSE R_MailAddress
                                  END,
                  R_SendSnmp =  CASE 
                                    WHEN ( c_sendSNMP IS NOT NULL AND c_sendSNMP <> '' ) THEN RTRIM( c_sendSNMP )
                                    ELSE R_SendSnmp
                                  END,
                  R_SnmpServer = CASE 
                                    WHEN ( c_ParamSNMP IS NOT NULL AND c_ParamSNMP <> '' ) THEN RTRIM( c_ParamSNMP )
                                    ELSE R_SnmpServer
                                  END,
                  R_SendSms = CASE 
                                WHEN ( c_sendSMS IS NOT NULL AND c_sendSMS <> '' ) THEN RTRIM( c_sendSMS )
                                ELSE R_SendSms
                              END,
                  R_SmsAddress = CASE 
                                  WHEN ( c_ParamSMS IS NOT NULL AND c_ParamSMS <> '' ) THEN RTRIM( c_ParamSMS )
                                  ELSE R_SmsAddress
                                END,
                  R_SendSyslog = CASE 
                                  WHEN ( c_sendSyslog IS NOT NULL AND c_sendSyslog <> '' ) THEN RTRIM( c_sendSyslog )
                                  ELSE R_SendSyslog
                                END,
                  R_SyslogServer = CASE 
                                    WHEN ( c_ParamSyslog IS NOT NULL AND c_ParamSyslog <> '' ) THEN RTRIM( c_ParamSyslog )
                                    ELSE R_SyslogServer
                                  END,
                  R_DisplayInDashboard = CASE 
                                          WHEN ( c_dispInDashboard IS NOT NULL AND c_dispInDashboard <> '' ) THEN RTRIM( c_dispInDashboard )
                                          ELSE R_DisplayInDashboard
                                        END
              WHERE R_AlarmID = n_alarmRef;
      
              PERFORM Alarm.Msg_RefreshAlarm( 0, 'FD', '38', n_alarmRef, 0, n_userRef );
              n_alarmMsg := n_alarmMsg + 1;
              n_sendAck := 1;
            END;  
      
          ----------------------------------------------
          -- Alarm Reemission Request
          ----------------------------------------------
          ELSIF ( n_msgOperationCode = x'F9'::int4 ) THEN
            FOR Alarm IN 
              ( SELECT T1.D_RAlarmID, T1.D_AlarmID, T1.D_PerceivedSeverity, T1.D_ObjOwnerRef
                FROM ALARM.D_ALARM   T1
                WHERE T1.D_PerceivedSeverity > 0 )
            LOOP 
              RAISE NOTICE ' Alarm Reemission asked by User : %', c_userId;
              n_return := ALARM.Forward_Alarm( Alarm.D_AlarmID, Alarm.D_RAlarmID, 'FF', Alarm.D_PerceivedSeverity );
            END LOOP;
          END IF;
          
          
          ------------------------------------------------------------------------------
          -- Acknowledge the D_FLUX message
          ------------------------------------------------------------------------------
          UPDATE EASYSHARE.D_FLUX
          SET Status = -2
          WHERE EventId = n_msgEventId;
          
          --------------------------------------------------
          -- Send Answer Message
          --------------------------------------------------
          IF ( n_sendAck = 1 ) THEN
            INSERT INTO EASYSHARE.D_FLUX( AppFrom, AppTo, TransactionType, ComponentType, InvokeId, OperationCode, Param, DT_SEND )
              SELECT AppTo, AppFrom, x'65'::int4, x'92'::int4, InvokeId, OperationCode,
                    '1F' || CAST( n_alarmRef AS VARCHAR ) || cc_separator, NOW() + 1 * INTERVAL '1 second'
              FROM tt_Mgt_AdmAlarmMsg T1
              WHERE EventId = n_msgEventId;
          ELSE
            DELETE FROM EASYSHARE.D_FLUX
            WHERE RoutingModule = 1
              AND AppTo = cn_admAlarmRef
              AND ComponentType = x'91'::int4
              AND Param = c_msgParam
              AND EventId < n_msgEventId;
          END IF;
        EXCEPTION
          WHEN OTHERS THEN
            DECLARE
              pg_exception    T_EXCEPTION;
              n_errorId       int4;
              n_msgObjRef     int4;
              c_msgObjClass   varchar(5);
              c_msgObjId      varchar(255);
            BEGIN
              GET STACKED DIAGNOSTICS pg_exception.ReturnedSqlState = RETURNED_SQLSTATE,
                                      pg_exception.ColumnName = COLUMN_NAME,
                                      pg_exception.ConstraintName = CONSTRAINT_NAME,
                                      pg_exception.MessageText = MESSAGE_TEXT,
                                      pg_exception.TableName = TABLE_NAME,
                                      pg_exception.SchemaName = SCHEMA_NAME,
                                      pg_exception.PgExceptionDetail = PG_EXCEPTION_DETAIL,
                                      pg_exception.PgExceptionHint = PG_EXCEPTION_HINT,
                                      pg_exception.PgExceptionContext = PG_EXCEPTION_CONTEXT;

              n_errorId := AQSATOOLS.LogError( pg_exception );

              RAISE NOTICE 'ALARM.Adm_Alarm % : Error Processing Message', c_pkgVersion;
            END;
        END;
      END LOOP;

      --------------------------------------------------
      -- Process return messages of ComAlarm
      --------------------------------------------------
      BEGIN
        SELECT OBJ_REF INTO STRICT n_comAlarmRef
        FROM EASYSHARE.D_OBJECT 
        WHERE OBJ_CLASS_TAG = '5A'
                  AND UPPER(OBJ_ID) = 'AQSA_COMALARM'
                  AND OBJ_STATES NOT LIKE '1%';
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          BEGIN
            SELECT OBJ_REF INTO STRICT n_comAlarmRef
            FROM EASYSHARE.D_OBJECT 
            WHERE OBJ_CLASS_TAG = '5A'
              AND UPPER(OBJ_ID) LIKE '%::AQSA_COMALARM'
              AND OBJ_REF > x'100000'::int4
              AND OBJ_STATES NOT LIKE '1%';
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              n_comAlarmRef := -1;
          END;
      END;

      IF ( n_comAlarmRef <> -1)
      THEN      
        BEGIN      
          FOR AlarmMsgResp IN 
            ( SELECT AppFrom, EventId, ComponentType, OperationCode, Param
              FROM tt_Mgt_AdmAlarmMsg
              WHERE ComponentType = x'92'::int4 OR ComponentType = x'93'::int4
                AND AppFrom = n_comAlarmRef
              ORDER BY EventId )
          LOOP 
            n_msgEventId  := AlarmMsgResp.EventId;
            n_msgCompType := AlarmMsgResp.ComponentType;
            n_msgOperationCode   := AlarmMsgResp.OperationCode;
            c_msgParam    := AlarmMsgResp.Param;
            c_alarmTech   := EASYSHARE.GetTagValueFromList( 'TE', c_msgParam );
            IF (n_msgOperationCode = x'FF'::int4) 
            THEN
              c_AlarmOperation := 'Add';
            ELSE
              c_AlarmOperation := 'Clear';
            END IF;
            
            IF (c_alarmTech = 'SMTP')
            THEN
              c_SpecificProblem := EASYSHARE.GetTagValueFromList( '1E', c_msgParam );
              c_AlarmObject := EASYSHARE.GetTagValueFromList( 'OB', c_msgParam );
              c_AlarmDest := EASYSHARE.GetTagValueFromList( 'TO', c_msgParam );
              c_EventDate := EASYSHARE.GetTagValueFromList( 'DT', c_msgParam );
              c_objClass  := EASYSHARE.GetTagValueFromList( 'CL', c_msgParam );
              n_objRef    := CAST( EASYSHARE.GetTagValueFromList( 'RE', c_msgParam ) AS INT4 );
              IF (n_msgCompType = x'92'::int4)
              THEN
                c_msgAlarmLog :=  '13Email type : alarm' ||
                                  ', Email recipient : ' || c_AlarmDest ||
                                  ', Timestamp : ' || c_EventDate ||
                                  ', Subject : ' || c_SpecificProblem || ' - ' || c_AlarmObject ||
                                  ', Operation : ' || c_AlarmOperation ||
                                  ', Delivery Status : delivered' || cc_separator;
              ELSIF (n_msgCompType = x'93'::int4)
              THEN
                c_ErrAlarmSent := EASYSHARE.GetTagValueFromList( 'EM', c_msgParam );
                c_msgAlarmLog :=  '13Email type : alarm' ||
                                  ', Email recipient : ' || c_AlarmDest ||
                                  ', Timestamp : ' || c_EventDate ||
                                  ', Subject : ' || c_SpecificProblem || ' - ' || c_AlarmObject ||
                                  ', Operation : ' || c_AlarmOperation ||
                                  ', Delivery Status : undelivered' ||
                                  ', Cause : ' || c_ErrAlarmSent || cc_separator;
              END IF;
              n_logRecID := EASYSHARE.L_ActivityWrite( 80121, NULL, c_objClass, n_objRef, NULL, NULL, 0, c_msgAlarmLog );
            END IF;
          END LOOP;
        END;
      END IF;
      
      -----------------------------------------------------------------
      -- Delete D_FLUX processed messages
      -----------------------------------------------------------------
      DELETE FROM EASYSHARE.D_FLUX
      USING tt_Mgt_AdmAlarmMsg T2
      WHERE D_FLUX.EventId = T2.EventId;
    
      -----------------------------------------------------------------
      -- Audit Active Alarms
      -----------------------------------------------------------------
    END; --<<AuditSeverityLevel>>
  
    n_return := ALARM.Audit_Alarm();
  
    n_nbAdded := 0;

    -------------------------------------------------------
    -- Check for Refresh of New Alarms
    -------------------------------------------------------
    CREATE TEMPORARY TABLE IF NOT EXISTS tt_Mgt_User
    (
      UserRef         INT4       NOT NULL,
      UserClass       VARCHAR(5) NULL,
      UserProfileRef  INT4       NULL
    );
    DELETE FROM tt_Mgt_User;
	
    FOR Alarm IN 
      ( SELECT T1.D_RAlarmID, T1.D_AlarmID, T1.D_PerceivedSeverity, T1.D_ObjClassTag, T1.D_ObjRef, T1.D_ObjOwnerRef
        FROM ALARM.D_ALARM   T1
        WHERE T1.D_ClearedDate IS NULL
          AND T1.D_PerceivedSeverity <> 0
          AND T1.D_AlarmID NOT IN ( SELECT D_ALarmID FROM tt_Mgt_DAlarm )
        UNION
        SELECT T1.D_RAlarmID, T1.D_AlarmID, T1.D_PerceivedSeverity, T1.D_ObjClassTag, T1.D_ObjRef, T1.D_ObjOwnerRef
        FROM ALARM.D_ALARM   T1,
             tt_Mgt_DAlarm    T2
        WHERE T1.D_ClearedDate IS NULL
          AND T1.D_PerceivedSeverity <> 0
          AND T2.D_AlarmID = T1.D_AlarmID
          AND T2.D_PerceivedSeverity = 0
      )
    LOOP
      n_nbAdded := n_nbAdded + 1;
  
      n_alarmType := Alarm.D_RAlarmID;
      n_alarmRef := Alarm.D_AlarmID;
      n_perceivedSeverity := Alarm.D_PerceivedSeverity;
      c_objClass := Alarm.D_ObjClassTag;
      n_objRef := Alarm.D_ObjRef;
      n_objOwnerRef := Alarm.D_ObjOwnerRef;


      -------------------------------------------------------------------------------------------------------------
      -- Get all the users linked to the Alarm Owner Group.
      -- Depending on the User Profile, some of them may not be authorized to see the alarms.
      -- Depending on the User Profile, alarm group may not contain same number of alarms
      -------------------------------------------------------------------------------------------------------------
      DELETE FROM tt_Mgt_User;
      INSERT INTO tt_Mgt_User( UserRef, UserClass )
        SELECT OBJ_REF, CASE WHEN OBJ_CLASS_TAG LIKE '-%' THEN SUBSTRING( OBJ_CLASS_TAG, 2, 2 ) ELSE OBJ_CLASS_TAG END 
        FROM EASYSHARE.D_OBJECT  
        WHERE ( OBJ_CLASS_TAG LIKE '5%' OR OBJ_CLASS_TAG LIKE '-5_5_' )
          AND OBJ_REF_PARENT = n_objOwnerRef
          AND OBJ_STATES = '222';
          
      UPDATE tt_Mgt_User
      SET UserProfileRef = T3.OBJ_REF_PARENT
      FROM EASYSHARE.R_Class    T2,
           EASYSHARE.D_OBJECT   T3
      WHERE T2.Class_Tag = tt_Mgt_User.UserClass
        AND T2.Param_Tag = '6A'  -- Profile Class
        AND T3.OBJ_REF = tt_Mgt_User.UserRef
        AND T3.OBJ_CLASS_TAG = '-' || tt_Mgt_User.UserClass || T2.Param_ValueDefault;

      GET DIAGNOSTICS n_rowcount = ROW_COUNT;
      IF ( n_rowcount > 0 ) THEN
        n_userProfileFound := 1;
        ---------------------------------------------------------
        -- Remove users who don't have ability to see the alarm
        -- Get Reference of Remaining user for Refresh
        ---------------------------------------------------------
        DELETE FROM tt_Mgt_User
        USING EASYSHARE.D_ParamETER  T2
        WHERE tt_Mgt_User.UserProfileRef IS NOT NULL
          AND T2.OBJ_REF = tt_Mgt_User.UserProfileRef
          AND T2.Param_TAG = c_objClass || 'FA'
          AND T2.Param_VALUE = '0';
                     
        SELECT UserRef INTO n_userRef
        FROM tt_Mgt_User
        LIMIT 1;

        ----------------------------------------------------------------------
        -- Refresh the Alarm Creation
        -- Duplicate the Refresh to all authorized users
        ----------------------------------------------------------------------
        PERFORM Alarm.Msg_RefreshAlarm( 0, 'FF', '36', n_alarmRef, 0, n_userRef, in_refreshOtherClass => 0 );

        INSERT INTO EASYSHARE.D_FLUX( AppFrom, AppTo, TransactionType, ComponentType, InvokeId, OperationCode, Param )
          SELECT T1.AppFrom, T2.UserRef, T1.TransactionType, T1.ComponentType, T1.InvokeId, T1.OperationCode, T1.Param
          FROM EASYSHARE.D_FLUX T1,
               tt_Mgt_User      T2
          WHERE T1.AppFrom = cn_admRefresh
            AND T1.AppTo = n_userRef
            AND T1.OperationCode = x'F9'::int4
            AND T1.InvokeId = n_alarmRef
            AND T1.Param LIKE '1AFF%'
            AND T2.UserRef <> T1.AppTo;
      ELSE
        n_userProfileFound := 0;

        ----------------------------------------------------------------------
        -- Refresh the Alarm Creation
        ----------------------------------------------------------------------
        PERFORM Alarm.Msg_RefreshAlarm( 0, 'FF', '36', n_alarmRef, 0, n_objOwnerRef, in_refreshOtherClass => 0 );
      END IF;

      -----------------------------------------------------------------   
      -- Redirect the Alarm creation towards the External alarm system
      -----------------------------------------------------------------   
      n_return := ALARM.Forward_Alarm( n_alarmRef, n_alarmType, 'FF', n_perceivedSeverity );

      -----------------------------------------------------------------   
      -- Send Alarm POPUP
      -----------------------------------------------------------------   
      n_return := ALARM.Msg_NewAlarm( n_alarmRef );
 
      ---------------------------------------------------------------------------------
      -- In case of User Profiles, Alarm group has to be refreshed per User Profile
      ---------------------------------------------------------------------------------
      SELECT OBJ_CLASS_TAG INTO c_objOwnerClass
      FROM EASYSHARE.D_OBJECT 
      WHERE OBJ_REF = n_objOwnerRef
        AND OBJ_CLASS_TAG NOT LIKE '-%';
        
      SELECT T2.OBJ_REF INTO n_alarmGroupRef
      FROM EASYSHARE.D_OBJECT    T1,
           EASYSHARE.D_ParamETER T2,
           EASYSHARE.D_ParamETER T3
      WHERE T1.OBJ_CLASS_TAG = '35'
        AND T2.OBJ_REF = T1.OBJ_REF
        AND T2.Param_TAG = '20'
        AND T2.Param_VALUE = CAST( n_perceivedSeverity AS VARCHAR )
        AND T3.OBJ_REF = T2.OBJ_REF
        AND T3.Param_TAG = '21'
        AND STRPOS( T3.Param_VALUE, COALESCE( c_objOwnerClass, '5B' ) ) > 0;
  
      IF ( n_userProfileFound = 0 ) THEN
        PERFORM Alarm.Msg_RefreshAlarm( 0, 'FD', '35', n_alarmGroupRef, 0, n_objOwnerRef, in_refreshOtherClass => 0 );
      ELSE
        FOR UserProf IN ( SELECT UserProfileRef, MIN(UserRef) AS UserRef FROM tt_Mgt_User GROUP BY UserProfileRef )
        LOOP
          -- Only send Refresh message for one User
          PERFORM Alarm.Msg_RefreshAlarm( 0, 'FD', '35', n_alarmGroupRef, 0, UserProf.UserRef, in_refreshOtherClass => 0 );
          
          -- Duplicate the message for each User with the same Profile
          INSERT INTO EASYSHARE.D_FLUX( AppFrom, AppTo, TransactionType, ComponentType, InvokeId, OperationCode, Param )
            SELECT T1.AppFrom, T2.UserRef, T1.TransactionType, T1.ComponentType, T1.InvokeId, T1.OperationCode, T1.Param
            FROM EASYSHARE.D_FLUX T1,
                 tt_Mgt_User      T2
            WHERE T1.AppFrom = cn_admRefresh
              AND T1.AppTo = UserProf.UserRef
              AND T1.OperationCode = x'F9'::int4
              AND T1.InvokeId = n_alarmGroupRef
              AND T1.Param LIKE '1AFD%'
              AND COALESCE( T2.UserProfileRef, -1 ) = COALESCE( UserProf.UserProfileRef, -1 )
              AND T2.UserRef <> T1.AppTo;
        END LOOP;
      END IF;

      -- Execute Post-Operation
      n_return := ALARM.Deal_PostAlarm( n_alarmType, 0, n_perceivedSeverity, n_objRef, c_objClass );
    END LOOP;

    -------------------------------------------------------
    -- Check for Refresh of Deleted Alarms
    -------------------------------------------------------
    n_nbDeleted := 0;
    FOR Alarm IN 
      ( SELECT T1.D_RAlarmID, T1.D_AlarmID, T1.D_PerceivedSeverity, T1.D_ObjClassTag, T1.D_ObjRef, T1.D_ObjOwnerRef
        FROM tt_Mgt_DAlarm     T1,
             ALARM.R_ALARM    T2
       WHERE T1.D_PerceivedSeverity <> 0
         AND T1.D_AlarmID NOT IN ( SELECT D_ALarmID FROM ALARM.D_ALARM )
         AND T2.R_AlarmID = T1.D_RAlarmID
       UNION
       SELECT T1.D_RAlarmID, T1.D_AlarmID, T1.D_PerceivedSeverity, T1.D_ObjClassTag, T1.D_ObjRef, T1.D_ObjOwnerRef
       FROM tt_Mgt_DAlarm     T1,
            ALARM.D_ALARM    T2,
            ALARM.R_ALARM    T3
       WHERE T1.D_PerceivedSeverity <> 0
         AND T2.D_AlarmID = T1.D_AlarmID
         AND T2.D_PerceivedSeverity = 0
         AND T3.R_AlarmID = T1.D_RAlarmID
     )
    LOOP 
      n_nbDeleted := n_nbDeleted + 1;

      n_alarmType := Alarm.D_RAlarmID;
      n_alarmRef := Alarm.D_AlarmID;
      n_perceivedSeverity := Alarm.D_PerceivedSeverity;
      c_objClass := Alarm.D_ObjClassTag;
      n_objRef := Alarm.D_ObjRef;
      n_objOwnerRef := Alarm.D_ObjOwnerRef;

      SELECT L_LogRecId INTO n_logRecId
      FROM tt_Mgt_DAlarm 
      WHERE D_AlarmID = n_alarmRef;

      -------------------------------------------------------------------------------------------------------------
      -- Get all the users linked to the Alarm Owner Group.
      -- Depending on the User Profile, some of them may not be authorized to see the alarms.
      -- Depending on the User Profile, alarm group may not contain same number of alarms
      -------------------------------------------------------------------------------------------------------------
      DELETE FROM tt_Mgt_User;
      INSERT INTO tt_Mgt_User( UserRef, UserClass )
        SELECT OBJ_REF, CASE WHEN OBJ_CLASS_TAG LIKE '-%' THEN SUBSTRING( OBJ_CLASS_TAG, 2, 2 ) ELSE OBJ_CLASS_TAG END 
        FROM EASYSHARE.D_OBJECT  
        WHERE ( OBJ_CLASS_TAG LIKE '5%' OR OBJ_CLASS_TAG LIKE '-5_5_' )
          AND OBJ_REF_PARENT = n_objOwnerRef
          AND OBJ_STATES = '222';
    
      UPDATE tt_Mgt_User
      SET UserProfileRef = T3.OBJ_REF_PARENT
      FROM EASYSHARE.R_Class    T2,
           EASYSHARE.D_OBJECT   T3
      WHERE T2.Class_Tag = tt_Mgt_User.UserClass
        AND T2.Param_Tag = '6A'  -- Profile Class
        AND T3.OBJ_REF = tt_Mgt_User.UserRef
        AND T3.OBJ_CLASS_TAG = '-' || tt_Mgt_User.UserClass || T2.Param_ValueDefault;
		
      GET DIAGNOSTICS n_rowcount = ROW_COUNT;
      IF ( n_rowcount > 0 ) 
      THEN
        n_userProfileFound := 1;
        ---------------------------------------------------------
        -- Remove users who don't have ability to see the alarm
        -- Get Reference of Remaining user for Refresh
        ---------------------------------------------------------
        DELETE FROM tt_Mgt_User
        USING EASYSHARE.D_ParamETER  T2
        WHERE tt_Mgt_User.UserProfileRef IS NOT NULL
          AND T2.OBJ_REF = tt_Mgt_User.UserProfileRef
          AND T2.Param_TAG = c_objClass || 'FA'
          AND T2.Param_VALUE = '0';

        SELECT UserRef INTO n_userRef
        FROM tt_Mgt_User
        LIMIT 1;

        IF ( FOUND )
        THEN
          ----------------------------------------------------------------------
          -- Refresh the Alarm Deletion
          -- Duplicate the Refresh to all authorized users
          ----------------------------------------------------------------------
          n_return :=  ALARM.Msg_RefreshAlarm( n_logRecId, 'FE', '36', n_alarmRef, n_logWindowID, n_userRef );

          INSERT INTO EASYSHARE.D_FLUX( AppFrom, AppTo, TransactionType, ComponentType, InvokeId, OperationCode, Param )
            SELECT T1.AppFrom, T2.UserRef, T1.TransactionType, T1.ComponentType, T1.InvokeId, T1.OperationCode, T1.Param
            FROM EASYSHARE.D_FLUX T1,
                tt_Mgt_User      T2
            WHERE T1.AppFrom = cn_admRefresh
              AND T1.AppTo = n_userRef
              AND T1.OperationCode = x'F9'::int4
              AND T1.InvokeId = n_alarmRef
              AND ( T1.Param LIKE '1AFE%' OR T1.Param LIKE '1AFA%' )
              AND T2.UserRef <> T1.AppTo;
        END IF;
      ELSE
        n_userProfileFound := 0;

        ----------------------------------------------------------------------
        -- Refresh the Alarm Deletion
        ----------------------------------------------------------------------
        PERFORM Alarm.Msg_RefreshAlarm( n_logRecId, 'FE', '36', n_alarmRef, n_logWindowID, n_objOwnerRef );

      END IF;

      -- Get OwnerRef Class
      BEGIN
        SELECT OBJ_CLASS_TAG INTO STRICT c_objOwnerClass
        FROM EASYSHARE.D_OBJECT 
        WHERE OBJ_REF = n_objOwnerRef
          AND OBJ_CLASS_TAG NOT LIKE '-%';
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
        c_objOwnerClass := '';
      END;

      -----------------------------------------------------------------------
      -- Refresh the Alarm Log for other users if other classes have the 
      -- ability to see all alarms...
      -----------------------------------------------------------------------
      IF ( LENGTH( c_viewAllAlarmsClass ) > 0 ) THEN
        FOR UserGroup IN
          ( SELECT DISTINCT T1.OBJ_REF_PARENT
            FROM EASYSHARE.D_OBJECT   T1,
                 EASYSHARE.D_OBJECT   T2
            WHERE T1.OBJ_CLASS_TAG LIKE '5_'
              AND STRPOS( c_viewAllAlarmsClass, T1.OBJ_CLASS_TAG ) > 0
              AND T1.OBJ_CLASS_TAG <> c_objOwnerClass
              AND T2.OBJ_REF = T1.OBJ_REF_PARENT
              AND T2.OBJ_CLASS_TAG NOT LIKE '-%'
          )
        LOOP 
          n_userGroupRef := UserGroup.OBJ_REF_PARENT;
                 
          PERFORM Alarm.Msg_RefreshAlarm( n_logRecId, 'FA', '36', n_alarmRef, n_logWindowID, n_userGroupRef, in_refreshOtherClass => 0 );
        END LOOP;
      END IF;

      -- Redirect the Alarm deletion towards the External alarm system
      n_return := ALARM.Forward_Alarm( n_alarmRef, n_alarmType, 'FE', n_perceivedSeverity );
  
      ---------------------------------------------------------------------------------
      -- In case of User Profiles, Alarm group has to be refreshed per User Profile
      ---------------------------------------------------------------------------------
      -- Refresh the Alarm Group
      SELECT T2.OBJ_REF INTO n_alarmGroupRef
      FROM EASYSHARE.D_OBJECT    T1,
           EASYSHARE.D_ParamETER T2,
           EASYSHARE.D_ParamETER T3
      WHERE T1.OBJ_CLASS_TAG = '35'
        AND T2.OBJ_REF = T1.OBJ_REF
        AND T2.Param_TAG = '20'
        AND T2.Param_VALUE = CAST( n_perceivedSeverity AS VARCHAR )
        AND T3.OBJ_REF = T2.OBJ_REF
        AND T3.Param_TAG = '21'
        AND STRPOS( T3.Param_VALUE, COALESCE( c_objOwnerClass, '5B' ) ) > 0;
  
      IF ( n_userProfileFound = 0 ) THEN
        PERFORM Alarm.Msg_RefreshAlarm( 0, 'FD', '35', n_alarmGroupRef, 0, n_objOwnerRef, in_refreshOtherClass => 0 );
      ELSE
        FOR UserProf IN ( SELECT UserProfileRef, MIN(UserRef) AS UserRef FROM tt_Mgt_User GROUP BY UserProfileRef )
        LOOP
          -- Only send Refresh message for one User
          PERFORM Alarm.Msg_RefreshAlarm( 0, 'FD', '35', n_alarmGroupRef, 0, UserProf.UserRef, in_refreshOtherClass => 0 );
          
          -- Duplicate the message for each User with the same Profile
          INSERT INTO EASYSHARE.D_FLUX( AppFrom, AppTo, TransactionType, ComponentType, InvokeId, OperationCode, Param )
            SELECT T1.AppFrom, T2.UserRef, T1.TransactionType, T1.ComponentType, T1.InvokeId, T1.OperationCode, T1.Param
            FROM EASYSHARE.D_FLUX T1,
                 tt_Mgt_User      T2
            WHERE T1.AppFrom = cn_admRefresh
              AND T1.AppTo = UserProf.UserRef
              AND T1.OperationCode = x'F9'::int4
              AND T1.InvokeId = n_alarmGroupRef
              AND T1.Param LIKE '1AFD%'
              AND COALESCE( T2.UserProfileRef, -1 ) = COALESCE( UserProf.UserProfileRef, -1 )
              AND T2.UserRef <> T1.AppTo;
        END LOOP;
      END IF;

      -- Execute Post-Operation
      n_return := ALARM.Deal_PostAlarm( n_alarmType, n_perceivedSeverity, 0, n_objRef, c_objClass );

    END LOOP;

    -------------------------------------------------------
    -- Check for Refresh of Updated Alarms
    -------------------------------------------------------
    n_nbUpdated := 0;
    FOR Alarm IN   
     ( SELECT T1.D_RAlarmID, T1.D_AlarmID, T1.D_PerceivedSeverity AS OldSeverity, T2.D_PerceivedSeverity AS NewSeverity, T1.D_ObjClassTag, T1.D_ObjRef, T1.D_ObjOwnerRef
       FROM tt_Mgt_DAlarm     T1,
            ALARM.D_ALARM    T2,
            ALARM.R_ALARM    T3
       WHERE T1.D_AlarmID = T2.D_AlarmID
         AND T1.D_PerceivedSeverity <> T2.D_PerceivedSeverity
         AND T1.D_PerceivedSeverity <> 0
         AND T2.D_PerceivedSeverity <> 0
         AND T3.R_AlarmID = T1.D_RAlarmID
    )
    LOOP
      n_nbUpdated := n_nbUpdated + 1;
  
      n_alarmType := Alarm.D_RAlarmID;
      n_alarmRef := Alarm.D_AlarmID;
      n_oldSeverity := Alarm.OldSeverity;
      n_newSeverity := Alarm.NewSeverity;
      c_objClass := Alarm.D_ObjClassTag;
      n_objRef := Alarm.D_ObjRef;
      n_objOwnerRef := Alarm.D_ObjOwnerRef;

      -------------------------------------------------------------------------------------------------------------
      -- Get all the users linked to the Alarm Owner Group.
      -- Depending on the User Profile, some of them may not be authorized to see the alarms.
      -- Depending on the User Profile, alarm group may not contain same number of alarms
      -------------------------------------------------------------------------------------------------------------
      DELETE FROM tt_Mgt_User;
      INSERT INTO tt_Mgt_User( UserRef, UserClass )
        SELECT OBJ_REF, CASE WHEN OBJ_CLASS_TAG LIKE '-%' THEN SUBSTRING( OBJ_CLASS_TAG, 2, 2 ) ELSE OBJ_CLASS_TAG END 
        FROM EASYSHARE.D_OBJECT  
        WHERE ( OBJ_CLASS_TAG LIKE '5%' OR OBJ_CLASS_TAG LIKE '-5_5_' )
          AND OBJ_REF_PARENT = n_objOwnerRef
          AND OBJ_STATES = '222';
          
      UPDATE tt_Mgt_User
      SET UserProfileRef = T3.OBJ_REF_PARENT
      FROM EASYSHARE.R_Class    T2,
           EASYSHARE.D_OBJECT   T3
      WHERE T2.Class_Tag = tt_Mgt_User.UserClass
        AND T2.Param_Tag = '6A'  -- Profile Class
        AND T3.OBJ_REF = tt_Mgt_User.UserRef
        AND T3.OBJ_CLASS_TAG = '-' || tt_Mgt_User.UserClass || T2.Param_ValueDefault;
		
      GET DIAGNOSTICS n_rowcount = ROW_COUNT;
      IF ( n_rowcount > 0 ) THEN
        n_userProfileFound := 1;
        ---------------------------------------------------------
        -- Remove users who don't have ability to see the alarm
        -- Get Reference of Remaining user for Refresh
        ---------------------------------------------------------
        DELETE FROM tt_Mgt_User
        USING EASYSHARE.D_ParamETER  T2
        WHERE tt_Mgt_User.UserProfileRef IS NOT NULL
          AND T2.OBJ_REF = tt_Mgt_User.UserProfileRef
          AND T2.Param_TAG = c_objClass || 'FA'
          AND T2.Param_VALUE = '0';
		                       
        SELECT UserRef INTO n_userRef
        FROM tt_Mgt_User
        LIMIT 1;

        IF ( FOUND )  
        THEN
          ----------------------------------------------------------------------
          -- Delete the Alarm with the Old Severity Level
          -- Duplicate the Refresh to all authorized users
          ----------------------------------------------------------------------
          PERFORM Alarm.Msg_RefreshAlarm( 0, 'FE', '36', n_alarmRef, 0, n_userRef, in_refreshOtherClass => 0 );

          INSERT INTO EASYSHARE.D_FLUX( AppFrom, AppTo, TransactionType, ComponentType, InvokeId, OperationCode, Param )
            SELECT T1.AppFrom, T2.UserRef, T1.TransactionType, T1.ComponentType, T1.InvokeId, T1.OperationCode, T1.Param
            FROM EASYSHARE.D_FLUX T1,
                tt_Mgt_User      T2
            WHERE T1.AppFrom = cn_admRefresh
              AND T1.AppTo = n_userRef
              AND T1.OperationCode = x'F9'::int4
              AND T1.InvokeId = n_alarmRef
              AND T1.Param LIKE '1AFE%'
              AND T2.UserRef <> T1.AppTo;
        END IF;
      ELSE
        n_userProfileFound := 0;

        ----------------------------------------------------------------------
        -- Delete the Alarm with the Old Severity Level
        ----------------------------------------------------------------------
        PERFORM Alarm.Msg_RefreshAlarm( 0, 'FE', '36', n_alarmRef, 0, n_objOwnerRef, in_refreshOtherClass => 0 );
      END IF;

      -- Redirect the Alarm deletion towards the External alarm system
      n_return := ALARM.Forward_Alarm( n_alarmRef, n_alarmType, 'FE', n_oldSeverity );
  
      -- Refresh the Alarm Group
      SELECT OBJ_CLASS_TAG INTO c_objOwnerClass
      FROM EASYSHARE.D_OBJECT 
      WHERE OBJ_REF = n_objOwnerRef
        AND OBJ_CLASS_TAG NOT LIKE '-%';

      SELECT T2.OBJ_REF INTO n_alarmGroupRef
      FROM EASYSHARE.D_OBJECT    T1,
           EASYSHARE.D_ParamETER T2,
           EASYSHARE.D_ParamETER T3
      WHERE T1.OBJ_CLASS_TAG = '35'
        AND T2.OBJ_REF = T1.OBJ_REF
        AND T2.Param_TAG = '20'
        AND T2.Param_VALUE = CAST( n_oldSeverity AS VARCHAR )
        AND T3.OBJ_REF = T2.OBJ_REF
        AND T3.Param_TAG = '21'
        AND STRPOS( T3.Param_VALUE, COALESCE( c_objOwnerClass, '5B' ) ) > 0;
  
      IF ( n_userProfileFound = 0 ) THEN
        PERFORM Alarm.Msg_RefreshAlarm( 0, 'FD', '35', n_alarmGroupRef, 0, n_objOwnerRef, in_refreshOtherClass => 0 );
      ELSE
        FOR UserProf IN ( SELECT UserProfileRef, MIN(UserRef) AS UserRef FROM tt_Mgt_User GROUP BY UserProfileRef )
        LOOP
          -- Only send Refresh message for one User
          PERFORM Alarm.Msg_RefreshAlarm( 0, 'FD', '35', n_alarmGroupRef, 0, UserProf.UserRef, in_refreshOtherClass => 0 );
          
          -- Duplicate the message for each User with the same Profile
          INSERT INTO EASYSHARE.D_FLUX( AppFrom, AppTo, TransactionType, ComponentType, InvokeId, OperationCode, Param )
            SELECT T1.AppFrom, T2.UserRef, T1.TransactionType, T1.ComponentType, T1.InvokeId, T1.OperationCode, T1.Param
            FROM EASYSHARE.D_FLUX T1,
                 tt_Mgt_User      T2
            WHERE T1.AppFrom = cn_admRefresh
              AND T1.AppTo = UserProf.UserRef
              AND T1.OperationCode = x'F9'::int4
              AND T1.InvokeId = n_alarmGroupRef
              AND T1.Param LIKE '1AFD%'
              AND COALESCE( T2.UserProfileRef, -1 ) = COALESCE( UserProf.UserProfileRef, -1 )
              AND T2.UserRef <> T1.AppTo;
        END LOOP;
      END IF;

      ----------------------------------------------------------------------
      -- Add the Alarm with the New Severity Level
      ----------------------------------------------------------------------
      IF ( n_userProfileFound = 0 ) THEN
        PERFORM Alarm.Msg_RefreshAlarm( 0, 'FF', '36', n_alarmRef, 0, n_objOwnerRef, in_refreshOtherClass => 0 );
      ELSE
        PERFORM Alarm.Msg_RefreshAlarm( 0, 'FF', '36', n_alarmRef, 0, n_userRef, in_refreshOtherClass => 0 );

        ----------------------------------------------------------------------
        -- Duplicate the Refresh to all authorized users
        ----------------------------------------------------------------------
        INSERT INTO EASYSHARE.D_FLUX( AppFrom, AppTo, TransactionType, ComponentType, InvokeId, OperationCode, Param )
          SELECT T1.AppFrom, T2.UserRef, T1.TransactionType, T1.ComponentType, T1.InvokeId, T1.OperationCode, T1.Param
          FROM EASYSHARE.D_FLUX T1,
               tt_Mgt_User      T2
          WHERE T1.AppFrom = cn_admRefresh
            AND T1.AppTo = n_userRef
            AND T1.OperationCode = x'F9'::int4
            AND T1.InvokeId = n_alarmRef
            AND T1.Param LIKE '1AFF%'
            AND T2.UserRef <> T1.AppTo;
      END IF;

/*
      -----------------------------------------------------------------------------------------------------------------
      -- Remove the D_FLUX messages for users who do not have the ability to see the object on which Alarm was raised
      -----------------------------------------------------------------------------------------------------------------
      IF ( n_userProfileFound = 1 ) THEN
        FOR NonAuthorizedUsers IN ( SELECT UserRef FROM tt_Mgt_User T1
                                    WHERE UserProfileRef IS NOT NULL
                                      AND EXISTS ( SELECT 1
                                                   FROM EASYSHARE.D_ParamETER  T2
                                                   WHERE T2.OBJ_REF = T1.UserProfileRef
                                                     AND T2.Param_TAG = c_objClass || 'FA'
                                                     AND Param_VALUE = '0'
                                                 )
                                  )
        LOOP
          DELETE FROM EASYSHARE.D_FLUX
          WHERE AppFrom = cn_admRefresh
            AND AppTo = NonAuthorizedUsers.UserRef
            AND OperationCode = TO_NUMBER( 'F9', 'XX' )
            AND InvokeId = n_alarmRef
            AND Param LIKE '1AFF%';
        END LOOP;
      END IF;
*/
      -- Redirect the Alarm creation towards the External alarm system
      n_return := ALARM.Forward_Alarm( n_alarmRef, n_alarmType, 'FF', n_newSeverity );

      -- Refresh the Alarm Group
      SELECT OBJ_CLASS_TAG INTO c_objOwnerClass
      FROM EASYSHARE.D_OBJECT 
      WHERE OBJ_REF = n_objOwnerRef
        AND OBJ_CLASS_TAG NOT LIKE '-%';

      SELECT T2.OBJ_REF INTO n_alarmGroupRef
      FROM EASYSHARE.D_OBJECT    T1,
           EASYSHARE.D_ParamETER T2,
           EASYSHARE.D_ParamETER T3
      WHERE T1.OBJ_CLASS_TAG = '35'
        AND T2.OBJ_REF = T1.OBJ_REF
        AND T2.Param_TAG = '20'
        AND T2.Param_VALUE = CAST( n_newSeverity AS VARCHAR )
        AND T3.OBJ_REF = T2.OBJ_REF
        AND T3.Param_TAG = '21'
        AND STRPOS( T3.Param_VALUE, COALESCE( c_objOwnerClass, '5B' ) ) > 0;
  
      IF ( n_userProfileFound = 0 ) THEN
        PERFORM Alarm.Msg_RefreshAlarm( 0, 'FD', '35', n_alarmGroupRef, 0, n_objOwnerRef, in_refreshOtherClass => 0 );
      ELSE
        FOR UserProf IN ( SELECT UserProfileRef, MIN(UserRef) AS UserRef FROM tt_Mgt_User GROUP BY UserProfileRef )
        LOOP
          -- Only send Refresh message for one User
          PERFORM Alarm.Msg_RefreshAlarm( 0, 'FD', '35', n_alarmGroupRef, 0, UserProf.UserRef, in_refreshOtherClass => 0 );
          
          -- Duplicate the message for each User with the same Profile
          INSERT INTO EASYSHARE.D_FLUX( AppFrom, AppTo, TransactionType, ComponentType, InvokeId, OperationCode, Param )
            SELECT T1.AppFrom, T2.UserRef, T1.TransactionType, T1.ComponentType, T1.InvokeId, T1.OperationCode, T1.Param
            FROM EASYSHARE.D_FLUX T1,
                 tt_Mgt_User      T2
            WHERE T1.AppFrom = cn_admRefresh
              AND T1.AppTo = UserProf.UserRef
              AND T1.OperationCode = x'F9'::int4
              AND T1.InvokeId = n_alarmGroupRef
              AND T1.Param LIKE '1AFD%'
              AND COALESCE( T2.UserProfileRef, -1 ) = COALESCE( UserProf.UserProfileRef, -1 )
              AND T2.UserRef <> T1.AppTo;
        END LOOP;
      END IF;

      -- Execute Post-Operation
      n_return := ALARM.Deal_PostAlarm( n_alarmType, n_oldSeverity, n_newSeverity, n_objRef, c_objClass );
    END LOOP;
  
  EXCEPTION
    WHEN OTHERS THEN
      DECLARE
        pg_exception    T_EXCEPTION;
        n_errorId       int4;
        n_msgObjRef     int4;
        c_msgObjClass   varchar(5);
        c_msgObjId      varchar(255);
      BEGIN
        GET STACKED DIAGNOSTICS pg_exception.ReturnedSqlState = RETURNED_SQLSTATE,
                                pg_exception.ColumnName = COLUMN_NAME,
                                pg_exception.ConstraintName = CONSTRAINT_NAME,
                                pg_exception.MessageText = MESSAGE_TEXT,
                                pg_exception.TableName = TABLE_NAME,
                                pg_exception.SchemaName = SCHEMA_NAME,
                                pg_exception.PgExceptionDetail = PG_EXCEPTION_DETAIL,
                                pg_exception.PgExceptionHint = PG_EXCEPTION_HINT,
                                pg_exception.PgExceptionContext = PG_EXCEPTION_CONTEXT;

        n_errorId := AQSATOOLS.LogError( pg_exception );

        RAISE NOTICE 'ALARM.Adm_Alarm % : Error', c_pkgVersion;
      END;
  END;

  IF ( ( n_nbAdded <> 0 )
      OR ( n_nbDeleted <> 0 )
      OR ( n_nbUpdated <> 0 )
      OR ( n_alarmMsg <> 0 ) ) THEN

      ts_end := clock_timestamp();
      n_durationMs := AQSATOOLS.DateDiff('ms', ts_begin, ts_end);
      RAISE NOTICE '% Added, % Deleted, % Updated', n_nbAdded, n_nbDeleted, n_nbUpdated;
      RAISE NOTICE 'ALARM.Adm_Alarm % : End', c_pkgVersion;
      RAISE NOTICE '-----------------------';
      RAISE NOTICE E'% -> % (% Milliseconds)\n', TO_CHAR( ts_begin, 'YYYY/MM/DD HH24:MI:SS.MS' ), TO_CHAR( ts_end, 'YYYY/MM/DD HH24:MI:SS.MS' ), n_durationMs;
  END IF;
  RETURN 0;
 END
$$ LANGUAGE 'plpgsql';

 SELECT  AQSATOOLS.StoreFunctionPkg ( 'ALARM', 'pg_Alarm_Mgt', 'Adm_Alarm' );
 
/*
  ===========================================================================
  -- AUDIT ACTIVE ALARMS 
  ===========================================================================
*/

DROP FUNCTION IF EXISTS ALARM.Audit_Alarm;
CREATE FUNCTION ALARM.Audit_Alarm()
RETURNS INT4 AS $$

  DECLARE
    n_objOwnerRef       INT4;
    n_logWindowId       INT4;
    n_Exists            INT4;
    n_return            INT4;
    cn_admAlarmRef      INT4;
    AuditedAlarm        RECORD;
    PopupAlarm          RECORD;
    GroupedAlarm        RECORD;
    ClearGroupedAlarm   RECORD;
    ClearedAlarm        RECORD;
  
  BEGIN
  
    --------------------------------------------
    -- Get Last Alarm Identifier
    --------------------------------------------
    cn_admAlarmRef := 121;
    n_logWindowID := 4 ;
  
    -------------------------------------------------------------------
    -- Management of Active alarms of type 1
    -------------------------------------------------------------------

    FOR AuditedAlarm IN ( SELECT T1.D_AlarmID, T1.D_PerceivedSeverity, LEAST( floor( ( AQSATOOLS.DateDiff('mi', T1.D_EventDate, NOW()) ) / CAST( T3.Param_VALUE AS INT4 ) ) + CAST( T2.R_DefaultSeverity AS INT4 ), 4 ) AS Severity
                          FROM ALARM.D_Alarm         T1,
                               ALARM.R_Alarm         T2,
                               EASYSHARE.D_ParamETER T3
                          WHERE T1.D_ClearedDate IS NULL             -- Active Alarm
                            AND T2.R_AlarmID = T1.D_RAlarmID
                            AND ( T2.R_AlarmType & 1 ) <>  0    -- Alarm whose severity depends on Activation Duration
                            AND T2.R_ParamTagAudit IS NOT NULL       -- Tag containing the Threshold value
                            AND T3.OBJ_REF = T1.D_ObjRef
                            AND T3.Param_TAG = T2.R_ParamTagAudit
                            AND ( T3.Param_VALUE SIMILAR TO '[0-9]+' )
                            AND T3.Param_VALUE > '0'
--                            AND T1.D_PerceivedSeverity <> LEAST( floor( ( ( SYSDATE - T1.D_EventDate ) / cc_one_minute ) / ( TO_NUMBER( T3.Param_VALUE ) ) ) + TO_NUMBER( T2.R_DefaultSeverity ), 4 )
                        )
    LOOP
      IF ( AuditedAlarm.D_PerceivedSeverity <> AuditedAlarm.Severity ) THEN
        UPDATE ALARM.D_Alarm
        SET D_PerceivedSeverity = AuditedAlarm.Severity
        WHERE D_AlarmID = AuditedAlarm.D_AlarmID;
      END IF;
    END LOOP;

    ----------------------------------------------------------------
    -- Send a POPUP message for Alarms of type 4
    ----------------------------------------------------------------
    FOR PopupAlarm IN 
      ( SELECT D_AlarmID 
        FROM ALARM.R_Alarm T1,
             ALARM.D_ALARM T2
        WHERE ( T1.R_AlarmType & 4 ) <> 0
          AND T2.D_RAlarmID = T1.R_AlarmID
          AND T2.D_ClearedDate IS NULL
      )
    LOOP 
      -- Send Popup message to GUI
      n_return := ALARM.Msg_NewAlarm( PopupAlarm.D_AlarmID );
    END LOOP;
  
    -- Update End date for Alarms of type 4
    UPDATE ALARM.D_ALARM
    SET D_ClearedDate = NOW(),
        D_ClearedBy = 'System'
    FROM ALARM.R_ALARM T2
    WHERE D_ALARM.D_ClearedDate IS NULL
      AND ( T2.R_AlarmType & 4 ) <> 0
      AND T2.R_AlarmID = D_ALARM.D_RAlarmID;
  
    -------------------------------------------------------------------------------------
    -- Clear Alarms related to LOCK objects
    -- Alarms of type 8 are not cleared in this case...
    -------------------------------------------------------------------------------------
    UPDATE ALARM.D_Alarm
    SET D_ClearedDate = NOW(),
        D_ClearedBy = 'System'
    FROM ALARM.R_Alarm T2,
         EASYSHARE.D_OBJECT T3
    WHERE T2.R_AlarmID = D_Alarm.D_RAlarmID
      AND ( T2.R_AlarmType & 8 ) = 0
      AND T3.OBJ_REF = D_Alarm.D_ObjRef
      AND SUBSTRING( T3.OBJ_STATES, 1, 1 ) = '1' ;

    -------------------------------------------------------------------------------------
    -- Clear Alarms related to DELETED objects
    -------------------------------------------------------------------------------------
    UPDATE ALARM.D_Alarm
    SET D_ClearedDate = NOW(),
        D_ClearedBy = 'System'
    WHERE NOT EXISTS ( SELECT T2.OBJ_REF 
                       FROM EASYSHARE.D_OBJECT T2
                       WHERE T2.OBJ_REF = D_Alarm.D_ObjRef
                     );
  
  
    ------------------------------------------------------------
    -- Close Alarm of Type 2 whose severity is equal to 0
    -- Restore the Perceived Severity to the previous one...
    ------------------------------------------------------------
    UPDATE ALARM.D_Alarm
    SET D_ClearedDate = NOW(),
        D_ClearedBy = 'System',
        D_PerceivedSeverity = T3.D_PerceivedSeverity 
    FROM tt_Mgt_DAlarm T3,
         ALARM.R_Alarm T2
    WHERE T3.D_AlarmID = D_Alarm.D_AlarmID
      AND T3.D_PerceivedSeverity > 0
      AND D_Alarm.D_PerceivedSeverity = 0
      AND T2.R_AlarmID = D_Alarm.D_RAlarmID
      AND ( T2.R_AlarmType & 2 ) <> 0;
    
    -- Delete new Alarms whose severity is 0...
    DELETE FROM ALARM.D_Alarm
    USING ALARM.R_Alarm T2,
          tt_Mgt_DAlarm T3
    WHERE D_Alarm.D_PerceivedSeverity = 0
	  AND T2.R_AlarmID = D_Alarm.D_RAlarmID
      AND ( T2.R_AlarmType & 2 ) <> 0
      AND T3.D_AlarmID = D_Alarm.D_AlarmID
      AND T3.D_PerceivedSeverity > 0;
  
     -----------------------------------------------------------------------------------------
     -- Management of alarm Messages of type 32 who should not exist anymore -> Severity = 0
     -----------------------------------------------------------------------------------------
     DELETE FROM ALARM.D_Alarm
     USING ALARM.R_Alarm T1
     WHERE ( T1.R_AlarmType & 32 ) <> 0
       AND D_Alarm.D_RAlarmID = T1.R_AlarmID
       AND D_Alarm.D_PerceivedSeverity = 0
       AND D_Alarm.D_EventDate + T1.R_AlarmLatency * INTERVAL '1 day' < NOW();    -- R_AlarmLatency in days....
  
      CREATE TEMPORARY TABLE IF NOT EXISTS TT_MGT_GroupedAlarm
      (
        D_ObjRef            INT4          NOT NULL,
        R_AlarmID           INT4          NOT NULL,
        PerceivedSeverity   INT4          NOT NULL,
        AlarmThreshold      INT4          NOT NULL,
        AlarmFrequency      NUMERIC       NOT NULL
      );
     DELETE FROM TT_MGT_GroupedAlarm;
	
     n_return := EASYSHARE.StringToList ('1;2;3;4');	
	
     INSERT INTO TT_MGT_GroupedAlarm( D_ObjRef, R_AlarmId, AlarmThreshold, AlarmFrequency, PerceivedSeverity  )
       SELECT T2.D_ObjRef, T2.D_RAlarmID, T1.R_AlarmThreshold, T1.R_AlarmLatency, T1.R_DefaultSeverity + ( COUNT( T2.D_AlarmID ) / T1.R_AlarmThreshold ) - 1 AS PerceivedSeverity
       FROM ALARM.R_Alarm T1,
            ALARM.D_Alarm T2
       WHERE ( T1.R_AlarmType & 32 ) <> 0
        AND T1.R_AlarmGrouped LIKE 'N%'
        AND T2.D_RAlarmID = T1.R_AlarmID
        AND T2.D_AlarmThreshold IS NULL
        AND T2.D_PerceivedSeverity = 0
       GROUP BY T2.D_ObjRef, T2.D_RAlarmID, T1.R_DefaultSeverity, T1.R_AlarmThreshold, T1.R_AlarmLatency
       HAVING COUNT(T2.D_AlarmID) >= T1.R_AlarmThreshold
       UNION ALL
       SELECT D_ObjRef, D_RAlarmId, R_AlarmThreshold, R_AlarmLatency, MAX( PerceivedSeverity ) AS PerceivedSeverity
       FROM ( SELECT T2.D_ObjRef, T2.D_RAlarmID, T1.R_AlarmThreshold, T1.R_AlarmLatency, T3.Severity AS PerceivedSeverity
              FROM ALARM.R_Alarm T1,
                   ALARM.D_Alarm T2,
                   ( SELECT CAST(FieldValue AS INT4) AS Severity FROM ArrayList ) T3
              WHERE ( T1.R_AlarmType & 32 ) <> 0
               AND T1.R_AlarmGrouped LIKE 'N%'
               AND T2.D_RAlarmID = T1.R_AlarmID
               AND T2.D_PerceivedSeverity = 0
               AND T2.D_AlarmThreshold >= T3.Severity
              GROUP BY T2.D_ObjRef, T2.D_RAlarmID, T1.R_AlarmThreshold, T1.R_AlarmLatency, T3.Severity
              HAVING COUNT(*) >= T1.R_AlarmThreshold       
            ) AlarmSeverity
       GROUP BY  D_ObjRef, D_RAlarmId, R_AlarmThreshold, R_AlarmLatency
       UNION ALL
       SELECT 0 AS D_ObjRef, T2.D_RAlarmID,  T1.R_AlarmThreshold, T1.R_AlarmLatency, T1.R_DefaultSeverity + ( COUNT( T2.D_AlarmID ) / T1.R_AlarmThreshold ) - 1 AS PerceivedSeverity
       FROM ALARM.R_Alarm T1,
            ALARM.D_Alarm T2
       WHERE ( T1.R_AlarmType & 32 ) <> 0
         AND T1.R_AlarmGrouped LIKE 'Y%'
         AND T2.D_RAlarmID = T1.R_AlarmID
         AND T2.D_PerceivedSeverity = 0
       GROUP BY T2.D_RAlarmID, T1.R_DefaultSeverity, T1.R_AlarmThreshold, T1.R_AlarmLatency
       HAVING COUNT( T2.D_AlarmID ) >= T1.R_AlarmThreshold;

    FOR GroupedAlarm IN ( SELECT * FROM TT_MGT_GroupedAlarm )
    LOOP 
      -- If no alarm of detected severity exists for the identified object
      IF NOT EXISTS ( SELECT D_AlarmID 
                      FROM ALARM.D_Alarm 
                      WHERE D_ObjRef = GroupedAlarm.D_ObjRef
                        AND D_RAlarmID = GroupedAlarm.R_AlarmID
                        AND D_PerceivedSeverity = GroupedAlarm.PerceivedSeverity 
                    )
      THEN
        IF NOT EXISTS ( SELECT D_AlarmID 
                        FROM ALARM.D_Alarm 
                        WHERE D_ObjRef = GroupedAlarm.D_ObjRef
                          AND D_RAlarmID = GroupedAlarm.R_AlarmID
                          AND D_PerceivedSeverity <> 0 
                      )
        THEN
          -- Create a new alarm
          IF ( GroupedAlarm.D_ObjRef <> 0 ) THEN
            INSERT INTO ALARM.D_Alarm( D_RALarmID, D_ObjClassTag, D_ObjRef, D_ObjId, D_ObjOwnerRef, D_EventType, D_EventDate, D_ClearedDate, D_ProbableCause, D_SpecificProblem, D_PerceivedSeverity, D_AdditionalInformation )
              SELECT T1.D_RAlarmID, T1.D_ObjClassTag, T1.D_ObjRef, T1.D_ObjId, T1.D_ObjOwnerRef, T2.R_AlarmCategory, NOW(), NULL, T2.R_ProbableCause, T2.R_SpecificProblem, GroupedAlarm.PerceivedSeverity, 
                     'Condition was met at least ' || CAST( GroupedAlarm.AlarmThreshold AS INT4 ) || ' times during last ' || 
                     CASE WHEN ( GroupedAlarm.AlarmFrequency * 24 % 24 ) = 0 THEN CAST( GroupedAlarm.AlarmFrequency AS VARCHAR ) || ' Day(s)' ELSE CAST( ROUND( GroupedAlarm.AlarmFrequency * 24 ) AS VARCHAR ) || ' Hour(s)' END
              FROM ALARM.D_Alarm T1,
                   ALARM.R_Alarm T2
              WHERE T1.D_RAlarmID = GroupedAlarm.R_AlarmID
                AND T1.D_ObjRef = GroupedAlarm.D_ObjRef
                AND T2.R_AlarmID = T1.D_RAlarmID 
              LIMIT 1;
          ELSE
            INSERT INTO ALARM.D_Alarm( D_RALarmID, D_ObjClassTag, D_ObjRef, D_ObjId, D_ObjOwnerRef, D_EventType, D_EventDate, D_ClearedDate, D_ProbableCause, D_SpecificProblem, D_PerceivedSeverity, D_AdditionalInformation )
              SELECT T1.R_AlarmID, T1.R_ObjClassTag, 0, 'All', 0, T1.R_AlarmCategory, NOW(), NULL, T1.R_ProbableCause, T1.R_SpecificProblem, GroupedAlarm.PerceivedSeverity,
                     'Condition was met at least ' || CAST( GroupedAlarm.AlarmThreshold AS INT4 ) || ' times during last ' || 
                     CASE WHEN ( GroupedAlarm.AlarmFrequency * 24 % 24 ) = 0 THEN CAST( GroupedAlarm.AlarmFrequency AS VARCHAR ) || ' Day(s)' ELSE CAST( ROUND( GroupedAlarm.AlarmFrequency * 24 ) AS VARCHAR ) || ' Hour(s)' END
              FROM ALARM.R_Alarm T1
              WHERE T1.R_AlarmID = GroupedAlarm.R_AlarmID
              LIMIT 1;
          END IF;
        ELSE
          -- Update Alarm Severity
          UPDATE ALARM.D_Alarm
          SET D_PerceivedSeverity = GroupedAlarm.PerceivedSeverity
          WHERE D_RAlarmID = GroupedAlarm.R_AlarmID
            AND D_ObjRef = GroupedAlarm.D_ObjRef
            AND D_PerceivedSeverity <> 0;
        END IF;
      END IF;
    END LOOP;

    --------------------------------------------------------------------
    -- Management of alarms of type 32 who should not exist anymore...
    --------------------------------------------------------------------
    FOR ClearGroupedAlarm IN ( SELECT T2.D_AlarmID
                               FROM ALARM.R_Alarm      T1,
                                    ALARM.D_Alarm      T2
                               WHERE ( T1.R_AlarmType & 32 ) <> 0
                                 AND T2.D_RAlarmID = T1.R_AlarmID
                                 AND T2.D_PerceivedSeverity > 0
                                 AND T2.D_ClearedDate IS NULL
                                 AND NOT EXISTS ( SELECT T3.R_AlarmId
                                                  FROM TT_MGT_GroupedAlarm  T3
                                                  WHERE T3.R_AlarmId = T1.R_AlarmID
                                                    AND T3.D_ObjRef = T2.D_ObjRef
                                                )
                             )
    LOOP
      UPDATE ALARM.D_ALARM
      SET D_PerceivedSeverity = 0,
          D_ClearedDate = NOW()
      WHERE D_AlarmID = ClearGroupedAlarm.D_AlarmID;
    END LOOP;
  
    FOR ClearedAlarm IN
       ( SELECT D_AlarmID 
         FROM ALARM.D_Alarm 
         WHERE D_ClearedDate IS NOT NULL
           AND D_PerceivedSeverity <> 0
       )
    LOOP
         
      INSERT INTO ALARM.L_Alarm( L_AlarmID, L_RAlarmID, L_ObjClassTag, L_ObjRef, L_ObjID, L_ObjOwnerRef, L_EventType, L_EventDate, L_ProbableCause, L_SpecificProblem, L_PerceivedSeverity, L_AdditionalInformation, 
                                 L_AcknowledgedBy, L_AcknowledgedDate, L_ClearedBy, L_ClearedDate, L_Remark )
        SELECT D_AlarmID, D_RAlarmID, D_ObjClassTag, D_ObjRef, D_ObjID, D_ObjOwnerRef, D_EventType, D_EventDate, D_ProbableCause, D_SpecificProblem, D_PerceivedSeverity, D_AdditionalInformation, D_AcknowledgedBy,
               D_AcknowledgedDate, D_ClearedBy, D_ClearedDate, D_Remark 
        FROM ALARM.D_Alarm 
        WHERE D_AlarmID = ClearedAlarm.D_AlarmId;
    
      -- For type 4 Alarms, send a refresh message to Alarm logs window
      IF EXISTS ( SELECT  
                  FROM ALARM.D_Alarm T1,
                       ALARM.R_ALARM T2
                  WHERE T1.D_AlarmID = ClearedAlarm.D_AlarmId
                    AND T2.R_AlarmID = T1.D_RAlarmID
                    AND ( T2.R_AlarmType & 4 ) <> 0 )
      THEN
        SELECT D_ObjOwnerRef INTO n_objOwnerRef
        FROM ALARM.D_Alarm 
        WHERE D_AlarmID = ClearedAlarm.D_AlarmId;
              
        PERFORM Alarm.Msg_RefreshAlarm( n_logRecId, 'FA', '36', ClearedAlarm.D_AlarmId, n_logWindowID, n_objOwnerRef );
      END IF;
    
      UPDATE tt_Mgt_DAlarm
      SET L_LogRecId = ClearedAlarm.D_AlarmId
      WHERE D_AlarmID = ClearedAlarm.D_AlarmId;
    END LOOP;
  
    ------------------------------------------------------------------------
    -- Delete Cleared Alarms
    ------------------------------------------------------------------------
    DELETE FROM ALARM.D_Alarm
    WHERE D_ClearedDate IS NOT NULL;
	
    RETURN 0;
  END
$$ LANGUAGE 'plpgsql';

 SELECT  AQSATOOLS.StoreFunctionPkg ( 'ALARM', 'pg_Alarm_Mgt', 'Audit_Alarm' );
 
/*
  ===========================================================================
  -- SAVE/ EXPORT ALARM LOG
  ===========================================================================
*/

DROP FUNCTION IF EXISTS ALARM.Backup_Log;
CREATE FUNCTION ALARM.Backup_Log
 (
   d_inBeginDate        timestamp,
   d_inEndDate          timestamp,
   c_backupFile         varchar,
   c_backupFileHeader   varchar,
   n_maxRows            int4 default 0,
   n_keepRows           int4 default 0,
   n_purgeExportedRows  int4 default 0
 )
RETURNS INT4 AS $$

 DECLARE
  c_addInfo          varchar(255);
  c_addText          varchar(255);
  c_backupSql        varchar(255);
  c_backupTable      varchar(255);
  c_cmd              varchar(255);
  c_cvsSeparator     varchar(5);
  c_fieldSeparator   varchar(5);
  c_separator        varchar(5);

  n_activityRef      int4;
  n_indexBegin       int4;
  n_indexEnd         int4;
  n_item             int4;
  n_logRows          int4;
  n_maxLength        int4;
  n_msgRef           int4;
  n_ownerRef         int4;
  n_return           int4;
  d_beginDate        timestamp;
  d_endDate          timestamp;

 BEGIN

  --------------------------
  -- Initialize variables...
  --------------------------

  c_cvsSeparator := ',';
  c_fieldSeparator := '~|';
  c_separator := '~|';
  n_ownerRef := 105;
  n_activityRef := 80205;
  c_addText := '11Alarm' ||  c_separator || '12' || c_backupFile || c_separator;
  n_return := 0;
  d_beginDate := d_inBeginDate;
  d_endDate := d_inEndDate;

  -----------------------------------------------------------------
  -- Initialize default d_beginDate and d_endDate if not provided
  -----------------------------------------------------------------
  d_beginDate := COALESCE( d_beginDate, TO_TIMESTAMP('2000/01/01', 'YYYY/MM/DD') );
  d_endDate   := COALESCE( d_endDate, TO_TIMESTAMP('2030/01/01', 'YYYY/MM/DD') );

  ------------------------------------------------------
  -- Prepare Alarm Log Data in table ALARM.TT_MGT_BackupAlarm
  ------------------------------------------------------
  CREATE TEMPORARY TABLE IF NOT EXISTS TT_MGT_BackupAlarm
  (
    AlarmId             INT             NOT NULL,
    EventDate           DATE            NOT NULL,
    AlarmState          VARCHAR2(30)    NOT NULL,
    EntityId            VARCHAR2(255)   NOT NULL,
    EventType           VARCHAR2(50)    NOT NULL,    
    ProbableCause       VARCHAR2(255)   NOT NULL,
    SpecificProblem     VARCHAR2(255)   NOT NULL,
    PerceivedSeverity   VARCHAR2(20)    NOT NULL,
    AckBy               VARCHAR2(30)    NOT NULL DEFAULT (''),
    AckDate             VARCHAR2(20)    NOT NULL DEFAULT (''),
    ClrBy               VARCHAR2(30)    NOT NULL DEFAULT (''),
    ClrDate             VARCHAR2(20)    NOT NULL DEFAULT (''),
    Remark              VARCHAR2(512)
  );
  DELETE FROM TT_MGT_BackupAlarm;
	
  -- First get Closed Alarms
  INSERT INTO TT_MGT_BackupAlarm( AlarmId, EventDate, AlarmState, EntityId, EventType, ProbableCause, SpecificProblem, PerceivedSeverity, AckBy, AckDate, ClrBy, ClrDate, Remark )
    SELECT L_AlarmId, L_EventDate, 'CLOSED', L_ObjID, L_EventType, L_ProbableCause, L_SpecificProblem,
           CASE L_PerceivedSeverity
             WHEN 0  THEN 'Indeterminate'
             WHEN 1  THEN 'Warning'
             WHEN 2  THEN 'Minor'
             WHEN 3  THEN 'Major'
             WHEN 4  THEN 'Critical'
             ELSE 'Not Defined'
           END,
           L_AcknowledgedBy, COALESCE( TO_CHAR(L_AcknowledgedDate, 'YYYY/MM/DD HH24:MI:SS'), '' ), L_ClearedBy, COALESCE( TO_CHAR(L_ClearedDate, 'YYYY/MM/DD HH24:MI:SS'), '' ), L_Remark
    FROM ALARM.L_ALARM
    WHERE L_EventDate BETWEEN d_beginDate AND d_endDate;

  -- If only export over max rows -> Don't export Tree but only Log
  IF ( n_maxRows = 0 )
  THEN
    -- Then get Active Alarms
    INSERT INTO TT_MGT_BackupAlarm( AlarmId, EventDate, AlarmState, EntityId, EventType, ProbableCause, SpecificProblem, PerceivedSeverity, AckBy, AckDate, ClrBy, ClrDate )
      SELECT D_AlarmId, D_EventDate, 'ACTIVE', D_ObjID, D_EventType, D_ProbableCause, D_SpecificProblem,
             CASE D_PerceivedSeverity
               WHEN 0  THEN 'Indeterminate'
               WHEN 1  THEN 'Warning'
               WHEN 2  THEN 'Minor'
               WHEN 3  THEN 'Major'
               WHEN 4  THEN 'Critical'
               ELSE 'Not Defined'
             END,
             D_AcknowledgedBy, COALESCE( TO_CHAR(D_AcknowledgedDate, 'YYYY/MM/DD HH24:MI:SS'), '' ), D_ClearedBy, COALESCE( TO_CHAR(D_ClearedDate, 'YYYY/MM/DD HH24:MI:SS'), '' )
      FROM ALARM.D_ALARM
      WHERE D_EventDate BETWEEN d_beginDate AND d_endDate;
  END IF;


  -------------------------------------------------------------
  -- Prepare Files to store data
  -- Prepare dynamic query to select only authorized columns...
  -------------------------------------------------------------
  CREATE TEMPORARY TABLE IF NOT EXISTS TT_MGT_BackupLog
  (
    BackupRef   SERIAL  NOT NULL,
    AlarmId     INT4,
    LogLine     VARCHAR(4000) 
  );

  DELETE FROM TT_MGT_BackupLog;

  INSERT INTO TT_MGT_BackupLog( AlarmId, LogLine )
  VALUES ( 0, c_backupFileHeader );

  INSERT INTO TT_MGT_BackupLog( AlarmId, LogLine )
    SELECT AlarmId, LogLine
    FROM ( SELECT AlarmId,
                  TO_CHAR( EventDate, 'YYYY/MM/DD HH24:MI:SS' ) || c_cvsSeparator || AlarmState || c_cvsSeparator || 
                  EntityId || c_cvsSeparator || EventType || c_cvsSeparator || ProbableCause || c_cvsSeparator ||
                  SpecificProblem || c_cvsSeparator || PerceivedSeverity || c_cvsSeparator || 
                  ClrBy || c_cvsSeparator || ClrDate AS LogLine
           FROM TT_MGT_BackupAlarm
           ORDER BY EventDate 
         ) T1;

  GET DIAGNOSTICS n_logRows = ROW_COUNT;

  -----------------------------------------------------------------
  -- Export only Latest rows if configured...
  -----------------------------------------------------------------
 <<ExitProcedure>>
 LOOP
  IF ( n_maxRows > 0 )
  THEN
    IF ( n_logRows >= n_maxRows )
    THEN
      -- Export only the oldest Lines that won't be kept in the Log
      DELETE FROM TT_MGT_BackupLog WHERE BackupRef > ( n_logRows - n_keepRows  + 1 );
    ELSE
      EXIT ExitProcedure;
    END IF;
  END IF;

  BEGIN
    --EASYSHARE.pFile.DumpTableToFile ('TT_MGT_BACKUPLOG', c_backupFile, 'SELECT LogLine FROM ALARM.TT_MGT_BackupLog ORDER BY BackupRef', CHR(9), 0);
	c_cmd := 'COPY ( SELECT LogLine FROM TT_MGT_BackupLog ORDER BY BackupRef ) TO ''' || c_backupFile || ''' DELIMITER E''\t''';
	EXECUTE c_cmd;
    n_return := 0;
  EXCEPTION
  WHEN OTHERS THEN
    n_return := 1;
  END;

  IF ( n_return = 1 )
  THEN
    RAISE NOTICE ' Failed to create temporary file to store Alarm Log';
  ELSE
    IF ( n_purgeExportedRows = 1 )
    THEN
      DELETE FROM ALARM.L_Alarm WHERE L_AlarmId IN ( SELECT AlarmId FROM TT_MGT_BackupLog );
    END IF;
  END IF;

--<<UpdActivity>>

  -- In case of failure...
  IF ( n_return = 1 )
  THEN
    n_activityRef := 50206;
    c_addInfo := 'ALARM: 1EAlarm Log' || c_separator;
  END IF;
  
  n_msgRef := EASYSHARE.L_ActivityWrite ( n_activityRef, NULL, '5A', n_ownerRef, n_ownerRef, NULL, 0, c_addText, c_addInfo, 0, 0, 1, 0 );

   EXIT ExitProcedure;
 END LOOP; --<<ExitProcedure>>
  DELETE FROM TT_MGT_BackupLog;
  DELETE FROM TT_MGT_BackupAlarm;

  /*---------------------------------------------------------------------------*/
  /* Fin de la procedure                                                       */
  /*---------------------------------------------------------------------------*/

  RETURN 0;
 END
$$ LANGUAGE 'plpgsql';

 SELECT  AQSATOOLS.StoreFunctionPkg ( 'ALARM', 'pg_Alarm_Mgt', 'Backup_Log' );
 
/*
  ===========================================================================
  -- ACTIVATION / DEACTIVATION OF ALARMS LINKED TO AN ALARM MESSAGE 
  ===========================================================================
*/

DROP FUNCTION IF EXISTS ALARM.Deal_AlarmMsg;
CREATE FUNCTION ALARM.Deal_AlarmMsg
 (
    n_msgAlarm    INT4    DEFAULT 0,
    n_msgActivity INT4    DEFAULT 0,
    c_msgParam    VARCHAR DEFAULT ''
 )
RETURNS INT4 AS $$

  DECLARE
     n_alarmActive           INT4;
     n_alarmCanceled         INT4;
     n_alarmID               INT4;
     n_alarmMsgID            INT4;
     n_alarmOperation        INT4;          -- Operation : 1 -> Activation - 2 -> Desactivation
      
     n_alarmType             INT4;
     n_index                 INT4;
     n_indexEnd              INT4;
     n_linkedRef             INT4;
     n_logRecId              INT4;
     n_objRef                INT4;
     n_objOwnerRef           INT4;
     n_perceivedSeverity     INT4;
     n_Severity              INT4;
     n_Threshold             INT4;
     n_pos                   INT4;
     n_recordID              INT4;
     n_rowCount              INT4;
     n_addALISMname          INT4;
     n_return                INT4;
     c_linkedObj             text;
     c_logText               text;
     c_logInfo               text;
     c_objClassTag           VARCHAR(5);
     c_objId                 VARCHAR(255);
     c_string                text;
     c_sqlQuery              text;
     c_msgObjectIdentifier   VARCHAR(255);
     c_msgProbableCause      text;
     c_msgSpecificProblem    text;
     c_systemSpecificProblem text;
     c_excluActivityMsg      text;
     c_tag                   VARCHAR(4);
     c_tagValue              VARCHAR(255);
     c_userId                VARCHAR(60);
     c_pkgVersion            VARCHAR(10);
     cc_separator            VARCHAR(4);
     n_Exists                INT4;
     cn_tagLength            INT4;
     cn_admAlarmRef          INT4;
  
  BEGIN
    c_pkgVersion := AQSATOOLS.GetPkgVersion ('ALARM', 'pg_Alarm_Mgt');
    RAISE NOTICE 'ALARM.Deal_AlarmMsg % : Begin', c_pkgVersion;
    cc_separator := '~|';
    cn_tagLength := 2;
    cn_admAlarmRef := 121;

    -----------------------------------------------------
    -- Do some initialization
    -----------------------------------------------------
  
    n_alarmMsgId := floor( n_msgAlarm / 10 ) * 10;
    n_alarmOperation := n_msgAlarm - n_alarmMsgId;
    n_Threshold := -1;

  
    --------------------------------------------------------------------
    -- Extract data relative to the alarm from L_Activity table
    --------------------------------------------------------------------
  
    IF (n_msgActivity <> 0)
    THEN
      SELECT MOI, MOC, UserId, AddText, REPLACE( AddInfo, 'MSG: ', '' ), ObjOwnerRef INTO n_objRef, c_objClassTag, c_userId, c_logText, c_logInfo, n_objOwnerRef
      FROM EASYSHARE.L_ACTIVITY 
      WHERE LogRecId = n_msgActivity;

      BEGIN
        SELECT Param_VALUE INTO STRICT c_excluActivityMsg
        FROM EASYSHARE.D_ParamETER
        WHERE OBJ_REF = cn_admAlarmRef
          AND Param_TAG = '26';
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
        c_excluActivityMsg := '';
      END;
      
      n_return := EASYSHARE.StringToList (c_excluActivityMsg, ';');      
      -- Text related to the original Activity message
      BEGIN
        SELECT T2.AddText INTO STRICT c_logText
        FROM EASYSHARE.L_ACTIVITY T1,
             EASYSHARE.L_ACTIVITY T2
        WHERE T1.LogRecId = n_msgActivity
          AND T1.LogRecID_Parent > 0
          AND T2.LogRecId = T1.LogRecID_Parent
          AND CAST(T1.EVENTYPE AS VARCHAR) NOT IN (SELECT FIELDVALUE FROM ArrayList);
      EXCEPTION
        WHEN NO_DATA_FOUND THEN NULL;
      END;
    ELSE
      n_objRef      := CAST( EASYSHARE.GetTagValueFromList( '1F', c_msgParam ) AS INT4 );
      n_Severity    := CAST( EASYSHARE.GetTagValueFromList( 'SV', c_msgParam ) AS INT4 );
      n_Threshold   := CAST( COALESCE(NULLIF(EASYSHARE.GetTagValueFromList( 'TH', c_msgParam ), ''), '-1') AS INT4 );
      c_objClassTag := EASYSHARE.GetTagValueFromList( '1C', c_msgParam );
      c_logText     := EASYSHARE.GetTagValueFromList( '40', c_msgParam );
      c_logInfo     := EASYSHARE.GetTagValueFromList( '41', c_msgParam ) || cc_separator;
      c_msgObjectIdentifier := EASYSHARE.GetTagValueFromList( 'OI', c_msgParam );
      c_msgProbableCause    := EASYSHARE.GetTagValueFromList( 'PC', c_msgParam );
      c_msgSpecificProblem  := EASYSHARE.GetTagValueFromList( 'SP', c_msgParam );

      BEGIN
        SELECT OBJ_SIGNATURE INTO STRICT n_objOwnerRef
        FROM EASYSHARE.D_OBJECT 
        WHERE OBJ_REF = n_objRef
          AND OBJ_CLASS_TAG = c_objClassTag;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN 
        n_objOwnerRef := 15001;
      END;

      c_userId  := 'System';
    END IF;
  
    --------------------------------------------------------------------
    -- Get Alarm Type and Identifier
    --------------------------------------------------------------------
  <<ExitProcedure>>
  LOOP
    BEGIN
      SELECT R_AlarmID, R_AlarmType, R_AlarmActive, CASE WHEN c_msgSpecificProblem IS NULL THEN R_SpecificProblem ELSE c_msgSpecificProblem END INTO STRICT n_alarmID, n_alarmType, n_alarmActive, c_msgSpecificProblem
      FROM ALARM.R_Alarm 
      WHERE R_AlarmMsgID = n_alarmMsgId
        AND R_ObjClassTag = c_objClassTag;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE NOTICE ' Alarm Msg Cancelled : %, Action : %, Obj Ref: %, Obj Class : %, Add Text : %, Add Info : %', n_alarmMsgId, n_alarmOperation, n_objRef, c_objClassTag, c_logText, c_logInfo;
        EXIT ExitProcedure;
    END;
  
    IF ( n_alarmActive = 0 ) THEN
      RAISE NOTICE ' Alarm Inactive -> R_AlarmID : %', n_alarmID;
      EXIT ExitProcedure;
    END IF;
  
    ------------------------------------------------------------------------------------------------------------
    -- In case the specific problem contains some mask, fill the Specific problem with the tags retrieved
    -- from the Log Additional Information
    -- Replace <%FI> with the contents of FI tag in AddInfo 
    ------------------------------------------------------------------------------------------------------------
    IF ( c_msgSpecificProblem LIKE '%<!%__>%' ESCAPE '!' ) THEN
      WHILE ( c_msgSpecificProblem LIKE '%<!%__>%' ESCAPE '!' )
      LOOP
        c_tag := SUBSTRING( c_msgSpecificProblem, STRPOS( c_msgSpecificProblem, '<%' ) + 2, 2 );
        c_tagValue := EASYSHARE.GetTagValueFromList( c_tag, c_logInfo );
  
        c_msgSpecificProblem := REPLACE( c_msgSpecificProblem, '<%' || c_tag || '>', c_tagValue );
      END LOOP;
    END IF;    
  
    ------------------------------------------------------------------------------------------------------------
    -- Build the label related to the Alarm Object
    -- The label is built upon the OBJ_ID of D_OBJECT table in the general case.
    -- In case of multi-Parameter alarm, the alarm object name is concatenation of objects related to the alarm
    -- In case of Alarm Type 512, the object id uses the Parent OBJ_ID as a prefix
    ------------------------------------------------------------------------------------------------------------
    BEGIN
      SELECT OBJ_ID INTO STRICT c_objId
      FROM EASYSHARE.D_OBJECT 
      WHERE OBJ_REF = n_objRef
        AND OBJ_CLASS_TAG = c_objClassTag;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      c_objId := '';
    END;
    
    IF ( ( n_alarmType & 64 ) <> 0 ) 
    THEN
      c_objId := '';
    END IF;
    
    IF ( ( n_alarmType & 512 ) <> 0 ) 
    THEN
      SELECT T2.OBJ_ID || ' - ' || c_objId INTO c_objId
      FROM EASYSHARE.D_OBJECT   T1,
            EASYSHARE.D_OBJECT   T2
      WHERE T1.OBJ_REF = n_objRef
        AND T1.OBJ_CLASS_TAG = c_objClassTag
        AND T2.OBJ_REF = T1.OBJ_REF_PARENT
        AND T2.OBJ_CLASS_TAG NOT LIKE '-%';
    END IF;


    IF ( ( n_alarmType & 16 ) <> 0 ) THEN
      n_pos := STRPOS(c_logInfo, 'ALARM: ') + LENGTH('ALARM: ');
      IF ( n_pos <> 0 ) THEN
        c_string := SUBSTRING( c_logInfo, n_pos, LENGTH( c_logInfo ) - n_pos + 1 );
      END IF;
  
      WHILE ( LENGTH( c_string ) > 0 ) 
      LOOP 
        n_indexEnd := STRPOS(c_string, cc_separator) - 1;
        IF ( n_indexEnd > 0 ) THEN
          c_tag := SUBSTRING(c_string, 1, cn_tagLength);
          c_tagValue := SUBSTRING(c_string, 1 + cn_tagLength, n_indexEnd - cn_tagLength);
        END IF;
        c_string := AQSATOOLS.STUFF( c_string, 1, n_indexEnd + cn_separLength, NULL );
        IF ( c_tag = '1F' ) THEN
          SELECT CASE 
                   WHEN LENGTH(c_objId) > 0 THEN c_objId || ' - ' || OBJ_ID
                   ELSE OBJ_ID
                 END INTO c_objId
          FROM EASYSHARE.D_OBJECT 
          WHERE OBJ_REF = c_tagValue
            AND OBJ_CLASS_TAG NOT LIKE '-%';
        END IF;
        
        IF ( c_tag = '1E' ) THEN
          c_objId := CASE 
                       WHEN LENGTH(c_objId) > 0 THEN c_objId || ' - ' || c_tagValue
                       ELSE c_tagValue
                     END;
        END IF;
      END LOOP;
    END IF;
    
    -- Try to get LIID linked to the alarm in case of EQPT alarm
    IF ( ( n_alarmType & 8 ) <> 0 ) THEN
      n_pos := STRPOS( c_logInfo, 'ALARM LIID: ' );
      IF ( n_pos <> 0 ) THEN
        n_pos := n_pos + LENGTH( 'ALARM LIID: ' );
        c_string := SUBSTRING( c_logInfo, n_pos, LENGTH( c_logInfo ) - n_pos + 1 );
      END IF;
      
      WHILE ( LENGTH(c_string) > 0 ) 
      LOOP 
        n_indexEnd := STRPOS( c_string, cc_separator ) - 1;
        IF ( n_indexEnd > 0 ) THEN
          c_tag := SUBSTRING( c_string, 1, cn_tagLength );
          c_tagValue := SUBSTRING( c_string, 1 + cn_tagLength, n_indexEnd - cn_tagLength );
        END IF;
        
        c_string := AQSATOOLS.STUFF( c_string, 1, n_indexEnd + cn_separLength, NULL );
        
        IF ( c_tag = '21' ) THEN
          c_objId := c_objId || ' - LIID : ' || c_tagValue;
        ELSIF ( c_tag = 'RE' ) THEN
          c_objId := c_tagValue;
        END IF;
      END LOOP;
    END IF;
  
    -- Processing specific to Event Viewer Alarms
    IF ( c_objClassTag = '3F' ) THEN
      BEGIN
        SELECT R_SpecificProblem INTO STRICT c_systemSpecificProblem
        FROM ALARM.R_ALARM 
        WHERE R_AlarmID = n_msgAlarm / 10;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN NULL;
      END;
  
      IF ( c_systemSpecificProblem IS NULL OR c_systemSpecificProblem = '' OR c_systemSpecificProblem = '%SPECIFIC_PROBLEM' ) THEN
        c_systemSpecificProblem := c_objId;
      ELSE
        c_systemSpecificProblem := c_systemSpecificProblem || SUBSTRING( c_logInfo, STRPOS( c_logInfo, 'Object Identifier : ' ) + 20, LENGTH( c_logInfo ) - STRPOS( c_logInfo, 'Object Identifier : ' ) - 19 );
      END IF;
      c_logInfo := c_logInfo || ',';
      c_objId := SUBSTRING( c_logInfo, STRPOS( c_logInfo, 'Computer : ' ) || LENGTH( 'Computer : ' ), STRPOS( c_logInfo, ',', STRPOS( c_logInfo, 'Computer : ') ) - STRPOS( c_logInfo, 'Computer : ' ) - LENGTH( 'Computer : ' ) );
    END IF;
    
    -- Take into account of the case when the ALIS-M is required to be added before the service name
    BEGIN
      SELECT CAST( COALESCE( Param_VALUE, '0' ) AS INT4 ) INTO STRICT n_addALISMname
      FROM EASYSHARE.D_ParamETER 
      WHERE OBJ_REF = cn_admAlarmRef
        AND Param_TAG = '2M';
    EXCEPTION
       WHEN NO_DATA_FOUND THEN n_addALISMname := 0;
    END;
      
    IF ( n_addALISMname = 1 AND c_objClassTag = '5A' AND  n_objref < 2000 ) THEN
      c_objId := current_user || '::' || c_objId;
    END IF;
    
    RAISE NOTICE ' Alarm Msg : %, Action : %, Obj ID : %, Obj Class : %', n_alarmMsgId, n_alarmOperation, c_objId, c_objClassTag;

    -- Processing specific to License Alarms
    IF ( c_objClassTag = '36' ) THEN
      n_pos := STRPOS(c_logInfo, 'ALARM: ') + LENGTH('ALARM: ');
      IF ( n_pos <> 0 ) THEN
        c_string := SUBSTRING( c_logInfo, n_pos, LENGTH( c_logInfo ) - n_pos + 1 );
      END IF;
  
      WHILE ( LENGTH( c_string ) > 0 ) 
      LOOP 
        n_indexEnd := STRPOS(c_string, cc_separator) - 1;
        IF ( n_indexEnd > 0 ) THEN
          c_tag := SUBSTRING(c_string, 1, cn_tagLength);
          c_tagValue := SUBSTRING(c_string, 1 + cn_tagLength, n_indexEnd - cn_tagLength);
        END IF;
        c_string := AQSATOOLS.STUFF( c_string, 1, n_indexEnd + cn_separLength, NULL );
         
        IF ( c_tag = '1E' ) THEN
          c_objId := c_tagValue;
        END IF;
      END LOOP;
    END IF;

    ----------------------------------------------------------------------
    -- Get Alarm linked to the message
    ----------------------------------------------------------------------
    IF ( n_alarmOperation = 1 ) THEN
      --------------------------------------------------------------------------------
      -- Don't process the Alarm message if the object has been configured for not
      -- generating Alarms
      -- This process assumes that the Param Alias is : Alarm and Value is 'No' or 0
      --------------------------------------------------------------------------------
      BEGIN
        SELECT 1 INTO STRICT n_alarmCanceled
        FROM EASYSHARE.R_Class      T1,
             EASYSHARE.D_ParamETER  T2
        WHERE T1.Class_Tag = c_objClassTag
          AND T1.Class_ParamAlias = 'Alarm'
          AND T2.OBJ_REF = n_objRef
          AND T2.Param_TAG = T1.Param_Tag
          AND ( T2.Param_VALUE = '0' OR T2.Param_VALUE LIKE 'N%' );
          
        RAISE NOTICE 'Alarm is Canceled : Object is configured for no Alarm Generation';
        EXIT ExitProcedure;
      EXCEPTION
        WHEN OTHERS THEN NULL;
      END;
      --------------------------------------------------------------------------------
      -- Deal Alarm Activation
      -- Activation of Alarm results in deactivation of alarms of lower level (This is
      -- only true for Operational State Alarms)
      --------------------------------------------------------------------------------
      c_linkedObj := NULL;
      IF ( n_alarmMsgId = 100 ) THEN
        c_linkedObj := ALARM.Get_LinkedObj( n_objRef, -1 );
      END IF;
      
      IF ( NULLIF(c_linkedObj, '') IS NOT NULL ) THEN
        c_sqlQuery := 
          'UPDATE ALARM.D_Alarm
           SET D_ClearedDate = NOW(),
               D_ClearedBy = ''System''
		   FROM ALARM.R_Alarm       T2,
                EASYSHARE.D_OBJECT  T3
           WHERE D_Alarm.D_ObjRef IN  ( ' || c_linkedObj || ' )
             AND T2.R_AlarmID = D_Alarm.D_RAlarmID
             AND T2.R_ParamTagAudit = ''1J''
             AND T2.R_AlarmMsgID = 100
             AND T3.OBJ_REF = D_Alarm.D_ObjRef
             AND STRPOS(T3.OBJ_CLASS_TAG, ''-'') = 0
             AND T3.OBJ_CLASS_TAG <> ''5A''';
					  
        EXECUTE c_sqlQuery;
      END IF;
      
      -------------------------------------------------------------------------------------
      -- Alarm Activation messages are not taken into account if an alarm of higher level
      -- is already active on linked object
      -- This is true only for Operational State Alarms
      -------------------------------------------------------------------------------------
      c_linkedObj := NULL;
      IF ( n_alarmMsgId = 100 AND c_objClassTag NOT IN ( '5A' ) ) THEN
        c_linkedObj := ALARM.Get_LinkedObj( n_objRef, 1 );
      END IF;
  
      CREATE TEMPORARY TABLE IF NOT EXISTS tt_Mgt_LinkedObject
      (
        ObjRef INT4  NOT NULL
      );
      DELETE FROM tt_Mgt_LinkedObject;
	
      IF ( NULLIF(c_linkedObj, '') IS NOT NULL ) THEN
        c_sqlQuery := 'INSERT INTO tt_Mgt_LinkedObject( ObjRef )
                         SELECT T1.D_ObjRef
                         FROM ALARM.D_Alarm T1,
                              ALARM.R_Alarm T2
                         WHERE T1.D_ObjRef IN ( ' || c_linkedObj || ' )
                           AND T1.D_ClearedDate IS NULL
                           AND T2.R_AlarmID = T1.D_RAlarmID
                           AND T2.R_AlarmMsgID = 100';

        EXECUTE c_sqlQuery;
        GET DIAGNOSTICS n_rowcount = ROW_COUNT;
        
        IF ( n_rowCount <> 0 ) THEN
          RAISE NOTICE ' Alarm is ignored because higher level is active';
          DELETE FROM tt_Mgt_LinkedObject;
          EXIT ExitProcedure;
        END IF;
      END IF;
  
      -------------------------------------------------------------------------------------------------    
      -- Add the alarm -> If same alarm is already active on the same object, older alarm is cleared
      -- For some alarm types, severity is just updated...
      -------------------------------------------------------------------------------------------------    
  
      UPDATE ALARM.D_Alarm
      SET D_ClearedDate = NOW(),
          D_ClearedBy = c_userId
      FROM ALARM.R_Alarm T1
      WHERE D_Alarm.D_RAlarmID = n_alarmID
        AND D_Alarm.D_ObjRef = n_objRef
        AND ( ( n_alarmType & 16 ) = 0 OR D_Alarm.D_ObjId = c_objId )
        AND ( ( n_alarmType & 256 ) = 0 OR D_SpecificProblem = c_msgSpecificProblem )
        AND D_Alarm.D_ClearedDate IS NULL
        AND T1.R_AlarmID = D_ALARM.D_RAlarmID
        AND ( T1.R_AlarmType & 2 ) = 0
        AND ( T1.R_AlarmType & 32 ) = 0;

      -- Update severity of existing alarm for type 2 alarms...
      IF (n_msgActivity <> 0)
      THEN
        UPDATE ALARM.D_Alarm
        SET D_PerceivedSeverity = T3.Severity,
            D_ClearedDate = CASE 
                              WHEN T3.Severity = 0 THEN NOW()
                              ELSE D_ClearedDate
                            END,
            D_ClearedBy = CASE 
                            WHEN T3.Severity = 0 THEN 'Sytem'
                            ELSE D_ClearedBy
                          END,
            D_SpecificProblem = CASE 
                                  WHEN ( NULLIF(c_systemSpecificProblem, '') IS NOT NULL ) THEN c_systemSpecificProblem
                                  ELSE D_SpecificProblem
                                END,
            D_AdditionalInformation = c_logText,
            D_AlarmThreshold = n_Threshold
        FROM ALARM.R_Alarm        T2,
             EASYSHARE.L_ACTIVITY T3
        WHERE T2.R_AlarmID = D_Alarm.D_RAlarmID
          AND ( T2.R_AlarmType & 2 ) <> 0
          AND ( T2.R_AlarmType & 32 ) = 0
          AND T3.LogRecId = n_msgActivity 
          AND D_Alarm.D_RAlarmID = n_alarmID
          AND D_Alarm.D_ObjRef = n_objRef
          AND ( ( n_alarmType & 16 ) = 0 OR D_Alarm.D_ObjId = c_objId )
          AND ( ( n_alarmType & 256 ) = 0 OR D_SpecificProblem = c_msgSpecificProblem )
          AND D_Alarm.D_ClearedDate IS NULL;
      ELSE
        UPDATE ALARM.D_Alarm
        SET D_PerceivedSeverity = n_Severity,
            D_ClearedDate = CASE 
                              WHEN n_Severity = 0 THEN NOW()
                              ELSE D_ClearedDate
                            END,
            D_ClearedBy = CASE 
                            WHEN n_Severity = 0 THEN 'Sytem'
                            ELSE D_ClearedBy
                          END,
            D_SpecificProblem = CASE 
                                  WHEN ( NULLIF(c_systemSpecificProblem, '') IS NOT NULL ) THEN c_systemSpecificProblem
                                  ELSE D_SpecificProblem
                                END,
            D_AdditionalInformation = c_logText,
            D_AlarmThreshold = n_Threshold
        FROM ALARM.R_Alarm  T2
        WHERE T2.R_AlarmID = D_Alarm.D_RAlarmID
          AND ( T2.R_AlarmType & 2 ) <> 0
          AND ( T2.R_AlarmType & 32 ) = 0
          AND D_Alarm.D_RAlarmID = n_alarmID
          AND D_Alarm.D_ObjRef = n_objRef
          AND ( ( n_alarmType & 16 ) = 0 OR D_Alarm.D_ObjId = c_objId )
          AND ( ( n_alarmType & 256 ) = 0 OR D_SpecificProblem = c_msgSpecificProblem )
          AND D_Alarm.D_ClearedDate IS NULL;
      END IF;

      -- Insert new alarm if not already existing on same object
      INSERT INTO ALARM.D_Alarm( D_RALarmID, D_ObjClassTag, D_ObjRef, D_ObjId, D_ObjOwnerRef, D_EventType, D_EventDate, D_ClearedDate, D_ProbableCause, D_SpecificProblem, D_PerceivedSeverity, D_AdditionalInformation, D_AlarmThreshold )
        SELECT n_alarmID, c_objClassTag, n_objRef, 
               CASE 
                 WHEN NULLIF(c_msgObjectIdentifier, '') IS NOT NULL THEN c_msgObjectIdentifier
                 ELSE c_objId
               END ObjectId, 
               n_objOwnerRef, T1.R_AlarmCategory, NOW(), NULL, 
               CASE
                 WHEN NULLIF(c_msgProbableCause, '') IS NOT NULL THEN c_msgProbableCause
                 ELSE T1.R_ProbableCause
               END ProbableCause, 
               CASE 
                 WHEN NULLIF(c_msgSpecificProblem, '') IS NOT NULL THEN c_msgSpecificProblem
                 WHEN NULLIF(c_systemSpecificProblem, '') IS NOT NULL THEN c_systemSpecificProblem
                 ELSE T1.R_SpecificProblem
               END D_SpecificProblem,
               CASE 
                 WHEN ( c_objClassTag = '36' ) THEN n_Severity
                 WHEN ( ( T1.R_AlarmType & 32 ) = 0 ) THEN T1.R_DefaultSeverity
                 ELSE 0
               END PerceivedSeverity,
               c_logText,
               CASE
                 WHEN ( ( T1.R_AlarmType & 32 ) <> 0 ) THEN n_Severity
                 ELSE n_Threshold
               END
        FROM ALARM.R_Alarm T1
        WHERE T1.R_AlarmID = n_alarmID
          AND ( ( ( T1.R_AlarmType & 32 ) <> 0 )
                  OR NOT EXISTS ( SELECT D_AlarmID 
                                  FROM ALARM.D_Alarm 
                                  WHERE D_RAlarmID = n_alarmID
                                    AND D_ObjRef = n_objRef
                                    AND ( ( n_alarmType & 16 ) = 0 OR D_ObjId = c_objId )
                                    AND ( ( n_alarmType & 256 ) = 0 OR D_SpecificProblem = c_msgSpecificProblem )
                                    AND D_ClearedDate IS NULL 
                                ) 
              )
          RETURNING D_AlarmId INTO n_recordID;
			  
      GET DIAGNOSTICS n_rowcount = ROW_COUNT;
      IF ( n_rowcount <> 0 ) THEN
          
        -- In case of Type 2 alarm, take the severity from the Log Activity message
        IF (n_msgActivity <> 0)
        THEN
          UPDATE ALARM.D_Alarm
          SET D_PerceivedSeverity = T3.Severity 
          FROM ALARM.R_Alarm        T2,
               EASYSHARE.L_ACTIVITY T3
          WHERE T2.R_AlarmID = D_Alarm.D_RAlarmID
            AND ( T2.R_AlarmType & 2 ) <> 0
            AND T3.LogRecId = n_msgActivity 
            AND D_Alarm.D_AlarmID = n_recordID;
        ELSE
          UPDATE ALARM.D_Alarm
          SET D_PerceivedSeverity = n_Severity 
          FROM ALARM.R_Alarm  T2
          WHERE T2.R_AlarmID = D_Alarm.D_RAlarmID
            AND ( T2.R_AlarmType & 2 ) <> 0
            AND D_Alarm.D_AlarmID = n_recordID;
        END IF;
      END IF;
    ELSIF ( n_alarmOperation = 2 ) THEN
      ----------------------------------------------------------------------------------
      -- Deal Alarm Deactivation
      -- Deactivation of Operational State Alarm on Internal Module leads to activation
      -- of Operational State Alarm on linked objects (MSC)
      ----------------------------------------------------------------------------------
      IF ( c_objClassTag = '5A' ) 
      THEN
        c_linkedObj := NULL;
        c_linkedObj := ALARM.Get_LinkedObj( n_objRef, -1 );
        IF ( c_linkedObj IS NOT NULL AND c_linkedObj <> '' ) 
        THEN
          n_index := 1;
          n_linkedRef := CAST( COALESCE(NULLIF(EASYSHARE.GetItemFromList( n_index, c_linkedObj, ','), ''), '0') AS INT4 );
          WHILE ( n_linkedRef > 0 ) 
          LOOP 
            INSERT INTO ALARM.D_ALARM( D_RAlarmID, D_ObjClassTag, D_ObjRef, D_ObjId, D_ObjOwnerRef, D_EventType, D_EventDate, D_ClearedDate, D_ProbableCause, D_SpecificProblem, D_PerceivedSeverity, 
                                       D_AdditionalInformation, D_AlarmThreshold )
              SELECT T1.R_AlarmID, T1.R_ObjClassTag, T2.OBJ_REF, T2.OBJ_ID, T2.OBJ_SIGNATURE, T1.R_AlarmCategory, NOW(), NULL, T1.R_ProbableCause, T1.R_SpecificProblem, T1.R_DefaultSeverity,
                     NULL, n_Threshold
              FROM ALARM.R_Alarm      T1,
                   EASYSHARE.D_OBJECT T2
              WHERE T1.R_AlarmID = 200
                AND T1.R_AlarmActive = 1
                AND T2.OBJ_REF = n_linkedRef
                AND T2.OBJ_CLASS_TAG IN ( '43','47' )
                AND NOT EXISTS ( SELECT 1 
                                 FROM ALARM.D_ALARM T3
                                 WHERE T3.D_RAlarmID = T1.R_AlarmID
                                   AND T3.D_ObjRef = T2.OBJ_REF 
                               );
            n_index := n_index + 1;
            n_linkedRef := CAST( COALESCE(NULLIF(EASYSHARE.GetItemFromList( n_index, c_linkedObj, ',' ), ''), '0') AS INT4 );
          END LOOP;
        END IF;
      END IF;
      
      UPDATE ALARM.D_Alarm
      SET D_ClearedDate = NOW(),
          D_ClearedBy = c_userId
      WHERE D_RAlarmID = n_alarmID
        AND D_ObjRef = n_objRef
        AND ( ( n_alarmType & 16 ) = 0 OR D_ObjId = c_objId )
        AND ( ( n_alarmType & 256 ) = 0 OR D_SpecificProblem = c_msgSpecificProblem )
        AND D_ClearedDate IS NULL;
    END IF;

    EXIT ExitProcedure;
  END LOOP; --<<ExitProcedure>>
    RAISE NOTICE 'ALARM.Deal_AlarmMsg % : End', c_pkgVersion;
    RETURN 0;
  END
$$ LANGUAGE 'plpgsql';

 SELECT  AQSATOOLS.StoreFunctionPkg ( 'ALARM', 'pg_Alarm_Mgt', 'Deal_AlarmMsg' );
 
/*
  ===========================================================================
  -- ACTIVATION / DEACTIVATION OF ALARMS LINKED TO AN ALARM MESSAGE 
  ===========================================================================
*/

DROP FUNCTION IF EXISTS ALARM.Deal_PostAlarm;
CREATE FUNCTION ALARM.Deal_PostAlarm
  (
    n_alarmType        INT4    DEFAULT NULL,
    n_previousSeverity INT4    DEFAULT NULL,
    n_currentSeverity  INT4    DEFAULT NULL,
    n_objRef           INT4    DEFAULT NULL,
    c_objClass         VARCHAR DEFAULT NULL
  )
RETURNS INT4 AS $$

  DECLARE
    n_flagActive           INT4;
    c_customerName         VARCHAR(60);
    c_alarmSpecificProblem VARCHAR(255);
    c_objClassId           VARCHAR(30);
    c_objId                VARCHAR(255);
    c_connectTime          VARCHAR(30);
  
  BEGIN
    
    n_flagActive := 0;
    
    BEGIN
      SELECT OBJ_ID INTO STRICT c_customerName
      FROM EASYSHARE.D_OBJECT 
      WHERE OBJ_REF = 100;
    EXCEPTION
       WHEN NO_DATA_FOUND THEN NULL;
    END;
  
    RETURN 0;
  END
$$ LANGUAGE 'plpgsql';

 SELECT  AQSATOOLS.StoreFunctionPkg ( 'ALARM', 'pg_Alarm_Mgt', 'Deal_PostAlarm' );
 
/*
  ===========================================================================
  -- FORWARD ALARM MESSAGE TO EXTERNAL ALARM MANAGEMENT TOOL 
  ===========================================================================
*/

DROP FUNCTION IF EXISTS ALARM.Forward_Alarm;
CREATE FUNCTION ALARM.Forward_Alarm
  (
    n_alarmRef    INT4    DEFAULT NULL,
    n_alarmDefRef INT4    DEFAULT NULL,
    c_OperationCode      VARCHAR DEFAULT NULL,
    n_severity    INT4    DEFAULT NULL 
  ) 
RETURNS INT4 AS $$

  DECLARE
    d_currentDate           TIMESTAMP;
    c_daysOff               VARCHAR(255);
    c_formatedCurrentDate   VARCHAR(30);
    c_forwardBeginHour      VARCHAR(30);
    c_forwardEndHour        VARCHAR(30);
    c_serverName            VARCHAR(100);
    c_ObjId                 VARCHAR(255);
    cc_separator            VARCHAR(4);
    n_ObjRef                INT4;
    n_alarmSeverity         INT4;
    n_comAlarmRef           INT4;
    n_forwardAlarm          INT4;      -- 0 : Alarm Discarded  -  1 : Alarm Forwarded
    n_forwardAlarmReference INT4;
    n_forwardAlarmThreshold INT4;
    n_InvokeId              INT4;
    n_optionForwardAlarm    INT4;      -- 0 : Never Forward  - 1 : Always Forward  -  2 : Conditionnal Forward
    n_return                INT4;
    n_rowcount              INT4;
    cn_admAlarmRef          INT4;
  
  BEGIN
    n_alarmSeverity := n_severity;
    cn_admAlarmRef := 121;
    cc_separator := '~|';

    ---------------------------------------------------------------------
    -- Get Alarm Redirection option
    ---------------------------------------------------------------------
  
    BEGIN
      SELECT CAST( Param_VALUE AS INT4 ) INTO STRICT n_optionForwardAlarm
      FROM EASYSHARE.D_ParamETER 
      WHERE OBJ_REF = cn_admAlarmRef
        AND Param_TAG = '20';
    EXCEPTION
       WHEN NO_DATA_FOUND THEN
         n_optionForwardAlarm := 0;
    END;
  
    IF ( n_optionForwardAlarm = 0 ) THEN
       RETURN 0;
    END IF;
     
    d_currentDate := NOW();
     
    BEGIN
      SELECT OBJ_REF INTO STRICT n_comAlarmRef
      FROM EASYSHARE.D_OBJECT 
      WHERE OBJ_CLASS_TAG = '5A'
        AND UPPER(OBJ_ID) = 'AQSA_COMALARM'
        AND OBJ_STATES NOT LIKE '1%';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        BEGIN
          SELECT OBJ_REF INTO STRICT n_comAlarmRef
          FROM EASYSHARE.D_OBJECT 
          WHERE OBJ_CLASS_TAG = '5A'
            AND UPPER(OBJ_ID) LIKE '%::AQSA_COMALARM'
            AND OBJ_REF > x'100000'::int4
            AND OBJ_STATES NOT LIKE '1%';
        EXCEPTION
           WHEN NO_DATA_FOUND THEN
             RAISE NOTICE ' AQSA_ComAlarm module locked -> Alarm is not redirected';
             RETURN -1;
        END;
    END;
  
    --------------------------------------------------------------------
    -- If Alarm redirection is not systematic, look if this alarm has
    -- to be redirected now...
    --------------------------------------------------------------------
  <<ForwardOrDiscardAlarm>>
  LOOP
    IF ( n_optionForwardAlarm = 1 ) THEN
      n_forwardAlarm := 1;
    ELSE
      n_forwardAlarm := 0;
      
      -- Check Alarm Forwarding based on the current alarm severity
      SELECT  R_ExternalRef, R_ExternalSeverityThreshold INTO n_forwardAlarmReference, n_forwardAlarmThreshold
      FROM ALARM.R_Alarm
      WHERE R_AlarmID = n_alarmDefRef;

      -- Dans le cas d'une alarme de fin (@STROperationCode = 'FE'), il faut rechercher
      -- la severite de l'alarme d'origine pour savoir si elle doit etre renvoyee
      IF ( c_OperationCode = 'FE' ) THEN
        BEGIN
          SELECT T1.D_PerceivedSeverity INTO STRICT n_alarmSeverity
          FROM tt_Mgt_DAlarm         T1        -- Ex alarmes courantes avant suppression
          WHERE T1.D_AlarmID = n_alarmRef;
        EXCEPTION
           WHEN NO_DATA_FOUND THEN n_alarmSeverity := 0;
        END;
      END IF;

      IF ( n_forwardAlarmReference = 0 OR n_forwardAlarmThreshold > 4 OR n_forwardAlarmThreshold > n_alarmSeverity ) THEN
        EXIT ForwardOrDiscardAlarm;
      END IF;
      
      -- Check Alarm Forwarding based on the current time
      BEGIN
        SELECT Param_VALUE INTO STRICT c_forwardBeginHour
        FROM EASYSHARE.D_ParamETER 
        WHERE OBJ_REF = cn_admAlarmRef
          AND Param_TAG = '21';
  
        SELECT Param_VALUE 
        INTO STRICT c_forwardEndHour
        FROM EASYSHARE.D_ParamETER 
        WHERE OBJ_REF = cn_admAlarmRef
          AND Param_TAG = '22';
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
           c_forwardBeginHour := '';
           c_forwardEndHour := '';
      END;
  
      c_formatedCurrentDate := TO_CHAR( d_currentDate, 'HH24:MI:SS' );
  
      IF ( ( c_forwardBeginHour < c_forwardEndHour AND c_formatedCurrentDate BETWEEN c_forwardBeginHour AND c_forwardEndHour )
        OR ( c_forwardBeginHour > c_forwardEndHour AND c_formatedCurrentDate NOT BETWEEN c_forwardEndHour AND c_forwardBeginHour ) ) THEN
  
        RAISE NOTICE ' Off hour dectected : Alarm forwarded to External Alarm Management';
        n_forwardAlarm := 1;
        EXIT ForwardOrDiscardAlarm;
      END IF;
      
      -- Check Alarm Forwarding due to Week-End
      BEGIN
        SELECT Param_VALUE INTO STRICT c_daysOff
        FROM EASYSHARE.D_ParamETER 
        WHERE OBJ_REF = cn_admAlarmRef
          AND Param_TAG = '25';
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
           c_daysOff := '';
      END;
  
      c_formatedCurrentDate := TO_CHAR( d_currentDate, 'd' );
      IF ( STRPOS(c_daysOff, c_formatedCurrentDate, 1) <> 0 ) THEN
        RAISE NOTICE ' Off day (week-end) : Alarm forwarded to External Alarm Management';
        n_forwardAlarm := 1;
        EXIT ForwardOrDiscardAlarm;
      END IF;
      
      -- Check Alarm Forwarding due to Fixed Day Off
      BEGIN
        SELECT Param_VALUE INTO STRICT c_daysOff
        FROM EASYSHARE.D_ParamETER 
        WHERE OBJ_REF = cn_admAlarmRef
          AND Param_TAG = '30';
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
           c_daysOff := '';
      END;
  
      c_formatedCurrentDate := TO_CHAR( d_currentDate, 'MM/DD' );
      IF ( STRPOS( c_daysOff, c_formatedCurrentDate, 1 ) <> 0 ) THEN
        RAISE NOTICE ' Fixed Off day (%) : Alarm forwarded to External Alarm Management', c_formatedCurrentDate;
        n_forwardAlarm := 1;
        EXIT ForwardOrDiscardAlarm;
      END IF;
  
      -- Check Alarm Forwarding due to Not Fixed Day Off
      BEGIN
        SELECT Param_VALUE INTO STRICT c_daysOff
        FROM EASYSHARE.D_ParamETER 
        WHERE OBJ_REF = cn_admAlarmRef
          AND Param_TAG = '31';
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
           c_daysOff := '';
      END;
  
      c_formatedCurrentDate := TO_CHAR( d_currentDate, 'YYYY/MM/DD' );
      IF ( STRPOS( c_daysOff, c_formatedCurrentDate, 1 ) <> 0 ) THEN
        RAISE NOTICE ' Fixed Off day (%) : Alarm forwarded to External Alarm Management', c_formatedCurrentDate;
        n_forwardAlarm := 1;
        EXIT ForwardOrDiscardAlarm;
      END IF;
    END IF;

    EXIT ForwardOrDiscardAlarm;	
  END LOOP; --<<ForwardOrDiscardAlarm>>
    -----------------------------------------------------------------------------
    -- Redirect the Alarm message to External module via D_FLUX
    -----------------------------------------------------------------------------
  
    IF ( n_forwardAlarm = 1 ) THEN
      -----------------------------------------------------------------
      -- Get the option of addition of server name
      -----------------------------------------------------------------
      BEGIN
        SELECT T2.Param_VALUE INTO STRICT c_serverName
        FROM EASYSHARE.D_ParamETER T1,
             EASYSHARE.D_ParamETER T2
        WHERE T1.OBJ_REF = cn_admAlarmRef
          AND T2.OBJ_REF = cn_admAlarmRef
          AND T1.Param_TAG = '2S'
          AND T2.Param_TAG = '2T'
          AND T1.Param_VALUE LIKE 'Y%';
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
           c_serverName := '';
      END;
   
      IF ( LENGTH(c_serverName) > 0 ) THEN
        IF ( c_OperationCode = x'FF'::int4 ) THEN
          SELECT T1.D_ObjID, T1.D_ObjRef INTO c_ObjId, n_ObjRef
          FROM ALARM.D_ALARM   T1,
               ALARM.R_ALARM   T2
          WHERE T1.D_AlarmID = n_alarmRef
            AND T2.R_AlarmID = T1.D_RAlarmID;
        ELSE
          SELECT T1.D_ObjID, T1.D_ObjRef INTO c_ObjId,  n_ObjRef
           FROM tt_Mgt_DAlarm  T1,
                ALARM.R_ALARM T2
           WHERE T1.D_AlarmID = n_alarmRef
             AND T2.R_AlarmID = T1.D_RAlarmID;
        END IF;
        
        -- Find the correspond server hostname in the list after '::'
        IF ( n_ObjRef < x'100000'::int4 ) THEN
           -- Alis-M
           c_ObjId := 'ALIS-M::';
        ELSE
           c_ObjId := CASE 
                        WHEN STRPOS(c_ObjId, '::') > 0 THEN SUBSTRING( c_ObjId, 1, STRPOS(c_ObjId, '::') || 1 )
                        ELSE c_ObjId || '::'
                      END;
        END IF;
        c_serverName := EASYSHARE.GetTagValueFromList( c_ObjId, c_serverName, ';' );
      END IF;
  
      -- Send the alarm
      IF ( c_OperationCode = 'FF' ) THEN
        n_return := ALARM.List_DestAlarm (n_alarmDefRef);
        n_InvokeId := 2 * n_alarmRef;
        
        INSERT INTO EASYSHARE.D_FLUX( AppFrom, AppTo, TransactionType, ComponentType, InvokeId, OperationCode, Param, DefenseClass )
          SELECT cn_admAlarmRef, n_comAlarmRef, x'65'::int4, x'91'::int4, n_InvokeId, x'FF'::int4,
                 '1F' || CAST( n_alarmRef AS VARCHAR ) || cc_separator ||
                 'SV' || CAST( n_alarmSeverity AS VARCHAR ) || cc_separator ||
                 'NB' || CAST( T2.R_ExternalRef AS VARCHAR ) || cc_separator ||
                 'MS' || CASE 
                           WHEN LENGTH( c_serverName ) > 0 THEN c_serverName || ' - ' 
                            ELSE ''
                         END || T1.D_SpecificProblem || ' - ' || T1.D_ObjID || cc_separator ||
                 'DT' || TO_CHAR( T1.D_EventDate, 'YYYY/MM/DD HH24:MI:SS' ) || cc_separator ||
                 'TE' || T3.AlarmTechno || cc_separator ||
                 'TO' || T3.AlarmDest || cc_separator ||
                 '1E' || T2.R_SpecificProblem || cc_separator ||
                 'OB' || T1.D_ObjID || cc_separator ||
                 'CL' || T1.D_ObjClassTag || cc_separator ||
                 'RE' || CAST(T1.D_ObjRef AS VARCHAR) || cc_separator ||
                 'TY' || CAST(ALARM.GetAlarmType(T2.R_AlarmCategory) AS VARCHAR ) || cc_separator ||
                 'CA' || T2.R_ProbableCause || cc_separator ||
                 'SP' || T1.D_AdditionalInformation || cc_separator ||
                 'MT' || 'ALARM' || cc_separator ||
                 'TH' || CASE 
                           WHEN T1.D_AlarmThreshold IS NULL THEN 'N/A'
                           WHEN T1.D_AlarmThreshold = -1 THEN 'N/A'
                           ELSE CAST(T1.D_AlarmThreshold AS VARCHAR)
                         END || cc_separator,
                 3 
          FROM ALARM.D_ALARM T1,
               ALARM.R_ALARM T2,
               TT_MGT_ForwardAlarm T3
          WHERE T1.D_AlarmID = n_alarmRef
            AND T2.R_AlarmID = T1.D_RAlarmID
            AND T3.R_AlarmID = T2.R_AlarmID;
           
        GET DIAGNOSTICS n_rowcount = ROW_COUNT;
        IF ( n_rowcount <> 0 ) THEN
          RAISE NOTICE ' Alarm Creation Redirected to External Module';
        END IF;
      ELSE
        n_return := ALARM.List_DestAlarm (n_alarmDefRef);
        n_InvokeId := 2 * n_alarmRef + 1;
  
        INSERT INTO EASYSHARE.D_FLUX( AppFrom, AppTo, TransactionType, ComponentType, InvokeId, OperationCode, Param, DefenseClass )
          SELECT cn_admAlarmRef, n_comAlarmRef, x'65'::int4, x'91'::int4, n_InvokeId, x'FE'::int4,
                 '1F' || CAST( n_alarmRef AS VARCHAR ) || cc_separator ||
                 'SV0' || cc_separator ||
                 'NB' || CAST( T2.R_ExternalRef AS VARCHAR ) || cc_separator ||
                 'MS' || CASE 
                           WHEN LENGTH( c_serverName ) > 0 THEN c_serverName || ' - ' 
                           ELSE ''
                         END || T1.D_SpecificProblem || ' - ' || T1.D_ObjID || cc_separator ||
                 'DT' || TO_CHAR( NOW(), 'YYYY/MM/DD HH24:MI:SS' ) || cc_separator ||
                 'TE' || T3.AlarmTechno || cc_separator ||
                 'TO' || T3.AlarmDest || cc_separator ||
                 '1E' || T2.R_SpecificProblem || cc_separator ||
                 'OB' || T1.D_ObjID || cc_separator ||
                 'CL' || T1.D_ObjClassTag || cc_separator ||
                 'RE' || CAST(T1.D_ObjRef AS VARCHAR) || cc_separator ||
                 'TY' || CAST(ALARM.GetAlarmType(T2.R_AlarmCategory) AS VARCHAR) || cc_separator ||
                 'CA' || T2.R_ProbableCause || cc_separator ||
                 'MT' || 'ALARM' || cc_separator ||
                 'TH' || 'N/A' || cc_separator,
                 3 
          FROM tt_Mgt_DAlarm     T1,
               ALARM.R_ALARM     T2,
               TT_MGT_ForwardAlarm T3
          WHERE T1.D_AlarmID = n_alarmRef
            AND T2.R_AlarmID = T1.D_RAlarmID
            AND T3.R_AlarmID = T2.R_AlarmID;
  
        GET DIAGNOSTICS n_rowcount = ROW_COUNT;
        IF ( n_rowcount <> 0 ) THEN
          RAISE NOTICE ' Alarm Deletion Redirected to External Module';
        END IF;
      END IF;
    END IF;
    RETURN 0;
  END
$$ LANGUAGE 'plpgsql';

 SELECT  AQSATOOLS.StoreFunctionPkg ( 'ALARM', 'pg_Alarm_Mgt', 'Forward_Alarm' );
 
/*
  ===========================================================================
  -- RETURNS ALARM'S CODE RELATIVE TO ALARM'S CATEGORY
  ===========================================================================
*/

DROP FUNCTION IF EXISTS ALARM.GetAlarmType;
CREATE FUNCTION ALARM.GetAlarmType
 (
    c_AlarmCategory  VARCHAR
 )
RETURNS INT4 AS $$

  DECLARE
    c_inCategory    VARCHAR(50);
    n_AlarmType     INT4;

  BEGIN  
    c_inCategory := UPPER( c_AlarmCategory );

    IF    ( c_inCategory LIKE 'INTERCEPT%' )
    THEN
      n_AlarmType := 1;
    ELSIF ( c_inCategory LIKE 'IAP%' )
    THEN
      n_AlarmType := 2;
    ELSIF ( c_inCategory LIKE 'MEDIATION%' )
    THEN
      n_AlarmType := 3;
    ELSIF ( c_inCategory LIKE 'COLLECTION%' )
    THEN
      n_AlarmType := 4;
    ELSIF ( c_inCategory LIKE 'SYSTEM%' )
    THEN
      n_AlarmType := 5;
    ELSIF ( c_inCategory LIKE 'LICENSING%' )
    THEN
      n_AlarmType := 6;
    ELSE
      n_AlarmType := 5;
    END IF;
    
    RETURN ( n_AlarmType );
  END
$$ LANGUAGE 'plpgsql';

 SELECT  AQSATOOLS.StoreFunctionPkg ( 'ALARM', 'pg_Alarm_Mgt', 'GetAlarmType' );
 
/*
  ===========================================================================
  -- GET OBJECTS LINKED TO THE PROVIDED ONE (PARENTS OR CHILDREN)
  ===========================================================================
*/

DROP FUNCTION IF EXISTS ALARM.Get_LinkedObj;
CREATE FUNCTION ALARM.Get_LinkedObj
 (
    n_objRef    INT4 DEFAULT NULL,
    n_way       INT4 DEFAULT 1
    --c_LinkedObj OUT VARCHAR2
 )
RETURNS VARCHAR AS $$

  DECLARE
    c_string       VARCHAR(4000);
    c_LinkedObj    VARCHAR(4000);
    n_linkedObjRef INT4;
    n_treeLevel    INT4;
    n_rowcount     INT4;
    tuple          RECORD;
  
  BEGIN
  
    ---------------------------------------------------------------------------
    -- Insert the object itself in the tree window
    ---------------------------------------------------------------------------
    n_treeLevel := 0;
    
    CREATE TEMPORARY TABLE IF NOT EXISTS tt_Mgt_TreeObject
    (
      ObjRef    INT4 NOT NULL,
      TreeLevel INT4 NOT NULL
    );

    DELETE FROM tt_Mgt_TreeObject;
    INSERT INTO tt_Mgt_TreeObject( ObjRef, TreeLevel )
      SELECT OBJ_REF, n_treeLevel 
      FROM EASYSHARE.D_OBJECT 
      WHERE OBJ_REF = n_objRef
        AND STRPOS(OBJ_CLASS_TAG, '-') = 0;
        
    ---------------------------------------------------------------------------
    -- Insert parents / children depending on the way
    ---------------------------------------------------------------------------
    GET DIAGNOSTICS n_rowcount = ROW_COUNT;
    WHILE ( n_rowcount > 0 ) 
    LOOP 
        
      n_treeLevel := n_treeLevel + 1;
      IF ( n_way > 0 ) THEN
        -- Get Parents
        INSERT INTO tt_Mgt_TreeObject( ObjRef, TreeLevel )
          SELECT DISTINCT T2.OBJ_REF_PARENT, n_treeLevel 
          FROM tt_Mgt_TreeObject   T1,
               EASYSHARE.D_OBJECT T2
          WHERE T1.ObjRef = T2.OBJ_REF
            AND T2.OBJ_REF_PARENT NOT IN ( SELECT ObjRef FROM tt_Mgt_TreeObject );
  
      ELSIF ( n_way < 0 ) THEN
        -- Get Children
        INSERT INTO tt_Mgt_TreeObject( ObjRef, TreeLevel )
          SELECT DISTINCT T2.OBJ_REF, n_treeLevel 
          FROM tt_Mgt_TreeObject   T1,
               EASYSHARE.D_OBJECT T2
          WHERE T1.ObjRef = T2.OBJ_REF_PARENT
            AND T2.OBJ_REF NOT IN ( SELECT ObjRef FROM tt_Mgt_TreeObject  );
      END IF;
      GET DIAGNOSTICS n_rowcount = ROW_COUNT;
    END LOOP;
    
    c_string := '';
    FOR tuple IN ( SELECT ObjRef FROM tt_Mgt_TreeObject WHERE ObjRef <> n_objRef )
    LOOP
      c_string := c_string || CAST( tuple.ObjRef AS VARCHAR ) || ', ';
    END LOOP;
  
    -- Remove terminating separator
    IF ( LENGTH( c_string ) > 2  ) THEN
      c_LinkedObj := SUBSTRING( c_string, 1, LENGTH( c_string ) - 2 );
    END IF;
    RETURN ( c_LinkedObj );
  END
$$ LANGUAGE 'plpgsql';

 SELECT  AQSATOOLS.StoreFunctionPkg ( 'ALARM', 'pg_Alarm_Mgt', 'Get_LinkedObj' );
 
/*
  ===========================================================================
  --  ALARM LOG PURGE
  ===========================================================================
*/

DROP FUNCTION IF EXISTS ALARM.L_AlarmPurge;
CREATE FUNCTION ALARM.L_AlarmPurge
 (
    n_delay    INT4 DEFAULT 30,
    n_maxAlarm INT4 DEFAULT 10000
 )
RETURNS INT4 AS $$

  DECLARE
    n_nbrMsg      INT4;
    n_nbrMsgDel   INT4;
    n_nbrMsgMax   INT4;
    n_delOnNumber INT4;
    n_delOnDate   INT4;
    d_firstDelete TIMESTAMP;
    d_begin       TIMESTAMP;
    c_pkgVersion  VARCHAR(10);
    
  
  BEGIN
     c_pkgVersion := AQSATOOLS.GetPkgVersion ('ALARM', 'pg_Alarm_Mgt');
     d_begin := NOW();-- Utilise a des fins statistiques
  
     -- Max Number of messages that will be deleted
     n_nbrMsgMax := 1000;
     
     -- Get number of messages in the Log Table
     SELECT COUNT( L_AlarmID ) INTO n_nbrMsg
     FROM ALARM.L_Alarm;
     
     ----------------------------------------------
     -- Get the number of messages to delete
     ----------------------------------------------
     n_nbrMsgDel := CASE 
                      WHEN ( (n_nbrMsg - n_maxAlarm) > 0 ) AND ( (n_nbrMsg - n_maxAlarm) < n_nbrMsgMax ) THEN n_nbrMsg - n_maxAlarm
                      WHEN ( (n_nbrMsg - n_maxAlarm) > 0 ) AND ( (n_nbrMsg - n_maxAlarm) > n_nbrMsgMax ) THEN n_nbrMsgMax
                      ELSE 0
                    END;
                    
    ---------------------------------------------------------
    -- Process the Purge...
    ---------------------------------------------------------
  
    IF ( n_nbrMsgDel > 0 ) THEN
      -- Get the Date of the first Record to Delete
      SELECT MAX( L_ClearedDate ) INTO d_firstDelete FROM ( SELECT L_ClearedDate FROM Alarm.L_Alarm ORDER BY L_ClearedDate ) T1 LIMIT n_nbrMsgDel;
      DELETE FROM ALARM.L_Alarm WHERE L_ClearedDate <= d_firstDelete;
      
      GET DIAGNOSTICS n_delOnNumber = ROW_COUNT;
    ELSE
      DELETE FROM Alarm.L_Alarm
      WHERE L_ClearedDate + n_delay * INTERVAL '1 day' < NOW();
        GET DIAGNOSTICS n_delOnDate = ROW_COUNT;
     END IF;
  
  
     --------------------------------------------------------------
     -- Terminate the procedure
     --------------------------------------------------------------
     IF ( n_delOnNumber > 0
       OR n_delOnDate > 0 ) THEN
     
        RAISE NOTICE '% Deleted on Number, % Deleted on Date', n_delOnNumber, n_delOnDate;
        RAISE NOTICE 'ALARM.L_AlarmPurge % : End', c_pkgVersion;
     END IF;
     RETURN 0;
  END
$$ LANGUAGE 'plpgsql';

 SELECT  AQSATOOLS.StoreFunctionPkg ( 'ALARM', 'pg_Alarm_Mgt', 'L_AlarmPurge' );
 
/*
  ===========================================================================
  --  PREPARE THE ALARMS RECIPIENTS LIST
  ===========================================================================
*/

DROP FUNCTION IF EXISTS ALARM.List_DestAlarm;
CREATE FUNCTION ALARM.List_DestAlarm
 (
    n_alarmID        INT4
 )
RETURNS INT4 AS $$

  DECLARE
    c_destAlarmList  VARCHAR(255);
    c_destAlam       VARCHAR(100);
    n_sendOption     INT4;
    n_countDest      INT4;

  BEGIN
    -------------------------------------------------------------------
    -- Initialization of some useful data
    -------------------------------------------------------------------
    CREATE TEMPORARY TABLE IF NOT EXISTS TT_MGT_ForwardAlarm
    (
      R_AlarmID       INT4           NOT NULL,
      AlarmTechno     VARCHAR(10)    NOT NULL,
      AlarmDest       VARCHAR(100)   NULL
    );
    DELETE FROM TT_MGT_ForwardAlarm;

    -------------------------------------------------------------------
    -- Filling of TT_MGT_ForwardAlarm table
    -------------------------------------------------------------------
    c_destAlarmList := '';

    -- Check mail list
    BEGIN
      SELECT 1 INTO STRICT n_sendOption
      FROM ALARM.R_AlarmForward
      WHERE R_AlarmID = n_alarmID
        AND R_SendMail LIKE 'Y%';
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      n_sendOption := 0;
    END;

    IF (n_sendOption = 1)
    THEN
      SELECT R_MailAddress INTO c_destAlarmList
      FROM ALARM.R_AlarmForward
      WHERE R_AlarmID = n_alarmID;

      n_countDest := EASYSHARE.GetNumberOfItemsInList (c_destAlarmList, ';');
      IF (n_countDest > 0)
      THEN
        FOR i IN 1 .. n_countDest
        LOOP
          c_destAlam := EASYSHARE.GetItemFromList(i, c_destAlarmList, ';');
          INSERT INTO TT_MGT_ForwardAlarm (R_AlarmID, AlarmTechno, AlarmDest)
          VALUES (n_alarmID, 'SMTP', c_destAlam);
        END LOOP;
      END IF;
    END IF;

    c_destAlarmList := '';

    -- Check SNMP list
    BEGIN
      SELECT 1 INTO STRICT n_sendOption
      FROM ALARM.R_AlarmForward
      WHERE R_AlarmID = n_alarmID
        AND R_SendSnmp LIKE 'Y%';
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      n_sendOption := 0;
    END;

    IF (n_sendOption = 1)
    THEN
      SELECT R_SnmpServer INTO c_destAlarmList
      FROM ALARM.R_AlarmForward
      WHERE R_AlarmID = n_alarmID;

      n_countDest := EASYSHARE.GetNumberOfItemsInList (c_destAlarmList, ';');
      IF (n_countDest > 0)
      THEN
        FOR i IN 1 .. n_countDest
        LOOP
          c_destAlam := EASYSHARE.GetItemFromList(i, c_destAlarmList, ';');
          INSERT INTO TT_MGT_ForwardAlarm (R_AlarmID, AlarmTechno, AlarmDest)
          VALUES (n_alarmID, 'NETSNMP', c_destAlam);
        END LOOP;
      END IF;
    END IF;

    c_destAlarmList := '';

    -- Check SMS list
    BEGIN
      SELECT 1 INTO STRICT n_sendOption
      FROM ALARM.R_AlarmForward
      WHERE R_AlarmID = n_alarmID
        AND R_SendSms LIKE 'Y%';
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      n_sendOption := 0;
    END;

    IF (n_sendOption = 1)
    THEN
      SELECT R_SmsAddress INTO c_destAlarmList
      FROM ALARM.R_AlarmForward
      WHERE R_AlarmID = n_alarmID;

      n_countDest := EASYSHARE.GetNumberOfItemsInList (c_destAlarmList, ';');
      IF (n_countDest > 0)
      THEN
        FOR i IN 1 .. n_countDest
        LOOP
          c_destAlam := EASYSHARE.GetItemFromList(i, c_destAlarmList, ';');
          INSERT INTO TT_MGT_ForwardAlarm (R_AlarmID, AlarmTechno, AlarmDest)
          VALUES (n_alarmID, 'SMS', c_destAlam);
        END LOOP;
      END IF;
    END IF;

    c_destAlarmList := '';

    -- Check Syslog list
    BEGIN
      SELECT 1 INTO STRICT n_sendOption
      FROM ALARM.R_AlarmForward
      WHERE R_AlarmID = n_alarmID
        AND R_SendSyslog LIKE 'Y%';
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      n_sendOption := 0;
    END;

    IF (n_sendOption = 1)
    THEN
      SELECT R_SyslogServer INTO c_destAlarmList
      FROM ALARM.R_AlarmForward
      WHERE R_AlarmID = n_alarmID;

      n_countDest := EASYSHARE.PSTRING.GetNumberOfItemsInList (c_destAlarmList, ';');
      IF (n_countDest > 0)
      THEN
        FOR i IN 1 .. n_countDest
        LOOP
          c_destAlam := EASYSHARE.PSTRING.GetItemFromList(i, c_destAlarmList, ';');
          INSERT INTO TT_MGT_ForwardAlarm (R_AlarmID, AlarmTechno, AlarmDest)
          VALUES (n_alarmID, 'SYSLOG', c_destAlam);
        END LOOP;
      END IF;
    END IF;
 
    RETURN 0;
  END
$$ LANGUAGE 'plpgsql';

 SELECT  AQSATOOLS.StoreFunctionPkg ( 'ALARM', 'pg_Alarm_Mgt', 'List_DestAlarm' );
 
 /*
  ===========================================================================
  --  CALLS MSG_REFRESH FUNCTION IF NOT IN HTTP/REST MODE
  ===========================================================================
*/

DROP FUNCTION IF EXISTS ALARM.Msg_RefreshAlarm;
CREATE FUNCTION ALARM.Msg_RefreshAlarm
 (
    n_logRecId            int4,
    ic_Operation          varchar default 'FA',   -- Mise a jour journal
    ic_Moc                varchar default NULL,
    in_Moi                int4    default NULL,
    n_refresh             int4    default 1,      -- Fenetres a rafraichir
    in_ownerRef           int4    default NULL,   -- Groupe d'usagers devant recevoir le message de refresh
    in_refreshOtherClass  int4    default 1,
    n_confRefresh         int4    default 1 
 )
RETURNS INT4 AS $Msg_RefreshAlarm$

  DECLARE
    cn_admRefresh     int4 := 103;

    b_refreshJavaGui  boolean;
  
  BEGIN

    ----------------------------------------------------------------------
    -- Check if Java GUI is to be refreshed... else only refresh ConfData
    ----------------------------------------------------------------------
    BEGIN
      SELECT false INTO STRICT b_refreshJavaGui
      FROM EASYSHARE.D_PARAMETER
      WHERE OBJ_REF = cn_admrefresh
        AND PARAM_TAG = '20'
        AND PARAM_VALUE = '0';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN b_refreshJavaGui := true;
    END;

    IF ( b_refreshJavaGui = true )
    THEN
      PERFORM EASYSHARE.MSG_REFRESH( n_logRecId, ic_Operation, ic_Moc, in_Moi, n_refresh, in_ownerRef, in_refreshOtherClass, n_confRefresh );
    END IF;
    RETURN 0;
  END

$Msg_RefreshAlarm$ LANGUAGE 'plpgsql';

 SELECT  AQSATOOLS.StoreFunctionPkg ( 'ALARM', 'pg_Alarm_Mgt', 'Msg_RefreshAlarm' );

