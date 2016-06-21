 DECLARE 
   theName VARCHAR2(30);
 BEGIN
  theName := '80B_32DF_4EA6_AB47_1';
 
     DECLARE
      v_object_type2 VARCHAR2(30);
    BEGIN
      SELECT   OBJECT_TYPE 
      INTO     v_object_type2 
      FROM     user_OBJECTS 
      WHERE    OBJECT_TYPE = 'TRIGGER' AND UPPER(OBJECT_NAME) = 'TR_' || theName;

      EXECUTE IMMEDIATE 'DROP TRIGGER TR_' || theName;
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;

    DECLARE
      v_object_type3 VARCHAR2(30);
    BEGIN
      SELECT   OBJECT_TYPE 
      INTO     v_object_type3 
      FROM     user_OBJECTS 
      WHERE    OBJECT_TYPE = 'PROCEDURE' AND UPPER(OBJECT_NAME) = 'DEQ_' || theName;

      EXECUTE IMMEDIATE 'DROP PROCEDURE DEQ_' || theName;
    EXCEPTION
    WHEN OTHERS THEN
      NULL;
    END;

    DECLARE
      v_object_type4 VARCHAR2(30);
    BEGIN
      SELECT   OBJECT_TYPE 
      INTO     v_object_type4 
      FROM     user_OBJECTS 
      WHERE    OBJECT_TYPE = 'QUEUE' AND UPPER(OBJECT_NAME) = 'QUE_' || theName;

      DBMS_AQADM.STOP_QUEUE('QUE_' || theName); 
      DBMS_AQADM.DROP_QUEUE(queue_name => 'QUE_' || theName);
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;

    DECLARE
      v_object_type5 VARCHAR2(30);
    BEGIN
      SELECT   OBJECT_TYPE 
      INTO     v_object_type5 
      FROM     user_OBJECTS 
      WHERE    OBJECT_TYPE = 'TABLE' AND UPPER(OBJECT_NAME) = 'QT_' || theName;

      DBMS_AQADM.DROP_QUEUE_TABLE(queue_table => 'QT_' || theName, force => TRUE); 
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
    
    DECLARE
      v_object_type VARCHAR2(30);
    BEGIN
      SELECT   OBJECT_TYPE 
      INTO     v_object_type
      FROM     user_OBJECTS 
      WHERE    OBJECT_TYPE = 'TYPE' AND UPPER(OBJECT_NAME) = 'TBL_' || theName;

      EXECUTE IMMEDIATE 'DROP TYPE TBL_' || theName;
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;    
    
    DECLARE
      v_object_type6 VARCHAR2(30);
    BEGIN
      SELECT   OBJECT_TYPE 
      INTO     v_object_type6
      FROM     user_OBJECTS 
      WHERE    OBJECT_TYPE = 'TYPE' AND UPPER(OBJECT_NAME) = 'TYPE_' || theName;

      EXECUTE IMMEDIATE 'DROP TYPE TYPE_' || theName;
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
    
    DECLARE
      v_object_type6 VARCHAR2(30);
    BEGIN
      SELECT   OBJECT_TYPE 
      INTO     v_object_type6
      FROM     user_OBJECTS 
      WHERE    OBJECT_TYPE = 'JOB' AND UPPER(OBJECT_NAME) = 'JOB_' || theName;

      EXECUTE IMMEDIATE 'DROP JOB JOB_' || theName;
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
END;