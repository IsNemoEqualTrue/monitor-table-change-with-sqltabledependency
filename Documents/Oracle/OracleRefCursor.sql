CREATE OR REPLACE PROCEDURE GET_MESSAGES (p_recordset OUT SYS_REFCURSOR) 
AS 
  obj customer_message;
  payload customer_messages;
  v_msgid RAW(16);

  queueopts DBMS_AQ.DEQUEUE_OPTIONS_T;
  msgprops DBMS_AQ.MESSAGE_PROPERTIES_T;
   
  no_messages EXCEPTION;
  end_of_group EXCEPTION;
  PRAGMA EXCEPTION_INIT (no_messages, -25228);
  PRAGMA EXCEPTION_INIT (end_of_group, -25235);
BEGIN
  queueopts.wait := 5;
  queueopts.navigation := DBMS_AQ.FIRST_MESSAGE;
  payload := customer_messages();

  LOOP
    BEGIN
      DBMS_AQ.DEQUEUE ('customer_queue', queueopts, msgprops, obj, v_msgid);

      payload.EXTEND;
      payload(payload.LAST) := obj;

      queueopts.navigation := DBMS_AQ.NEXT_MESSAGE;
      queueopts.wait := DBMS_AQ.NO_WAIT;

    EXCEPTION
      WHEN end_of_group THEN
        EXIT;
    END;
  END LOOP;
   
  OPEN p_recordset FOR SELECT message_type, message_content FROM TABLE(payload);  
   
EXCEPTION
  WHEN no_messages THEN
    OPEN p_recordset FOR SELECT NULL AS message_type, NULL AS message_content FROM DUAL;
END;




DECLARE
  l_cursor  SYS_REFCURSOR;
    l_ename   VARCHAR2(50);
  l_empno   VARCHAR2(4000);
BEGIN
  GET_MESSAGES (p_recordset => l_cursor);
            
  LOOP 
    FETCH l_cursor
    INTO  l_ename, l_empno;
    EXIT WHEN l_cursor%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE(l_ename || ' - ' || l_empno );
  END LOOP;
  CLOSE l_cursor;
END;
/

SELECT NULL AS message_type, NULL AS message_content FROM DUAL;
