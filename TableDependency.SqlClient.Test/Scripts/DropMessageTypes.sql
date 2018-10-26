DECLARE @name varchar(200) 

DECLARE emp_cursor CURSOR FOR     
SELECT name from sys.service_message_types 
where name like 'dbo_%'  
  
OPEN emp_cursor    
  
FETCH NEXT FROM emp_cursor     
INTO @name   


WHILE @@FETCH_STATUS = 0    
BEGIN  

EXECUTE ('DROP MESSAGE TYPE [' + @name + ']')

FETCH NEXT FROM emp_cursor     
INTO @name  


END     
CLOSE emp_cursor;    
DEALLOCATE emp_cursor;   