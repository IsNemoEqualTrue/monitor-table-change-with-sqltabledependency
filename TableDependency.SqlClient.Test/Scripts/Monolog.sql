--*********************************************************************************************************************
--Even though we want to model a monolog we still need a sender and receiver.
--The receiver is where the important work is done. 
--The sender is merely there for completeness in the dialog.   
--*********************************************************************************************************************

--Set it up...
CREATE QUEUE SenderQ;
CREATE QUEUE ReceiverQ;
CREATE SERVICE SenderSvc ON QUEUE SenderQ;
CREATE SERVICE ReceiverSvc ON QUEUE ReceiverQ ([DEFAULT]);
GO

--to get the monolog pattern to work the Sender must have an activator proc.
--It's sole job is to END CONVERSATION on the sending side.  
create PROCEDURE SenderQ_ActivatorProcedure
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE 
		@mt sysname, 
		@h uniqueidentifier;
	
	--this can be made far more performant and resilient, but this is the most
	--basic pattern
    RECEIVE TOP (1)
        @mt = message_type_name,
        @h	= conversation_handle
    FROM SenderQ;
    IF @mt = (N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog') OR @mt = (N'http://schemas.microsoft.com/SQL/ServiceBroker/Error') 
    BEGIN
        END CONVERSATION @h;
    END
END;
GO

--now attach the activator proc to the Q.  
--Here we set the STATUS to OFF for the demo, we'll enable it below.  
ALTER QUEUE SenderQ 
	WITH ACTIVATION
	(
		procedure_name = dbo.SenderQ_ActivatorProcedure,
		max_queue_readers = 1,
		EXECUTE AS OWNER,
		STATUS = ON
	);
GO


--The sender is responsible for pushing the asynchronous message to the receiver.  
--The COMMIT is the "synchronous" transaction boundary.  The "message" is the asynchronous
--process we want to run in an autonomous transaction.  
DECLARE @h UNIQUEIDENTIFIER;
BEGIN DIALOG @h 
	FROM SERVICE [SenderSvc]
	TO SERVICE 'ReceiverSvc'
	WITH ENCRYPTION = OFF;
	
--SEND ON CONVERSATION @h (0x); --> null
--SEND ON CONVERSATION @h (convert(varbinary,'')); --> null
--SEND ON CONVERSATION @h (convert(varbinary,' ')); --> 0x20
--SEND ON CONVERSATION @h (0x0); --> 0x00

SEND ON CONVERSATION @h (CONVERT(NVARCHAR(MAX),'')) --> null

--note that we SEND but we do *not* END CONVERSATION.  
--if we did that would be the dreaded "fire-and-forget" anti-pattern.  
--Remember:  The target always ENDs CONVERSATION first!
--END CONVERSATION @h;

--everything seems ok so far
select conversation_handle, message_type_name, convert(xml,message_body) as MsgBody, message_body from SenderQ
select conversation_handle, message_type_name, convert(xml,message_body) as MsgBody, message_body from ReceiverQ
select * from sys.conversation_endpoints WITH (NOLOCK);




--now receiver processes the message
DECLARE @rh UNIQUEIDENTIFIER;
WAITFOR(
    RECEIVE 
    TOP(1) @rh = conversation_handle 
    FROM ReceiverQ 
), TIMEOUT 2000 ;

	--do whatever work you want to do HERE
	--note that we first END CONVERSATION on the receiver side.  
	END CONVERSATION @rh;
GO


--and now the message is automatically acknowledged and the conversation is ended
select conversation_handle, message_type_name, convert(xml,message_body) as MsgBody, message_body from SenderQ
select conversation_handle, message_type_name, convert(xml,message_body) as MsgBody, message_body from ReceiverQ
select * from sys.conversation_endpoints WITH (NOLOCK);


