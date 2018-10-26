use TableDependencyDB

declare @h uniqueidentifier, @count int = 0;

while (1=1)
begin
	set @h = null;
	select top(1) @h = conversation_handle
		from sys.conversation_endpoints
		--where state_desc = N'STARTED_OUTBOUND'
	if (@h is null)
	begin
		break
	end
	end conversation @h with cleanup;
	set @count += 1;
	if (@count > 1000)
	begin
		commit;
		set @count = 0;
		begin transaction;
	end
end


select *  from sys.conversation_endpoints WITH (NOLOCK)