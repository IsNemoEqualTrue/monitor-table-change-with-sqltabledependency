#region License

// TableDependency, SqlTableDependency
// Copyright (c) 2015-2020 Christian Del Bianco. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

#endregion

using Microsoft.Data.SqlClient;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.Status;

public class StatusLostConnectionTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    public class StatusLostConnectionTestModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Surname { get; set; } = string.Empty;
    }

    private SqlTableDependency<StatusLostConnectionTestModel>? _tableDependency;
    private static readonly string TableName = typeof(StatusLostConnectionTestModel).Name;
    private readonly Dictionary<TableDependencyStatus, bool> _statuses = Enum.GetValues<TableDependencyStatus>().ToDictionary(s => s, _ => false);
    private Exception? _ex;

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText =
            $"CREATE TABLE [{TableName}]( "
            + "[Id][int] IDENTITY(1, 1) NOT NULL, "
            + "[First Name] [NVARCHAR](50) NOT NULL, "
            + "[Second Name] [NVARCHAR](50) NOT NULL, "
            + "[Born] [DATETIME] NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Test()
    {
        string naming;

        try
        {
            // ARRANGE
            var mapper = new ModelToTableMapper<StatusLostConnectionTestModel>();
            mapper.AddMapping(c => c.Name, "FIRST name");
            mapper.AddMapping(c => c.Surname, "Second Name");
            _tableDependency = await SqlTableDependency<StatusLostConnectionTestModel>.CreateSqlTableDependencyAsync(DependencyConnectionString, tableName: TableName, mapper: mapper, ct: TestContext.Current.CancellationToken);
            _tableDependency.ReconnectTimeout = TimeSpan.Zero; // asserts the immediate stop, not the reconnect
            _tableDependency.OnChanged += TableDependency_Changed;
            _tableDependency.OnStatusChanged += TableDependency_OnStatusChanged;
            _tableDependency.OnExceptionAsync = TableDependency_OnExceptionAsync; // Test using async exception handling
            naming = _tableDependency.NamingPrefix;

            await _tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

            var taskModifyTableContent = ModifyTableContent();
            await Task.Delay(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            // ACT
            await KillSqlTableDependencyDbConnection();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

            // ASSERT
            Assert.True(_statuses[TableDependencyStatus.Starting]);
            Assert.True(_statuses[TableDependencyStatus.Started]);
            Assert.True(_statuses[TableDependencyStatus.WaitingForNotification]);
            Assert.True(_statuses[TableDependencyStatus.StopDueToError]);
            Assert.False(_statuses[TableDependencyStatus.StopDueToCancellation]);

            Assert.Equal(TableDependencyStatus.StopDueToError, _tableDependency.Status);
            Assert.IsType<SqlException>(_ex);

            // Make sure all tasks have finished
            await Task.WhenAll(taskModifyTableContent);
        }
        finally
        {
            if (_tableDependency is not null)
                await _tableDependency.DisposeAsync();
        }

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private async Task TableDependency_OnExceptionAsync(ExceptionEventArgs e)
        => _ex = e.Exception;

    private void TableDependency_OnStatusChanged(StatusChangedEventArgs e)
    {
        _statuses[e.Status] = true;
        Assert.Equal(_tableDependency?.Status, e.Status);
    }

    private static void TableDependency_Changed(RecordChangedEventArgs<StatusLostConnectionTestModel> e)
    {
        // Implementation not needed
    }

    private async Task ModifyTableContent()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('Ismano', 'Del Bianco')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task KillSqlTableDependencyDbConnection()
    {
        var initialCatalog = new SqlConnectionStringBuilder(ConnectionString).InitialCatalog;
        // Kill the dependency's login (least-privilege), not the admin login the kill runs under.
        var userId = new SqlConnectionStringBuilder(DependencyConnectionString).UserID;

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        // Exclude the current session (@@SPID) to avoid trying to kill our own process which causes a SQL exception
        sqlCommand.CommandText = $"DECLARE @kill varchar(8000); SET @kill = ''; SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), spid) + ';' FROM master..sysprocesses WHERE dbid = db_id('{initialCatalog}') and loginame = '{userId}' and spid <> @@SPID; EXEC(@kill);";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}