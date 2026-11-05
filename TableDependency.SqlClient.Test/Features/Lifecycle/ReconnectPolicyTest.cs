using Microsoft.Data.SqlClient;

namespace TableDependency.SqlClient.Test.Features.Lifecycle;

public class ReconnectPolicyTest
{
    [Fact]
    public void NextDelay_DoublesFromOneSecondAndCapsAtSixty()
    {
        // ARRANGE
        double[] expected = [1, 2, 4, 8, 16, 32, 60, 60];
        var delay = TimeSpan.Zero;
        List<double> seconds = [];

        // ACT
        for (var i = 0; i < expected.Length; i++)
        {
            delay = ReconnectPolicy.NextDelay(delay);
            seconds.Add(delay.TotalSeconds);
        }

        // ASSERT
        Assert.Equal(expected, seconds);
    }

    [Theory]
    [InlineData(false, 10, 1800, true)]      // receive failure inside the window is retried
    [InlineData(true, 10, 1800, false)]      // subscriber fault is never retried, even as a SqlException
    [InlineData(false, 1800, 1800, false)]   // window exhausted
    [InlineData(false, 10, 0, false)]        // zero restores the immediate stop
    [InlineData(false, 999_999, -1, true)]   // infinite never gives up
    public void ShouldReconnect_RetriesOnlyReceiveFailuresInsideTheWindow(bool dispatching, int outageSeconds, int timeoutSeconds, bool expected)
    {
        // ARRANGE
        var timeout = timeoutSeconds < 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(timeoutSeconds);

        // ACT
        var retry = ReconnectPolicy.ShouldReconnect(CreateSqlException(), dispatching, TimeSpan.FromSeconds(outageSeconds), timeout);

        // ASSERT
        Assert.Equal(expected, retry);
    }

    [Fact]
    public void ShouldReconnect_DoesNotRetryNonSqlFailures()
    {
        // ARRANGE
        var exception = new InvalidOperationException("mapper fault");

        // ACT
        var retry = ReconnectPolicy.ShouldReconnect(exception, false, TimeSpan.Zero, TimeSpan.FromMinutes(30));

        // ASSERT
        Assert.False(retry);
    }

    [Fact]
    public void ReconnectTimeout_RejectsNegativeValues()
    {
        // ARRANGE
        var property = typeof(SqlTableDependency<object>).GetProperty(nameof(SqlTableDependency<>.ReconnectTimeout))!;
        var instance = System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject(typeof(SqlTableDependency<object>));

        // ACT
        var exception = Record.Exception(() => property.SetValue(instance, TimeSpan.FromSeconds(-1)));

        // ASSERT
        Assert.IsType<ArgumentOutOfRangeException>(exception?.InnerException);
    }

    // Real SqlException from a failed login: the type has no public constructor.
    private static SqlException CreateSqlException()
    {
        try
        {
            using var connection = new SqlConnection("Server=127.0.0.1,1;Connect Timeout=1;Pooling=false");
            connection.Open();
        }
        catch (SqlException exception)
        {
            return exception;
        }

        throw new InvalidOperationException("Expected the connection to fail.");
    }
}