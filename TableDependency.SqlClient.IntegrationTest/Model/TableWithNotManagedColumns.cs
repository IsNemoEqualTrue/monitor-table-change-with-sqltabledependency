namespace TableDependency.SqlClient.IntegrationTest.Model
{
    public class TableWithNotManagedColumns
    {
        public int Id { get; set; }
        public string FirstName { get; set; }
        public string SecondName { get; set; }
        public string ManagedColumnBecauseIsVarcharMAX { get; set; }
    }
}