namespace TableDependency.SqlClient.IntegrationTest.Model
{
    public class Issue_0003_Model
    {
        public int Id { get; set; }
        public string FirstName { get; set; }
        public string SecondName { get; set; }
        public string NotManagedColumnBecauseIsVarcharMax { get; set; }
        public string NotManagedColumnBecauseIsXml { get; set; }
    }
}