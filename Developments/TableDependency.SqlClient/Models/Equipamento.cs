using System;

namespace TableDependency.SqlClient.Development.Models
{
    public class Equipamento
    {
        public int Id { get; set; }
        public DateTime CriadoDatahora { get; set; }
        public int CriadoPor { get; set; }
        public bool Excluido { get; set; }
        public int IdEquipamentoTipo { get; set; }
        public string NumeroSerial { get; set; }
    }
}