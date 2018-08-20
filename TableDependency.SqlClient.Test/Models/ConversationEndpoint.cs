using System;

namespace TableDependency.SqlClient.Test.Models
{
    public class ConversationEndpoint
    {
        public Guid ConversationHandle { get; set; }
        public Guid ConversationId { get; set; }
        public int IsInitiator { get; set; }
        public int ServiceContractId { get; set; }
        public Guid ConversationGroupId { get; set; }
        public int ServiceId { get; set; }
        public DateTime Lifetime { get; set; }
        public string State { get; set; }
        public string StateDesc { get; set; }
        public string FarService { get; set; }
        public string FarBrokerInstance { get; set; }
        public int PrincipalId { get; set; }
        public int FarPrincipalId { get; set; }
        public Guid OutboundSessionKeyIdentifier { get; set; }
        public Guid InboundSessionKeyIdentifier {get; set; }
        public DateTime SecurityTimestamp { get; set; }
        public DateTime DialogTimer { get; set; }
    }
}