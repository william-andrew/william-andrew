using System.Collections.Generic;
using TP;

namespace MyRM
{
    public class TransactionData
    {
        public Dictionary<RID, Resource> Resources { get; set; }
        public Dictionary<Customer, HashSet<RID>> Reservations { get; set; }

        public TransactionData()
        {
            Resources = new Dictionary<RID, Resource>();
            Reservations = new Dictionary<Customer, HashSet<RID>>();
        }
    }
}