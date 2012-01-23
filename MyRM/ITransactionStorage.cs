using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using TP;
namespace MyRM
{
    interface ITransactionStorage
    {
        Dictionary<Customer, HashSet<RID>> GetReservations();
        void SetReservations(Dictionary<Customer, HashSet<RID>> reservations);
        Dictionary<RID, Resource> GetResources();
        void SetResources(Dictionary<RID, Resource> resources);
    }
}
