using System;
using System.Collections.Generic;
using System.Linq;
using TP;

namespace MyTM
{
    public class ResourceManagerList
    {
        public DateTime TransactionStartTime { get; private set; }
        public List<RM> ResourceManagers { get; private set; }
    
        public ResourceManagerList() :this(null)
        {
        }

        public ResourceManagerList(RM rm)
        {
            TransactionStartTime = DateTime.Now;
            ResourceManagers = new List<RM> ();
            if (rm != null) 
                ResourceManagers.Add(rm);
        }

        public bool Contains(RM rm)
        {
            var name = rm.GetName();
            return ResourceManagers.Any(r => String.Compare(r.GetName(), name, StringComparison.OrdinalIgnoreCase) == 0);
        }

        public void Add(RM rm)
        {
            ResourceManagers.Add(rm);
        }
    }
}