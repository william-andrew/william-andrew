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

        /// <summary>
        /// Need to prepopulate the name. Because once the RM is down in 2pc, we will not able to get the name. 
        /// </summary>
        public List<string> RMNames { get; private set; } 
        public ResourceManagerList() :this(null)
        {
        }

        
        public ResourceManagerList(RM rm)
        {
            TransactionStartTime = DateTime.Now;
            ResourceManagers = new List<RM> ();
            RMNames = new List<string>();
            if (rm != null) 
                Add(rm);
        }

        public bool Contains(RM rm)
        {
            var name = rm.GetName();
            return ResourceManagers.Any(r => String.Compare(r.GetName(), name, StringComparison.OrdinalIgnoreCase) == 0);
        }

        public void Add(RM rm)
        {
            ResourceManagers.Add(rm);
            RMNames.Add(rm.GetName());
        }


    }
}