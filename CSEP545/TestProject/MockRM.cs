using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TestProject
{
    public class MockRM : TP.RM
    {
        private string name = "MockRM";
        public TP.XaResponse CommitResponse = TP.XaResponse.XA_OK;
        public TP.XaResponse PrepareResponse = TP.XaResponse.XA_OK;
        public TP.XaResponse AbortResponse = TP.XaResponse.XA_OK;
        #region RM Members

        public string GetName()
        {
            return name;
        }

        public void SetName(string name)
        {
            this.name = name;
        }

        public void Enlist(TP.Transaction context)
        {
           
        }

        public void Ping() { }

        public TP.XaResponse Commit(TP.Transaction context)
        {
            return this.CommitResponse;
        }

        public TP.XaResponse Abort(TP.Transaction context)
        {
            return this.AbortResponse;
        }

        public TP.XaResponse Prepare(TP.Transaction context)
        {
            return this.PrepareResponse;
        }

        public bool Add(TP.Transaction context, TP.RID resource, int count, int price)
        {
            return true;
        }

        public bool Delete(TP.Transaction context, TP.RID resource, int count)
        {
            return true;
        }

        public bool Delete(TP.Transaction context, TP.RID resource)
        {
            return true;
        }

        public int Query(TP.Transaction context, TP.RID resource)
        {
            return 1;
        }

        public int QueryPrice(TP.Transaction context, TP.RID resource)
        {
            return 2;
        }

        public TP.Customer[] ListCustomers(TP.Transaction context)
        {
            return null;
        }

        public string[] ListResources(TP.Transaction context, TP.RID.Type type)
        {
            return new string[] { "item1", "item2" };
        }

        public bool Reserve(TP.Transaction context, TP.Customer c, TP.RID resource)
        {
            return true;
        }

        public string QueryReserved(TP.Transaction context, TP.Customer customer)
        {
            return string.Empty;
        }

        public int QueryReservedPrice(TP.Transaction context, TP.Customer customer)
        {
            return 3;
        }

        public void UnReserve(TP.Transaction context, TP.Customer c)
        {
        }

        public void Shutdown()
        {
        }

        #endregion

        #region RM Members


        public void SelfDestruct(int writesToWait, int readsToWait, int commitsToWait, int abortsToWait)
        {
            
        }

        #endregion
    }
}
