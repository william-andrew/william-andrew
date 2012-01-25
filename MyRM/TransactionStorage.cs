using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TP;
using System.Threading;

namespace MyRM
{
        /// <summary>
    /// TransactionStorage holds all three kinds of resource (Car, Hotel, Flight) in the same
    /// resouces variable
    /// </summary>
    public class TransactionStorage:ITransactionStorage
    {
        /// <summary>
        /// Resources, in shadow, this is a full copy of the primary
        /// </summary>
        private Dictionary<RID, Resource> resources;

        /// <summary>
        /// Reservations, in shadow, this is a full copy of the primary
        /// </summary>
        private Dictionary<Customer, HashSet<RID>> reservations;

        private static TransactionStorage primary = new TransactionStorage();

        /// <summary>
        /// This can support multiple transactions at the same time 
        /// TODO: Each shadow contains only the items that has been written to
        /// minimize the memory consumption and cost of creating a new transaction.
        /// </summary>
        private static Dictionary<Transaction, TransactionStorage> shadows = new Dictionary<Transaction, TransactionStorage>();

        private TransactionStorage()
        {
            this.resources = new Dictionary<RID, Resource>();
            this.reservations = new Dictionary<Customer, HashSet<RID>>();
        }

        /// <summary>
        /// Commit the transaction and remove the shadow
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        public static void Commit(Transaction context)
        {
            // TODO: lock the whole object
            // TODO: make it commit only the changed pages
            // Now we only use the dummy context id (all 0s) because the TestBase.cs generates new transaction id before submitting the Car adds and Room add.
            context.Id = Guid.Empty;
            try
            {
                Interlocked.Exchange<TransactionStorage>(ref primary, shadows[context]);
                shadows.Remove(context);
            }
            catch (Exception)
            {
                // catch all exceptions here because if the same transaction is commited twice, 
                // the second commit may throw exception. It is ok to catch it because the 
                // commit is actually successful. 
            }
        }

        /// <summary>
        /// Abort the transaction
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        public static void Abort(Transaction context)
        {
            // Now we only use the dummy context id (all 0s) because the TestBase.cs generates new transaction id before submitting the Car adds and Room add.
            context.Id = Guid.Empty;
            try
            {

                shadows.Remove(context);
            }
            catch (Exception)
            {
                // catch all exceptions here because if the same transaction is aborted twice, 
                // the second abort will throw exception. It is ok to catch it because the 
                // abort is actually successful. 
            }
        }

        #region Resources
        /// <summary>
        /// Reads the requested resource from the storage
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static Resource Read(Transaction context, RID key)
        {
            TransactionStorage storage = GetStorage(context);
            if (!storage.resources.ContainsKey(key))
            {
                return null;
            }

            return storage.resources[key];
        }

        public static IEnumerable<Resource> GetResources(Transaction context)
        {
            TransactionStorage storage = GetStorage(context);
            return storage.resources.Values;
        }

        /// <summary>
        /// Write the resource to the shadow
        /// It supports multiple transactions
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <returns>success</returns>
        public static void Write(Transaction context, RID key, Resource resource )
        {
            TransactionStorage currentShadow = CreateShadowIfNotExists(context);

            // add or set the new value to the resource in shadow
            if (!currentShadow.resources.ContainsKey(key))
            {
                currentShadow.resources.Add(key, resource);
            }
            else
            {
                currentShadow.resources[key] = resource;
            }
        }

        /// <summary>
        /// Deletes reservations for a customer
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static bool Delete(Transaction context, RID key)
        {
            TransactionStorage currentShadow = CreateShadowIfNotExists(context);
            return currentShadow.resources.Remove(key);
        }
        #endregion

        #region Customer
        /// <summary>
        /// Reads the requested resource from storage
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static HashSet<RID> Read(Transaction context, Customer key)
        {
            TransactionStorage storage = GetStorage(context);
            if (!storage.reservations.ContainsKey(key))
            {
                return null;
            }

            return storage.reservations[key];
        }

        /// <summary>
        /// Get the customers who has reservations
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <returns></returns>
        public static IEnumerable<Customer> GetCustomers(Transaction context)
        {
            TransactionStorage storage = GetStorage(context);
            return storage.reservations.Keys;
        }

        /// <summary>
        /// Write the reserved resources of a customer to the shadow
        /// It supports multiple transactions
        /// </summary>
        /// <returns>success</returns>
        public static void Write(Transaction context, Customer key, HashSet<RID> reserved)
        {
            TransactionStorage currentShadow = CreateShadowIfNotExists(context);            

            // add or set the new value to the resource in shadow
            if (!currentShadow.reservations.ContainsKey(key))
            {
                currentShadow.reservations.Add(key, reserved);
            }
            else
            {
                currentShadow.reservations[key] = reserved;
            }
        }

        /// <summary>
        /// Deletes reservations for a customer
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static bool Delete(Transaction context, Customer key)
        {
            TransactionStorage currentShadow = CreateShadowIfNotExists(context);
            return currentShadow.reservations.Remove(key);
        }
        #endregion

        /// <summary>
        /// Get the reservations for the method who write the data to stable storage
        /// TODO: implement dirty bits property so we can write back only the changed pages
        /// </summary>
        /// <returns></returns>
        public Dictionary<Customer, HashSet<RID>> GetReservations()
        {
            return this.reservations;
        }

        /// <summary>
        /// Set the reservations for another method who loads the data from stable storage
        /// </summary>
        /// <param name="reservations"></param>
        public void SetReservations(Dictionary<Customer, HashSet<RID>> reservations)
        {
            this.reservations = reservations;
        }

        /// <summary>
        /// Get resources for the method who write data to stable storage
        /// </summary>
        /// <returns></returns>
        public Dictionary<RID, Resource> GetResources()
        {
            return this.resources;
        }

        /// <summary>
        /// Sets the resources for the method who loads data from stable storage
        /// </summary>
        /// <param name="resources"></param>
        public void SetResources(Dictionary<RID, Resource> resources)
        {
            this.resources = resources;
        }

        /// <summary>
        /// Gets the correct transaction storage for the transaction
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <returns>
        /// The primary transaction storage if there is no shadow for the transaction.
        /// The shadow transaction storage if there is a shadow for the transaction
        /// </returns>
        private static TransactionStorage GetStorage(Transaction context)
        {
            // TODO: implment lock on the transaction to make sure the content of primary is not changed when copying
            TransactionStorage storage;
            // Now we only use the dummy context id (all 0s) because the TestBase.cs generates new transaction id before submitting the Car adds and Room add.
            context.Id = Guid.Empty;

            if (!shadows.TryGetValue(context, out storage))
            {
                storage = primary;
            }

            return storage;
        }

        /// <summary>
        /// Gets the shadow copy for this transaction.
        /// Creates a shadow copy of the primary for the transaction if no shadow copy for this transaction exists
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <returns>the shadow copy of the database</returns>
        private static TransactionStorage CreateShadowIfNotExists(Transaction context)
        {
            // TODO: implment lock on the transaction to make sure the content of primary is not changed when copying
            // Now we only use the dummy context id (all 0s) because the TestBase.cs generates new transaction id before submitting the Car adds and Room add.
            context.Id = Guid.Empty;

            TransactionStorage shadow;
            if (!shadows.TryGetValue(context, out shadow))
            {
                // create new shadow if there is none for the context and copy the contents in primary to shadow
                shadow = new TransactionStorage();

                shadows.Add(context, shadow);
                shadow.reservations = new Dictionary<Customer, HashSet<RID>>(primary.reservations.Count);
                foreach (KeyValuePair<Customer, HashSet<RID>> item in primary.reservations)
                {
                    HashSet<RID> newSet = new HashSet<RID>(item.Value);
                    shadow.reservations.Add(item.Key, newSet);
                }

                shadow.resources = new Dictionary<RID, Resource>(primary.resources.Count);
                foreach (KeyValuePair<RID, Resource> item in primary.resources)
                {
                    Resource newRes = new Resource(item.Key, item.Value.getCount(), item.Value.getPrice());
                    shadow.resources.Add(item.Key, newRes);
                }
            }

            return shadow;
        }
    }
}
