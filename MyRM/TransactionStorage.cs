using System;
using System.Collections.Generic;
using TP;
using System.Threading;

namespace MyRM
{
    /// <summary>
    /// TransactionStorage holds all three kinds of resource (Car, Hotel, Flight) in the same
    /// resouces variable
    /// </summary>
    public class TransactionStorage
    {
        private bool _isInitialized;
        private TransactionData _primary = new TransactionData();

        /// <summary>
        /// This can support multiple transactions at the same time 
        /// TODO: Each shadow contains only the items that has been written to
        /// minimize the memory consumption and cost of creating a new transaction.
        /// </summary>
        private readonly Dictionary<Transaction, TransactionData> _shadows = new Dictionary<Transaction, TransactionData>();

        // Generic data persistence 
        private readonly IDatabase _database;

        public TransactionStorage(IDatabase database)
        {
            _database = database;
        }

        private void Init()
        {
            LoadFromDisk();
            _isInitialized = true;
        }

        /// <summary>
        /// Commit the transaction and remove the shadow
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        public void Commit(Transaction context)
        {
            // TODO: make it commit only the changed pages
            // TODO: Make sure pages written by other transactions get updated in this shadow before this shadow is commited. 
            // Making dirty record is enough for this operation because lockmanager ensures no other transactions can write to the same resource
            try
            {
                lock (_shadows)
                {
                    Interlocked.Exchange(ref _primary, _shadows[context]);
                    _shadows.Remove(context);
                }
                WriteBackToDisk();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                // catch all exceptions here because if the same transaction is commited twice, 
                // the second commit may throw exception. It is ok to catch it because the 
                // commit is actually successful. 
            }
        }

        /// <summary>
        /// Abort the transaction
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        public void Abort(Transaction context)
        {

            try
            {
                lock (_shadows)
                {
                    _shadows.Remove(context);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
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
        public Resource Read(Transaction context, RID key)
        {
            TransactionData storage = GetStorage(context);
            if (!storage.Resources.ContainsKey(key))
            {
                return null;
            }

            return storage.Resources[key];
        }

        public IEnumerable<Resource> GetResources(Transaction context)
        {
            TransactionData storage = GetStorage(context);
            return storage.Resources.Values;
        }

        /// <summary>
        /// Write the resource to the shadow
        /// It supports multiple transactions
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <param name="key"> </param>
        /// <param name="resource"> </param>
        /// <returns>success</returns>
        public void Write(Transaction context, RID key, Resource resource)
        {
            TransactionData currentShadow = CreateShadowIfNotExists(context);

            // add or set the new value to the resource in shadow
            if (!currentShadow.Resources.ContainsKey(key))
            {
                currentShadow.Resources.Add(key, resource);
            }
            else
            {
                currentShadow.Resources[key] = resource;
            }
        }

        /// <summary>
        /// Deletes reservations for a customer
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool Delete(Transaction context, RID key)
        {
            TransactionData currentShadow = CreateShadowIfNotExists(context);
            return currentShadow.Resources.Remove(key);
        }
        #endregion

        #region Customer
        /// <summary>
        /// Reads the requested resource from storage
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <param name="key"></param>
        /// <returns></returns>
        public HashSet<RID> Read(Transaction context, Customer key)
        {
            TransactionData storage = GetStorage(context);
            if (!storage.Reservations.ContainsKey(key))
            {
                return null;
            }

            return storage.Reservations[key];
        }

        /// <summary>
        /// Get the customers who has reservations
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <returns></returns>
        public IEnumerable<Customer> GetCustomers(Transaction context)
        {
            TransactionData storage = GetStorage(context);
            return storage.Reservations.Keys;
        }

        /// <summary>
        /// Write the reserved resources of a customer to the shadow
        /// It supports multiple transactions
        /// </summary>
        /// <returns>success</returns>
        public void Write(Transaction context, Customer key, HashSet<RID> reserved)
        {
            TransactionData currentShadow = CreateShadowIfNotExists(context);            

            // add or set the new value to the resource in shadow
            if (!currentShadow.Reservations.ContainsKey(key))
            {
                currentShadow.Reservations.Add(key, reserved);
            }
            else
            {
                currentShadow.Reservations[key] = reserved;
            }
        }

        /// <summary>
        /// Deletes reservations for a customer
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool Delete(Transaction context, Customer key)
        {
            TransactionData currentShadow = CreateShadowIfNotExists(context);
            return currentShadow.Reservations.Remove(key);
        }
        #endregion

        /// <summary>
        /// Get the reservations for the method who write the data to stable storage
        /// TODO: implement dirty bits property so we can write back only the changed pages
        /// </summary>
        /// <returns></returns>
        public Dictionary<Customer, HashSet<RID>> GetReservations()
        {
            return _primary.Reservations;
        }

        /// <summary>
        /// Set the reservations for another method who loads the data from stable storage
        /// </summary>
        private void WriteBackToDisk()
        {
            string xml = SerializationHelper.SerializeReservations(_primary.Reservations);
            _database.WriteTable(Constants.ReservationTableName, xml);
            xml = SerializationHelper.SerializeResource(_primary.Resources);
            _database.WriteTable(Constants.ResourcesTableName, xml);
        }

        /// <summary>
        /// Get resources for the method who write data to stable storage.
        /// 
        /// If the database does not exists on disk, it will create an empty one in memory. 
        /// </summary>
        /// <returns></returns>
        private void LoadFromDisk()
        {
            var xml = _database.ReadTable(Constants.ResourcesTableName);
            _primary.Resources = string.IsNullOrEmpty(xml) ? new Dictionary<RID, Resource>() : SerializationHelper.DeserializeResources(xml);

            xml = _database.ReadTable(Constants.ReservationTableName);
            _primary.Reservations = string.IsNullOrEmpty(xml) ? new Dictionary<Customer, HashSet<RID>>() : SerializationHelper.DeserializeReservations(xml);
        }

        /// <summary>
        /// Gets the correct transaction storage for the transaction
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <returns>
        /// The primary transaction storage if there is no shadow for the transaction.
        /// The shadow transaction storage if there is a shadow for the transaction
        /// </returns>
        private TransactionData GetStorage(Transaction context)
        {
            TransactionData storage;

            lock (_shadows)
            {
                if (!_isInitialized)
                {
                    Init();
                }
                if (!_shadows.TryGetValue(context, out storage))
                {
                    storage = _primary;
                }
            }
            return storage;
        }

        /// <summary>
        /// Gets the shadow copy for this transaction.
        /// Creates a shadow copy of the primary for the transaction if no shadow copy for this transaction exists
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <returns>the shadow copy of the database</returns>
        private TransactionData CreateShadowIfNotExists(Transaction context)
        {
            if (!_isInitialized)
            {
                Init();
            }

            TransactionData shadow;
            if (!_shadows.TryGetValue(context, out shadow))
            {                
                // create new shadow if there is none for the context and copy the contents in primary to shadow
                shadow = new TransactionData();
                lock (_shadows)
                {
                    _shadows.Add(context, shadow);
                    shadow.Reservations = new Dictionary<Customer, HashSet<RID>>(_primary.Reservations.Count);
                    foreach (KeyValuePair<Customer, HashSet<RID>> item in _primary.Reservations)
                    {
                        var newSet = new HashSet<RID>(item.Value);
                        shadow.Reservations.Add(item.Key, newSet);
                    }

                    shadow.Resources = new Dictionary<RID, Resource>(_primary.Resources.Count);
                    foreach (KeyValuePair<RID, Resource> item in _primary.Resources)
                    {
                        var newRes = new Resource(item.Key, item.Value.getCount(), item.Value.getPrice());
                        shadow.Resources.Add(item.Key, newRes);
                    }
                }
            }

            return shadow;
        }
    }
}

