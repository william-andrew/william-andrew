using System;
using System.Collections.Generic;
using System.Linq;
using MyRM.Storage;
using TP;

namespace MyRM
{
    /// <summary>
    /// TransactionStorage holds all three kinds of resource (Car, Hotel, Flight) in the same
    /// resouces variable
    /// </summary>
    public class TransactionStorage
    {
        // Generic data persistence 
        private readonly ISimpleDatabase _simpleDatabase;

        public TransactionStorage(ISimpleDatabase simpleDatabase)
        {
            _simpleDatabase = simpleDatabase;
        }

        /// <summary>
        /// Prepare the transaction and save changes to durable storage
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        public void Prepare(Transaction context)
        {
            _simpleDatabase.Prepare(context);
        }

        /// <summary>
        /// Commit the transaction and remove the shadow
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        public void Commit(Transaction context)
        {
            _simpleDatabase.Commit(context);
        }

        /// <summary>
        /// Abort the transaction
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        public void Abort(Transaction context)
        {
            try
            {
                _simpleDatabase.Abort(context);
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
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
            try
            {
                var record = _simpleDatabase.ReadRecord(context, Constants.ResourcesTableName, key.ToString());
                return SerializationHelper.ConvertRowToResource(record);
            }
            catch(RecordNotFoundException)
            {
                return null;
            }
        }

        public IEnumerable<Resource> GetResources(Transaction context)
        {
            var rows = _simpleDatabase.ReadAllRecords(context, Constants.ResourcesTableName);
            return rows.Select(r => SerializationHelper.ConvertRowToResource(r.Value)).ToList();
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
            var row = SerializationHelper.ConvertResourceToRow(resource);
            _simpleDatabase.UpsertRecord(context, Constants.ResourcesTableName, key.ToString(), row);
        }

        /// <summary>
        /// Deletes reservations for a customer
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool Delete(Transaction context, RID key)
        {
            //TransactionData currentShadow = CreateShadowIfNotExists(context);
            //return currentShadow.Resources.Remove(key);
            try
            {
                _simpleDatabase.DeleteRecord(context, Constants.ResourcesTableName, key.ToString());
                return true;
            }
            catch(RecordNotFoundException)
            {
                return false;
            }
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
            try
            {
                var record = _simpleDatabase.ReadRecord(context, Constants.ReservationTableName, key.Id.ToString());
                Console.WriteLine("DB: Read {0} returns {1}", key, record.DataString);
                return SerializationHelper.ConvertRowToReservation(key.Id.ToString(), record);
            }
            catch (RecordNotFoundException)
            {
                Console.WriteLine("Read reservation: {0}, returns no record.", key);
                return null;
            }
        }

        /// <summary>
        /// Get the customers who has reservations
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <returns></returns>
        public IEnumerable<Customer> GetCustomers(Transaction context)
        {
            var rows = _simpleDatabase.ReadAllRecords(context, Constants.ReservationTableName);
            return rows.Select(r => new Customer(r.Key)).ToList();
        }

        /// <summary>
        /// Write the reserved resources of a customer to the shadow
        /// It supports multiple transactions
        /// </summary>
        /// <returns>success</returns>
        public void Write(Transaction context, Customer key, HashSet<RID> reserved)
        {
            var row = SerializationHelper.ConvertReservationToRow(key.Id.ToString(), reserved);
            Console.WriteLine("DB: Writing {0} for {1}" , row.DataString, key);
            _simpleDatabase.UpsertRecord(context, Constants.ReservationTableName, key.Id.ToString(), row);
        }

        /// <summary>
        /// Deletes reservations for a customer
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool Delete(Transaction context, Customer key)
        {
            try
            {
                _simpleDatabase.DeleteRecord(context, Constants.ReservationTableName, key.Id.ToString());
                return true;
            }
            catch (RecordNotFoundException)
            {
                return false;
            }
        }
        #endregion
    }
}

