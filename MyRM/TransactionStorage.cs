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
        private readonly IDatabaseFileAccess _database;

        public TransactionStorage(IDatabaseFileAccess database)
        {
            _database = database;
        }

        /// <summary>
        /// Commit the transaction and remove the shadow
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        public void Commit(Transaction context)
        {
            _database.Commit(context);
        }

        /// <summary>
        /// Abort the transaction
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        public void Abort(Transaction context)
        {
            try
            {
                _database.Abort(context);
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
                var record = _database.ReadRecord(context, Constants.ResourcesTableName, key.ToString());
                return SerializationHelper.ConvertRowToResource(record);
            }
            catch(RecordNotFoundException)
            {
                return null;
            }
        }

        public IEnumerable<Resource> GetResources(Transaction context)
        {
            var rows = _database.ReadAllRecords(context, Constants.ResourcesTableName);
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
            _database.UpsertRecord(context, Constants.ResourcesTableName, key.ToString(), row);
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
                _database.DeleteRecord(context, Constants.ResourcesTableName, key.ToString());
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
                var record = _database.ReadRecord(context, Constants.ReservationTableName, key.Id.ToString());
                return SerializationHelper.ConvertRowToReservation(key.Id.ToString(), record);
            }
            catch (RecordNotFoundException)
            {
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
            var rows = _database.ReadAllRecords(context, Constants.ReservationTableName);
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
            _database.UpsertRecord(context, Constants.ReservationTableName, key.Id.ToString(), row);
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
                _database.DeleteRecord(context, Constants.ReservationTableName, key.Id.ToString());
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

