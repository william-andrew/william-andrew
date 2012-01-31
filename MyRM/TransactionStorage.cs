﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;
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
        /// <summary>
        /// Resources, in shadow, this is a full copy of the primary
        /// </summary>
        private Dictionary<RID, Resource> resources;

        /// <summary>
        /// Reservations, in shadow, this is a full copy of the primary
        /// </summary>
        private Dictionary<Customer, HashSet<RID>> reservations;
        private bool isInitialized = false;

        private const string ReservationTableName = "Reservations";
        private const string ResourcesTableName = "Resources";

        private static TransactionStorage primary = new TransactionStorage();

        // Generic data persistence 
        private MyDatabase myDatabase;

        /// <summary>
        /// This can support multiple transactions at the same time 
        /// TODO: Each shadow contains only the items that has been written to
        /// minimize the memory consumption and cost of creating a new transaction.
        /// </summary>
        private static Dictionary<Transaction, TransactionStorage> shadows = new Dictionary<Transaction, TransactionStorage>();

        private TransactionStorage()
        {
            myDatabase = new MyDatabase("FIXME");
            myDatabase.CreateTable(ResourcesTableName);
            myDatabase.CreateTable(ReservationTableName);
        }

        public void Init()
        {
            myDatabase = new MyDatabase("FIXME");
            myDatabase.CreateTable(ResourcesTableName);
            myDatabase.CreateTable(ReservationTableName);

            this.LoadFromDisk();
            this.isInitialized = true;
        }

        /// <summary>
        /// For unit test to clean up the whole object
        /// </summary>
        private static void CleanUp()
        {
            primary = new TransactionStorage();
            shadows = new Dictionary<Transaction, TransactionStorage>();
            primary.Init();
        }
        /// <summary>
        /// Commit the transaction and remove the shadow
        /// </summary>
        /// <param name="context">Design for supporting multiple transactions</param>
        public static void Commit(Transaction context)
        {
            // TODO: make it commit only the changed pages
            // TODO: Make sure pages written by other transactions get updated in this shadow before this shadow is commited. Making dirty record is enough for this operation because lockmanager ensures no other transactions can write to the same resource
            try
            {
                lock (shadows)
                {
                    Interlocked.Exchange<TransactionStorage>(ref primary, shadows[context]);
                    shadows.Remove(context);
                }
                primary.WriteBackToDisk();
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

            try
            {
                lock (shadows)
                {
                    shadows.Remove(context);
                }
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
        public void WriteBackToDisk()
        {
            string xml = SerializeReservations(this.reservations);
            myDatabase.WriteTable(ReservationTableName, xml);
            xml = SerializeResource(this.resources);
            myDatabase.WriteTable(ResourcesTableName, xml);
        }

        /// <summary>
        /// Get resources for the method who write data to stable storage.
        /// 
        /// If the database does not exists on disk, it will create an empty one in memory. 
        /// </summary>
        /// <returns></returns>
        public Dictionary<RID, Resource> LoadFromDisk()
        {
            var xml = myDatabase.ReadTable(ResourcesTableName);
            if (string.IsNullOrEmpty(xml))
            {
                this.resources = new Dictionary<RID, Resource>();
            }
            else
            {
                this.resources = DeserializeResources(xml);
            }

            xml = myDatabase.ReadTable(ReservationTableName);
            if (string.IsNullOrEmpty(xml))
            {
                this.reservations = new Dictionary<Customer, HashSet<RID>>();
            }
            else
            {
                this.reservations = DeserializeReservations(xml);
            }

            return this.resources;
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
            TransactionStorage storage;

            lock (shadows)
            {
                if (!primary.isInitialized)
                {
                    primary.Init();
                }
                if (!shadows.TryGetValue(context, out storage))
                {
                    storage = primary;
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
        private static TransactionStorage CreateShadowIfNotExists(Transaction context)
        {
            if (!primary.isInitialized)
            {
                primary.Init();
            }


            TransactionStorage shadow;
            if (!shadows.TryGetValue(context, out shadow))
            {                
                // create new shadow if there is none for the context and copy the contents in primary to shadow
                shadow = new TransactionStorage();
                lock (shadows)
                {
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
            }

            return shadow;
        }

        private Dictionary<Customer, HashSet<RID>> DeserializeReservations(string xml)
        {
            var result = new Dictionary<Customer, HashSet<RID>>();
            if (xml == null)
                return result;

            var xdoc = XDocument.Load(new StringReader(xml));
            var root = xdoc.Element("Reservations");
            if (root == null)
                return result;

            foreach(var e in root.Elements())
            {
                var c = new Customer
                                 {
                                     Id = new Guid(e.Attribute("Customer").Value)
                                 };

                var items = new HashSet<RID>(); 
            
                foreach (var i in e.Elements())
                {
                    TP.RID.Type t;
                    Enum.TryParse(i.Attribute("Type").Value, true, out t);

                    var value = i.Attribute("Name").Value;
                    var rid = new RID(t, value);
                    items.Add(rid);
                }
                result.Add(c, items);
            }
            return result;
        }

        private static string SerializeReservations(Dictionary<Customer, HashSet<RID>> reservations)
        {
            var xdoc = new XDocument();
            var root = new XElement("Reservations");
            xdoc.Add(root);

            foreach (var c in reservations)
            {
                var e = new XElement("Reservation", new XAttribute("Customer", c.Key.Id.ToString()));
                root.Add(e);
                foreach (var r in c.Value)
                {
                    e.Add(new XElement("RID", new XAttribute("Type", r.getType()), new XAttribute("Name", r.getName())));
                }
            }
            return xdoc.ToString();
        }

        private string SerializeResource(Dictionary<RID, Resource> dictionary)
        {
            var xdoc = new XDocument();
            var root = new XElement("Resources");
            xdoc.Add(root);

            foreach (var r in dictionary)
            {
                var e = new XElement("Resource", new XAttribute("Type",  r.Key.getType()), 
                    new XAttribute("Name",  r.Key.getName()),
                    new XAttribute("Price",  r.Value.getPrice()),
                    new XAttribute("Count",  r.Value.getCount())                                       
                    );
                root.Add(e);
            }
            return xdoc.ToString();            
        }

        private Dictionary<RID, Resource> DeserializeResources(string xml)
        {
            var result = new Dictionary<RID, Resource>();
            if (xml == null)
                return result;

            var xdoc = XDocument.Load(new StringReader(xml));
            var root = xdoc.Element("Resources");

            if (root == null)
                return result;

            foreach (var e in root.Elements())
            {
                TP.RID.Type t;
                Enum.TryParse(e.Attribute("Type").Value, true, out t);
                var name = e.Attribute("Name").Value;
                var c = int.Parse(e.Attribute("Count").Value);
                var p = int.Parse(e.Attribute("Price").Value);

                var key = new RID(t, name);
                var item = new Resource(key, c, p);

                result.Add(key, item);
            }
            return result;
        }
    }
}

