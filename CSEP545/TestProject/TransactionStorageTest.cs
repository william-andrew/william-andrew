using MyRM;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using MyRM.Storage;
using TP;
using System.Collections.Generic;

namespace TestProject
{
    [TestClass]
    public class TransactionStorageTest
    {
        [TestMethod]
        public void SetReservationsTest()
        {
            var customer = new Customer {Id = Guid.NewGuid()};
            var db = CreateDatabase();
            var target = new TransactionStorage(db);
            var reservations = new Dictionary<Customer, HashSet<RID>>
                                                                  {
                                                                      {
                                                                          customer,
                                                                          new HashSet<RID>
                                                                              {
                                                                                  new RID(53),
                                                                                  new RID(TP.RID.Type.ROOM, "Room")
                                                                              }
                                                                          }
                                                                  };
            // TODO: need to fix this because the class signiture changed
            //target.SetReservations(reservations);
            //var result = target.GetReservations();

            //Assert.IsTrue(result.ContainsKey(customer));
            //Assert.AreEqual(2, result[customer].Count);

            //Assert.IsTrue(result[customer].Contains(new RID(53)));
            //Assert.IsTrue(result[customer].Contains(new RID(TP.RID.Type.ROOM, "Room")));
        }

        [TestMethod]
        public void SetResourcesTest()
        {
            var db = CreateDatabase();
            var target = new TransactionStorage(db);
            var resources = new Dictionary<RID, Resource>();
            var rid1 = new RID(TP.RID.Type.ROOM, "Motel6");
            resources.Add(rid1, new Resource(rid1, 1000, 20));
            var rid2 = new RID(TP.RID.Type.FLIGHT, "53");
            resources.Add(rid2, new Resource(rid2, 260, 550));

            // TODO: need to fix this because the class signiture changed
            //target.SetResources(resources);

            //var result = target.GetResources();
            //Assert.IsTrue(result.ContainsKey(rid1));
            //Assert.IsTrue(result.ContainsKey(rid2));

            //Assert.AreEqual(260, result[rid2].getCount());
            //Assert.AreEqual(550, result[rid2].getPrice());
            //Assert.AreEqual("53", result[rid2].getName());
        }

        /// <summary>
        ///A test for Abort
        /// Also test that write is write to shadow and the read from this transaction will get the new value
        ///</summary>
        [TestMethod()]
        public void AbortTest()
        {
            Transaction context = new Transaction();
            RID rid = new RID(RID.Type.FLIGHT, "test");
            var db = CreateDatabase();
            TransactionStorage tr = new TransactionStorage(db);
            tr.Write(context, rid, new Resource(rid, 10, 11));
            //Resource res = tr.Read(context, rid);
            //Assert.AreEqual(11, res.getPrice());
            tr.Abort(context);
            var res = tr.Read(context, rid);
            Assert.IsNull(res, "Transaction aborted, but record still there");
        }

        /// <summary>
        ///A test for Commit
        /// Also test that write is writen to shadow and the read from other transaction will get the old value
        ///</summary>
        [TestMethod()]
        public void CommitTest()
        {
            Transaction context = new Transaction();
            Transaction context1 = new Transaction();
            RID rid = new RID(RID.Type.FLIGHT, Guid.NewGuid().ToString().Substring(0, 24));
            var db = CreateDatabase();
            TransactionStorage tr = new TransactionStorage(db);
            tr.Write(context, rid, new Resource(rid, 10, 11));
            Resource res = tr.Read(context1, rid);
            Assert.IsNull(res);
            tr.Commit(context);
            res = tr.Read(context, rid);
            Assert.AreEqual(11, res.getPrice());
        }

        /// <summary>
        ///A test for Delete resource
        /// Also test Write for resource
        ///</summary>
        [TestMethod()]
        public void WriteDeleteBeforeCommit()
        {
            Transaction context = new Transaction();
            RID rid = new RID(RID.Type.FLIGHT, "test");
            var db = CreateDatabase();
            TransactionStorage tr = new TransactionStorage(db);
            tr.Write(context, rid, new Resource(rid, 10, 11));
            tr.Delete(context, rid);
            Resource res = tr.Read(context, rid);
            Assert.IsNull(res);
        }

        /// <summary>
        ///A test for Delete customers
        ///Also tests GetCustomers;
        ///</summary>
        [TestMethod()]
        public void DeleteTest1()
        {
            Transaction context = new Transaction();
            RID rid = new RID(RID.Type.FLIGHT, "test");
            Customer c = new Customer();
            HashSet<RID> reservations = new HashSet<RID> {rid};
            var db = CreateDatabase();
            TransactionStorage tr = new TransactionStorage(db);
            tr.Write(context, c, reservations);
            tr.Delete(context, c);
            List<Customer> cs = new List<Customer>(tr.GetCustomers(context));
            Assert.AreEqual(0, cs.Count);
        }

        /// <summary>
        ///A test for Read & Write customer resevations
        ///</summary>
        [TestMethod()]
        public void ReadWriteTest()
        {
            Transaction context = new Transaction();
            RID rid = new RID(RID.Type.FLIGHT, "test");
            Customer c = new Customer();
            HashSet<RID> reservations = new HashSet<RID> {rid};
            var db = CreateDatabase();
            TransactionStorage tr = new TransactionStorage(db);
            tr.Write(context, c, reservations);
            HashSet<RID> actual = tr.Read(context, c);
            Assert.IsTrue(reservations.SetEquals(actual));
        }

        private DatabaseFileAccess CreateDatabase()
        {
            var db = new DatabaseFileAccess("TEST_" + Guid.NewGuid(), requiresExplictCommit: true);
            db.CreateTable(Constants.ReservationTableName, 96, 36);
            db.CreateTable(Constants.ResourcesTableName, 96, 36);
            return db;
        }
    }
}
