using MyRM;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
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
            var target = new TransactionStorage_Accessor();
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
            target.SetReservations(reservations);
            var result = target.GetReservations();

            Assert.IsTrue(result.ContainsKey(customer));
            Assert.AreEqual(2, result[customer].Count);

            Assert.IsTrue(result[customer].Contains(new RID(53)));
            Assert.IsTrue(result[customer].Contains(new RID(TP.RID.Type.ROOM, "Room")));
        }

        [TestMethod]
        public void SetResourcesTest()
        {
            var target = new TransactionStorage_Accessor();
            var resources = new Dictionary<RID, Resource>();
            var rid1 = new RID(TP.RID.Type.ROOM, "Motel6");
            resources.Add(rid1, new Resource(rid1, 1000, 20));
            var rid2 = new RID(TP.RID.Type.FLIGHT, "53");
            resources.Add(rid2, new Resource(rid2, 260, 550));

            target.SetResources(resources);

            var result = target.GetResources();
            Assert.IsTrue(result.ContainsKey(rid1));
            Assert.IsTrue(result.ContainsKey(rid2));

            Assert.AreEqual(260, result[rid2].getCount());
            Assert.AreEqual(550, result[rid2].getPrice());
            Assert.AreEqual("53", result[rid2].getName());
        }
    }
}
