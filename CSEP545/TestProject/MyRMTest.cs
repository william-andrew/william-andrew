using MyRM;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using TP;

namespace TestProject
{   
    /// <summary>
    ///This is a test class for MyRMTest and is intended
    ///to contain all MyRMTest Unit Tests
    ///</summary>
    [TestClass]
    public class MyRMTest
    {
        [TestCleanup]
        public void Cleanup()
        {
            MyRM.MyRM rm = new MyRM.MyRM();
            rm.SetName("test");
        }

        /// <summary>
        ///A test for Add
        ///</summary>
        [TestMethod()]
        public void AddTest()
        {
            MyRM.MyRM_Accessor rm = MockRM();           
            Transaction context = new Transaction(); 
            RID i = new RID(RID.Type.ROOM, "test1");
            int count = 1; 
            int price = 3; 
            rm.Add(context, i, count, price);
            Assert.AreEqual(price, rm.QueryPrice(context, i));
        }

        /// <summary>
        ///A test for Commit
        /// also test QueryPrice
        ///</summary>
        [TestMethod()]
        public void CommitTest()
        {
            MyRM.MyRM_Accessor rm = MockRM();
            
            rm.SetName("test");
            Transaction context = new Transaction();
            Transaction context1 = new Transaction();
            RID i = new RID(RID.Type.ROOM, "test1");
            int count = 1;
            int price = 3;
            rm.Add(context, i, count, price);
            rm.Commit(context);
            Assert.AreEqual(price, rm.QueryPrice(context1, i));
        }

        /// <summary>
        ///A test for Delete,
        /// also test Query, Reserve, QueryReservedPrice, QueryReserved
        ///</summary>
        [TestMethod()]
        [ExpectedException(typeof(ArgumentException))]
        public void DeleteTest()
        {
            MyRM.MyRM_Accessor rm = MockRM();
            rm.SetName("test" + Guid.NewGuid());
            Transaction context = new Transaction();
            Customer c = new Customer();
            RID rid = new RID(RID.Type.ROOM, "test1");
            int count = 5;
            int price = 3;
            Assert.IsFalse(rm.Delete(context, rid));
            rm.Add(context, rid, count, price);
            rm.Delete(context, rid, 2);
            Assert.AreEqual(3, rm.Query(context, rid));
            rm.Reserve(context, c, rid);
            rm.Delete(context, rid);
            Assert.AreEqual(0, rm.QueryReservedPrice(context, c));
            Assert.IsTrue(string.IsNullOrEmpty(rm.QueryReserved(context, c)));
            rm.Query(context, rid);
            Assert.Fail("shall not hit this line.");
        }

        /// <summary>
        ///A test for Enlist
        ///</summary>
        [TestMethod()]
        public void EnlistTest()
        {
            MyRM.MyRM_Accessor rm = MockRM();
            rm.Enlist(new Transaction());
        }

        /// <summary>
        ///A test for SelfDestruct
        ///</summary>
        //[TestMethod()]
        public void SelfDestructTest()
        {
            MyRM.MyRM_Accessor rm = MockRM();
            int diskWritesToWait = 0; // TODO: Initialize to an appropriate value
            rm.SelfDestruct(diskWritesToWait);
            Assert.Inconclusive("A method that does not return a value cannot be verified.");
        }

        /// <summary>
        ///A test for Shutdown
        ///</summary>
        //[TestMethod()]
        public void ShutdownTest()
        {
            MyRM.MyRM rm = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            rm.Shutdown();
            Assert.Inconclusive("A method that does not return a value cannot be verified.");
        }

        /// <summary>
        ///A test for UnReserve
        ///</summary>
        [TestMethod()]
        public void UnReserveTest()
        {
            MyRM.MyRM_Accessor rm = MockRM();
            Transaction context = new Transaction();
            Customer c = new Customer();
            RID rid = new RID(RID.Type.ROOM, "test1");
            RID rid1 = new RID(RID.Type.CAR, "test1");
            int count = 5;
            int price = 3;
            Assert.IsFalse(rm.Delete(context, rid));
            rm.Add(context, rid, count, price);
            rm.Add(context, rid1, count, price);
            rm.Reserve(context, c, rid);
            Assert.AreEqual(count, rm.Query(context, rid1));
            Assert.AreEqual(count - 1, rm.Query(context, rid));
            rm.UnReserve(context, c);
            Assert.AreEqual(count, rm.Query(context, rid1));
            Assert.AreEqual(count, rm.Query(context, rid));
        }

        private MyRM.MyRM_Accessor MockRM()
        {
            var tm = new MyTM.MyTM();
            var rm = new MyRM_Accessor
                                              {
                                                  TransactionStorage = new TransactionStorage(new MockDatabase()),
                                                  TransactionManager = tm                                             
                                              };
            tm.Register(rm);
            return rm;
        }
    }
}
