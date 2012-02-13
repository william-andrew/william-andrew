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
    [TestClass()]
    public class MyRMTest
    {
        [TestCleanup()]
        public void Cleanup()
        {
            MyRM.MyRM target = new MyRM.MyRM();
            target.SetName("test");
        }

        /// <summary>
        ///A test for Add
        ///</summary>
        [TestMethod()]
        public void AddTest()
        {
            MyRM.MyRM_Accessor target = MockRM();
            Transaction context = new Transaction(); 
            RID i = new RID(RID.Type.ROOM, "test1");
            int count = 1; 
            int price = 3; 
            target.Add(context, i, count, price);
            Assert.AreEqual(price, target.QueryPrice(context, i));
        }

        /// <summary>
        ///A test for Commit
        /// also test QueryPrice
        ///</summary>
        [TestMethod()]
        public void CommitTest()
        {
            MyRM.MyRM_Accessor target = MockRM();
            target.SetName("test");
            Transaction context = new Transaction();
            Transaction context1 = new Transaction();
            RID i = new RID(RID.Type.ROOM, "test1");
            int count = 1;
            int price = 3;
            target.Add(context, i, count, price);
            target.Commit(context);
            Assert.AreEqual(price, target.QueryPrice(context1, i));
        }

        /// <summary>
        ///A test for Delete,
        /// also test Query, Reserve, QueryReservedPrice, QueryReserved
        ///</summary>
        [TestMethod()]
        [ExpectedException(typeof(ArgumentException))]
        public void DeleteTest()
        {
            MyRM.MyRM_Accessor target = MockRM();
            target.SetName("test" + Guid.NewGuid());
            Transaction context = new Transaction();
            Customer c = new Customer();
            RID rid = new RID(RID.Type.ROOM, "test1");
            int count = 5;
            int price = 3;
            Assert.IsFalse(target.Delete(context, rid));
            target.Add(context, rid, count, price);
            target.Delete(context, rid, 2);
            Assert.AreEqual(3, target.Query(context, rid));
            target.Reserve(context, c, rid);
            target.Delete(context, rid);
            Assert.AreEqual(0, target.QueryReservedPrice(context, c));
            Assert.IsTrue(string.IsNullOrEmpty(target.QueryReserved(context, c)));
            target.Query(context, rid);
            Assert.Fail("shall not hit this line.");
        }

        /// <summary>
        ///A test for Enlist
        ///</summary>
        //[TestMethod()]
        public void EnlistTest()
        {
            MyRM.MyRM_Accessor target = MockRM();
            Transaction context = null; // TODO: Initialize to an appropriate value
            target.Enlist(context);
            Assert.Inconclusive("A method that does not return a value cannot be verified.");
        }

        /// <summary>
        ///A test for SelfDestruct
        ///</summary>
        //[TestMethod()]
        public void SelfDestructTest()
        {
            MyRM.MyRM_Accessor target = MockRM();
            int diskWritesToWait = 0; // TODO: Initialize to an appropriate value
            target.SelfDestruct(diskWritesToWait);
            Assert.Inconclusive("A method that does not return a value cannot be verified.");
        }

        /// <summary>
        ///A test for Shutdown
        ///</summary>
        //[TestMethod()]
        public void ShutdownTest()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            target.Shutdown();
            Assert.Inconclusive("A method that does not return a value cannot be verified.");
        }

        /// <summary>
        ///A test for UnReserve
        ///</summary>
        [TestMethod()]
        public void UnReserveTest()
        {
            MyRM.MyRM_Accessor target = MockRM();
            Transaction context = new Transaction();
            Customer c = new Customer();
            RID rid = new RID(RID.Type.ROOM, "test1");
            RID rid1 = new RID(RID.Type.CAR, "test1");
            int count = 5;
            int price = 3;
            Assert.IsFalse(target.Delete(context, rid));
            target.Add(context, rid, count, price);
            target.Add(context, rid1, count, price);
            target.Reserve(context, c, rid);
            Assert.AreEqual(count, target.Query(context, rid1));
            Assert.AreEqual(count - 1, target.Query(context, rid));
            target.UnReserve(context, c);
            Assert.AreEqual(count, target.Query(context, rid1));
            Assert.AreEqual(count, target.Query(context, rid));
        }

        private MyRM.MyRM_Accessor MockRM()
        {
            MyRM.MyRM_Accessor accessor = new MyRM_Accessor();
            accessor.TransactionStorage = new TransactionStorage(new MockDatabase());      
            return accessor;
        }
    }
}
