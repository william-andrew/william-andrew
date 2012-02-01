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
            
        }

        /// <summary>
        ///A test for Add
        ///</summary>
        //[TestMethod()]
        public void AddTest()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            Transaction context = null; // TODO: Initialize to an appropriate value
            RID i = null; // TODO: Initialize to an appropriate value
            int count = 0; // TODO: Initialize to an appropriate value
            int price = 0; // TODO: Initialize to an appropriate value
            bool expected = false; // TODO: Initialize to an appropriate value
            bool actual;
            actual = target.Add(context, i, count, price);
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }

        /// <summary>
        ///A test for Commit
        ///</summary>
        //[TestMethod()]
        public void CommitTest()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            Transaction context = null; // TODO: Initialize to an appropriate value
            target.Commit(context);
            Assert.Inconclusive("A method that does not return a value cannot be verified.");
        }

        /// <summary>
        ///A test for Delete
        ///</summary>
        //[TestMethod()]
        public void DeleteTest()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            Transaction context = null; // TODO: Initialize to an appropriate value
            RID rid = null; // TODO: Initialize to an appropriate value
            bool expected = false; // TODO: Initialize to an appropriate value
            bool actual;
            actual = target.Delete(context, rid);
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }

        /// <summary>
        ///A test for Delete
        ///</summary>
        //[TestMethod()]
        public void DeleteTest1()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            Transaction context = null; // TODO: Initialize to an appropriate value
            RID rid = null; // TODO: Initialize to an appropriate value
            int count = 0; // TODO: Initialize to an appropriate value
            bool expected = false; // TODO: Initialize to an appropriate value
            bool actual;
            actual = target.Delete(context, rid, count);
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }

        /// <summary>
        ///A test for Enlist
        ///</summary>
        //[TestMethod()]
        public void EnlistTest()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            Transaction context = null; // TODO: Initialize to an appropriate value
            target.Enlist(context);
            Assert.Inconclusive("A method that does not return a value cannot be verified.");
        }

        /// <summary>
        ///A test for ListCustomers
        ///</summary>
        //[TestMethod()]
        public void ListCustomersTest()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            Transaction context = null; // TODO: Initialize to an appropriate value
            Customer[] expected = null; // TODO: Initialize to an appropriate value
            Customer[] actual;
            actual = target.ListCustomers(context);
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }

        /// <summary>
        ///A test for ListResources
        ///</summary>
        //[TestMethod()]
        public void ListResourcesTest()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            Transaction context = null; // TODO: Initialize to an appropriate value
            RID.Type type = new RID.Type(); // TODO: Initialize to an appropriate value
            string[] expected = null; // TODO: Initialize to an appropriate value
            string[] actual;
            actual = target.ListResources(context, type);
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }

        /// <summary>
        ///A test for Query
        ///</summary>
        //[TestMethod()]
        public void QueryTest()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            Transaction context = null; // TODO: Initialize to an appropriate value
            RID rid = null; // TODO: Initialize to an appropriate value
            int expected = 0; // TODO: Initialize to an appropriate value
            int actual;
            actual = target.Query(context, rid);
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }

        /// <summary>
        ///A test for QueryPrice
        ///</summary>
        //[TestMethod()]
        public void QueryPriceTest()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            Transaction context = null; // TODO: Initialize to an appropriate value
            RID rid = null; // TODO: Initialize to an appropriate value
            int expected = 0; // TODO: Initialize to an appropriate value
            int actual;
            actual = target.QueryPrice(context, rid);
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }

        /// <summary>
        ///A test for QueryReserved
        ///</summary>
        //[TestMethod()]
        public void QueryReservedTest()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            Transaction context = null; // TODO: Initialize to an appropriate value
            Customer customer = null; // TODO: Initialize to an appropriate value
            string expected = string.Empty; // TODO: Initialize to an appropriate value
            string actual;
            actual = target.QueryReserved(context, customer);
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }

        /// <summary>
        ///A test for QueryReservedPrice
        ///</summary>
        //[TestMethod()]
        public void QueryReservedPriceTest()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            Transaction context = null; // TODO: Initialize to an appropriate value
            Customer customer = null; // TODO: Initialize to an appropriate value
            int expected = 0; // TODO: Initialize to an appropriate value
            int actual;
            actual = target.QueryReservedPrice(context, customer);
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }

        /// <summary>
        ///A test for Reserve
        ///</summary>
        //[TestMethod()]
        public void ReserveTest()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            Transaction context = null; // TODO: Initialize to an appropriate value
            Customer c = null; // TODO: Initialize to an appropriate value
            RID i = null; // TODO: Initialize to an appropriate value
            bool expected = false; // TODO: Initialize to an appropriate value
            bool actual;
            actual = target.Reserve(context, c, i);
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("Verify the correctness of this test method.");
        }

        /// <summary>
        ///A test for SelfDestruct
        ///</summary>
        //[TestMethod()]
        public void SelfDestructTest()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
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
        //[TestMethod()]
        public void UnReserveTest()
        {
            MyRM.MyRM target = new MyRM.MyRM(); // TODO: Initialize to an appropriate value
            Transaction context = null; // TODO: Initialize to an appropriate value
            Customer c = null; // TODO: Initialize to an appropriate value
            target.UnReserve(context, c);
            Assert.Inconclusive("A method that does not return a value cannot be verified.");
        }
    }
}
