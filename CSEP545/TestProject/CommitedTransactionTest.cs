using MyTM;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using TP;
using System.IO;

namespace TestProject
{
    
    
    /// <summary>
    ///This is a test class for CommitedTransactionTest and is intended
    ///to contain all CommitedTransactionTest Unit Tests
    ///</summary>
    [TestClass()]
    public class CommitedTransactionTest
    {
        [TestCleanup]
        public void Cleanup()
        {
            File.Delete(TwoPhraseCommit_Accessor.LogFileName);
            TwoPhraseCommit_Accessor.isInitialized = false;
        }

        /// <summary>
        ///A test for SetState
        ///</summary>
        [TestMethod()]
        public void SetStateTest()
        {
            Transaction context = new Transaction(); 
            ResourceManagerList rms = new ResourceManagerList(MyRMTest.MockRM());
            rms.Add(MyRMTest.MockRM());
            CommitedTransaction_Accessor target = new CommitedTransaction_Accessor(context, rms); 
            CommitState state = CommitState.Prepared;
            target.SetState(0, state);
            Assert.IsFalse(target.stepWaitEvent.WaitOne(300));
            target.SetState(1, state);
            Assert.IsTrue(target.stepWaitEvent.WaitOne(1000));
        }

        /// <summary>
        ///A test for StartCommit
        ///</summary>
        [TestMethod()]
        public void StartCommitTest()
        {
            Transaction context = new Transaction();
            ResourceManagerList rms = new ResourceManagerList(MyRMTest.MockRM());
            rms.Add(MyRMTest.MockRM());
            CommitedTransaction_Accessor target = new CommitedTransaction_Accessor(context, rms);
            CommitedTransaction_Accessor.stepTimeout = 1000;
            target.StartCommit();
            Assert.IsTrue(target.DoneEvent.WaitOne(1000));
        }

        /// <summary>
        ///A test for ToString
        ///</summary>
        [TestMethod()]
        public void ToStringTest()
        {
            Transaction context = new Transaction();
            ResourceManagerList rms = new ResourceManagerList(MyRMTest.MockRM());
            rms.Add(MyRMTest.MockRM());
            CommitedTransaction_Accessor target = new CommitedTransaction_Accessor(context, rms);
            string expected = context.Id.ToString() + "\tNone\ttest,test,";
            string actual;
            actual = target.ToString();
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        ///A test for FromString
        ///</summary>
        [TestMethod()]
        public void FromStringTest()
        {
            Transaction context = new Transaction();
            string input = context.Id.ToString() + "\tCommitted\ttest,test1,";
            CommitedTransaction actual;
            actual = CommitedTransaction.FromString(input);
            Assert.AreEqual(CommitState.Committed, actual.State);
            Assert.AreEqual(context.Id, actual.Context.Id);
            Assert.AreEqual(2, actual.ResouceManagers.ResourceManagers.Count);
        }
    }
}
