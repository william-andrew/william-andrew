using MyTM;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using TP;
using System.IO;
namespace TestProject
{
    /// <summary>
    ///This is a test class for TwoPhraseCommitTest and is intended
    ///to contain all TwoPhraseCommitTest Unit Tests
    ///</summary>
    [TestClass()]
    public class TwoPhraseCommitTest
    {

        /// <summary>
        ///A test for Commit
        ///</summary>
        [TestMethod()]
        public void CommitTestWithoutOldLog()
        {
            File.Delete(TwoPhraseCommit_Accessor.LogFileName);
            Transaction context = new Transaction();
            ResourceManagerList rms = new ResourceManagerList(MyRMTest.MockRM()); 
            CommitedTransaction actual;
            actual = TwoPhraseCommit.Commit(context, rms);
            if (!actual.DoneEvent.WaitOne(1000))
            {
                Assert.Fail("Timeout in commit");
            }
            string log;
            using (StreamReader sr = new StreamReader(TwoPhraseCommit_Accessor.LogFileName))
            {
                log = sr.ReadToEnd();
            }

            Assert.AreEqual(context.Id.ToString("d") + "\tDone\ttest,", log);
        }

        /// <summary>
        ///A test for Commit with recovering from previous states
        ///</summary>
        [TestMethod()]
        public void CommitRecoveryTest()
        {
            string log = "12345678-1234-1234-1234-123456789012\tCommitted\ttest4,\r12345678-1234-1234-1234-123456789013\tPrepared\ttest1,\r12345678-1234-1234-1234-123456789014\tNone\ttest2,\r12345678-1234-1234-1234-123456789012\tRollbacked\ttest3,";
            using (StreamWriter sw = new StreamWriter(TwoPhraseCommit_Accessor.LogFileName))
            {
                sw.Write(log);
            }

            Transaction context = new Transaction();
            ResourceManagerList rms = new ResourceManagerList(MyRMTest.MockRM());
            CommitedTransaction actual;
            actual = TwoPhraseCommit.Commit(context, rms);
            if (!actual.DoneEvent.WaitOne(2000))
            {
                Assert.Fail("Timeout in commit");
            }

            using (StreamReader sr = new StreamReader(TwoPhraseCommit_Accessor.LogFileName))
            {
                log = sr.ReadToEnd();
            }

            string expected = "12345678-1234-1234-1234-123456789012\tRollbacked\ttest4,\r12345678-1234-1234-1234-123456789013\tDone\ttest1,\r" + context.Id.ToString("d") + "\tDone\ttest,\r";
            Assert.AreEqual(expected, log);
        }
    }
}
