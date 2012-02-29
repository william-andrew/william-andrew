﻿using MyTM;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using TP;
using System.IO;
using System.Collections.Generic;

namespace TestProject
{
    /// <summary>
    ///This is a test class for TwoPhraseCommitTest and is intended
    ///to contain all TwoPhraseCommitTest Unit Tests
    ///</summary>
    [TestClass()]
    public class TwoPhraseCommitTest
    {
        [TestCleanup]
        public void Cleanup()
        {
            File.Delete(TwoPhraseCommit_Accessor.LogFileName);
            TwoPhraseCommit_Accessor.isInitialized = false;
        }
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
            using (var sr = new StreamReader(TwoPhraseCommit_Accessor.LogFileName))
            {
                log = sr.ReadToEnd();
            }

            string expected = context.Id.ToString("d") + "\tDone\ttest,\r";
            Assert.IsTrue(log.Contains(expected)); 
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

        private void AssertToLogEqual(string log1, string log2)
        {
            HashSet<string> a = new HashSet<string>();
            string[] lines = log1.Split(new char[] { '\r' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (string line in lines)
            {
                a.Add(line);
            }

            lines = log2.Split(new char[] { '\r' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (string line in lines)
            {
                Assert.IsTrue(a.Contains(line));
            }

        }
    }
}
