using MyRM;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using TP;
using MyWC;
using MyTM;
using System.Threading;

namespace TestProject
{
    
    
    /// <summary>
    ///This is a test class for MyLMTest and is intended
    ///to contain all MyLMTest Unit Tests
    ///</summary>
    [TestClass()]
    public class MyLMTest
    {
        /// <summary>
        ///A test for LockForRead
        ///</summary>
        [TestMethod()]
        public void LockForReadTest()
        {
            MyLM target = new MyLM();
            target.setDeadlockTimeout(1000);
            Transaction context = new Transaction();
            Transaction context2 = new Transaction();
            Lockable resource = new RID(RID.Type.CAR, "test");
            target.LockForRead(context, resource);
            target.LockForRead(context2, resource);
        }

        /// <summary>
        ///A test for LockForRead
        ///</summary>
        [TestMethod()]
        [ExpectedException(typeof(MyLM.DeadLockDetected))]
        public void LockForReadTest2()
        {
            MyLM target = new MyLM();
            target.setDeadlockTimeout(1000);
            Transaction context = new Transaction();
            Transaction context2 = new Transaction();
            Lockable resource = new RID(RID.Type.CAR, "test");
            target.setDeadlockTimeout(10);
            target.LockForRead(context, resource);
            target.LockForWrite(context2, resource);
        }

        /// <summary>
        ///A test for LockForWrite
        ///</summary>
        [TestMethod()]
        [ExpectedException(typeof(MyLM.DeadLockDetected))]
        public void LockForWriteTest()
        {
            MyLM target = new MyLM();
            target.setDeadlockTimeout(1000);
            Transaction context = new Transaction();
            Transaction context2 = new Transaction();
            Lockable resource = new RID(RID.Type.CAR, "test");
            target.setDeadlockTimeout(10);
            target.LockForWrite(context, resource);
            target.LockForWrite(context2, resource);
        }

        /// <summary>
        ///A test for Lock convert from read to write
        /// rl1[x], wl1[x], success
        ///</summary>
        [TestMethod()]
        public void LockConversionTest1()
        {
            MyLM target = new MyLM();
            target.setDeadlockTimeout(1000);
            Transaction context = new Transaction();
            Transaction context2 = new Transaction();
            Lockable resource = new RID(RID.Type.CAR, "test");
            target.LockForRead(context, resource);
            target.LockForWrite(context, resource);
        }

        /// <summary>
        ///A test for Lock convert from read to write
        /// rl1[x], rl2[x], wl1[x], deadlock
        ///</summary>
        [TestMethod()]
        [ExpectedException(typeof(MyLM.ResourceLocked))]
        public void LockConversionTest2()
        {
            MyLM target = new MyLM();
            target.setDeadlockTimeout(1000);
            Transaction context = new Transaction();
            Transaction context2 = new Transaction();
            Lockable resource = new RID(RID.Type.CAR, "test");
            target.setDeadlockTimeout(10);
            target.LockForRead(context, resource);
            target.LockForRead(context2, resource);
            target.LockForWrite(context, resource);
        }

        /// <summary>
        ///A test for Lock convert from read to write
        /// rl1[x], rl2[x], wl3[x], wl1[x], url2[x], uwl1[x], success
        ///</summary>
        [TestMethod()]
        public void LockConversionTest4()
        {
            MyLM target = new MyLM();
            target.setDeadlockTimeout(1000);
            Transaction context = new Transaction();
            Transaction context2 = new Transaction();
            Transaction context3 = new Transaction();
            Lockable resource = new RID(RID.Type.CAR, "test");
            target.LockForRead(context, resource);
            target.LockForRead(context2, resource);
            ManualResetEvent e = new ManualResetEvent(false);
            ManualResetEvent e1 = new ManualResetEvent(false);
            ThreadPool.QueueUserWorkItem((obj) =>
            {
                target.LockForWrite(context3, resource);
                e1.Set();
            });

            ThreadPool.QueueUserWorkItem((obj) =>
            {
                target.LockForWrite(context, resource);
                Thread.Sleep(200);
                target.UnlockWrite(context, resource);
                e.Set();
            });
            target.UnlockRead(context2, resource);

            if (!e.WaitOne(3000))
            {
                Assert.Fail("Failed");
            }
            if (!e1.WaitOne(3000))
            {
                Assert.Fail("Failed");
            }
        }

        /// <summary>
        ///A test for Lock convert from read to write
        /// rl1[x], rl2[x], wl1[x], url2[x], success
        ///</summary>
        [TestMethod()]
        public void LockConversionTest3()
        {
            MyLM target = new MyLM();
            Transaction context = new Transaction();
            Transaction context2 = new Transaction();
            Lockable resource = new RID(RID.Type.CAR, "test");
            target.LockForRead(context, resource);
            target.LockForRead(context2, resource);
            ManualResetEvent e = new ManualResetEvent(false);
            ThreadPool.QueueUserWorkItem((obj) =>
            {
                Thread.Sleep(200);
                target.UnlockRead(context2, resource);
                e.Set();
            });
            target.LockForWrite(context, resource);
            if (!e.WaitOne(2000))
            {
                Assert.Fail("Failed");
            }
        }

        /// <summary>
        ///A test for UnlockAll
        ///</summary>
        [TestMethod()]
        public void UnlockAllTest()
        {
            MyLM target = new MyLM();
            Transaction context = new Transaction();
            Transaction context2 = new Transaction();
            Lockable resource = new RID(RID.Type.CAR, "test");
            Lockable resource1 = new RID(RID.Type.FLIGHT, "test");
            target.LockForRead(context, resource);
            target.LockForRead(context, resource1);
            target.UnlockAll(context);
            target.LockForWrite(context2, resource);
            target.LockForWrite(context2, resource1);
        }

        /// <summary>
        ///A test for UnlockRead
        ///</summary>
        [TestMethod()]
        public void UnlockReadTest()
        {
            MyLM target = new MyLM();
            Transaction context = new Transaction();
            Transaction context2 = new Transaction();
            Lockable resource = new RID(RID.Type.CAR, "test");
            target.LockForRead(context, resource);
            target.UnlockRead(context, resource);
            target.LockForWrite(context2, resource);
        }

        /// <summary>
        ///A test for UnlockWrite
        ///</summary>
        [TestMethod()]
        public void UnlockWriteTest()
        {
            MyLM target = new MyLM();
            Transaction context = new Transaction();
            Transaction context2 = new Transaction();
            Lockable resource = new RID(RID.Type.CAR, "test");
            target.LockForWrite(context, resource);
            target.UnlockWrite(context, resource);
            target.LockForWrite(context2, resource);
        }
    }
}
