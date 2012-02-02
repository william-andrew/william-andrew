using System.Runtime.Serialization;
using System.Collections.Generic;
using TP;

namespace MyRM
{
    /// <summary>
    /*  Base implementation of interface LM (see TP.LM)
        Doesn't do lock conversion, so T1(read) -> T1(write) will cause T1 
        to deadlock with itself. */
    /// </summary> 
    public class MyLM : TP.LM
    {
        // Lock table
        Dictionary<TP.Lockable,ResourceEntry> ResourceTable;
        private long deadlockTimeout;
        public static readonly int DEFAULT_DEADLOCK_TIMEOUT = 10000;
        public MyLM()
        {
            this.ResourceTable = new Dictionary<TP.Lockable, ResourceEntry>();
            deadlockTimeout = DEFAULT_DEADLOCK_TIMEOUT;
        }

        // Useful if throwing exceptions when the resource is locked
        public class ResourceLocked : System.Exception
        {
            public ResourceLocked()
                : base()
            {
            }

            public ResourceLocked(string message)
                : base(message)
            {
            }

            public ResourceLocked(string message, System.Exception e)
                : base(message, e)
            {
            }
        }

        /*  Deadlock Exception */
        [System.Serializable()]
        public class DeadLockDetected : System.Exception
        {
            public DeadLockDetected()
                : base()
            {
            }

            public DeadLockDetected(string message)
                : base(message)
            {
            }

            public DeadLockDetected(string message, System.Exception e)
                : base(message, e)
            {
            }

            // Constructor: This one is needed for exception serialization
            public DeadLockDetected(SerializationInfo info, StreamingContext context)
                : base(info, context)
            {
            }
        }

        public enum LockMode
        {
            Null,
            Read,	// Read Mode
            Update, // Update Mode
            Write,	// Write Mode
            _Length // The number of LockModes
        }

        // Defines types of lock requests, which can be granted while holding other locks
        static bool[,] CompatibilityTable = new bool[(int)MyLM.LockMode._Length, (int)MyLM.LockMode._Length]
		{
			{	// Null
				true, true, false, true	
			},
			{	// Read
				true, true, false, false
			},
			{	// Update
				false, false, false, false
			},
			{	// Write
				true, false, false, false
			}
		};

        // Class for a shared resource
        class ResourceEntry
        {
            /// <summary>
            /*	A ResourceEntry is a lock entry in the lock table.  It includes a separate hash table, 
                  "transactions", for each lock mode, containing transactions holding a lock in that mode, 
                  and a field "locked" containing the strongest lock mode held by any transaction on this resource.
                  A transaction should have exactly one lock type on a resource, corresponding to the 
                  strongest lock mode granted for that resource */
            /// </summary>
            public ResourceEntry()
            {
                this.transactions = new System.Collections.Hashtable[(int)MyLM.LockMode._Length];
                this.locked = MyLM.LockMode.Null;
            }

            /* Can a LockMode lock request be granted?
               Returns true if request can be granted, false otherwise */
            public bool Compatible(LockMode request)
            {
                return CompatibilityTable[(int)this.locked, (int)request];
            }

            /// <summary>
            /// Checks whether the transaction has a specific lock on the resource
            /// </summary>
            /// <param name="context"></param>
            /// <param name="request"></param>
            /// <returns></returns>
            public bool HasLock(Transaction context, LockMode request)
            {
                return this.transactions[(int)request] != null && this.transactions[(int)request][context] != null;
            }

            /* Add a lock of type _request_ for transaction _context_ */
            public void Register(TP.Transaction context, LockMode request)
            {
                // First get the hash table for this lock mode on this resource entry, if it exists
                System.Collections.Hashtable transactionList = this.transactions[(int)request];

                // If there is no hash table for this lock mode, create one
                if (transactionList == null)
                {
                    transactionList = new System.Collections.Hashtable();
                    this.transactions[(int)request] = transactionList;
                }

                // Add the transaction to the list for _request_ lock mode
                transactionList[context] = context;

                // Update the strongest lock mode, if necessary	  
                if (request > locked)
                    locked = request;

                // If we set locked mode to Update lock mode, we do not reset 
                // the event because we still want to track the read locks.
                if (locked != LockMode.Update)
                    this.UnlockEvent.Reset();
            }

            /// <summary>
            /*  Release a lock of mode _request_ for transaction _context_
                Return if context does not hold a lock for this resource */
            /// </summary>
            public void Unregister(TP.Transaction context, LockMode request)
            {
                // First get the hash table for this lock mode
                System.Collections.Hashtable transactionList = this.transactions[(int)request];

                if (transactionList == null || transactionList[context] == null)
                {
                    // This transaction wasn't registered, return immediately
                    return;
                }

                transactionList.Remove(context);

                for (LockMode l = request; l > LockMode.Null; locked = --l)
                {
                    // recalculate the strongest lock mode
                    System.Collections.Hashtable nextTransactionList = this.transactions[(int)l];
                    if (nextTransactionList == null)
                        continue;
                    if (nextTransactionList.Count > 0)
                        break;
                }

                // common case plus special case where we want to convert a read lock to write lock
                if (request > locked || (this.locked == LockMode.Update && this.transactions[(int)LockMode.Read].Count == 0))
                    // if anyone was waiting for this lock, they should recheck
                    this.UnlockEvent.Set();
            }


            System.Threading.ManualResetEvent evnt;

            // Define a property for UnlockEvent
            public System.Threading.ManualResetEvent UnlockEvent
            {
                get
                {
                    /* Avoid race condition where two threads can create 
                       two different evnt objects for one resource */
                    lock (this)
                    {
                        if (evnt == null)
                            evnt = new System.Threading.ManualResetEvent(false);
                    }
                    return evnt;
                }
            }

            public bool DownGradedLockRequest(TP.Transaction context, LockMode request)
            {
                System.Collections.Hashtable transactionList = this.transactions[(int)LockMode.Write];
                if (request == LockMode.Read && (transactionList != null && transactionList[context] != null))
                    return true;

                return false;
            }

            // One entry for each LockMode
            System.Collections.Hashtable[] transactions;
            LockMode locked;
        }


        /* Lock passed in resource _resource_ in mode _mode_				 
          This method needs additional code to implement lock conversion.
          It does deadlock detection by timeout */
        private void Lock(TP.Transaction context, TP.Lockable resource, LockMode mode)
        {
            ResourceEntry lockTarget;

            /* Get exclusive access to the lock table
               This avoids race conditions, such as two conflicting locks being granted to concurrent threads (i.e., transactions),
               or two physical resources created for one logical resource on behalf of two threads. */

            lock (this.ResourceTable)
            {
                // Pick the needed resource from ResourceTable
                
                this.ResourceTable.TryGetValue(resource, out lockTarget);
                
                // Create a ResourceEntry for resource, if there is none
                if (lockTarget == null)
                {
                    lockTarget = new ResourceEntry();
                    this.ResourceTable[resource] = lockTarget;
                }
            }

            bool triedLockConvertion = false;
            for (int c = 0; ; c++)
            {
                /* If someone else holds a lock 
                   (the loop already executed once and failed to set the lock)
                   wait for 5 seconds for the lock to be released and
                   if it doesn't happen, timeout for deadlock,
                   else try again to set the lock */
                if (c > 0)
                    if (!lockTarget.UnlockEvent.WaitOne(DEFAULT_DEADLOCK_TIMEOUT, false))
                    {
                        // if other transactions still hold some locks, we failed to convert the read lock to write lock
                        // Therefore, put the read lock back, remove the update lock and throw exception
                        if (triedLockConvertion)
                        {
                            lockTarget.Register(context, LockMode.Read);
                            lockTarget.Unregister(context, LockMode.Update);
                            throw new ResourceLocked(string.Format("Cannot convert read lock on {0} to write lock", resource));
                        }
                        else
                            throw new DeadLockDetected(string.Format("Resource {0} timed out", resource));
                    }

                if (c > 0 && !triedLockConvertion)
                    System.Console.WriteLine(string.Format("Attempt {0} in resource {1}", c, resource));

                // Get exclusive access to the resource
                lock (lockTarget)
                {
                    // return if the lock is already registered for this transaction
                    if (lockTarget.HasLock(context, mode))
                    {
                        return;
                    }

                    // Set the lock, if you can
                    if (lockTarget.Compatible(mode))
                    {
                        lockTarget.Register(context, mode);
                        return;
                    }
                    else if (lockTarget.DownGradedLockRequest(context, mode))
                        // context has a write lock on lockTarget and requested a read lock so no action is required.
                        return;

                    // Lock conversion when requesting write lock on the existing read lock by this transaction
                    if (mode == LockMode.Write)
                    {
                        if (lockTarget.HasLock(context, LockMode.Read))
                        {
                            // release the read lock and obtain Update lock. 
                            lockTarget.Unregister(context, LockMode.Read);
                            lockTarget.Register(context, LockMode.Update);
                            triedLockConvertion = true;
                        }
                        else if (lockTarget.HasLock(context, LockMode.Update))
                        {
                            // unlock the Update lock if conversion from read to write lock succeed.
                            lockTarget.Unregister(context, LockMode.Update);
                            lockTarget.Register(context, mode);
                            return;
                        }
                    }
                }
            }

            // Debug
            throw new System.Exception("Internal Error");
        }


        // Get a read lock for the resource
        public void LockForRead(TP.Transaction context, TP.Lockable resource)
        {
            Lock(context, resource, MyLM.LockMode.Read);
        }


        // Get a write lock for the resource
        public void LockForWrite(TP.Transaction context, TP.Lockable resource)
        {
            Lock(context, resource, MyLM.LockMode.Write);
        }


        // Unlock a resource: find the entry and call unregister lock
        private void Unlock(TP.Transaction context, TP.Lockable resource, LockMode mode)
        {
            ResourceEntry lockTarget;

            // Get exclusive access to the lock table
            lock (this.ResourceTable)
            {
                this.ResourceTable.TryGetValue(resource, out lockTarget);

                // Check if the resource wasn't locked, and if so, then return
                if (lockTarget == null)
                    return;
            }

            // Get exclusive access to the resource
            lock (lockTarget)
            {
                lockTarget.Unregister(context, mode);
            }

        }


        // A shortcut to unlock a read lock
        public void UnlockRead(TP.Transaction context, TP.Lockable resource)
        {
            Unlock(context, resource, LockMode.Read);
        }


        // A shortcut to unlock a write lock
        public void UnlockWrite(TP.Transaction context, TP.Lockable resource)
        {
            Unlock(context, resource, LockMode.Write);
        }


        // Unlock all resources for the passed in transaction
        public void UnlockAll(TP.Transaction context)
        {
            // Loop over the resources
            lock (this.ResourceTable)
            {
                // Get resource enumerator
                System.Collections.IDictionaryEnumerator resenum = ResourceTable.GetEnumerator();

                // Loop over resources
                while (resenum.MoveNext())
                {
                    ResourceEntry lockTarget = (ResourceEntry)resenum.Value;
                    lock (lockTarget)
                    {
                        // Unregister all unlock modes for current resource
                        for (int lockMode = (int)LockMode.Read; lockMode < (int)LockMode._Length; lockMode++)
                        {
                            lockTarget.Unregister(context, (LockMode)lockMode);
                        }
                    }
                }
            }

            System.Console.WriteLine("----Unlocked all for Tx: {0}--------", context.Id);
        }


        public void setDeadlockTimeout(long ms)
        {
            deadlockTimeout = ms;
        }

        public long getDeadlockTimeout()
        {
            return deadlockTimeout;
        }
    }
}

