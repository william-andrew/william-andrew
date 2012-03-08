using System.Collections.Generic;
using TP;
using System.Threading;
using System.IO;
using System;

namespace MyTM
{
    public enum CommitState
    {
        None,      // the default state
        Committed, // when the 2PC starts to commmit
        Prepared,  // All RMs are prepared
        Done,      // All RMs are commited
        Rollbacked // Transaction rollbacked
    }

    // TODO: implemnt fault recovery
    // 1. load the commited transaciton log for disk and restore the states
    // 2. redo or undo the changes;
    public static class TwoPhaseCommit
    {
        public const string LogFileName = "commitedTransaction.log";
        private static Dictionary<Transaction, CommitedTransaction> committedTransactions = new Dictionary<Transaction, CommitedTransaction>();
        private static bool isInitialized = false;
        private static object syncRoot = new object();

        public static bool PrepareFail
        {
            get;
            set;
        }

        public static bool CommitFail
        {
            get;
            set;
        }
        public static bool IsInitialized
        {
            get { return isInitialized; }
        }

        static TwoPhaseCommit()
        {
            StartUp();
        }
        /// <summary>
        /// Initialize the class 
        /// If the log file exists, it will try to recover from previous run first by looking for 
        /// committed transactions which are in CommitState.Committed || CommitState.Prepared state.
        /// </summary>
        public static void StartUp()
        {
            lock (syncRoot)
            {
                if (!isInitialized)
                {
                    if (File.Exists(LogFileName))
                    {
                        string line;
                        lock (committedTransactions)
                        {
                            committedTransactions = new Dictionary<Transaction, CommitedTransaction>();
                            using (StreamReader reader = new StreamReader(LogFileName))
                            {
                                while ((line = reader.ReadLine()) != null)
                                {
                                    if (!string.IsNullOrWhiteSpace(line))
                                    {
                                        CommitedTransaction trans = CommitedTransaction.FromString(line);
                                        // only recover the transaction which needs to be recovered
                                        if (trans.State == CommitState.Committed || trans.State == CommitState.Prepared)
                                        {
                                            committedTransactions[trans.Context] = trans;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    isInitialized = true;

                    // The DB recovery code shall have locked the resources before this statement. 
                    // Therefore, the subsequential query on the modified resources will be blocked until the recover process is finished
                    foreach (CommitedTransaction trans in committedTransactions.Values)
                    {
                        if (trans.State == CommitState.Committed || trans.State == CommitState.Prepared)
                        {
                            trans.Recover();
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Commits a transaction
        /// Initialize and recover from previous failure if necessary.
        /// See document for Init for details
        /// Use new thread to do two phrase commit on the transaction
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="rms"></param>
        /// <returns></returns>
        public static CommitedTransaction Commit(Transaction context, ResourceManagerList rms)
        {
            StartUp();

            CommitedTransaction trans = new CommitedTransaction(context, rms);
            lock (syncRoot)
            {
                if (!committedTransactions.ContainsKey(context))
                {
                    committedTransactions.Add(context, trans);
                }
                else
                {
                    trans = committedTransactions[context];
                }
            }

            ThreadPool.QueueUserWorkItem(o =>
                {
                    if (trans.State < CommitState.Committed)
                    {
                        trans.State = CommitState.Committed;
                        trans.StartCommit(PrepareFail, CommitFail);
                    }
                });
            return trans;

        }

        public static void PrintMessage()
        {
            foreach (var r in committedTransactions.Values)
            {
                if (!string.IsNullOrWhiteSpace(r.Message))
                {
                    Console.WriteLine(r.Message);
                    r.Message = string.Empty;
                }

            }
        }

        public static void DoneCommit(Transaction context)
        {
            lock (syncRoot)
            {
                if (committedTransactions.ContainsKey(context) && !string.IsNullOrWhiteSpace(committedTransactions[context].Message))
                {
                    Console.WriteLine(committedTransactions[context].Message);
                }
                committedTransactions.Remove(context);
                // do not flush to log because having extra done transaction does not hurt recovery 
                // and recovery will correct it when loading the log
            }
        }

        
        /// <summary>
        /// Flush all committedTransactions to disk
        /// </summary>
        public static void WriteLog()
        {
            string fileContent = string.Empty;
            lock (syncRoot)
            {
                foreach (CommitedTransaction t in committedTransactions.Values)
                {
                    fileContent += t.ToString() + "\r";
                }

                using (StreamWriter writer = new StreamWriter(LogFileName))
                {
                    writer.Write(fileContent);
                }
            }
        }

        private static void Cleanup()
        {
            isInitialized = false;
            committedTransactions = new Dictionary<Transaction, CommitedTransaction>();
        }
    }
}
