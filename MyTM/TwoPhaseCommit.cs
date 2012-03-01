using System.Collections.Generic;
using TP;
using System.Threading;
using System.IO;

namespace MyTM
{
    public enum CommitState
    {
        None,
        Committed,
        Prepared,
        Done,
        Rollbacked
    }

    // TODO: implemnt fault recovery
    // 1. load the commited transaciton log for disk and restore the states
    // 2. redo or undo the changes;
    public static class TwoPhaseCommit
    {
        private const string LogFileName = "commitedTransaction.log";
        private static Dictionary<Transaction, CommitedTransaction> committedTransactions = new Dictionary<Transaction, CommitedTransaction>();
        private static bool isInitialized = false;
        private static object syncRoot = new object();

        /// <summary>
        /// Initialize the class 
        /// If the log file exists, it will try to recover from previous run first by looking for 
        /// committed transactions which are in CommitState.Committed || CommitState.Prepared state.
        /// </summary>
        public static void Init()
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

                        foreach (CommitedTransaction trans in committedTransactions.Values)
                        {
                            if (trans.State == CommitState.Committed || trans.State == CommitState.Prepared)
                            {
                                trans.Recover();
                            }
                        }
                    }

                    isInitialized = true;
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
            Init();
            CommitedTransaction trans = new CommitedTransaction(context, rms);
            lock (committedTransactions)
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

            ThreadPool.QueueUserWorkItem(o=>trans.StartCommit());
            return trans;
        }

        public static void DoneCommit(Transaction context)
        {
            lock (committedTransactions)
            {
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
            lock (committedTransactions)
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
    }
}
