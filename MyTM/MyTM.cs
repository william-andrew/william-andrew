using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Http;
using System.Runtime.Serialization.Formatters;
using System.Threading;
using TP;


namespace MyTM
{
    /// <summary>
    /*  Transaction Manager */
    /// </summary>
    public class MyTM : MarshalByRefObject, TP.TM
    {
        public class TransactionState{}
        private static Dictionary<string, RM> _resourceManagers = _resourceManagers = new Dictionary<string, RM>();
        private readonly Dictionary<Transaction, ResourceManagerList> _resourceManagersEnlistedInTransactions
            = new Dictionary<Transaction, ResourceManagerList>();


        private static readonly AutoResetEvent ShutdownEvent = new AutoResetEvent(false);
        private const int TransactionTimeout = 12000;
        private readonly Thread _transactionScavengerThread;

        public MyTM()
        {
            Console.WriteLine("TM: Transaction Manager instantiated");
            // by default, the TwoPhaseCommit shall not have the debugging properties PrepareFail and CommitFail set
            TwoPhaseCommit.PrepareFail = false;
            TwoPhaseCommit.CommitFail = false;
            TwoPhaseCommit.StartUp();
            _transactionScavengerThread = new Thread(TransactionScavenger) { Name = "TransactionScavengerThread" };
            _transactionScavengerThread.Start();
        }

        /// <summary>
        /// Wait until tm is ready to serve
        /// </summary>
        public void WaitTillReady()
        {
            while (!TwoPhaseCommit.IsInitialized)
            {
                Thread.Sleep(100);
            }
        }

        // Scan transactions and abort them if times out
        public void TransactionScavenger()
        {
            while (ShutdownEvent.WaitOne(300000, false) == false)
            {
                try
                {
                    Console.WriteLine("TM: Scavenging stale transactions started...");

                    lock (_resourceManagersEnlistedInTransactions)
                    {
                        var transactionsAborted = new List<Transaction>();

                        Console.WriteLine("TM: {0} transactions ", _resourceManagersEnlistedInTransactions.Keys.Count);

                        foreach(var item in _resourceManagersEnlistedInTransactions)
                        {
                            var transaction = item.Key;
                            var rmList = item.Value.ResourceManagers;
                            var t = DateTime.Now.Subtract(item.Value.TransactionStartTime);

                            if (t.Seconds > TransactionTimeout)
                            {
                                foreach (RM r in rmList)
                                {
                                    r.Abort(transaction);
                                }
                                Console.WriteLine(string.Format("TM: stale transaction {0} aborted", transaction));
                                transactionsAborted.Add(transaction);
                            }
                        }

                        foreach (var transaction in transactionsAborted)
                        {
                            _resourceManagersEnlistedInTransactions.Remove(transaction);
                        }

                        Console.WriteLine("TM: Scavenging stale transactions ended.");
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }
        }

        public RM GetResourceMananger(string name)
        {
            return StaticGetResourceMananger(name);
        }

        public static RM StaticGetResourceMananger(string name)
        {
            lock (_resourceManagers)
            {
                foreach (RM rm in _resourceManagers.Values)
                {
                    if (String.Compare(rm.GetName(), name, StringComparison.OrdinalIgnoreCase) == 0)
                        return rm;
                }
            }
            return null;
        }

        public TP.Transaction Start()
        {
            WaitTillReady();
            var context = new Transaction();
            Console.WriteLine(string.Format("TM: Transaction {0} started", context.Id));
            return context;
        }

        public void Ping()
        {
        }

        /// <summary>
        ///	 Call from WC in response to a client's commit
        /// </summary>
        /// <param name="context"></param>
        public void Commit(TP.Transaction context)
        {
            WaitTillReady();
            lock (_resourceManagersEnlistedInTransactions)
            {
                if (_resourceManagersEnlistedInTransactions.ContainsKey(context))
                {
                    CommitedTransaction trans = TwoPhaseCommit.Commit(context, _resourceManagersEnlistedInTransactions[context]);
                    trans.DoneEvent.WaitOne(TransactionTimeout * 1000);
                    _resourceManagersEnlistedInTransactions.Remove(context);
                }
            }
            Console.WriteLine(string.Format("TM: Transaction {0} commited", context.Id));
        }

        /// <summary>
        // Call from WC in response to a client's abort
        /// </summary>
        /// <param name="context"></param>
        public void Abort(TP.Transaction context)
        {
            WaitTillReady();
            lock (_resourceManagersEnlistedInTransactions)
            {
                if (_resourceManagersEnlistedInTransactions.ContainsKey(context))
                {
                    var list = _resourceManagersEnlistedInTransactions[context].ResourceManagers;

                    foreach (RM r in list)
                    {
                        r.Abort(context);
                    }

                    _resourceManagersEnlistedInTransactions.Remove(context);
                }
            }

            Console.WriteLine(string.Format("TM: Transaction {0} aborted", context.Id));
        }

        /*  Called by RM.
            This method notifies TM that it is involved in a given transaction
            TM keeps track of which RM is enlisted with which transaction to do distributed transactions */
        /// <summary>
        /// </summary>
        /// <param name="context"></param>
        /// <param name="enlistingRM"> </param>
        public bool Enlist(TP.Transaction context, string enlistingRM)
        {
            var rm = GetResourceMananger(enlistingRM);
            if (rm == null)
            {
                throw new ApplicationException(enlistingRM + " not registered.");
            }

            lock (_resourceManagersEnlistedInTransactions)
            {
                if (_resourceManagersEnlistedInTransactions.ContainsKey(context))
                {
                    var list = _resourceManagersEnlistedInTransactions[context];

                    if (!list.Contains(rm))
                    {
                        _resourceManagersEnlistedInTransactions[context].Add(rm);
                    }
                }
                else
                {
                    _resourceManagersEnlistedInTransactions.Add(context, new ResourceManagerList(rm));
                }

            }

            Console.WriteLine(string.Format("TM: Transaction {0} enlisted for {1}", context.Id, enlistingRM));
            return true;
        }

        public void Register(string msg)
        {
            string[] URL = msg.Split('$');
            Console.WriteLine("Register " + URL[0]);
            TP.RM newRM = (TP.RM)Activator.GetObject(typeof(TP.RM), URL[0]);
            try
            {
                newRM.SetName(URL[1]);
            }
            catch (RemotingException e)
            {
                Console.WriteLine(e.ToString());
            }
            lock (_resourceManagers)
            {
                _resourceManagers[newRM.GetName()] = newRM;
            }
        }

        //TODO: REFACTOR THIS FOR TESTING
        public void Register(TP.RM rm)
        {
            _resourceManagers[rm.GetName()] = rm;
            
        }

        public static void StaticRegister(TP.RM rm)
        {
            _resourceManagers[rm.GetName()] = rm;
            Console.WriteLine("Re-Register " + rm.GetName());
        }
        
        public void shutdown()
        {
            // TODO DO PROPER SHUTDOWN HERE
            ShutdownEvent.Set();

            if (null != _transactionScavengerThread)
                _transactionScavengerThread.Join();
        }

        class TMParser : CommandLineParser
        {
            public TMParser()
            {
                Add("p", "Port", "The port this transaction manager listens on", "8089");
            }
        }

        static void Main(string[] args)
        {
            TMParser parser = new TMParser();
            if (!parser.Parse(args))
            {
                return;
            }

            var serverProv = new SoapServerFormatterSinkProvider { TypeFilterLevel = TypeFilterLevel.Full };

            var clientProv = new SoapClientFormatterSinkProvider();

            IDictionary props = new Hashtable();
            props["port"] = Int32.Parse(parser["p"]);

            var channel = new HttpChannel(props, clientProv, serverProv);
            ChannelServices.RegisterChannel(channel, false);

            RemotingConfiguration.RegisterWellKnownServiceType
                (Type.GetType("MyTM.MyTM")  // full type name
                        , "TM.soap"         // URI
                        , System.Runtime.Remoting.WellKnownObjectMode.Singleton	// instancing mode
                );

            while (true)
            {
                Thread.Sleep(2000);
                TwoPhaseCommit.PrintMessage();
            }
        }

        #region TM Members

        /// <summary>
        ///   Exit (simulate a failure) on certain condition
        ///   Now supports 
        ///   1. exit before all RM is prepared (send Prepare to 1 RM then killed)
        ///   2. exit before all RM are done (send commited to 1 RM then killed)
        /// </summary>
        /// <param name="prepareFailed"></param>
        /// <param name="commitFailed"></param>
        public void SelfDestruct(bool prepareFailed, bool commitFailed)
        {
            TwoPhaseCommit.CommitFail = commitFailed;
            TwoPhaseCommit.PrepareFail = prepareFailed;
        }

        #endregion
    }
}
