using System;
using System.Collections.Concurrent;
using TP;
using System.Runtime.Remoting.Channels;
using System.Runtime.Serialization.Formatters;
using System.Collections;
using System.Runtime.Remoting.Channels.Http;
using System.Runtime.Remoting;
using System.Collections.Generic;

namespace MyTM
{
    /// <summary>
    /*  Transaction Manager */
    /// </summary>
    public class MyTM : System.MarshalByRefObject, TP.TM
    {
        private readonly HashSet<RM> _resourceManagers;
        private readonly Dictionary<Transaction, List<RM>> _resourceManagersInTransaction = new Dictionary<Transaction, List<RM>>();  

        public MyTM()
        {
            System.Console.WriteLine("Transaction Manager instantiated");
            _resourceManagers = new HashSet<RM>();
        }

        public RM GetResourceMananger(string name)
        {
            lock (_resourceManagers)
            {
                foreach (RM rm in _resourceManagers)
                {
                    if (String.Compare(rm.GetName(), name, StringComparison.OrdinalIgnoreCase) == 0)
                        return rm;
                }
            }
            return null;
        }

        public TP.Transaction Start()
        {
            var context = new Transaction();
            System.Console.WriteLine(string.Format("TM: Transaction {0} started", context.Id));
            return context;
        }

        /// <summary>
        //	 Call from WC in response to a client's commit
        /// </summary>
        /// <param name="context"></param>
        public void Commit(TP.Transaction context)
        {
            lock (_resourceManagersInTransaction)
            {                            
                if (!_resourceManagersInTransaction.ContainsKey(context))
                    throw new ApplicationException("Transaction not found " + context);

                var list = _resourceManagersInTransaction[context];

                foreach(RM r in list)
                {
                    r.Commit(context);
                }

                _resourceManagersInTransaction.Remove(context);
            }

            //foreach (RM rm in _resourceManagers)
           // {
             //   rm.Commit(context);
            //}
            System.Console.WriteLine(string.Format("Transaction {0} commited", context.Id));
        }

        /// <summary>
        // Call from WC in response to a client's abort
        /// </summary>
        /// <param name="context"></param>
        public void Abort(TP.Transaction context)
        {
            lock (_resourceManagersInTransaction)
            {
                if (!_resourceManagersInTransaction.ContainsKey(context))
                    throw new ApplicationException("Transaction not found " + context);

                var list = _resourceManagersInTransaction[context];

                foreach (RM r in list)
                {
                    r.Abort(context);
                }

                _resourceManagersInTransaction.Remove(context);
            }

            //foreach (RM rm in _resourceManagers)
            //{
            //    rm.Abort(context);
            //}
            System.Console.WriteLine(string.Format("Transaction {0} aborted", context.Id));
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

            lock (_resourceManagersInTransaction)
            {
                if (_resourceManagersInTransaction.ContainsKey(context))
                {
                    var list = _resourceManagersInTransaction[context];
                    
                    if (!list.Contains(rm))
                    {
                        list.Add(rm);
                    }
                }
                else
                {
                    _resourceManagersInTransaction.Add(context, new List<RM> {rm});
                }
            
            }

            System.Console.WriteLine(string.Format("Transaction {0} enlisted for {1}", context.Id, enlistingRM));
            return true;
        }

        public void Register(string msg)
        {
            string[] URL = msg.Split('$');
            Console.WriteLine("Register " + URL[0]);
            TP.RM newRM = (TP.RM)System.Activator.GetObject(typeof(TP.RM), URL[0]);
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
                _resourceManagers.Add(newRM);
            }
        }

        //TODO: REFACTOR THIS FOR TESTING
        public void Register(TP.RM rm)
        {
            _resourceManagers.Add(rm);
        }

        public void shutdown()
        {
            // TODO DO PROPER SHUTDOWN HERE
        }


        protected void init(String[] args)
        {
        }


        protected void initStorage()
        {
            // TODO create commit log
        }


        protected void recovery()
        {
            // TODO Abort/commit/garbage collect
        }


        protected void startUp()
        {
            // TODO start garbage collector?
        }


        protected void readyToServe()
        {
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

            SoapServerFormatterSinkProvider serverProv = new SoapServerFormatterSinkProvider();
            serverProv.TypeFilterLevel = TypeFilterLevel.Full;

            SoapClientFormatterSinkProvider clientProv = new SoapClientFormatterSinkProvider();

            IDictionary props = new Hashtable();
            props["port"] = Int32.Parse(parser["p"]);

            HttpChannel channel = new HttpChannel(props, clientProv, serverProv);
            ChannelServices.RegisterChannel(channel, false);

            RemotingConfiguration.RegisterWellKnownServiceType
                (Type.GetType("MyTM.MyTM")								// full type name
                        , "TM.soap"												// URI
                        , System.Runtime.Remoting.WellKnownObjectMode.Singleton	// instancing mode
                );

            while (true)
            {
                System.Threading.Thread.Sleep(100000);
            }
        }



        public void OnRmPrepared(Transaction context, string rm)
        {
            throw new NotImplementedException();
        }

        public void OnRmDone(Transaction context, string rm)
        {
            throw new NotImplementedException();
        }
    }
}
