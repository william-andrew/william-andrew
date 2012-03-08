using System;
using System.Collections.Generic;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Http;
using System.Text;
using MyRM.Storage;
using TP;
using System.Diagnostics;
using System.Net;
using System.Threading;

namespace MyRM
{
    /// <summary>
    /// class MyRM implements TP.RM
    /// </summary>
    public class MyRM : MarshalByRefObject, RM
    {
        private readonly MyLM _lockManager;
        private TransactionStorage _transactionStorage;
        private string _name;
        static TM _transactionManager;
        static string tmUrl = string.Empty;
        static string[] urls;
        private static bool isReady = true;
        public int VoteNoOnPrepare
        {
            get;
            set;
        }
        public int VoteNoOnCommit
        {
            get;
            set;
        }
        public int VoteNoOnAbort
        {
            get;
            set;
        }

        public int NumberCommits
        {
            get;
            private set;
        }

        public int NumberAborts
        {
            get;
            private set;
        }

        public int NumberPrepares
        {
            get;
            private set;
        }

        internal class GlobalState
        {
            public enum RunMode
            {
                Loop,
                Wait,
                Kill
            }

            public static RunMode Mode = RunMode.Loop;

            public const string DefaultName = "MyRM";
            const int MaxNameLength = 21;
            static string name;

            public static string Name
            {
                get { return name ?? (name = DefaultName); }
                set
                {
                    if (name == null)
                    {
                        string temp = value.Trim();
                        if (temp.IndexOfAny(System.IO.Path.GetInvalidPathChars()) < 0 && temp.Length <= MaxNameLength)
                        {
                            name = temp;
                        }
                        else
                        {
                            throw new ArgumentException(String.Format("\"{0}\" is not a valid MyRM Name", temp), "Name");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(String.Format("\"{0}\" is not valid at this time, MyRM Name is already set to \"{1}\"", value, name), "Name");
                    }
                }
            }

        }


        public MyRM()
        {
            this._lockManager = new MyLM();
            _name = "MyRM";
        }

        public void SetName(string name)
        {
            this._name = name;
            InitStorage();
        }

        public string GetName()
        {
            return _name;
        }

        private void WaitForReady()
        {
            while (!isReady || _transactionManager == null)
            {
                Thread.Sleep(100);
            }
        }

        // Property injection for testing only
        public TM TransactionManager
        {
            set { _transactionManager = value; }
        }

        class RMParser : CommandLineParser
        {
            public RMParser()
            {
                Add("p", "Port", "The port this Resource Manager listens on", "8081");
                Add("n", "Name", "The name of this Resource Manager", GlobalState.DefaultName);
                Add("tm", "TM", "The URL of the Transaction Manager.  Specify \"NONE\" to run this RM in stand alone mode", "http://localhost:8089/TM.soap");
            }
        }

        static void Main(string[] args)
        {
            isReady = false; // set isReady false because we want to make sure the actual code is not executed before the rm is going into the stable loop waiting.
            RMParser parser = new RMParser();
            if (!parser.Parse(args))
            {
                return;
            }

            GlobalState.Name = parser["n"].ToLower();
            string port_num = parser["p"];

            System.Collections.Specialized.ListDictionary channelProperties = new System.Collections.Specialized.ListDictionary();

            channelProperties.Add("port", port_num);
            channelProperties.Add("name", GlobalState.Name);

            HttpChannel channel = new HttpChannel(channelProperties, new SoapClientFormatterSinkProvider(), new SoapServerFormatterSinkProvider());

            System.Console.WriteLine(string.Format("Starting resource manager for {0} on port {1}", GlobalState.Name, port_num));
            System.Runtime.Remoting.Channels.ChannelServices.RegisterChannel(channel, false);

            System.Runtime.Remoting.RemotingConfiguration.RegisterWellKnownServiceType
            (Type.GetType("MyRM.MyRM")									// Assembly name
                  , "RM.soap"												// URI
                  , System.Runtime.Remoting.WellKnownObjectMode.Singleton	// Instancing mode
            );


            if (String.Compare(parser["tm"], "none", true) != 0)
            {
                while (_transactionManager == null)
                {
                    tmUrl = parser["tm"];
                    try
                    {
                        _transactionManager = (TP.TM)System.Activator.GetObject(typeof(TP.TM), tmUrl);

                        _transactionManager.Ping();
                        urls = channel.GetUrlsForUri("RM.soap");
                        foreach (string url in urls)
                        {
                            _transactionManager.Register(url + "$" + GlobalState.Name);

                        }
                    }
                    catch (ArgumentException)
                    {
                        _transactionManager = null;
                        Console.WriteLine("Waiting 1 second for Transaction Manager \"{0}\"", parser["tm"]);
                        System.Threading.Thread.Sleep(1000);
                    }
                }
            }

            Console.WriteLine("{0} RM: Transaction Manager retrieved at {1}", GlobalState.Name, parser["tm"]);

            while (GlobalState.Mode == GlobalState.RunMode.Loop)
            {
                try
                {
                    _transactionManager.Ping();
                }
                catch(WebException)
                {
                    _transactionManager = null;
                    ReconnectToTM();
                }
                isReady = true;
                System.Threading.Thread.Sleep(2000);
                    
            }

            int loopCount = 0;

            while (GlobalState.Mode == GlobalState.RunMode.Wait && loopCount < 15)
            {
                System.Threading.Thread.Sleep(1000);
                loopCount++;
                Console.WriteLine("{0}: Waiting for transaction complete ({1} second(s))", GlobalState.Name, loopCount);
            }

            Console.WriteLine("{0}: Exitting", GlobalState.Name);
        }

        /// <summary>
        /// try to reconnect to TM after tm failed. It need to register itself to TM as well
        /// </summary>
        public static void ReconnectToTM()
        {
            TM tempTM = null;
            while (tempTM == null)
            {
                Console.WriteLine("Trying to reconnect to TM");
                tempTM = (TP.TM)System.Activator.GetObject(typeof(TP.TM), tmUrl);

                try
                {
                    tempTM.Ping();
                    foreach (string url in urls)
                    {
                        tempTM.Register(url + "$" + GlobalState.Name);

                    }
                }
                catch (WebException)
                {
                    tempTM = null;
                    Console.WriteLine("Waiting 0.5 second for Transaction Manager \"{0}\"", tmUrl);
                    System.Threading.Thread.Sleep(500);
                }

            }
            Interlocked.Exchange<TM>(ref _transactionManager, tempTM); 
            Console.WriteLine("Connected to Transaction Manager \"{0}\"", tmUrl);
        }

        /// <summary>
        /// Currently does not involve TM, will do it in later step
        /// Call to TM to enlist for distributed transaction 
        /// </summary>
        /// <param name="context"></param>
        public void Enlist(TP.Transaction context)
        {
            WaitForReady();
            try
            {
                _transactionManager.Enlist(context, this.GetName());
                return;
            }
            catch (WebException)
            {
            }
            catch(ApplicationException)
            {
            }
            _transactionManager = null;
            ReconnectToTM();
            WaitForReady();
        }

        /// <summary>
        /// Currently does not involve TM, will do it in later step
        /// </summary>
        /// <param name="context"></param>
        public XaResponse Commit(TP.Transaction context)
        {
            WaitForReady();
            ++NumberCommits;
            if (NumberCommits >= this.VoteNoOnCommit && this.VoteNoOnCommit != 0)
            {
                return XaResponse.XAER_RMERR;
            }

            _transactionStorage.Commit(context);
            _lockManager.UnlockAll(context);
            return XaResponse.XA_OK;
        }

        /// <summary>
        /// Currently does not involve TM, will do it in later step
        /// </summary>
        /// <param name="context"></param>
        public XaResponse Abort(TP.Transaction context)
        {
            WaitForReady();

            ++NumberAborts;
            if (NumberAborts >= this.VoteNoOnAbort && this.VoteNoOnAbort != 0)
            {
                return XaResponse.XAER_RMERR;
            }
            _transactionStorage.Abort(context);
            _lockManager.UnlockAll(context);
            return XaResponse.XA_OK;
        }


        //we can take a look the standard XA/Open interface.
        public XaResponse Prepare(Transaction context)
        {
            WaitForReady();
            ++NumberPrepares;
            if (NumberPrepares >= this.VoteNoOnPrepare && this.VoteNoOnPrepare != 0)
            {
                return XaResponse.XAER_RMERR;
            }
            
            _transactionStorage.Prepare(context); 
            return XaResponse.XA_OK;
        }

        /// <summary>
        /// implemented using the new TransactionStorage class
        /// This method adds a resource to the available ones
        /// </summary>
        /// <param name="context"></param>
        /// <param name="i"></param>
        /// <param name="count"></param>
        /// <param name="price"></param>
        /// <returns></returns>
        public bool Add(TP.Transaction context, TP.RID i, int count, int price)
        {
            WaitForReady();
            Enlist(context);

            _lockManager.LockForWrite(context, i);
            Resource res = _transactionStorage.Read(context, i);
            if (res == null)
            {
                res = new Resource(i, count, price);
            }
            else
            {
                res.incrCount(count);
                res.setPrice(price);
            }

            _transactionStorage.Write(context, i, res);
            return true;
        }


        /// <summary>
        /// implemented using the new TransactionStorage class
        /// </summary>
        /// <param name="context"></param>
        /// <param name="rid"></param>
        /// <returns></returns>
        public bool Delete(TP.Transaction context, RID rid)
        {
            WaitForReady();
            Enlist(context);

            _lockManager.LockForWrite(context, rid);
            bool removed = _transactionStorage.Delete(context, rid);

            // drop all reservations on removed resource

            if (removed)
            {
                foreach (Customer c in _transactionStorage.GetCustomers(context))
                {
                    _lockManager.LockForWrite(context, c);
                    HashSet<RID> e = _transactionStorage.Read(context, c);
                    e.Remove(rid);
                    _transactionStorage.Write(context, c, e);
                }
            }

            return removed;
        }

        /// <summary>
        /// Deletes certain amount of resource.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="rid"></param>
        /// <param name="count"></param>
        /// <returns>true the given resources exists. False if not</returns>
        public bool Delete(Transaction context, RID rid, int count)
        {
            WaitForReady();
            Enlist(context);

            _lockManager.LockForWrite(context, rid);
            Resource resource = _transactionStorage.Read(context, rid);

            if (resource == null)
            {
                // silently discard
                return false;
            }
            if (resource.getCount() > count)
            {
                resource.decrCount(count);
            }
            else
            {
                resource.setCount(0);
            }

            _transactionStorage.Write(context, rid, resource);
            return true;
        }


        /// <summary>
        /*  NEED TO ADD CODE For STEP 2
              Calling shutdown causes RM to exit gracefully.
              This means, it waits for all the existing transactions 
              to end and enlist requests for new transactions are refused. 
              If any of the existing transactions blocks forever, 
              a retry/timeout mechanism is used to exit.
              No recovery is done on startup */
        /// </summary>
        public void Shutdown()
        {
            // TODO: Add lock code here if needed
        }

        /// <summary>
        ///   Exit (simulate a failure) after a specified number of disk writes.   
        ///   For example, if you set exitOnWrite to 1, the process will terminate
        ///   before the first write happens.
        ///   This is extended version supports selfdestructing at certain number of 
        ///   writes, prepares, aborts and commits.
        ///   The counters resets after this call;
        /// </summary>
        public void SelfDestruct(int exitOnWrite, int voteNoOnPrepare, int voteNoOnCommit, int voteNoOnAbort)
        {
            this._transactionStorage.EexitOnWrite = exitOnWrite;
            this.VoteNoOnPrepare = voteNoOnPrepare;
            this.VoteNoOnAbort = voteNoOnAbort;
            this.VoteNoOnCommit = voteNoOnCommit;
            this._transactionStorage.ResetCounters();
            this.NumberAborts = 0;
            this.NumberCommits = 0;
            this.NumberPrepares = 0;

        }

        /// <summary>
        /// Queries the # of given resource
        /// </summary>
        /// <param name="context"></param>
        /// <param name="rid"></param>
        /// <returns>returns the amount available for the specified item type */</returns>
        public int Query(TP.Transaction context, RID rid)
        {
            WaitForReady();
            Enlist(context);

            _lockManager.LockForRead(context, rid);
            Console.WriteLine("RM: Query");
            Resource resource = _transactionStorage.Read(context, rid);

            if (resource == null)
            {
                throw new ArgumentException(rid + " does not exist");
            }
            
            return resource.getCount();
        }

        /// <summary>
        /// Queries the price of given resource
        /// </summary>
        /// <param name="context"></param>
        /// <param name="rid"></param>
        /// <returns>returns the price for the specified item type</returns>
        public int QueryPrice(Transaction context, RID rid)
        {
            WaitForReady();
            Enlist(context);

            _lockManager.LockForRead(context, rid);
            Resource resource = _transactionStorage.Read(context, rid);

            if (resource == null)
            {
                throw new ArgumentException(rid + " does not exist");
            }

            return resource.getPrice();
        }

        /// <summary>
        /// Gets the list of reserved resources for the customer
        /// </summary>
        /// <param name="context"></param>
        /// <param name="customer"></param>
        /// <returns>the string of the list of reserved resources for the customer</returns>
        public String QueryReserved(Transaction context, Customer customer)
        {
            WaitForReady();
            Enlist(context);

            _lockManager.LockForRead(context, customer);
            StringBuilder buf = new StringBuilder(512);

            HashSet<RID> reserved = _transactionStorage.Read(context, customer);
            if (reserved != null)
            {
                foreach (RID rid in reserved)
                {
                    if (buf.Length > 0)
                    {
                        buf.Append(',');
                    }
                    buf.Append(rid);
                }
            }
            return buf.ToString();
        }

        /// <summary>
        /// Gets the total price of reserved resources for the customer
        /// </summary>
        /// <param name="context"></param>
        /// <param name="customer"></param>
        /// <returns>the total price of reserved resources for the customer</returns>
        public int QueryReservedPrice(Transaction context, Customer customer)
        {
            WaitForReady();
            Enlist(context);

            int bill = 0;

            _lockManager.LockForRead(context, customer);
            HashSet<RID> reserved = _transactionStorage.Read(context, customer);
            if (reserved != null)
            {
                foreach (RID rid in reserved)
                {
                    Resource r = _transactionStorage.Read(context, rid);
                    if (r == null)
                    {
                        throw new InvalidOperationException(rid + " does not exist in RM");
                    }
                    bill += r.getPrice();
                }
            }

            return bill;
        }

        /// <summary>
        /// Reserve a list of resources for a customer
        /// Decrease all reserved resources by 1.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="c"></param>
        /// <param name="i"></param>
        /// <returns>true if reservation is successful</returns>
        public bool Reserve(Transaction context, Customer c, RID i)
        {
            WaitForReady();
            Enlist(context);

            _lockManager.LockForWrite(context, c);
            _lockManager.LockForWrite(context, i);
            Resource resource = _transactionStorage.Read(context, i);

            if (resource == null)
            {
                throw new InvalidOperationException(i + " does not exist!");
            }
            if (resource.getCount() == 0)
            {
                return false;
            }

            HashSet<RID> r = _transactionStorage.Read(context, c) ?? new HashSet<RID>();

            r.Add(resource.getID());

            Console.WriteLine("Reserving flight: Stops={0}",  r.Count);
            _transactionStorage.Write(context, c, r);

            resource.decrCount();
            _transactionStorage.Write(context, i, resource);
            return true;
        }

        /// <summary>
        /// Remove reservations of a customer
        /// Increase all resources in this reservation by 1.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="c"></param>
        public void UnReserve(Transaction context, Customer c)
        {
            WaitForReady();
            Enlist(context);

            _lockManager.LockForWrite(context, c);
            HashSet<RID> r = _transactionStorage.Read(context, c);
            Console.WriteLine("Unreserve: {0}", r == null ? 0 : r.Count);
            if (r == null)
            {
                // silently discard
            }
            else
            {
                foreach (RID rid in r)
                {
                    _lockManager.LockForWrite(context, rid);
                    Resource resource = _transactionStorage.Read(context, rid);
                    if (resource == null)
                    {
                        // FIXME warn that the rID does not exist!
                    }
                    else
                    {
                        resource.incrCount();
                        _transactionStorage.Write(context, rid, resource);
                    }
                }

                _transactionStorage.Delete(context, c);
            }
        }

        /// <summary>
        /// List the resouces of a type
        /// </summary>
        /// <param name="context"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public String[] ListResources(Transaction context, RID.Type type)
        {
            WaitForReady();
            Enlist(context);

            List<string> result = new List<string>();
            foreach (Resource resource in _transactionStorage.GetResources(context))
            {
                if (type == resource.getType())
                {
                    result.Add(resource.ToString());
                }
            }
            return result.ToArray();
        }

        /// <summary>
        /// Get an array of all customers
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public Customer[] ListCustomers(Transaction context)
        {
            WaitForReady();
            Enlist(context);

            var customers = new List<Customer>(_transactionStorage.GetCustomers(context));
            return customers.ToArray();
        }

        /**
         * @todo setup {@link #selfDestruct(int)} here
         */

        protected void Init(String[] args)
        {
            // TODO set self destruct counter

        }

        protected void InitStorage()
        {
            var database = new SimpleDatabase("MYRM_" + _name, true);
            //TODO: BUGBUG: add validation to check the data and key length
            database.CreateTable(Constants.ReservationTableName, 96, 36);
            database.CreateTable(Constants.ResourcesTableName, 96, 36);
            _transactionStorage = new TransactionStorage(database);            
        }

        protected void Recovery()
        {
            // TODO recover state from simpleDatabase file
        }

        protected void StartUp()
        {
            // TODO deadlock detector, retry timeout
        }

        public void Ping()
        {
        }
    }
}
