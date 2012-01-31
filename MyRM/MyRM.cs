using System;
using System.Collections;
using TP;
using System.Collections.Generic;
using System.Text;
using System.Runtime.Remoting.Channels.Http;
using System.Runtime.Remoting.Channels;
namespace MyRM
{
    /// <summary>
    /// class MyRM implements TP.RM
    /// </summary>
    public class MyRM : System.MarshalByRefObject, TP.RM
    {
        MyLM lockManager;
        private string name;

        static TP.TM transactionManager = null;

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
            static string name = null;

            public static string Name
            {
                get
                {
                    if (name == null)
                    {
                        name = DefaultName;
                    }

                    return name;
                }
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
            this.lockManager = new MyLM();
            name = "MyRM";
        }

        public void SetName(string _name)
        {
            name = _name;
        }

        public string GetName()
        {
            return name;
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
                while (transactionManager == null)
                {
                    try
                    {
                        transactionManager = (TP.TM)System.Activator.GetObject(typeof(TP.TM), parser["tm"]);

                        Transaction tid = transactionManager.Start();
                        string[] urls = channel.GetUrlsForUri("RM.soap");
                        foreach (string url in urls)
                        {
                            transactionManager.Register(url + "$" + GlobalState.Name);

                        }

                        transactionManager.Abort(tid);

                    }
                    catch (ArgumentException)
                    {
                        transactionManager = null;
                        Console.WriteLine("Waiting 1 second for Transaction Manager \"{0}\"", parser["tm"]);
                        System.Threading.Thread.Sleep(1000);
                    }
                }


            }

            Console.WriteLine("{0} RM: Transaction Manager retrieved at {1}", GlobalState.Name, parser["tm"]);

            while (GlobalState.Mode == GlobalState.RunMode.Loop)
                System.Threading.Thread.Sleep(2000);

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
        /// Currently does not involve TM, will do it in later step
        /// Call to TM to enlist for distributed transaction 
        /// </summary>
        /// <param name="context"></param>
        public void Enlist(TP.Transaction context)
        {
            // transactionManager.Enlist(context);
        }

        /// <summary>
        /// Currently does not involve TM, will do it in later step
        /// </summary>
        /// <param name="context"></param>
        public void Commit(TP.Transaction context)
        {
            // transactionManager.Commit(context);
            TransactionStorage.Commit(context);
            lockManager.UnlockAll(context);
        }

        /// <summary>
        /// Currently does not involve TM, will do it in later step
        /// </summary>
        /// <param name="context"></param>
        public void Abort(TP.Transaction context)
        {
            // transactionManager.Abort(context);
            TransactionStorage.Abort(context);
            lockManager.UnlockAll(context);
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
            lockManager.LockForWrite(context, i);
            Resource res = TransactionStorage.Read(context, i);
            if (res == null)
            {
                res = new Resource(i, count, price);
            }
            else
            {
                res.incrCount(count);
                res.setPrice(price);
            }

            TransactionStorage.Write(context, i, res);
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
            lockManager.LockForWrite(context, rid);
            bool removed = TransactionStorage.Delete(context, rid);

            // drop all reservations on removed resource

            if (removed)
            {
                foreach (Customer c in TransactionStorage.GetCustomers(context))
                {
                    HashSet<RID> e = TransactionStorage.Read(context, c);
                    e.Remove(rid);
                    TransactionStorage.Write(context, c, e);
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
            lockManager.LockForWrite(context, rid);
            Resource resource = TransactionStorage.Read(context, rid); ;

            if (resource == null)
            {
                // silently discard
                return false;
            }
            else
            {
                if (resource.getCount() > count)
                {
                    resource.decrCount(count);
                }
                else
                {
                    resource.setCount(0);
                }

                TransactionStorage.Write(context, rid, resource);
            }
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
        /*    Exit after the specified number of disk writes.
              Support for this method requires a wrapper around _write_ system
              call that decrements the counter set by this method.

              This counter should be set by default to 0, which implies that the wrapper
              will do nothing.  If it is non-zero, the wrapper should decrement
              the counter, see if it becomes zero, and if so, call exit(), otherwise
              continue to write. */
        /// </summary>
        public void SelfDestruct(int diskWritesToWait)
        {
        }

        /// <summary>
        /// Queries the # of given resource
        /// </summary>
        /// <param name="context"></param>
        /// <param name="rid"></param>
        /// <returns>returns the amount available for the specified item type */</returns>
        public int Query(TP.Transaction context, RID rid)
        {
            lockManager.LockForRead(context, rid);
            Console.WriteLine("RM: Query");
            Resource resource = TransactionStorage.Read(context, rid);

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
            lockManager.LockForRead(context, rid);
            Resource resource = TransactionStorage.Read(context, rid);

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
            lockManager.LockForRead(context, customer);
            StringBuilder buf = new StringBuilder(512);

            HashSet<RID> reserved = TransactionStorage.Read(context, customer);
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
            int bill = 0;

            lockManager.LockForRead(context, customer);
            HashSet<RID> reserved = TransactionStorage.Read(context, customer);
            if (reserved != null)
            {
                foreach (RID rid in reserved)
                {
                    Resource r = TransactionStorage.Read(context, rid);
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
            lockManager.LockForWrite(context, c);
            lockManager.LockForWrite(context, i);
            Resource resource = TransactionStorage.Read(context, i);

            if (resource == null)
            {
                throw new InvalidOperationException(i + " does not exist!");
            }
            if (resource.getCount() == 0)
            {
                return false;
            }

            HashSet<RID> r = TransactionStorage.Read(context, c);
            if (r == null)
            {
                r = new HashSet<RID>();
            }

            r.Add(resource.getID());
            TransactionStorage.Write(context, c, r);
            resource.decrCount();
            TransactionStorage.Write(context, i, resource);
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
            lockManager.LockForWrite(context, c);
            HashSet<RID> r = TransactionStorage.Read(context, c);
            if (r == null)
            {
                // silently discard
            }
            else
            {
                foreach (RID rid in r)
                {
                    lockManager.LockForWrite(context, rid);
                    Resource resource = TransactionStorage.Read(context, rid);
                    if (resource == null)
                    {
                        // FIXME warn that the rID does not exist!
                    }
                    else
                    {
                        resource.incrCount();
                        TransactionStorage.Write(context, rid, resource);
                    }
                }

                TransactionStorage.Delete(context, c);
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
            List<string> result = new List<string>();
            foreach (Resource resource in TransactionStorage.GetResources(context))
            {
                if (type == resource.getType())
                {
                    result.Add(resource.toString());
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
            List<Customer> customers = new List<Customer>(TransactionStorage.GetCustomers(context));
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
            // TODO create database files, transaction logs
        }


        protected void Recovery()
        {
            // TODO recover state from database file
        }


        protected void StartUp()
        {
            // TODO deadlock detector, retry timeout
        }




    }
}
