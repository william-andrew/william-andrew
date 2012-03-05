using TP;
using System;
using System.Text;
using System.Collections.Generic;
using System.Runtime.Remoting.Channels.Http;
using System.Runtime.Remoting;
using System.Net;

namespace MyWC
{
    /// <summary>
    /// Workflow Controller
    /// </summary>
    public class MyWC : MarshalByRefObject, WC
    {
        /// <summary>
        /// Resource Manager for each resource type
        /// </summary>
        public static TP.RM Flights { get; set; }
        public static TP.RM Rooms { get; set; }
        public static TP.RM Cars { get; set; }
        public static TP.TM TransactionManager { get; set; }
        static string tmURL;

        /// <param name="c">Customer</param>
        /// <param name="flights">array of flight names</param>
        /// <param name="location">room location if room is true</param>
        /// <param name="car">true if request is for a car</param>
        /// <param name="room">true if request is for a room</param>
        /// <returns>price of reservation</returns>
        public bool ReserveItinerary(TP.Customer c, string[] flights, string location, bool car, bool room)
        {
            
            TP.Transaction tid = TransactionManager.Start();

            try
            {
                if (car)
                {
                    Cars.Reserve(tid, c, RID.forCar(location));
                }

                if (room)
                {
                    Rooms.Reserve(tid, c, RID.forRoom(location));
                }

                foreach (string flight in flights)
                {
                    Flights.Reserve(tid, c, RID.forFlight(flight));
                }

                TransactionManager.Commit(tid);
            }
            catch (Exception e)
            {
                TransactionManager.Abort(tid);
                throw;
            }
            return true;
        }

        public bool CancelItinerary(Customer customer)
        {
            TP.Transaction tid = TransactionManager.Start();
            try
            {
                if (Flights != null)
                {
                    Flights.UnReserve(tid, customer);
                }

                if (Cars != null)
                {
                    Cars.UnReserve(tid, customer);
                }

                if (Rooms != null)
                {
                    Rooms.UnReserve(tid, customer);
                }

                TransactionManager.Commit(tid);
            }
            catch (Exception e)
            {
                TransactionManager.Abort(tid);
                throw new Exception("caught an exception", e);
            }
            return true;
        }

        public int QueryItineraryPrice(Transaction context, Customer customer)
        {
            int bill = Flights.QueryReservedPrice(context, customer);
            bill += Cars.QueryReservedPrice(context, customer);
            bill += Rooms.QueryReservedPrice(context, customer);
            return bill;
        }

        public String QueryItinerary(Transaction context, Customer customer)
        {
            var buf = new StringBuilder(1024);
            buf.Append(Flights.QueryReserved(context, customer));
            if (buf.Length > 0) buf.Append(',');
            buf.Append(Cars.QueryReserved(context, customer));
            if (buf.Length > 0) buf.Append(',');
            buf.Append(Rooms.QueryReserved(context, customer));

            return buf.ToString();
        }



        /*************** Client interface methods **************************/
        public bool AddSeats(Transaction context, String flight, int flightSeats,
                int flightPrice)
        {
            return Flights.Add(context, RID.forFlight(flight), flightSeats, flightPrice);
        }

        public bool DeleteSeats(Transaction context, String flight, int numSeats)
        {
            return Flights.Delete(context, RID.forFlight(flight), numSeats);
        }

        public bool DeleteFlight(Transaction context, string flight)
        {
            return Flights.Delete(context, RID.forFlight(flight));

        }

        public bool AddRooms(Transaction context, String location, int numRooms,
                int price)
        {
            return Rooms.Add(context, RID.forRoom(location), numRooms, price);
        }

        public bool DeleteRooms(Transaction context, String location, int numRooms)
        {
            return Rooms.Delete(context, RID.forRoom(location), numRooms);
        }

        public bool AddCars(Transaction context, String location, int numCars,
               int price)
        {
            return Cars.Add(context, RID.forCar(location), numCars, price);
        }

        public bool DeleteCars(Transaction context, String location, int numCars)
        {
            return Cars.Delete(context, RID.forCar(location), numCars);
        }

        public int QueryFlight(Transaction context, String flight)
        {
            return Flights.Query(context, RID.forFlight(flight));
        }

        public int QueryFlightPrice(Transaction context, String flight)
        {
            return Flights.QueryPrice(context, RID.forFlight(flight));
        }

        public int QueryRoom(Transaction context, String location)
        {
            return Rooms.Query(context, RID.forRoom(location));
        }

        public int QueryRoomPrice(Transaction context, String location)
        {
            return Rooms.QueryPrice(context, RID.forRoom(location));
        }

        public int QueryCar(Transaction context, String location)
        {
            return Cars.Query(context, RID.forCar(location));
        }

        public int QueryCarPrice(Transaction context, String location)
        {
            return Cars.QueryPrice(context, RID.forCar(location));
        }

        public String[] ListFlights(Transaction context)
        {
            return Flights.ListResources(context, RID.Type.FLIGHT);
        }

        public String[] ListCars(Transaction context)
        {
            return Cars.ListResources(context, RID.Type.CAR);
        }

        public String[] ListRooms(Transaction context)
        {
            return Rooms.ListResources(context, RID.Type.ROOM);
        }

        public Customer[] ListCustomers(Transaction context)
        {
            var customers = new HashSet<Customer>();

            if (Flights != null)
            {
                foreach (Customer c in Flights.ListCustomers(context))
                    customers.Add(c);
            }

            if (Cars != null)
            {
                foreach (Customer c in Cars.ListCustomers(context))
                    customers.Add(c);
            }

            if (Rooms != null)
            {
                foreach (Customer c in Rooms.ListCustomers(context))
                    customers.Add(c);
            }
            
            var cs = new Customer[customers.Count];
            customers.CopyTo(cs);
            return cs;
        }

        public Transaction Start()
        {
            try
            {
                return TransactionManager.Start();
            }
            catch (WebException)
            {
                TransactionManager = null;
                ReconnectToTM();
            }

            return null;
        }

        public void Commit(Transaction context)
        {
            try
            {
                TransactionManager.Commit(context);
            }
            catch (WebException)
            {
                TransactionManager = null;
                ReconnectToTM();
            }
        }

        public void Abort(Transaction context)
        {
            try
            {
                TransactionManager.Abort(context);
            }
            catch (WebException)
            {
                TransactionManager = null;
                ReconnectToTM();
            }
        }

        protected void Init(String[] args)
        {

        }

        protected void InitStorage()
        {

        }

        protected void Recovery()
        {
        }

        protected void StartUp()
        {
        }

        protected void ReadyToServe()
        {
        }

        /// <summary>
        ///   Exit TM (simulate a failure) on certain condition
        ///   Now supports 
        ///   1. exit before all RM is prepared (send Prepare to 1 RM then killed)
        ///   2. exit before all RM are done (send commited to 1 RM then killed)
        ///   
        /// </summary>
        /// <param name="prepareFailed"></param>
        /// <param name="commitFailed"></param>
        public void TMSelfDestruct(bool prepareFailed, bool commitFailed)
        {
            TransactionManager.SelfDestruct(prepareFailed, commitFailed);
        }

        class WCParser : CommandLineParser
        {
            public WCParser()
            {

                //Add("f", "Flights RM", "The URL of the Flights Resource Manager", "http://localhost:8081/RM.soap");
                //Add("c", "Cars RM", "The URL of the Cars Resource Manager", "http://localhost:8082/RM.soap");
                //Add( "r", "Rooms RM", "The URL of the Rooms Resource Manager", "http://localhost:8083/RM.soap");
                Add("tms", "trasaction manager server", "the server TM running on", "http://localhost");
                Add("tmp", "transaction manager port", "The port of the Transaction Manager", "8089");
                Add("p", "Port", "The port this Workflow Controller listens on", "8086");
            }
        }
        /// <summary>
        /*  WC runs as a separate process looping forever,
            waiting for the calls from other processes */
        /// </summary>
        static void Main(string[] args)
        {
            WCParser parser = new WCParser();
            if (!parser.Parse(args))
            {
                return;
            }

            //string rmFlightsURL = parser["f"];
            //string rmRoomsURL = parser["r"];
            //string rmCarsURL = parser["c"];
            string tmPort = parser["tmp"];
            string tmServer = parser["tms"];
            tmURL = tmServer + ":" + tmPort + "/TM.soap";

            while (TransactionManager == null)
            {
                try
                {
                    TransactionManager = (TP.TM)Activator.GetObject(typeof(TP.TM), tmServer + ":" + tmPort + "/TM.soap");
                    Transaction tid = TransactionManager.Start();
                    TransactionManager.Abort(tid);
                }
                catch (RemotingException e)
                {
                    TransactionManager = null;
                    Console.WriteLine("Waiting 1 second for Transaction Manager \"{0}\"", tmURL);
                    System.Threading.Thread.Sleep(1000);
                }
            }

            Console.WriteLine("Transaction Manager retrieved at {0}:{1}", tmURL, tmPort);
            while (Flights == null || Rooms == null || Cars == null)
            {
                if (Flights == null)
                    Flights = TransactionManager.GetResourceMananger("flight");
                if (Rooms == null)
                    Rooms = TransactionManager.GetResourceMananger("room");
                if (Cars == null)
                    Cars = TransactionManager.GetResourceMananger("car");
            }
            if (Flights != null)
                Console.WriteLine("Get RM with the name:" + Flights.GetName());
            if (Rooms != null)
                Console.WriteLine("Get RM with the name:" + Rooms.GetName());
            if (Cars != null)
                Console.WriteLine("Get RM with the name:" + Cars.GetName());

            HttpChannel httpChannel = new HttpChannel(Int32.Parse(parser["p"]));
            System.Runtime.Remoting.Channels.ChannelServices.RegisterChannel(httpChannel, false);
            RemotingConfiguration.RegisterWellKnownServiceType
                (Type.GetType("MyWC.MyWC")							    // Assembly name
                , "WC.soap"												// URI
                , System.Runtime.Remoting.WellKnownObjectMode.Singleton	// Instancing mode
            );

            Console.WriteLine("Starting Workflow Controller on port {0}", parser["p"]);

            while (true)
            {
                System.Threading.Thread.Sleep(1000);
                try
                {
                    TransactionManager.Ping();
                }
                catch(WebException)
                {
                    TransactionManager = null;
                    ReconnectToTM();
                }
            }
        }

        /// <summary>
        /// try to reconnect to TM after tm failed. 
        /// </summary>
        public static void ReconnectToTM()
        {
            while (TransactionManager == null)
            {
                Console.WriteLine("Trying to reconnect to TM");
                TransactionManager = (TP.TM)Activator.GetObject(typeof(TP.TM), tmURL);
                try
                {
                    TransactionManager.Ping();
                }
                catch (WebException)
                {
                    TransactionManager = null;
                    Console.WriteLine("Waiting 1 second for Transaction Manager \"{0}\"", tmURL);
                    System.Threading.Thread.Sleep(1000);
                }
            }

            Console.WriteLine("Connected to Transaction Manager \"{0}\"", tmURL);
        }
    }
}