namespace CSEP545
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using TP;
    using System.IO;
    using MyTM;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Net;

    class MasterTest
    {
        public WC WorkflowControl
        {
            get;
            set;
        }

        public RM CarsRM
        {
            get;
            set;
        }

        public RM RoomsRM
        {
            get;
            set;
        }

        public RM FlightsRM
        {
            get;
            set;
        }

        public TM TransactionManager
        {
            get;
            set;
        }

        /// <summary>
        /// Submit an itinerary with car, flight and hotel. Show the values are updated. Demo Read, write and commit.
        /// </summary>
        public void AddOneItinerary()
        {
            StartUp();
            Customer c = new Customer("12345678-1234-1234-1234-123456789012");
            string[] flights = new string[] { "SEA->LAX", "LAX->LAV" };
            WorkflowControl.ReserveItinerary(c, flights, "Vegas", true, true);
            Transaction t = WorkflowControl.Start();
            string actual = WorkflowControl.QueryItinerary(t, c);
            Console.WriteLine(actual);
            Assert.AreEqual("F:SEA->LAX,F:LAX->LAV,C:Vegas,R:Vegas", actual);
            actual = PrintCars();
            Assert.AreEqual("Vegas,0,3;NewYork,10,30;", actual);
            actual = PrintRooms();
            Assert.AreEqual("Vegas,1,1;NewYork,20,10;", actual);
            actual = PrintFlights();
            Assert.AreEqual("SEA->LAX,9,2;LAX->LAV,11,3;SEA->JFK,8,4;", actual);
            actual = PrintCustomers();
            Assert.AreEqual("12345678-1234-1234-1234-123456789012;", actual);
        }

        /// <summary>
        /// Add an itinerary, kill RM before commit. Restart the RMs and you can see nothing changed (still c1 state). Demo shadow copy works after failure
        /// </summary>
        public void UncommittedScenario()
        {
            StartUp();
            Customer c = new Customer("12345678-1234-1234-1234-123456789012");
            FlightsRM.SelfDestruct(2, 0, 0, 0);
            string[] flights = new string[] { "SEA->LAX", "LAX->LAV" };
            try
            {
                WorkflowControl.ReserveItinerary(c, flights, "Vegas", true, true);
            }
            catch (WebException e)
            {
                // Expected error because one of the RM will be selfdestroyed. 
                Console.WriteLine(e.Message);
            }
            StopRMs();
            StartRMs();
            Transaction t = WorkflowControl.Start();
            string actual = WorkflowControl.QueryItinerary(t, c);
            Console.WriteLine(actual);
            Assert.AreEqual("", actual);
            actual = PrintCars();
            Assert.AreEqual("Vegas,1,3;NewYork,10,30;", actual);
            actual = PrintRooms();
            Assert.AreEqual("Vegas,2,1;NewYork,20,10;", actual);
            actual = PrintFlights();
            Assert.AreEqual("SEA->LAX,10,2;LAX->LAV,12,3;SEA->JFK,8,4;", actual);
            actual = PrintCustomers();
            Assert.AreEqual("", actual);
        }

        /// <summary>
        /// Have T1 and T2 read R1, both shall get the result immediately.  
        /// Have T1, T2 both write R2 concurrently, The result shall be correct. Demo locks and two transaction runs concurrently. 
        /// </summary>
        public void ConcurrentTransactions()
        {
            StartUp();
            Transaction t1 = WorkflowControl.Start();
            Transaction t2 = WorkflowControl.Start();
            Assert.AreEqual(WorkflowControl.QueryCar(t1, "Vegas"), WorkflowControl.QueryCar(t2, "Vegas"));
            AutoResetEvent sync1 = new AutoResetEvent(false);
            AutoResetEvent sync2 = new AutoResetEvent(false);
            ThreadPool.QueueUserWorkItem(o =>
                {
                    WorkflowControl.AddCars(t1, "Seattle", 1, 3);
                    WorkflowControl.Commit(t1);
                    sync1.Set();
                });
            ThreadPool.QueueUserWorkItem(o =>
                {
                    WorkflowControl.AddCars(t2, "Seattle", 2, 3);
                    WorkflowControl.Commit(t2);
                    sync2.Set();
                });
            // here is just to make sure t2 commits before t1, so the price will be 2. 
            sync1.WaitOne();
            sync2.WaitOne();
            
            string actual = PrintCars();
            Assert.AreEqual("Vegas,1,3;NewYork,10,30;Seattle,3,3;", actual);            
        }

        /// <summary>
        /// T1, T2 write R1, T1 commit, T2 abort, see only T1's change in. Demo abort.
        /// </summary>
        public void ConcurrentCommitAbort()
        {
            StartUp();
            Transaction t1 = WorkflowControl.Start();
            Transaction t2 = WorkflowControl.Start();
            AutoResetEvent sync1 = new AutoResetEvent(false);
            AutoResetEvent sync2 = new AutoResetEvent(false);
            ThreadPool.QueueUserWorkItem(o =>
            {
                WorkflowControl.AddCars(t1, "Seattle", 1, 3);
                WorkflowControl.Commit(t1);
                sync1.Set();
            });
            ThreadPool.QueueUserWorkItem(o =>
            {
                WorkflowControl.AddCars(t2, "Seattle", 2, 2);
                WorkflowControl.Abort(t2);
                sync2.Set();
            });
            // here is just to make sure t2 commits before t1, so the price will be 2. 
            sync1.WaitOne();
            sync2.WaitOne();

            string actual = PrintCars();
            Assert.AreEqual("Vegas,1,3;NewYork,10,30;Seattle,1,3;", actual);
        }

        /// <summary>
        /// All RMs returns prepared, except one fails to prepare, the transaction should abort
        /// </summary>
        public void RollbackAfterRmFailTransaction2PC()
        {
            StartUp();
            CarsRM.SelfDestruct(0, 1, 0, 0);
            Customer c = new Customer("12345678-1234-1234-1234-123456789012");
            string[] flights = new string[] { "SEA->LAX", "LAX->LAV" };
            WorkflowControl.ReserveItinerary(c, flights, "Vegas", true, true);
            // We shall see some rollback message from TM. 
            Pause();
            Transaction t = WorkflowControl.Start();
            string actual = WorkflowControl.QueryItinerary(t, c);
            Console.WriteLine(actual);
            Assert.AreEqual("", actual);
            actual = PrintCars();
            Assert.AreEqual("Vegas,1,3;NewYork,10,30;", actual);
            actual = PrintRooms();
            Assert.AreEqual("Vegas,2,1;NewYork,20,10;", actual);
            actual = PrintFlights();
            Assert.AreEqual("SEA->LAX,10,2;LAX->LAV,12,3;SEA->JFK,8,4;", actual);
            actual = PrintCustomers();
            Assert.AreEqual("", actual);
        }

        /// <summary>
        /// All RM prepared, one RM dies before receiving Commit, on recovery, the RM should recover the transaction, WC shouldn’t notice this
        /// </summary>
        public void ReCommitAfterRmFailTransaction2PC()
        {
            StartUp();
            CarsRM.SelfDestruct(0, 0, 1, 0);
            Customer c = new Customer("12345678-1234-1234-1234-123456789012");
            string[] flights = new string[] { "SEA->LAX", "LAX->LAV" };

            ThreadPool.QueueUserWorkItem(o =>
                {
                    Thread.Sleep(1000);
                    CarsRM.SelfDestruct(0, 0, 0, 0);
                });
            WorkflowControl.ReserveItinerary(c, flights, "Vegas", true, true);
            // We shall see some commit retry message from TM but the transaction shall success. 
            Transaction t = WorkflowControl.Start();
            string actual = WorkflowControl.QueryItinerary(t, c);
            Console.WriteLine(actual);
            Assert.AreEqual("F:SEA->LAX,F:LAX->LAV,C:Vegas,R:Vegas", actual);
            actual = PrintCars();
            Assert.AreEqual("Vegas,0,3;NewYork,10,30;", actual);
            actual = PrintRooms();
            Assert.AreEqual("Vegas,1,1;NewYork,20,10;", actual);
            actual = PrintFlights();
            Assert.AreEqual("SEA->LAX,9,2;LAX->LAV,11,3;SEA->JFK,8,4;", actual);
            actual = PrintCustomers();
            Assert.AreEqual("12345678-1234-1234-1234-123456789012;", actual);
        }

        /// <summary>
        /// TM dies before receive prepare from all RM, on recovery, the transaction abort
        /// </summary>
        public void TMFailsBeforeReceivingAllPrepare2PC()
        {
            StartUp();

            Transaction t = WorkflowControl.Start();
            WorkflowControl.AddCars(t, "Seattle", 10, 100);
            TransactionManager.SelfDestruct(true, false);
            ThreadPool.QueueUserWorkItem(o => WorkflowControl.Commit(t));

            // TM shall be kill, now restart it            
            Pause();

            StartTM();
            // shall see the transaction aborts
            t = WorkflowControl.Start();
            string actual = PrintCars();
            Assert.AreEqual("Vegas,1,3;NewYork,10,30;", actual);
            actual = PrintRooms();
            Assert.AreEqual("Vegas,2,1;NewYork,20,10;", actual);
            actual = PrintFlights();
            Assert.AreEqual("SEA->LAX,10,2;LAX->LAV,12,3;SEA->JFK,8,4;", actual);
            actual = PrintCustomers();
            Assert.AreEqual("", actual);
        }
        /// <summary>
        /// TM dies before receiving Done from all RMs, on recovery, TM should recommit
        /// </summary>
        public void TMFailsBeforeReceivingAllDone2PC()
        {
            StartUp();

            Transaction t = WorkflowControl.Start();
            TransactionManager.SelfDestruct(false, true);
            WorkflowControl.AddCars(t, "Seattle", 10, 100);
            ThreadPool.QueueUserWorkItem(o => WorkflowControl.Commit(t));

            // TM shall be kill, now restart it
            Pause();
            StartTM();
            // shall see the transaction recommits. 
            t = WorkflowControl.Start();
            string actual = PrintCars();
            Assert.AreEqual("Vegas,1,3;NewYork,10,30;Seattle,10,100;", actual);
            actual = PrintRooms();
            Assert.AreEqual("Vegas,2,1;NewYork,20,10;", actual);
            actual = PrintFlights();
            Assert.AreEqual("SEA->LAX,10,2;LAX->LAV,12,3;SEA->JFK,8,4;", actual);
            actual = PrintCustomers();
            Assert.AreEqual("", actual);
        }

        public void ExecuteAll()
        {
            Console.WriteLine("1. Submit an itinerary with car, flight and hotel. Show the values are updated. Demo Read, write and commit.");
            Pause();
            AddOneItinerary();
            Console.WriteLine("2. Add an itinerary, kill RM before commit. Restart the RMs and you can see nothing changed (still c1 state). Demo shadow copy works after failure");
            Pause();
            UncommittedScenario();
            Console.WriteLine("3. Have T1 and T2 read R1, both shall get the result immediately.Have T1, T2 both write R2 concurrently, The result shall be correct. Demo locks and two transaction runs concurrently.");
            Pause();
            ConcurrentTransactions();
            Console.WriteLine("4. T1, T2 write R1, T1 commit, T2 abort, see only T1's change in. Demo abort.");
            Pause();
            ConcurrentCommitAbort();
            Console.WriteLine("5. All RMs returns prepared, except one fails to prepare, the transaction should abort");
            Pause();
            RollbackAfterRmFailTransaction2PC();
            Console.WriteLine("6. All RM prepared, one RM dies before receiving Commit, on recovery, the RM should recover the transaction, WC shouldn’t notice this");
            Pause();
            ReCommitAfterRmFailTransaction2PC();
            Console.WriteLine("7. TM dies before receive prepare from all RM, on recovery, the transaction abort");
            Pause();
            TMFailsBeforeReceivingAllPrepare2PC();
            Console.WriteLine("8. TM dies before receiving Done from all RMs, on recovery, TM should recommit");
            Pause();
            TMFailsBeforeReceivingAllDone2PC();

            Console.WriteLine("All demo done");
            Pause();
            StopProcesses();
            //StartProcesses();
            //Pause();

            //TP.WC wc = (TP.WC)System.Activator.GetObject(typeof(RM), "http://localhost:8086/WC.soap");
            //RM rmcars = (RM)System.Activator.GetObject(typeof(RM), "http://localhost:8082/RM.soap");
            //RM rmrooms = (RM)System.Activator.GetObject(typeof(RM), "http://localhost:8083/RM.soap");

            //Transaction t = wc.Start();
            //Customer c = new Customer();
            //wc.AddCars(t, "Car1", 1, 1);
            //wc.AddRooms(t, "Room1", 2, 1);
            //wc.Commit(t);

            //string[] flights = new string[0];
            //wc.ReserveItinerary(c, flights, "Room1", false, true);
            //Console.WriteLine(wc.QueryItinerary(t, c));
            //string[] rooms = wc.ListRooms(t);
            //foreach (string r in rooms)
            //    Console.WriteLine(r);
            //wc.Commit(t);
            //Thread.Sleep(1000);

            //StopTM();

            //Pause("Press Enter To Start TM");
            //StartTM();

            //Pause("Press Enter to Start Cars RM");
            //StartCarsRM();

            //Pause("Press Enter to Start Rooms RM");
            //StartRoomsRM();

            //Pause("Press Enter to Exit");
            //StopProcesses();
        }
        private void InitInventory()
        {
            Transaction t = WorkflowControl.Start();
            WorkflowControl.AddCars(t, "Vegas", 1, 3);
            WorkflowControl.AddRooms(t, "Vegas", 2, 1);
            WorkflowControl.AddCars(t, "NewYork", 10, 30);
            WorkflowControl.AddRooms(t, "NewYork", 20, 10);
            WorkflowControl.AddSeats(t, "SEA->LAX", 10, 2);
            WorkflowControl.AddSeats(t, "LAX->LAV", 12, 3);
            WorkflowControl.AddSeats(t, "SEA->JFK", 8, 4);
            WorkflowControl.Commit(t);
        }

        private string PrintCustomers()
        {
            Transaction t = this.WorkflowControl.Start();
            Customer[] items = WorkflowControl.ListCustomers(t);
            string result = string.Empty;
            foreach (var r in items)
            {
                result += (r.Id + ";");
            }

            Console.WriteLine(result);
            return result;
        }

        private string PrintRooms()
        {
            Transaction t = this.WorkflowControl.Start();
            string[] items = WorkflowControl.ListRooms(t);
            string result = string.Empty;
            foreach (string r in items)
            {
                result += (r + ";");
            }

            Console.WriteLine(result);
            return result;
        }
        private string PrintCars()
        {
            Transaction t = this.WorkflowControl.Start();
            string[] items = WorkflowControl.ListCars(t);
            string result = string.Empty;
            foreach (string r in items)
            {
                result += (r + ";");
            }

            Console.WriteLine(result);
            return result;
        }
        private string PrintFlights()
        {
            Transaction t = this.WorkflowControl.Start();
            string[] items = WorkflowControl.ListFlights(t);
            string result = string.Empty;
            foreach (string r in items)
            {
                result += (r + ";");
            }

            Console.WriteLine(result);
            return result;
        }
        private void StartUp()
        {
            StopProcesses();
            File.Delete(TwoPhaseCommit.LogFileName);
            string[] files = Directory.GetFiles(Environment.CurrentDirectory, "MYRM_*");
            foreach (string file in files)
            {
                File.Delete(file);
            }
            files = Directory.GetFiles(Environment.CurrentDirectory, "*.manifest");
            foreach (string file in files)
            {
                File.Delete(file);
            }

            StartProcesses();
            this.WorkflowControl = (TP.WC)System.Activator.GetObject(typeof(RM), "http://localhost:8086/WC.soap");
            this.CarsRM = (RM)System.Activator.GetObject(typeof(RM), "http://localhost:8082/RM.soap");
            this.RoomsRM = (RM)System.Activator.GetObject(typeof(RM), "http://localhost:8083/RM.soap");
            this.FlightsRM = (RM)System.Activator.GetObject(typeof(RM), "http://localhost:8081/RM.soap");
            this.TransactionManager = (TM)System.Activator.GetObject(typeof(TM), "http://localhost:8089/TM.soap");
            InitInventory();
        }


        public void Pause(string message)
        {
            Console.WriteLine(message);
            Console.ReadLine();
        }

        public void Pause()
        {
            Pause("Press Enter to Continue");
        }

        static void StartTM()
        {
            Process.Start("MyTM.exe", "");
        }

        static void StartWC()
        {
            Process.Start("MyWC.exe", "");
        }

        static void StartCarsRM()
        {
            Process.Start("MyRM.exe", "-n car -p 8082");
        }

        static void StartRoomsRM()
        {
            Process.Start("MyRM.exe", "-n room -p 8083");
        }

        static void StartFlightsRM()
        {
            Process.Start("MyRM.exe", "-n flight -p 8081");
        }

        static void StartRMs()
        {
            StartRoomsRM();
            StartCarsRM();
            StartFlightsRM();
        }

        static void StartProcesses()
        {
            StartTM();
            StartWC();
            StartRMs();
        }

        static void StopTM()
        {
            StopProcess("MyTM");
        }

        static void StopWC()
        {
            StopProcess("MyWC");
        }

        static void StopRMs()
        {
            StopProcess("MyRM");
        }

        static void StopProcesses()
        {
            StopWC();
            StopRMs();
            StopTM();
        }

        static void StopProcess(string name)
        {
            foreach (Process p in Process.GetProcessesByName(name))
            {
                try
                {
                    p.Kill();
                }
                catch (Exception)
                {
                }
            }
        }
    }
}


