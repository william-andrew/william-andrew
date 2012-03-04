﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using TP;
using Transaction = TP.Transaction;
using System.Linq;
using System.IO;
using System;
namespace TestProject
{
    [TestClass()]
    public class MyWCTest
    {
        [TestInitialize]
        public void CleanPreviousData()
        {
            string[] files = Directory.GetFiles(Environment.CurrentDirectory, "MYRM*");
            foreach (string file in files)
            {
                File.Delete(file);
            }
        }
        /// <summary>
        ///A test for AddSeats
        ///</summary>
        [TestMethod]
        public void AddSeatsTest()
        {
            var wc = new MyWC.MyWC();
            var tm = new MyTM.MyTM();
            var rm = new MyRM.MyRM();
            rm.SetName("flight");
            rm.TransactionManager = tm;
            tm.Register(rm);
            MyWC.MyWC.Flights = tm.GetResourceMananger("flight");

            var context = tm.Start();
            wc.AddSeats(context, "FL", 100, 550);
            wc.AddSeats(context, "SG", 200, 250);
            tm.Commit(context);

            context = tm.Start();
            var result = wc.ListFlights(context);
            tm.Commit(context);
            Assert.AreEqual(2, result.Length);

            context = tm.Start();
            wc.AddSeats(context, "FL", 50, 450);
            tm.Commit(context);

            context = tm.Start();
            var c = wc.QueryFlight(context, "FL");
            Assert.AreEqual(150, c);
            var price = wc.QueryFlightPrice(context, "FL");
            Assert.AreEqual(450, price);
            tm.Abort(context);

            context = tm.Start();
            c = wc.QueryFlight(context, "SG");
            price = wc.QueryFlightPrice(context, "SG");
            Assert.AreEqual(200, c);
            Assert.AreEqual(250, price);
            tm.Abort(context);
        }

        /// <summary>
        ///A test for DeleteSeats
        ///</summary>
        [TestMethod]
        public void DeleteSeatsTest()
        {
            var wc = new MyWC.MyWC();
            var tm = new MyTM.MyTM();
            var rm = new MyRM.MyRM();
            rm.SetName("flight");
            rm.TransactionManager = tm;
            tm.Register(rm);
            MyWC.MyWC.Flights = tm.GetResourceMananger("flight");

            var context = tm.Start();
            var flights = wc.ListFlights(context);
            foreach(var f in flights)
            {
                wc.DeleteFlight(context, f.Split(',')[0]);
            }
            tm.Commit(context);

            context = tm.Start();
            Assert.IsTrue(wc.AddSeats(context, "FLX", 100, 550));
            Assert.IsTrue(wc.AddSeats(context, "SGX", 200, 250));
            tm.Commit(context);

            context = tm.Start();
            var result = wc.ListFlights(context);
            tm.Commit(context);
            Assert.AreEqual(2, result.Length);

            context = tm.Start();
            Assert.IsTrue(wc.DeleteSeats(context, "FLX", 50));
            Assert.IsTrue(wc.DeleteSeats(context, "SGX", 50));
            tm.Commit(context);

            context = tm.Start();
            var c1 = wc.QueryFlight(context, "FLX");
            var c2 = wc.QueryFlight(context, "SGX");
            Assert.AreEqual(100 - 50, c1);
            Assert.AreEqual(200 - 50, c2);
            tm.Abort(context);
        }

        /// <summary>
        ///A test for DeleteFlight
        ///</summary>
        [TestMethod]
        public void DeleteFlight()
        {
            var wc = new MyWC.MyWC();
            var tm = new MyTM.MyTM();
            var rm = new MyRM.MyRM();
            rm.SetName("flight");
            rm.TransactionManager = tm;
            tm.Register(rm);
            MyWC.MyWC.Flights = tm.GetResourceMananger("flight");

            var context = tm.Start();
            Assert.IsTrue(wc.AddSeats(context, "FLK", 100, 550));
            tm.Commit(context);

            context = tm.Start();
            var result = wc.ListFlights(context);
            tm.Commit(context);

            Assert.IsTrue((from f in result where f == "FLK,100,550" select f).Any());

            context = tm.Start();
            Assert.IsTrue(wc.DeleteFlight(context, "FLK"));
            result = wc.ListFlights(context);
            tm.Commit(context);

            Assert.IsFalse((from f in result where f == "FLK,100,550" select f).Any());
        }

        /// <summary>
        ///A test for AddRooms/DeleteRooms/QueryRoom/QueryRoomPrice
        ///</summary>
        [TestMethod]
        public void AddDeleteRooms()
        {
            var wc = new MyWC.MyWC();
            var tm = new MyTM.MyTM();
            var rm = new MyRM.MyRM();
            rm.SetName("room");
            rm.TransactionManager = tm;
            tm.Register(rm);
            MyWC.MyWC.Rooms = tm.GetResourceMananger("room");

            var context = tm.Start();
            Assert.IsTrue(wc.AddRooms(context, "SEATTLE", 100, 66));
            Assert.IsTrue(wc.AddRooms(context, "BEIJING", 200, 220));
            tm.Commit(context);

            context = tm.Start();
            Assert.AreEqual(100, wc.QueryRoom(context, "SEATTLE"));
            Assert.AreEqual(66, wc.QueryRoomPrice(context, "SEATTLE"));

            Assert.AreEqual(200, wc.QueryRoom(context, "BEIJING"));
            Assert.AreEqual(220, wc.QueryRoomPrice(context, "BEIJING"));
            tm.Commit(context);

            //add rooms
            context = tm.Start();
            Assert.IsTrue(wc.AddRooms(context, "SEATTLE", 10, 55));
            Assert.IsTrue(wc.AddRooms(context, "BEIJING", 20, 110));
            tm.Commit(context);

            context = tm.Start();
            Assert.AreEqual(100 + 10, wc.QueryRoom(context, "SEATTLE"));
            Assert.AreEqual(55, wc.QueryRoomPrice(context, "SEATTLE"));

            Assert.AreEqual(220, wc.QueryRoom(context, "BEIJING"));
            Assert.AreEqual(110, wc.QueryRoomPrice(context, "BEIJING"));
            tm.Commit(context);

            //delete rooms
            context = tm.Start();
            Assert.IsTrue(wc.DeleteRooms(context, "SEATTLE", 5));
            Assert.IsTrue(wc.DeleteRooms(context, "BEIJING", 10));
            tm.Commit(context);

            context = tm.Start();
            Assert.AreEqual(100 + 10 - 5, wc.QueryRoom(context, "SEATTLE"));
            Assert.AreEqual(55, wc.QueryRoomPrice(context, "SEATTLE"));
            
            Assert.AreEqual(220 - 10, wc.QueryRoom(context, "BEIJING"));
            Assert.AreEqual(110, wc.QueryRoomPrice(context, "BEIJING"));
            tm.Commit(context);
        }

        /// <summary>
        ///A test for AddCars/DeleteCars/QueryCar/QueryCarPrice
        ///</summary>
        [TestMethod]
        public void AddDeleteCars()
        {
            var wc = new MyWC.MyWC();
            var tm = new MyTM.MyTM();
            var rm = new MyRM.MyRM();
            rm.SetName("car");
            rm.TransactionManager = tm;
            tm.Register(rm);
            MyWC.MyWC.Cars = tm.GetResourceMananger("car");

            var context = tm.Start();
            Assert.IsTrue(wc.AddCars(context, "SEATTLE", 100, 66));
            Assert.IsTrue(wc.AddCars(context, "BEIJING", 200, 220));
            tm.Commit(context);

            context = tm.Start();
            Assert.AreEqual(100, wc.QueryCar(context, "SEATTLE"));
            Assert.AreEqual(66, wc.QueryCarPrice(context, "SEATTLE"));

            Assert.AreEqual(200, wc.QueryCar(context, "BEIJING"));
            Assert.AreEqual(220, wc.QueryCarPrice(context, "BEIJING"));
            tm.Commit(context);

            //add rooms
            context = tm.Start();
            Assert.IsTrue(wc.AddCars(context, "SEATTLE", 10, 55));
            Assert.IsTrue(wc.AddCars(context, "BEIJING", 20, 110));
            tm.Commit(context);

            context = tm.Start();
            Assert.AreEqual(100 + 10, wc.QueryCar(context, "SEATTLE"));
            Assert.AreEqual(55, wc.QueryCarPrice(context, "SEATTLE"));

            Assert.AreEqual(220, wc.QueryCar(context, "BEIJING"));
            Assert.AreEqual(110, wc.QueryCarPrice(context, "BEIJING"));
            tm.Commit(context);

            //delete rooms
            context = tm.Start();
            Assert.IsTrue(wc.DeleteCars(context, "SEATTLE", 5));
            Assert.IsTrue(wc.DeleteCars(context, "BEIJING", 10));
            tm.Commit(context);

            context = tm.Start();
            Assert.AreEqual(100 + 10 - 5, wc.QueryCar(context, "SEATTLE"));
            Assert.AreEqual(55, wc.QueryCarPrice(context, "SEATTLE"));

            Assert.AreEqual(220 - 10, wc.QueryCar(context, "BEIJING"));
            Assert.AreEqual(110, wc.QueryCarPrice(context, "BEIJING"));
            tm.Commit(context);
        }

        /// <summary>
        ///A test for Add/remove multiple resources in a transaction 
        ///</summary>
        [TestMethod]
        public void AddDeleteCombinationsOneByOne()
        {
            var wc = new MyWC.MyWC();
            var tm = new MyTM.MyTM();
            var rmf = new MyRM.MyRM();
            var rmc = new MyRM.MyRM();
            var rmr = new MyRM.MyRM();

            rmf.SetName("flight");
            rmf.TransactionManager = tm;

            rmc.SetName("car");
            rmc.TransactionManager = tm;

            rmr.SetName("room");
            rmr.TransactionManager = tm;

            tm.Register(rmf);
            tm.Register(rmc);
            tm.Register(rmr);

            MyWC.MyWC.Flights = tm.GetResourceMananger("flight");
            MyWC.MyWC.Cars = tm.GetResourceMananger("car");
            MyWC.MyWC.Rooms = tm.GetResourceMananger("room");

            var context = new Transaction();
            wc.AddSeats(context, "SEA-JFK", 3000, 300);
            tm.Commit(context);
            wc.AddCars(context, "NY", 2000, 200);
            tm.Commit(context);
            wc.AddRooms(context, "NY", 1000, 100);
            tm.Commit(context);

            context = new Transaction();
            Assert.AreEqual(200, wc.QueryCarPrice(context, "NY"));
            Assert.AreEqual(300, wc.QueryFlightPrice(context, "SEA-JFK"));
            Assert.AreEqual(100, wc.QueryRoomPrice(context, "NY"));
            tm.Commit(context);
        }

        /// <summary>
        ///A test for Add/remove multiple resources in a transaction 
        ///</summary>
        [TestMethod]
        public void AddDeleteCombinations()
        {
            var wc = new MyWC.MyWC();
            var tm = new MyTM.MyTM();
            var rmf = new MyRM.MyRM();
            var rmc = new MyRM.MyRM();
            var rmr = new MyRM.MyRM();

            rmf.SetName("flight");
            rmf.TransactionManager = tm;

            rmc.SetName("car");
            rmc.TransactionManager = tm;
            
            rmr.SetName("room");
            rmr.TransactionManager = tm;

            tm.Register(rmf);
            tm.Register(rmc);
            tm.Register(rmr);

            MyWC.MyWC.Flights = tm.GetResourceMananger("flight");
            MyWC.MyWC.Cars = tm.GetResourceMananger("car");
            MyWC.MyWC.Rooms = tm.GetResourceMananger("room");

            var context = new Transaction();
            wc.AddSeats(context, "SEA-JFK", 3000, 300);
            wc.AddCars(context, "NY", 2000, 200);
            wc.AddRooms(context, "NY", 1000, 100);
            tm.Commit(context);

            context = new Transaction();
            Assert.AreEqual(200, wc.QueryCarPrice(context, "NY"));
            Assert.AreEqual(300, wc.QueryFlightPrice(context, "SEA-JFK"));
            Assert.AreEqual(100, wc.QueryRoomPrice(context, "NY"));
            tm.Commit(context);
        }

        /// <summary>
        ///A test for Cancellation
        ///</summary>
        [TestMethod]
        public void CancelSingleFlight()
        {
            var wc = new MyWC.MyWC();
            var tm = new MyTM.MyTM();
            var rmf = new MyRM.MyRM();
            var rmc = new MyRM.MyRM();
            var rmr = new MyRM.MyRM();

            rmf.SetName("flight");
            rmf.TransactionManager = tm;

            rmc.SetName("car");
            rmc.TransactionManager = tm;

            rmr.SetName("room");
            rmr.TransactionManager = tm;

            tm.Register(rmf);
            tm.Register(rmc);
            tm.Register(rmr);

            MyWC.MyWC.Flights = tm.GetResourceMananger("flight");
            MyWC.MyWC.Cars = tm.GetResourceMananger("car");
            MyWC.MyWC.Rooms = tm.GetResourceMananger("room");
            MyWC.MyWC.TransactionManager = tm;

            var context = new Transaction();
            wc.AddSeats(context, "SEA-JFK", 1000, 200);
            tm.Commit(context);

            var c1 = new Customer();
            Assert.IsTrue(wc.ReserveItinerary(c1, new[] { "SEA-JFK" }, "NY", false, false));
            
            context = new Transaction();
            Assert.AreEqual(c1, wc.ListCustomers(context)[0]);
            Assert.IsFalse(String.IsNullOrEmpty(wc.QueryItinerary(context, c1)), "Itinerary not found");
            tm.Commit(context);

            context = new Transaction();
            Assert.AreEqual(1000 - 1, wc.QueryFlight(context, "SEA-JFK"));
            tm.Commit(context);

            wc.CancelItinerary(c1);

            context = new Transaction();
            Assert.AreEqual(1000, wc.QueryFlight(context, "SEA-JFK"));
            tm.Commit(context);
        }


        /// <summary>
        ///A test for Cancellation
        ///</summary>
        [TestMethod]
        public void CancelTwoFlights()
        {
            var wc = new MyWC.MyWC();
            var tm = new MyTM.MyTM();
            var rmf = new MyRM.MyRM();
            var rmc = new MyRM.MyRM();
            var rmr = new MyRM.MyRM();

            rmf.SetName("flight");
            rmf.TransactionManager = tm;

            rmc.SetName("car");
            rmc.TransactionManager = tm;

            rmr.SetName("room");
            rmr.TransactionManager = tm;

            tm.Register(rmf);
            tm.Register(rmc);
            tm.Register(rmr);

            MyWC.MyWC.Flights = tm.GetResourceMananger("flight");
            MyWC.MyWC.Cars = tm.GetResourceMananger("car");
            MyWC.MyWC.Rooms = tm.GetResourceMananger("room");
            MyWC.MyWC.TransactionManager = tm;

            var context = wc.Start();
            wc.AddSeats(context, "SEA-JFK", 1000, 200);
            wc.AddSeats(context, "JFK-PVG", 500, 150);
            tm.Commit(context);

            var c1 = new Customer();
            Assert.IsTrue(wc.ReserveItinerary(c1, new[] { "SEA-JFK", "JFK-PVG" }, "NY", false, false));

            context = wc.Start();
            Assert.AreEqual(c1, wc.ListCustomers(context)[0]);
            Assert.IsFalse(String.IsNullOrEmpty(wc.QueryItinerary(context, c1)), "Itinerary not found");
            wc.Commit(context);

            context = wc.Start();
            Assert.AreEqual(1000 - 1, wc.QueryFlight(context, "SEA-JFK"));
            Assert.AreEqual(500 - 1, wc.QueryFlight(context, "JFK-PVG"));
            wc.Commit(context);

            wc.CancelItinerary(c1);

            context = wc.Start();
            Assert.AreEqual(500, wc.QueryFlight(context, "JFK-PVG"), "incorrect flight total");
            Assert.AreEqual(1000, wc.QueryFlight(context, "SEA-JFK"), "incorrect flight total");
            wc.Commit(context);
        }

        /// <summary>
        ///A test for Reservation and Cancellation
        ///</summary>
        [TestMethod]
        public void Reservation()
        {
            var wc = new MyWC.MyWC();
            var tm = new MyTM.MyTM();
            var rmf = new MyRM.MyRM();
            var rmc = new MyRM.MyRM();
            var rmr = new MyRM.MyRM();

            rmf.SetName("flight");
            rmf.TransactionManager = tm;
            
            rmc.SetName("car");
            rmc.TransactionManager = tm;

            rmr.SetName("room");
            rmr.TransactionManager = tm;

            tm.Register(rmf);
            tm.Register(rmc);
            tm.Register(rmr);

            MyWC.MyWC.Flights = tm.GetResourceMananger("flight");
            MyWC.MyWC.Cars = tm.GetResourceMananger("car");
            MyWC.MyWC.Rooms = tm.GetResourceMananger("room");
            MyWC.MyWC.TransactionManager = tm;
            
            var context = wc.Start();
            wc.AddSeats(context, "SEA-JFK", 3000, 300);
            wc.AddSeats(context, "JFK-STV", 3000, 300);
            wc.AddCars(context, "NY", 2000, 200);
            wc.AddRooms(context, "NY", 1000, 100);
            wc.Commit(context);

            var c1 = new Customer();
            var c2 = new Customer();
            Assert.IsTrue(wc.ReserveItinerary(c1, new[] {"SEA-JFK", "JFK-STV"}, "NY", true, true));
            Assert.IsTrue(wc.ReserveItinerary(c2, new[] {"SEA-JFK"}, "NY", true, true));

            context = wc.Start();
            Assert.AreEqual(c1, wc.ListCustomers(context)[0]);
            Assert.AreEqual(c2, wc.ListCustomers(context)[1]);
            wc.Commit(context);

            context = wc.Start();
            Assert.AreEqual(3000 - 2, wc.QueryFlight(context, "SEA-JFK"));
            Assert.AreEqual(3000 - 1, wc.QueryFlight(context, "JFK-STV"));
            Assert.AreEqual(1000 - 2, wc.QueryRoom(context, "NY"));
            wc.Commit(context);

            wc.CancelItinerary(c1);
            wc.CancelItinerary(c2);

            context = wc.Start();
            Assert.AreEqual(1000, wc.QueryRoom(context, "NY"));
            Assert.AreEqual(3000, wc.QueryFlight(context, "SEA-JFK"));
            Assert.AreEqual(3000, wc.QueryFlight(context, "JFK-STV"));
            wc.Commit(context);
        }

        /// <summary>
        ///A test for Abort
        ///</summary>
        [TestMethod]
        public void AddSeatsAbortTest()
        {
            var wc = new MyWC.MyWC();
            var tm = new MyTM.MyTM();
            var rm = new MyRM.MyRM();
            rm.SetName("flight");
            rm.TransactionManager = tm;
            tm.Register(rm);
            MyWC.MyWC.Flights = tm.GetResourceMananger("flight");

            var context = new Transaction();
            wc.AddSeats(context, "FLK", 100, 550);
            wc.AddSeats(context, "SGK", 200, 250);
            tm.Abort(context);

            context = new Transaction();
            var result = wc.ListFlights(context);
            tm.Commit(context);
            Assert.IsFalse((from f in result where f == "FLK,100,550" select f).Any());
            Assert.IsFalse((from f in result where f == "SGK,200,250" select f).Any());
        }

        /// <summary>
        ///A test for AddSeats
        ///</summary>
        [TestMethod]
        public void AddSeatsTwoTransactions()
        {
            var wc = new MyWC.MyWC();
            var tm = new MyTM.MyTM();
            var rm = new MyRM.MyRM();
            rm.SetName("flight");
            rm.TransactionManager = tm;
            tm.Register(rm);
            MyWC.MyWC.Flights = tm.GetResourceMananger("flight");

            var context1 = new Transaction();
            var context2 = new Transaction();
            wc.AddSeats(context1, "FL_C", 100, 550);
            wc.AddSeats(context2, "SG_A", 200, 250);
            tm.Abort(context2);
            tm.Commit(context1);

            context1 = new Transaction();
            var result = wc.ListFlights(context1);
            tm.Commit(context1);
            Assert.IsTrue((from f in result where f == "FL_C,100,550" select f).Any());
            Assert.IsFalse((from f in result where f == "SG_A,200,250" select f).Any());
        }
    }
}
