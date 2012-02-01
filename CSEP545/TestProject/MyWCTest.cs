using Microsoft.VisualStudio.TestTools.UnitTesting;
using Transaction = TP.Transaction;

namespace TestProject
{
    [TestClass()]
    public class MyWCTest
    {
        /// <summary>
        ///A test for AddSeats
        ///</summary>
        [TestMethod()]
        public void AddSeatsTest()
        {
            var wc = new MyWC.MyWC();
            var tm = new MyTM.MyTM();
            var rm = new MyRM.MyRM();
            rm.SetName("flight");
            tm.Register(rm);
            MyWC.MyWC.Flights = tm.GetResourceMananger("flight");
 
            var context = new Transaction();
            var success = wc.AddSeats(context, "FL", 100, 550);

            tm.Commit(context);
          
        }
    }
}
