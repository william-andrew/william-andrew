using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MyRM;

namespace TestProject
{
   [TestClass]
    public class MyDatabaseTest
    {
        [TestMethod]
        public void GetDatabaseManifestTest()
        {
            var db = new MyDatabase("CarDatabase");
            db.CreateTable("Inventory.Car");
            db.WriteDatabaseManifest();

            db.CreateTable("Inventory.Room");            
            db.WriteDatabaseManifest();

            db.ReadDatabaseManifest();
            Assert.AreEqual(2, db.Tables.Length);

            db.WriteTable("Inventory.Car", "123");
            Assert.IsTrue(File.Exists(db.GetTableFilePath("Inventory.Car")));
            Assert.AreEqual("123", db.ReadTable("Inventory.Car"));

            db.WriteTable("Inventory.Car", "123456");
            Assert.IsTrue(File.Exists(db.GetTableFilePath("Inventory.Car")));
            Assert.AreEqual("123456", db.ReadTable("Inventory.Car"));
        }
    }
}
