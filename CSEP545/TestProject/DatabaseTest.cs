using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MyRM;

namespace TestProject
{
    public class MockDatabase:IDatabase
    {

        public void CreateTable(string tableName)
        {
        }

        public string DatabaseName
        {
            get { return "test"; }
        }

        public void Initialize(bool autoRecovery = true)
        {
           
        }

        public bool ContainsTable(string tableName)
        {
            return true;
        }

        public string ReadTable(string tableName)
        {
            return string.Empty;
        }

        public void WriteTable(string tableName, string data)
        {
            
        }
    }

   [TestClass]
    public class DatabaseTest
    {
        [TestMethod]
        public void GetDatabaseManifestTest()
        {
            var db = new Database("CarDatabase");
            db.CreateTable("Inventory.Car");
            db.CreateTable("Inventory.Room");            

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
