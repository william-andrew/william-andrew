using System;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MyRM.Storage;
using TP;

namespace TestProject
{
    [TestClass]
    public class DatabaseFileAccessTest
    {
        readonly UTF8Encoding _encoder = new UTF8Encoding();

        [TestMethod]
        public void PageReadWriteTest()
        {
            var target = new DatabaseFileAccess_Accessor("AAA");
            target.CreateTableFile("Car", 64, 1024, 1024);

            var a = target.ReadDataFileHeader("Car", 0);
            var b = target.ReadDataFileHeader("Car", 1);

            Page p1 = target.DiskReadPage("Car", 0, 0);
            Page p2 = target.DiskReadPage("Car", 12, 1);

            var encoder = new UTF8Encoding();
            var byteArray = encoder.GetBytes("Page Data");

            var row = new Row(p1.RowSize);
            Array.Copy(byteArray, row.Data, byteArray.Length);
            p1.UpdateRow(row, 8);

            target.DiskWritePage("Car", p1, p1.FileId);

            p1 = target.DiskReadPage("Car", 0, 0);
            Assert.AreEqual("Page Data", encoder.GetString(p1.Row(8).Data, 0, byteArray.Length));

            p2.UpdateRow(row, p2.RowsPerPage - 1);
            target.DiskWritePage("Car", p2, p2.FileId);

            var p3 = target.DiskReadPage("Car", 12, 1);
            Assert.AreEqual("Page Data", encoder.GetString(p3.Row(p3.RowsPerPage - 1).Data, 0, byteArray.Length));

            target.DiskWritePage("Car", p3, p3.FileId);
            var p4 = target.DiskReadPage("Car", 12, 1);

            Assert.AreEqual(p3.NextFreeRowIndex, p4.NextFreeRowIndex);
            Assert.AreEqual(p3.PageIndex, p4.PageIndex);
            Assert.AreEqual(p3.PageSize, p4.PageSize);
            Assert.AreEqual(p3.RowSize, p4.RowSize);
            Assert.AreEqual(p3.RowsPerPage, p4.RowsPerPage);
            Assert.AreEqual(p3.FileId, p4.FileId);
        }

        [TestMethod]
        public void ReadPageTable()
        {
            var db = new DatabaseFileAccess_Accessor("CCC");
            db.Initialize();
            var key = Guid.NewGuid().ToString();

            const int pageIndex = 100;
            const int rowIndex = 99;
            const int fileId = 1;

            db.CreateTable("Inventory.Car", 100);
            PageTable pt = db.DiskReadPageTable("Inventory.Car");
            pt.PageIndices[0].Key = key;
            pt.PageIndices[0].PageIndex = pageIndex;
            pt.PageIndices[0].RowIndex = rowIndex;
            pt.PageIndices[0].ActiveId = fileId;

            db.DiskWritePageTable("Inventory.Car", pt);

            pt = db.DiskReadPageTable("Inventory.Car");
            Assert.AreEqual(pt.PageIndices[0].Key, key);
            Assert.AreEqual(pt.PageIndices[0].PageIndex, pageIndex);
            Assert.AreEqual(pt.PageIndices[0].RowIndex, rowIndex);
            Assert.AreEqual(pt.PageIndices[0].ActiveId, fileId);
        }

        [TestMethod]
        [ExpectedException(typeof(RecordNotFoundException))]
        public void ReadPageRecordNotFound()
        {
            var db = new DatabaseFileAccess_Accessor("DDD");
            db.Initialize();
            db.CreateTable("Inventory.Car", 100);
            db.ReadPage("Inventory.Car", "key");
        }

        [TestMethod]
        public void InsertUpdateRecord()
        {
            var db = new DatabaseFileAccess_Accessor("EEE");
            db.Initialize();
            var key1 = new string('1', 36);
            var key2 = new string('2', 36);
            var key3 = new string('3', 36);
            var key4 = new string('4', 36);

            var encoder = new UTF8Encoding();

            db.CreateTable("Inventory.Car", 100);
            db.InsertRecord(null, "Inventory.Car", key1, new Row
            {
                Data = encoder.GetBytes("Seattle, 123")
            });

            var row = db.ReadRecord(null, "Inventory.Car", key1);
            Assert.AreEqual("Seattle, 123", row.DataString);

            db.InsertRecord(null, "Inventory.Car", key2, new Row
            {
                Data = encoder.GetBytes("New York, 456")
            });

            row = db.ReadRecord(null, "Inventory.Car", key1);
            Assert.AreEqual("Seattle, 123", row.DataString);

            row = db.ReadRecord(null, "Inventory.Car", key2);
            Assert.AreEqual("New York, 456", row.DataString);

            db.InsertRecord(null, "Inventory.Car", key3, new Row
            {
                Data = encoder.GetBytes("London, 789")
            });

            row = db.ReadRecord(null, "Inventory.Car", key1);
            Assert.AreEqual("Seattle, 123", row.DataString);

            row = db.ReadRecord(null, "Inventory.Car", key2);
            Assert.AreEqual("New York, 456", row.DataString);

            row = db.ReadRecord(null, "Inventory.Car", key3);
            Assert.AreEqual("London, 789", row.DataString);

            db.InsertRecord(null, "Inventory.Car", key4, new Row
            {
                Data = encoder.GetBytes("Phoenix, 012")
            });

            row = db.ReadRecord(null, "Inventory.Car", key1);
            Assert.AreEqual("Seattle, 123", row.DataString);

            row = db.ReadRecord(null, "Inventory.Car", key2);
            Assert.AreEqual("New York, 456", row.DataString);

            row = db.ReadRecord(null, "Inventory.Car", key3);
            Assert.AreEqual("London, 789", row.DataString);

            row = db.ReadRecord(null, "Inventory.Car", key4);
            Assert.AreEqual("Phoenix, 012", row.DataString);

            // Update
            db.UpdateRecord(null, "Inventory.Car", key1, new Row
            {
                Data = encoder.GetBytes("Seattle, key1")
            });

            row = db.ReadRecord(null, "Inventory.Car", key1);
            Assert.AreEqual("Seattle, key1", row.DataString);

            db.UpdateRecord(null, "Inventory.Car", key2, new Row
            {
                Data = encoder.GetBytes("New York, key2")
            });

            row = db.ReadRecord(null, "Inventory.Car", key2);
            Assert.AreEqual("New York, key2", row.DataString);

            db.UpdateRecord(null, "Inventory.Car", key3, new Row
            {
                Data = encoder.GetBytes("London, key3")
            });

            row = db.ReadRecord(null, "Inventory.Car", key3);
            Assert.AreEqual("London, key3", row.DataString);

            db.UpdateRecord(null, "Inventory.Car", key4, new Row
            {
                Data = encoder.GetBytes("Phoenix, key4")
            });

            row = db.ReadRecord(null, "Inventory.Car", key4);
            Assert.AreEqual("Phoenix, key4", row.DataString);

            db.DeleteRecord(null, "Inventory.Car", key4);

            try
            {
                row = db.ReadRecord(null, "Inventory.Car", key4);
                Assert.Fail();
            }
            catch(ApplicationException e)
            {
                Assert.AreEqual("record not found", e.Message);
            }

            row = db.ReadRecord(null, "Inventory.Car", key1);
            Assert.AreEqual("Seattle, key1", row.DataString);

            row = db.ReadRecord(null, "Inventory.Car", key2);
            Assert.AreEqual("New York, key2", row.DataString);

            row = db.ReadRecord(null, "Inventory.Car", key3);
            Assert.AreEqual("London, key3", row.DataString);

            db.InsertRecord(null, "Inventory.Car", key4, new Row
            {
                Data = encoder.GetBytes("Oregon, 345")
            });

            row = db.ReadRecord(null, "Inventory.Car", key1);
            Assert.AreEqual("Seattle, key1", row.DataString);

            row = db.ReadRecord(null, "Inventory.Car", key2);
            Assert.AreEqual("New York, key2", row.DataString);

            row = db.ReadRecord(null, "Inventory.Car", key3);
            Assert.AreEqual("London, key3", row.DataString);

            row = db.ReadRecord(null, "Inventory.Car", key4);
            Assert.AreEqual("Oregon, 345", row.DataString);
        } 
    }
}
