using System.IO;
using System.Text;
using MyRM;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using MyRM.Storage;

namespace TestProject
{
    [TestClass()]
    public class DatabaseFileAccessTest
    {
        [TestMethod()]
        public void PageReadWriteTest()
        {
            var target = new DatabaseFileAccess("foo");
            target.CreateTableFile("Car", 64, 1024, 1024);

            var a = target.ReadDataFileHeader("Car", 0);
            var b = target.ReadDataFileHeader("Car", 1);

            Page p1 = target.ReadPage("Car", 0, 0);
            Page p2 = target.ReadPage("Car", 1, 12);

            var encoder = new UTF8Encoding();
            var byteArray = encoder.GetBytes("Page Data");

            var row = new Row(p1.RowSize);
            Array.Copy(byteArray, row.Data, byteArray.Length);
            p1.UpdateRow(row, 8);

            target.WritePage("Car", p1, p1.ShadowId);

            p1 = target.ReadPage("Car", 0, 0);
            Assert.AreEqual("Page Data", encoder.GetString(p1.Row(8).Data, 0, byteArray.Length));

            p2.UpdateRow(row, p2.RowsPerPage - 1);
            target.WritePage("Car", p2, p2.ShadowId);

            var p3 = target.ReadPage("Car", 1, 12);
            Assert.AreEqual("Page Data", encoder.GetString(p3.Row(p3.RowsPerPage - 1).Data, 0, byteArray.Length));

            target.WritePage("Car", p3, p3.ShadowId);
            var p4 = target.ReadPage("Car", 1, 12);

            Assert.AreEqual(p3.NextFreeRowIndex, p4.NextFreeRowIndex);
            Assert.AreEqual(p3.PageIndex, p4.PageIndex);
            Assert.AreEqual(p3.PageSize, p4.PageSize);
            Assert.AreEqual(p3.RowSize, p4.RowSize);
            Assert.AreEqual(p3.RowsPerPage, p4.RowsPerPage);
            Assert.AreEqual(p3.ShadowId, p4.ShadowId);
        }

        [TestMethod]
        public void GetDatabaseManifestTest()
        {
            var db = new DatabaseFileAccess("Car");
            db.Initialize();

            db.CreateTable("Inventory.Car", 100);
            db.CreateTable("Inventory.Room", 64);

            db.ReadDatabaseManifest();
            Assert.AreEqual(2, db.Tables.Length);

            var encoder = new UTF8Encoding();
            var byteArray = encoder.GetBytes("Page Data");

            var page0 = db.ReadPage("Inventory.Car", 1);
            var row = new Row(page0.RowSize);
            Array.Copy(byteArray, row.Data, byteArray.Length);
            page0.UpdateRow(row, 8);
            db.WritePage("Inventory.Car", page0);

            var page1 = db.ReadPage("Inventory.Car", 1, 1);
            Assert.AreEqual("Page Data", encoder.GetString(page1.Row(8).Data, 0, byteArray.Length));

            db.CommitPage("Inventory.Car", page0);

            var page2 = db.ReadPage("Inventory.Car", 1);
            Assert.AreEqual("Page Data", encoder.GetString(page2.Row(8).Data, 0, byteArray.Length));

        }
    }
}
