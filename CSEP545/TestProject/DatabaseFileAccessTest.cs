﻿using System;
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

            Page p1 = target.ReadPage("Car", 0, 0);
            Page p2 = target.ReadPage("Car", 12, 1);

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

            var p3 = target.ReadPage("Car", 12, 1);
            Assert.AreEqual("Page Data", encoder.GetString(p3.Row(p3.RowsPerPage - 1).Data, 0, byteArray.Length));

            target.WritePage("Car", p3, p3.ShadowId);
            var p4 = target.ReadPage("Car", 12, 1);

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
            var db = new DatabaseFileAccess_Accessor("BBB");
            db.Initialize();

            const string currentTable = "GetDatabaseManifestTest";

            db.CreateTable("Inventory.Room", 64);
            db.CreateTable(currentTable, 100);

            db.ReadDatabaseManifest();
            Assert.AreEqual(2, db.Tables.Length);

            var page = db.ReadPage(currentTable, 0, 0);
            var row = new Row(page.RowSize);
            var byteArray = _encoder.GetBytes("Page Data");
            Array.Copy(byteArray, row.Data, byteArray.Length);
            page.UpdateRow(row, 8); //p0r8

            var pt = db.ReadPageTable(currentTable, 0);
            var key = Guid.NewGuid().ToString();
            pt.PageIndices[5] = new PageIndexEntry
                                    {
                                        Key = key,
                                        PageIndex = page.PageIndex,
                                        RowIndex = 1,
                                        ShadowId = page.ShadowId
                                    };

            db.WritePageTable(currentTable, pt, 0);

            var tid = new Transaction();
            var activeId = db.WritePage(tid, currentTable, key, page); //write into shadow

            var page1 = db.ReadPage(currentTable, 0, 1);
            Assert.AreEqual("Page Data", _encoder.GetString(page1.Row(8).Data, 0, byteArray.Length));

            db.CommitPage(tid, currentTable, page, activeId);

            var page2 = db.ReadPage(currentTable, key);
            Assert.AreEqual("Page Data", _encoder.GetString(page2.Row(8).Data, 0, byteArray.Length));
        }

        [TestMethod]
        public void ReadPageTable()
        {
            var db = new DatabaseFileAccess_Accessor("CCC");
            db.Initialize();
            var key = Guid.NewGuid().ToString();

            const int pageIndex = 100;
            const int rowIndex = 99;
            const int shadowId = 1;

            db.CreateTable("Inventory.Car", 100);
            PageTable pt = db.ReadPageTable("Inventory.Car", 0);
            pt.PageIndices[0].Key = key;
            pt.PageIndices[0].PageIndex = pageIndex;
            pt.PageIndices[0].RowIndex = rowIndex;
            pt.PageIndices[0].ShadowId = shadowId;

            db.WritePageTable("Inventory.Car", pt, 1);

            pt = db.ReadPageTable("Inventory.Car", 1);
            Assert.AreEqual(pt.PageIndices[0].Key, key);
            Assert.AreEqual(pt.PageIndices[0].PageIndex, pageIndex);
            Assert.AreEqual(pt.PageIndices[0].RowIndex, rowIndex);
            Assert.AreEqual(pt.PageIndices[0].ShadowId, shadowId);
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
    }
}
