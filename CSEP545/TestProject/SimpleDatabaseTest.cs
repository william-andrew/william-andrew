using System;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MyRM.Storage;
using TP;

namespace TestProject
{
    [TestClass]
    public class SimpleDatabaseTest
    {
        readonly UTF8Encoding _encoder = new UTF8Encoding();

        [TestCleanup]
        public void Cleanup()
        {
            CommonFunction.CleanUpAll();
        }

        [TestMethod]
        public void RowTest()
        {
            var row = new Row(100) {Data = _encoder.GetBytes("row data read/write")};
            Assert.AreEqual("row data read/write", row.DataString);

            row.Data = _encoder.GetBytes("abc");
            Assert.AreEqual("abc", row.DataString);
        }

        [TestMethod]
        public void IndexTest()
        {
            var target = new SimpleDatabase_Accessor("RRR", false, true);
            target.CreateTable("Car", 96, 36);
            PageTable pt = target.DiskReadPageTable("Car");
            pt.InsertIndex("aa", 1, 1, 1, Guid.Empty);
            pt.InsertIndex("bb", 1, 1, 1, Guid.Empty);
            var r = new RecordIndexEntry
                                     {
                                         Key = "aa"
                                     };
            pt.WipeoutIndex(r, Guid.Empty);
            Assert.IsFalse((from i in pt.RecordIndices where i.Key == "aa" select i).Any());
            pt.InsertIndex("aa", 1, 1, 1, Guid.Empty);
            Assert.IsTrue(pt.RecordIndices[0].Key == "aa");
        }

        [TestMethod]
        public void PageReadWriteTest()
        {
            var target = new SimpleDatabase_Accessor("AAA", false);
            target.CreateTableFile("Car", 64, 1024, 128);

            var a = target.ReadDataFileHeader("Car", 0);
            var b = target.ReadDataFileHeader("Car", 1);

            Page p1 = target.DiskReadPage("Car", 0, 0);
            Page p2 = target.DiskReadPage("Car", 12, 1);

            var encoder = new UTF8Encoding();
            var byteArray = encoder.GetBytes("Page Data");

            var row = p1.CreateEmptyRow();
            Array.Copy(byteArray, row.Data, byteArray.Length);
            p1.UpdateRow(row, 8);

            target.DiskWritePage("Car", p1, p1.FileId);

            p1 = target.DiskReadPage("Car", 0, 0);
            Assert.AreEqual("Page Data", encoder.GetString(p1.GetRow(8).Data, 0, byteArray.Length));

            p2.UpdateRow(row, p2.RowsPerPage - 1);
            target.DiskWritePage("Car", p2, p2.FileId);

            var p3 = target.DiskReadPage("Car", 12, 1);
            Assert.AreEqual("Page Data", encoder.GetString(p3.GetRow(p3.RowsPerPage - 1).Data, 0, byteArray.Length));

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
            var db = new SimpleDatabase_Accessor("CCC", false);
            db.Initialize();
            var key = Guid.NewGuid().ToString();

            const int pageIndex = 100;
            const int rowIndex = 99;
            const int fileId = 1;

            db.CreateTable("Inventory.Car", 100);
            PageTable pt = db.DiskReadPageTable("Inventory.Car");
            pt.RecordIndices[0].Key = key;
            pt.RecordIndices[0].PageIndex = pageIndex;
            pt.RecordIndices[0].RowIndex = rowIndex;
            pt.RecordIndices[0].ActiveFileId = fileId;

            db.DiskWritePageTable("Inventory.Car", pt);

            pt = db.DiskReadPageTable("Inventory.Car");
            Assert.AreEqual(pt.RecordIndices[0].Key, key);
            Assert.AreEqual(pt.RecordIndices[0].PageIndex, pageIndex);
            Assert.AreEqual(pt.RecordIndices[0].RowIndex, rowIndex);
            Assert.AreEqual(pt.RecordIndices[0].ActiveFileId, fileId);
        }

        [TestMethod]
        [ExpectedException(typeof(RecordNotFoundException))]
        public void ReadPageRecordNotFound()
        {
            var db = new SimpleDatabase_Accessor("DDD", true);
            db.Initialize();
            db.CreateTable("Inventory.Car", 100);
            db.ReadPage("Inventory.Car", "key");
        }

        [TestMethod]
        public void ReadShouldIgnoreUncommittedFromOtherTransactions()
        {
            var db = new SimpleDatabase_Accessor("2PC0", true);
            db.Initialize();
            const int rowSize = 100;
            var key1 = new string('1', 36);
            var key2 = new string('2', 36);

            var encoder = new UTF8Encoding();
            var tid = new Transaction();

            db.CreateTable("Inventory.Car", rowSize);
            db.InsertRecord(tid, "Inventory.Car", key1, new Row(rowSize)
                                                            {
                                                                Data = encoder.GetBytes("Seattle, 123")
                                                            });

            db.InsertRecord(tid, "Inventory.Car", key2, new Row(rowSize)
                                                            {
                                                                Data = encoder.GetBytes("New York, 456")
                                                            });

            var rows = db.ReadAllRecords(new Transaction(), "Inventory.Car");
            Assert.AreEqual(0, rows.Keys.Count);
        }

        [TestMethod]
        public void ReadShouldReadUncommittedRecordFromSameTransaction()
        {
            var db = new SimpleDatabase_Accessor("2PC1", true);
            db.Initialize();
            const int rowSize = 100;
            var key1 = new string('1', 36);
            var key2 = new string('2', 36);

            var encoder = new UTF8Encoding();
            var tid = new Transaction();

            db.CreateTable("Inventory.Car", rowSize);
            db.InsertRecord(tid, "Inventory.Car", key1, new Row(rowSize)
                                                            {
                                                                Data = encoder.GetBytes("Seattle, 123")
                                                            });

            db.InsertRecord(tid, "Inventory.Car", key2, new Row(rowSize)
                                                            {
                                                                Data = encoder.GetBytes("New York, 456")
                                                            });

            var rows = db.ReadAllRecords(tid, "Inventory.Car");
            Assert.AreEqual(2, rows.Keys.Count);
        }

        [TestMethod]
        public void ReadShouldReadUncommittedDeleteFromOtherTransaction()
        {
            var db = new SimpleDatabase_Accessor("2PC2", true);
            db.Initialize();
            const int rowSize = 100;
            var key1 = new string('1', 36);
            var key2 = new string('2', 36);

            var encoder = new UTF8Encoding();
            var tid = new Transaction();

            db.CreateTable("Inventory.Car", rowSize);
            db.InsertRecord(tid, "Inventory.Car", key1, new Row(rowSize)
                                                            {
                                                                Data = encoder.GetBytes("Seattle, 123")
                                                            });

            db.InsertRecord(tid, "Inventory.Car", key2, new Row(rowSize)
                                                            {
                                                                Data = encoder.GetBytes("New York, 456")
                                                            });

            db.Prepare(tid);
            db.Commit(tid);

            var rows = db.ReadAllRecords(new Transaction(), "Inventory.Car");
            Assert.AreEqual(2, rows.Keys.Count);

            var tid2 = new Transaction();
            db.DeleteRecord(tid2, "Inventory.Car", key1);
            rows = db.ReadAllRecords(new Transaction(), "Inventory.Car");
            Assert.AreEqual(2, rows.Keys.Count);

            rows = db.ReadAllRecords(tid2, "Inventory.Car");
            Assert.AreEqual(1, rows.Keys.Count);

            db.Prepare(tid2);
            db.Commit(tid2);

            rows = db.ReadAllRecords(new Transaction(), "Inventory.Car");
            Assert.AreEqual(1, rows.Keys.Count);
        }

        [TestMethod]
        public void InsertUpdateUpdateUpdateTest()
        {
            var db = new SimpleDatabase_Accessor("2PC3", true);
            db.Initialize();
            const int rowSize = 100;
            var key1 = new string('1', 36);

            var encoder = new UTF8Encoding();
            var tid = new Transaction();

            db.CreateTable("Inventory.Car", rowSize);
            db.InsertRecord(tid, "Inventory.Car", key1, new Row(rowSize)
            {
                Data = encoder.GetBytes("AAA")
            });

            db.UpdateRecord(tid, "Inventory.Car", key1, new Row(rowSize)
            {
                Data = encoder.GetBytes("AAABBB")
            });

            db.UpdateRecord(tid, "Inventory.Car", key1, new Row(rowSize)
            {
                Data = encoder.GetBytes("AAABBBCCC")
            });

            var rows = db.ReadAllRecords(tid, "Inventory.Car");
            Assert.AreEqual(1, rows.Keys.Count);

            rows = db.ReadAllRecords(new Transaction(), "Inventory.Car");
            Assert.AreEqual(0, rows.Keys.Count);

            db.Prepare(tid);
            db.Commit(tid);

            rows = db.ReadAllRecords(new Transaction(), "Inventory.Car");
            Assert.AreEqual(1, rows.Keys.Count);

            Assert.AreEqual("AAABBBCCC", rows[key1].DataString);
        }

        [TestMethod]
        public void InsertCommitUpdateUpdateUpdateTest()
        {
            var db = new SimpleDatabase_Accessor("2PC4", true);
            db.Initialize();
            const int rowSize = 100;
            var key1 = new string('1', 36);

            var encoder = new UTF8Encoding();
            var tid = new Transaction();

            db.CreateTable("Inventory.Car", rowSize);
            db.InsertRecord(tid, "Inventory.Car", key1, new Row(rowSize)
            {
                Data = encoder.GetBytes("AAA")
            });

            db.Prepare(tid);
            db.Commit(tid);

            var rows = db.ReadAllRecords(new Transaction(), "Inventory.Car");
            Assert.AreEqual(1, rows.Keys.Count);
            Assert.AreEqual("AAA", rows[key1].DataString);

            tid = new Transaction();
            db.UpdateRecord(tid, "Inventory.Car", key1, new Row(rowSize)
            {
                Data = encoder.GetBytes("AAABBB")
            });

            db.UpdateRecord(tid, "Inventory.Car", key1, new Row(rowSize)
            {
                Data = encoder.GetBytes("AAABBBCCC")
            });

            db.UpdateRecord(tid, "Inventory.Car", key1, new Row(rowSize)
            {
                Data = encoder.GetBytes("AAABBBCCCDDD")
            });

            db.Prepare(tid);
            db.Commit(tid);

            rows = db.ReadAllRecords(new Transaction(), "Inventory.Car");
            Assert.AreEqual(1, rows.Keys.Count);

            Assert.AreEqual("AAABBBCCCDDD", rows[key1].DataString);
        }

        [TestMethod]
        public void PrepareCommitAbortWithInvalidTransactionIdsTest()
        {
            var db = new SimpleDatabase_Accessor("2PC5", true);
            db.Initialize();
            const int rowSize = 100;

            var tid1 = new Transaction();
            var tid2 = new Transaction();
            var tid3 = new Transaction();

            db.CreateTable("Inventory.Car", rowSize);
            db.Prepare(tid1);
            db.Prepare(tid1);
            db.Commit(tid2);
            db.Commit(tid2);
            db.Abort(tid3);
            db.Abort(tid3);
        }

        [TestMethod]
        public void InsertCommitTest()
        {
            var db = new SimpleDatabase_Accessor("2PC6", true);
            db.Initialize();
            const int rowSize = 100;
            var key1 = new string('1', 36);

            var encoder = new UTF8Encoding();
            var tid = new Transaction();

            db.CreateTable("Inventory.Car", rowSize);
            db.InsertRecord(tid, "Inventory.Car", key1, new Row(rowSize)
            {
                Data = encoder.GetBytes("AAA")
            });

            db.Prepare(tid);
            db.Commit(tid);

            var rows = db.ReadAllRecords(new Transaction(), "Inventory.Car");
            Assert.AreEqual(1, rows.Keys.Count);

            Assert.AreEqual("AAA", rows[key1].DataString);
        }

        [TestMethod]
        public void InsertAbortTest()
        {
            var db = new SimpleDatabase_Accessor("2PC7", true);
            db.Initialize();
            const int rowSize = 100;
            var key1 = new string('1', 36);

            var encoder = new UTF8Encoding();
            var tid = new Transaction();

            db.CreateTable("Inventory.Car", rowSize);
            db.InsertRecord(tid, "Inventory.Car", key1, new Row(rowSize)
            {
                Data = encoder.GetBytes("AAA")
            });

            db.Prepare(tid);
            db.Abort(tid);

            var rows = db.ReadAllRecords(new Transaction(), "Inventory.Car");
            Assert.AreEqual(0, rows.Keys.Count);
        }

        [TestMethod]
        public void InsertDeleteCommitTest()
        {
            var db = new SimpleDatabase_Accessor("2PC8", true);
            db.Initialize();
            const int rowSize = 100;
            var key1 = new string('1', 36);

            var encoder = new UTF8Encoding();
            var tid = new Transaction();

            db.CreateTable("Inventory.Car", rowSize);
            db.InsertRecord(tid, "Inventory.Car", key1, new Row(rowSize)
            {
                Data = encoder.GetBytes("AAA")
            });
            db.DeleteRecord(tid, "Inventory.Car", key1);

            db.Prepare(tid);
            db.Commit(tid);

            var rows = db.ReadAllRecords(new Transaction(), "Inventory.Car");
            Assert.AreEqual(0, rows.Keys.Count);
        }

        [TestMethod]
        public void InsertUpdateRecord()
        {
            var db = new SimpleDatabase_Accessor("EEE", false);
            db.Initialize();
            const int rowSize = 100;
            var key1 = new string('1', 36);
            var key2 = new string('2', 36);
            var key3 = new string('3', 36);
            var key4 = new string('4', 36);

            var encoder = new UTF8Encoding();

            var tid = new Transaction();

            db.CreateTable("Inventory.Car", rowSize);
            db.InsertRecord(tid, "Inventory.Car", key1, new Row(rowSize)
            {
                Data = encoder.GetBytes("Seattle, 123")
            });

            var row = db.ReadRecord(tid, "Inventory.Car", key1);
            Assert.AreEqual("Seattle, 123", row.DataString);

            db.InsertRecord(tid, "Inventory.Car", key2, new Row(rowSize)
            {
                Data = encoder.GetBytes("New York, 456")
            });

            row = db.ReadRecord(tid, "Inventory.Car", key1);
            Assert.AreEqual("Seattle, 123", row.DataString);

            row = db.ReadRecord(tid, "Inventory.Car", key2);
            Assert.AreEqual("New York, 456", row.DataString);

            db.InsertRecord(tid, "Inventory.Car", key3, new Row(rowSize)
            {
                Data = encoder.GetBytes("London, 789")
            });

            row = db.ReadRecord(tid, "Inventory.Car", key1);
            Assert.AreEqual("Seattle, 123", row.DataString);

            row = db.ReadRecord(tid, "Inventory.Car", key2);
            Assert.AreEqual("New York, 456", row.DataString);

            row = db.ReadRecord(tid, "Inventory.Car", key3);
            Assert.AreEqual("London, 789", row.DataString);

            db.InsertRecord(tid, "Inventory.Car", key4, new Row(rowSize)
            {
                Data = encoder.GetBytes("Phoenix, 012")
            });

            row = db.ReadRecord(tid, "Inventory.Car", key1);
            Assert.AreEqual("Seattle, 123", row.DataString);

            row = db.ReadRecord(tid, "Inventory.Car", key2);
            Assert.AreEqual("New York, 456", row.DataString);

            row = db.ReadRecord(tid, "Inventory.Car", key3);
            Assert.AreEqual("London, 789", row.DataString);

            row = db.ReadRecord(tid, "Inventory.Car", key4);
            Assert.AreEqual("Phoenix, 012", row.DataString);

            // Update
            db.UpdateRecord(tid, "Inventory.Car", key1, new Row(rowSize)
            {
                Data = encoder.GetBytes("Seattle, key1")
            });

            row = db.ReadRecord(tid, "Inventory.Car", key1);
            Assert.AreEqual("Seattle, key1", row.DataString);

            db.UpdateRecord(tid, "Inventory.Car", key2, new Row(rowSize)
            {
                Data = encoder.GetBytes("New York, key2")
            });

            row = db.ReadRecord(tid, "Inventory.Car", key2);
            Assert.AreEqual("New York, key2", row.DataString);

            db.UpdateRecord(tid, "Inventory.Car", key3, new Row(rowSize)
            {
                Data = encoder.GetBytes("London, key3")
            });

            row = db.ReadRecord(tid, "Inventory.Car", key3);
            Assert.AreEqual("London, key3", row.DataString);

            db.UpdateRecord(tid, "Inventory.Car", key4, new Row(rowSize)
            {
                Data = encoder.GetBytes("Phoenix, key4")
            });

            row = db.ReadRecord(tid, "Inventory.Car", key4);
            Assert.AreEqual("Phoenix, key4", row.DataString);

            db.DeleteRecord(tid, "Inventory.Car", key4);

            RecordNotFoundException exception = null;
            try
            {
                row = db.ReadRecord(tid, "Inventory.Car", key4);
                Assert.Fail();
            }
            catch (RecordNotFoundException e)
            {
                exception = e;
            }
            Assert.IsNotNull(exception);

            row = db.ReadRecord(tid, "Inventory.Car", key1);
            Assert.AreEqual("Seattle, key1", row.DataString);

            row = db.ReadRecord(tid, "Inventory.Car", key2);
            Assert.AreEqual("New York, key2", row.DataString);

            row = db.ReadRecord(tid, "Inventory.Car", key3);
            Assert.AreEqual("London, key3", row.DataString);

            db.InsertRecord(tid, "Inventory.Car", key4, new Row(rowSize)
            {
                Data = encoder.GetBytes("Oregon, 345")
            });

            row = db.ReadRecord(tid, "Inventory.Car", key1);
            Assert.AreEqual("Seattle, key1", row.DataString);

            row = db.ReadRecord(tid, "Inventory.Car", key2);
            Assert.AreEqual("New York, key2", row.DataString);

            row = db.ReadRecord(tid, "Inventory.Car", key3);
            Assert.AreEqual("London, key3", row.DataString);

            row = db.ReadRecord(tid, "Inventory.Car", key4);
            Assert.AreEqual("Oregon, 345", row.DataString);
        }

        [TestMethod]
        public void PageAllocationTest()
        {
            var db = new SimpleDatabase_Accessor("MMM", false);
            db.Initialize();
            const int rowSize = 100;

            var tid = new Transaction();

            var encoder = new UTF8Encoding();
            db.CreateTable("Hotel", rowSize);

            var table = db.OpenTable("Hotel");

            var keys = new string[table.PageTable.RecordIndices.Length];
            var values = new string[table.PageTable.RecordIndices.Length];

            for (int index = 0; index < table.PageTable.RecordIndices.Length; index++)
            {
                keys[index] = Guid.NewGuid().ToString();
                values[index] = "VVVVVVVVVVVVVVVVVVVVV_" + index;
            }

            for (int index = 0; index < table.PageTable.RecordIndices.Length; index++)
            {
                db.InsertRecord(tid, table.Name, keys[index],
                                new Row(rowSize) { Data = encoder.GetBytes(values[index]) });
            }

            for (int index = 0; index < table.PageTable.RecordIndices.Length; index++)
            {
                var row = db.ReadRecord(tid, table.Name, keys[index]);
                Assert.AreEqual(values[index], row.DataString);
            }

            var rows = db.ReadAllRecords(null, "Hotel");
            Assert.AreEqual(table.PageTable.RecordIndices.Length, rows.Count);
        }
    }
}
