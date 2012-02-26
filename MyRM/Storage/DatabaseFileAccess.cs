using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using TP;

namespace MyRM.Storage
{
    public class DatabaseFileAccess
    {
        public const int DefaultPageSize = 4096;
        private const int DefaultPageNumber = 1024;
        public const int DefaultPageHeaderSize = 64;
        private const int DataFileHeaderSize = 512;

        private readonly string _databaseName;
        private bool _isInitialized;
        private Dictionary<string, int> _tables = new Dictionary<string, int>();

        private static readonly object DatabaseLock = new object();

        public DatabaseFileAccess(string databaseName)
        {
            _databaseName = databaseName;
        }

        public string DatabaseName
        {
            get { return _databaseName; }
        }

        public string[] Tables
        {
            get
            {
                var list = new string[_tables.Keys.Count];
                _tables.Keys.CopyTo(list, 0);
                return list;
            }
        }

        public void Initialize(bool autoRecovery = true)
        {
            lock (DatabaseLock)
            {
                if (File.Exists(DatabaseManifestFileName))
                {
                    ReadDatabaseManifest();
                }
                else
                {
                    if (IsRecoverable())
                    {
                        if (autoRecovery)
                            RecoverDatabase();
                        else
                            throw new ApplicationException("The database " + DatabaseName + " requires recovery." +
                                                           DebuggingInfo);
                    }
                    BuildNewDatabase();
                }

                _isInitialized = true;
            }
        }

        public bool ContainsTable(string tableName)
        {
            return _tables.ContainsKey(tableName);
        }

        public void CreateTable(string tableName, int rowSize, int keySize = 36)
        {
            EnsureInitialized();

            lock (DatabaseLock)
            {
                if (!ContainsTable(tableName))
                {
                    _tables.Add(tableName, 0);
                    CreateTableFile(tableName, rowSize);
                    CreatePageTable(tableName, keySize);
                    WriteDatabaseManifest();
                }
            }
        }

        public void InsertRecord(Transaction tid, string tableName, string key, Row record)
        {
            //read active page tableName
            var pageTable = this.DiskReadPageTable(tableName);

            //modify page tableName
            if ((from k in pageTable.PageIndices where k.Key == key select k).Any())
                throw new ApplicationException("duplicate key");

            //find an empty page
            Page page = null;
            //TODO: fix this algorithm
            foreach(var item in pageTable.PageIndices)
            {
                if (item.Key[0] != 0)
                {
                    page = DiskReadPage(tableName, item.PageIndex, item.ActiveId);
                    if (page.NextFreeRowIndex <= page.RowsPerPage)
                    {
                        break;
                    }
                }
                page = null;
            }

            if (page == null)
                page = DiskReadPage(tableName, 0, 0);
            //fix this bug
            //if (page == null)
             //   throw new ApplicationException("out of space");

            var rowId = page.InsertRow(record);
            page.FileId = page.FileId == 0 ? 1 : 0;

            //update page table
            pageTable.InsertIndex(key, page.PageIndex, rowId, page.FileId);

            //write page into the shawdow
            DiskWritePage(tableName, page, page.FileId);

            //save page tableName file
            UpdateShadowIdsForPage(pageTable, page.PageIndex, page.FileId);
            DiskWritePageTable(tableName, pageTable);

            //TODO: NOT COMMIT HERE
            CommitPage(tid, tableName, page);
        }

        public void UpdateRecord(Transaction tid, string tableName, string key, Row record)
        {
            //read active page tableName
            var pageTable = this.DiskReadPageTable(tableName);

            //modify page tableName
            var index = (from item in pageTable.PageIndices where item.Key == key select item).SingleOrDefault();

            if (index == null)
                throw new ApplicationException("record not found");

            var page = DiskReadPage(tableName, index.PageIndex, index.ActiveId);

            page.UpdateRow(record, index.RowIndex);
            page.FileId = page.FileId == 0 ? 1 : 0;

            //write page into the shawdow
            DiskWritePage(tableName, page, page.FileId);

            //update page table index
            index.ShadowId = page.FileId;
            index.IsDirty = 1;

            //save page tableName file
            UpdateShadowIdsForPage(pageTable, page.PageIndex, page.FileId);
            DiskWritePageTable(tableName, pageTable);

            //TODO: NOT COMMIT HERE
            CommitPage(tid, tableName, page);
        }

        private void UpdateShadowIdsForPage(PageTable pageTable, int pageIndex, int shadowId)
        {
            foreach(var item in pageTable.PageIndices)
            {
                if (item.PageIndex == pageIndex)
                {
                    item.IsDirty = 1;
                    item.ShadowId = shadowId;
                }
            }
        }

        public void DeleteRecord(Transaction tid, string tableName, string key)
        {
            //read active page tableName
            var pageTable = this.DiskReadPageTable(tableName);

            //modify page tableName
            var index = (from item in pageTable.PageIndices where item.Key == key select item).SingleOrDefault();

            if (index == null)
                throw new ApplicationException("record not found");

            var page = DiskReadPage(tableName, index.PageIndex, index.ActiveId);

            page.UpdateRow(new Row(page.RowSize), index.RowIndex);
            page.FileId = page.FileId == 0 ? 1 : 0;

            //write page into the shawdow
            DiskWritePage(tableName, page, page.FileId);

            //update page table index 
            pageTable.RemoveIndex(index);

            //save page tableName file
            UpdateShadowIdsForPage(pageTable, page.PageIndex, page.FileId);
            DiskWritePageTable(tableName, pageTable);

            //TODO: NOT COMMIT HERE
            CommitPage(tid, tableName, page);
        }

        public Row ReadRecord(Transaction tid, string tableName, string key)
        {
            //get active page table based on the shadow id
            var pt = DiskReadPageTable(tableName);

            var index = (from item in pt.PageIndices where item.Key == key select item).SingleOrDefault();
            
            if (index == null)
                throw new ApplicationException("record not found");

            var page = DiskReadPage(tableName, index.PageIndex, index.ActiveId);
            return page.Row(index.RowIndex);
        }

        /// <summary>
        /// Write page into the shadow and create an updated page tableName for it
        /// </summary>
        public void WritePage(Transaction tid, string tableName, string key, Page page)
        {
            if (page.TableName != tableName)
                throw new ArgumentException("page deson't below to the tableName");

            //read active page tableName
            var pageTable = this.DiskReadPageTable(tableName);

            //modify page tableName
            var pageTableIndex = (from k in pageTable.PageIndices where k.Key == key select k).Single();
            pageTableIndex.ShadowId = pageTableIndex.ActiveId == 0 ? 1 : 0;

            if (pageTableIndex.PageIndex != page.PageIndex)
                throw new ApplicationException("index doesn't match the page");

            //save page into shadow
            DiskWritePage(tableName, page, pageTableIndex.ShadowId);

            //save page tableName file
            DiskWritePageTable(tableName, pageTable);
        }

        public Page ReadPage(string tableName, string key)
        {
            //read active page tableName
            var pageTable = this.DiskReadPageTable(tableName);
            var pageTableIndex = (from k in pageTable.PageIndices where k.Key == key select k).SingleOrDefault();

            if (pageTableIndex == null)
                throw new RecordNotFoundException(key);

            return DiskReadPage(tableName, pageTableIndex.PageIndex, pageTableIndex.ActiveId);
        }

        public void CommitPage(Transaction tid, string tableName, Page page)
        {
            //Copy shadowIds into ActiveIds and save
            //this.DiskWritePageTable(tableName );

            var pageTable = this.DiskReadPageTable(tableName);
            var pageIndex = page.PageIndex;

            foreach (var item in pageTable.PageIndices)
            {
                if (item.PageIndex == pageIndex)
                {
                    if (item.IsDirty == 1)
                    {
                        item.ActiveId = item.ShadowId;
                        item.ShadowId = 0;
                        item.IsDirty = 0;
                    }
                }
            }

            DiskWritePageTable(tableName, pageTable);
        }

        private Page DiskReadPage(string tableName, int pageIndex, int fileId)
        {
            string filename = DatabaseName + "." + tableName + ".data." + fileId;
            DataFileHeader tableDataFileHeader = ReadDataFileHeader(tableName, fileId);

            if (pageIndex >= tableDataFileHeader.PageNum)
                throw new ApplicationException("pageIndex");

            var p = new Page
            {
                FileId = fileId,
                PageIndex = pageIndex,
                PageSize = tableDataFileHeader.PageSize,
                TableName = tableName,
                DataFileName = filename,
            };

            using (var fileStream = new FileStream(filename, FileMode.Open))
            {
                fileStream.Seek(DataFileHeaderSize + pageIndex * p.PageSize, SeekOrigin.Begin);
                var pageData = new byte[p.PageSize];
                fileStream.Read(pageData, 0, p.PageSize);
                p.Decode(pageData);
            }

            return p;
        }

        private void DiskWritePage(string tableName, Page page, int fileId)
        {
            string filename = DatabaseName + "." + page.TableName + ".data." + fileId;
            DataFileHeader tableDataFileHeader = ReadDataFileHeader(tableName, page.FileId);

            if (page.PageIndex >= tableDataFileHeader.PageNum)
                throw new ApplicationException("pageIndex");

            page.FileId = fileId;
            using (var fileStream = new FileStream(filename, FileMode.Open))
            {
                fileStream.Seek(DataFileHeaderSize + page.PageIndex * page.PageSize, SeekOrigin.Begin);
                byte[] pageData = page.Encode();
                fileStream.Write(pageData, 0, page.PageSize);
            }
        }

        private PageTable DiskReadPageTable(string tableName)
        {
            string filename = DatabaseName + "." + tableName + ".index";

            using (var fileStream = new FileStream(filename, FileMode.OpenOrCreate))
            {
                var buffer = new byte[DefaultPageSize];
                fileStream.Seek(0, SeekOrigin.Begin);
                fileStream.Read(buffer, 0, buffer.Length);
                return new PageTable(buffer);
            }
        }

        private void DiskWritePageTable(string tableName, PageTable pt)
        {
            string filename = DatabaseName + "." + tableName + ".index";

            using (var fileStream = new FileStream(filename, FileMode.OpenOrCreate))
            {
                var pageTable = pt.GetBytes();
                fileStream.Seek(0, SeekOrigin.Begin);
                fileStream.Write(pageTable, 0, pageTable.Length);
                fileStream.Flush();
            }
        }

        private void CreatePageTable(string tableName, int keySize)
        {
            string filename = DatabaseName + "." + tableName + ".index";

            if (File.Exists(filename))
                return;

            var pt = new PageTable(new byte[DefaultPageSize], DefaultPageSize, keySize);
            DiskWritePageTable(tableName, pt);
        }

        private void BuildNewDatabase()
        {
            WriteDatabaseManifest();
        }

        private void EnsureInitialized()
        {
            if (!_isInitialized)
                throw new ApplicationException("The database " + DatabaseName + " is not initialized." + DebuggingInfo);
        }

        private int GetProcessId()
        {
            Process currentProcess = Process.GetCurrentProcess();
            return currentProcess.Id;
        }

        private string DatabaseManifestFileName
        {
            get { return DatabaseName + @".manifest"; }
        }

        private string DebuggingInfo
        {
            get { return " PID=" + GetProcessId(); }
        }

        private void ReadDatabaseManifest()
        {
            if (!File.Exists(DatabaseManifestFileName))
            {
                throw new ApplicationException("The manifest of the database " + DatabaseName + " is missing." +
                                               DebuggingInfo);
            }

            XDocument xdoc = XDocument.Load(DatabaseManifestFileName);
            Dictionary<string, int> tables = xdoc.Descendants("Table").ToDictionary(e => e.Attribute("Name").Value,
                                                                                       e => Int32.Parse(e.Attribute("FileId").Value));
            _tables = tables;
        }

        private void WriteDatabaseManifest()
        {
            var xdoc = new XDocument();
            var root = new XElement("Tables");
            xdoc.Add(root);

            foreach (var t in _tables)
            {
                var e = new XElement("Table", new XAttribute("Name", t.Key), new XAttribute("FileId", t.Value));
                root.Add(e);
            }
            xdoc.Save(DatabaseManifestFileName + ".tmp");

            if (File.Exists(DatabaseManifestFileName))
            {
                File.Replace(DatabaseManifestFileName + ".tmp", DatabaseManifestFileName,
                             DatabaseManifestFileName + ".bak");
                File.Delete(DatabaseManifestFileName + ".bak");
            }
            else
            {
                File.Move(DatabaseManifestFileName + ".tmp", DatabaseManifestFileName);
            }
        }

        private void RecoverDatabase()
        {
            if (File.Exists(DatabaseManifestFileName + ".tmp"))
                File.Move(DatabaseManifestFileName + ".tmp", DatabaseManifestFileName);
            else if (File.Exists(DatabaseManifestFileName + ".bak"))
                File.Move(DatabaseManifestFileName + ".bak", DatabaseManifestFileName);
        }

        private bool IsRecoverable()
        {
            return (File.Exists(DatabaseManifestFileName + ".tmp") ||
                    File.Exists(DatabaseManifestFileName + ".bak"));
        }

        private void CreateTableFile(string tableName, int rowSize, int pageSize = DefaultPageSize, int pageNum = DefaultPageNumber)
        {
            for (int j = 0; j <= 1; j++)
            {
                string filename = DatabaseName + "." + tableName + ".data." + j;

                if (File.Exists(filename))
                    continue;

                var databaseHeader = new DataFileHeader
                {
                    PageNum = pageNum,
                    PageSize = pageSize,
                    Version = 1
                };

                var databaseHeaderBuffer = new byte[DataFileHeaderSize];
                byte[] binaryHeader = SerializationHelper.ObjectToByteArray(databaseHeader);
                Array.Copy(binaryHeader, databaseHeaderBuffer, binaryHeader.Length);

                //Mark Database data file header
                using (var fileStream = new FileStream(filename, FileMode.OpenOrCreate))
                {
                    fileStream.Seek(0, SeekOrigin.Begin);
                    fileStream.Write(databaseHeaderBuffer, 0, DataFileHeaderSize);
                    fileStream.Flush();
                }

                int rowsPerPage = (pageSize - DefaultPageHeaderSize) / rowSize;

                using (var fileStream = new FileStream(filename, FileMode.Open))
                {
                    for (int i = 0; i < pageNum; i++)
                    {
                        fileStream.Seek(DataFileHeaderSize + i * pageSize, SeekOrigin.Begin);

                        //Mark Page Header - {P}{.}{Page Index}{RowSize}{rowsPerPage}{NextFreeRowIndex} 
                        var encoder = new UTF8Encoding();

                        byte[] byteArray = encoder.GetBytes("P.");
                        fileStream.Write(byteArray, 0, byteArray.Length);

                        byteArray = BitConverter.GetBytes(i);
                        fileStream.Write(byteArray, 0, byteArray.Length);

                        byteArray = BitConverter.GetBytes(rowSize);
                        fileStream.Write(byteArray, 0, byteArray.Length);

                        byteArray = BitConverter.GetBytes(rowsPerPage);
                        fileStream.Write(byteArray, 0, byteArray.Length);

                        byteArray = BitConverter.GetBytes(0);
                        fileStream.Write(byteArray, 0, byteArray.Length);

                        fileStream.Flush();
                    }
                }
            }
        }

        private DataFileHeader ReadDataFileHeader(string tableName, int fileId)
        {
            string filename = DatabaseName + "." + tableName + ".data." + fileId;
            using (var fileStream = new FileStream(filename, FileMode.Open))
            {
                var buffer = new byte[DataFileHeaderSize];
                fileStream.Seek(0, SeekOrigin.Begin);
                fileStream.Read(buffer, 0, DataFileHeaderSize);
                object obj = SerializationHelper.ByteArrayToObject(buffer);
                return (DataFileHeader)obj;
            }
        }
    }
}