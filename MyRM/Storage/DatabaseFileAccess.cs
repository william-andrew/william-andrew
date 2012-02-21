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
            //shadow id of the index tableName
            var shadowId = _tables[tableName];

            //read active page tableName
            var pageTable = this.ReadPageTable(tableName, shadowId);

            //modify page tableName
            if ((from k in pageTable.PageIndices where k.Key == key select k).Any())
                throw new ApplicationException("duplicate key");

            //find an empty page
            Page page = null;
            //TODO: fix this algorithm
            foreach(var item in pageTable.PageIndices)
            {
                if (item.ShadowId != shadowId)
                    continue;

                page = ReadPage(tableName, item.PageIndex, item.ShadowId);
                if (page.NextFreeRowIndex <= page.RowsPerPage)
                {
                    break;
                }
                page = null;
            }

            if (page == null)
                throw new ApplicationException("out of space");

            var rowId = page.InsertRow(record);
            page.ShadowId = page.ShadowId == 0 ? 1 : 0;

            //update page table
            pageTable.InsertIndex(key, page.PageIndex, rowId, page.ShadowId);

            //write page into the shawdow
            WritePage(tableName, page, page.ShadowId);

            //save page tableName file
            WritePageTable(tableName, pageTable, shadowId == 0 ? 1 : 0);

            //TODO: NOT COMMIT HERE
            CommitPage(tid, tableName, page, shadowId == 0 ? 1 : 0);
        }

        public void UpdateRecord(Transaction tid, string tableName, string key, Row record)
        {
            //shadow id of the index tableName
            var shadowId = _tables[tableName];

            //read active page tableName
            var pageTable = this.ReadPageTable(tableName, shadowId);

            //modify page tableName
            var index = (from item in pageTable.PageIndices where item.Key == key select item).SingleOrDefault();

            if (index == null)
                throw new ApplicationException("record not found");

            var page = ReadPage(tableName, index.PageIndex, index.ShadowId);

            page.UpdateRow(record, index.RowIndex);
            page.ShadowId = page.ShadowId == 0 ? 1 : 0;

            //write page into the shawdow
            WritePage(tableName, page, page.ShadowId);

            //update page table index 
            index.ShadowId = page.ShadowId;

            //save page tableName file
            WritePageTable(tableName, pageTable, shadowId == 0 ? 1 : 0);

            //TODO: NOT COMMIT HERE
            CommitPage(tid, tableName, page, shadowId == 0 ? 1 : 0);
        }

        public void DeleteRecord(Transaction tid, string tableName, string key)
        {
            //shadow id of the index tableName
            var shadowId = _tables[tableName];

            //read active page tableName
            var pageTable = this.ReadPageTable(tableName, shadowId);

            //modify page tableName
            var index = (from item in pageTable.PageIndices where item.Key == key select item).SingleOrDefault();

            if (index == null)
                throw new ApplicationException("record not found");

            var page = ReadPage(tableName, index.PageIndex, index.ShadowId);

            page.UpdateRow(new Row(page.RowSize), index.RowIndex);
            page.ShadowId = page.ShadowId == 0 ? 1 : 0;

            //write page into the shawdow
            WritePage(tableName, page, page.ShadowId);

            //update page table index 
            pageTable.RemoveIndex(index);

            //save page tableName file
            WritePageTable(tableName, pageTable, shadowId == 0 ? 1 : 0);

            //TODO: NOT COMMIT HERE
            CommitPage(tid, tableName, page, shadowId == 0 ? 1 : 0);
        }

        public Row ReadRecord(Transaction tid, string tableName, string key)
        {
            //shadow id of the index tableName
            var shadowId = _tables[tableName];

            //get active page table based on the shadow id
            var pt = ReadPageTable(tableName, shadowId);

            var index = (from item in pt.PageIndices where item.Key == key select item).SingleOrDefault();
            
            if (index == null)
                throw new ApplicationException("record not found");

            var page = ReadPage(tableName, index.PageIndex, index.ShadowId);
            return page.Row(index.RowIndex);
        }

        public void Commit(Transaction tid)
        {
            // flush dirty pages 
            // remove the dirty pages in memory
            // forget transaction
        }

        /// <summary>
        /// Write page into the shadow and create an updated page tableName for it
        /// </summary>
        public int WritePage(Transaction tid, string tableName, string key, Page page)
        {
            if (page.TableName != tableName)
                throw new ArgumentException("page deson't below to the tableName");

            //shadow id of the index tableName
            var shadowId = _tables[tableName];

            //read active page tableName
            var pageTable = this.ReadPageTable(tableName, shadowId);

            //modify page tableName
            var pageTableIndex = (from k in pageTable.PageIndices where k.Key == key select k).Single();
            pageTableIndex.ShadowId = pageTableIndex.ShadowId == 0 ? 1 : 0;

            if (pageTableIndex.PageIndex != page.PageIndex)
                throw new ApplicationException("index doesn't match the page");

            //save page into shadow
            WritePage(tableName, page, pageTableIndex.ShadowId);

            //save page tableName file
            WritePageTable(tableName, pageTable, shadowId == 0 ? 1: 0);

            return pageTableIndex.ShadowId;
        }

        public Page ReadPage(string tableName, string key)
        {
            //shadow id of the index tableName
            var shadowId = _tables[tableName];

            //read active page tableName
            var pageTable = this.ReadPageTable(tableName, shadowId);
            var pageTableIndex = (from k in pageTable.PageIndices where k.Key == key select k).SingleOrDefault();

            if (pageTableIndex == null)
                throw new RecordNotFoundException(key);

            return ReadPage(tableName, pageTableIndex.PageIndex, pageTableIndex.ShadowId);
        }

        public void CommitPage(Transaction tid, string tableName, Page page, int shadowId)
        {
            if (!page.IsDirty)
                return;

            _tables[tableName] = shadowId;

            WriteDatabaseManifest();
        }

        private Page ReadPage(string tableName, int pageIndex, int shadowId)
        {
            string filename = tableName + ".data." + shadowId;
            DataFileHeader tableDataFileHeader = ReadDataFileHeader(tableName, shadowId);

            if (pageIndex >= tableDataFileHeader.PageNum)
                throw new ApplicationException("pageIndex");

            var p = new Page
            {
                ShadowId = shadowId,
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

        private void WritePage(string tableName, Page page, int shadowId)
        {
            string filename = page.TableName + ".data." + shadowId;
            DataFileHeader tableDataFileHeader = ReadDataFileHeader(tableName, page.ShadowId);

            if (page.PageIndex >= tableDataFileHeader.PageNum)
                throw new ApplicationException("pageIndex");

            page.ShadowId = shadowId;
            using (var fileStream = new FileStream(filename, FileMode.Open))
            {
                fileStream.Seek(DataFileHeaderSize + page.PageIndex * page.PageSize, SeekOrigin.Begin);
                byte[] pageData = page.Encode();
                fileStream.Write(pageData, 0, page.PageSize);
            }
        }

        private PageTable ReadPageTable(string tableName, int shadowId)
        {
            string filename = tableName + ".index." + shadowId;
            using (var fileStream = new FileStream(filename, FileMode.OpenOrCreate))
            {
                var buffer = new byte[DefaultPageSize];
                fileStream.Seek(0, SeekOrigin.Begin);
                fileStream.Read(buffer, 0, buffer.Length);
                return new PageTable(buffer);
            }
        }

        private void WritePageTable(string tableName, PageTable pt, int shadowId)
        {
            string filename = tableName + ".index." + shadowId;

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
            string filename = tableName + ".index." + 0;

            if (File.Exists(filename))
                return;

            var pt = new PageTable(new byte[DefaultPageSize], DefaultPageSize, keySize);
            WritePageTable(tableName, pt, 0);
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
                                                                                       e => Int32.Parse(e.Attribute("ShadowId").Value));
            _tables = tables;
        }

        private void WriteDatabaseManifest()
        {
            var xdoc = new XDocument();
            var root = new XElement("Tables");
            xdoc.Add(root);

            foreach (var t in _tables)
            {
                var e = new XElement("Table", new XAttribute("Name", t.Key), new XAttribute("ShadowId", t.Value));
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
                string filename = tableName + ".data." + j;

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

        private DataFileHeader ReadDataFileHeader(string tableName, int shadowId)
        {
            string filename = tableName + ".data." + shadowId;
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