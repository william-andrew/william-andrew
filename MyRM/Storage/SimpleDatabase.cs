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
    public class SimpleDatabase : ISimpleDatabase
    {
        public const int DefaultPageSize = 4096;
        public const int PageHeaderSize = 10; // {P$}{Page Index}{NextFreeRowIndex}

        private const int DefaultTotalPages = 16;
        private const int DataFileHeaderSize = 256;

        private readonly string _databaseName;
        private bool _isInitialized;
        private Dictionary<string, int> _tables = new Dictionary<string, int>();

        private readonly object _databaseLock = new object();

        public bool UseTwoPhaseCommit { get; private set; }

        private readonly Dictionary<Transaction, List<UpdateLog>> _transactionLogs =
            new Dictionary<Transaction, List<UpdateLog>>();

        private const int InsertedButNotCommitted = -1;
        private const int DeletededButNotCommitted = -2;

        public SimpleDatabase(string databaseName, bool useTwoPhaseCommit, bool isIntializationRequired = true)
        {
            _databaseName = databaseName;
            this.UseTwoPhaseCommit = useTwoPhaseCommit;

            if (isIntializationRequired)
                this.Initialize();
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
            lock (_databaseLock)
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
                            throw new ApplicationException("The simpleDatabase " + DatabaseName + " requires recovery." +
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

            if (!ContainsTable(tableName))
            {
                lock (_databaseLock)
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
        }

        public Table OpenTable(string tableName)
        {
            EnsureInitialized();

            if (!ContainsTable(tableName))
            {
                throw new ArgumentException(tableName);
            }

            var pageTable = this.DiskReadPageTable(tableName);
            var header = this.ReadDataFileHeader(tableName, 0);

            var table = new Table(tableName, pageTable, header);
            return table;
        }

        public Dictionary<string, Row> ReadAllRecords(Transaction tid, string tableName)
        {
            //get page table based
            var pageTable = this.DiskReadPageTable(tableName);

            var list = new Dictionary<string, Row>();
            foreach (var index in pageTable.RecordIndices)
            {
                if (index.Key[0] != 0)
                {
                    //ignore uncommitted insertion from other transaction
                    if (index.ActiveFileId == InsertedButNotCommitted && index.TransactionId != tid.Id)
                        continue;

                    //ignore uncommitted deletion from this transaction
                    if (index.ActiveFileId == DeletededButNotCommitted && index.TransactionId == tid.Id)
                        continue;

                    var key = Trim(index.Key);
                    list.Add(key, ReadRecord(tid, tableName, key));
                }
            }
            return list;
        }

        public void UpsertRecord(Transaction tid, string tableName, string key, Row record)
        {
            //get page table based
            var pageTable = this.DiskReadPageTable(tableName);

            if (!(from k in pageTable.RecordIndices where Trim(k.Key) == key select k).Any())
            {
                InsertRecord(tid, tableName, key, record);
            }
            else
            {
                UpdateRecord(tid, tableName, key, record);
            }
        }

        public Row ReadRecord(Transaction tid, string tableName, string key)
        {
            Console.WriteLine("DB: ReadRecord {0}, key={1}", tableName, key);

            //get page table based
            var pt = DiskReadPageTable(tableName);

            var index = (from item in pt.RecordIndices where Trim(item.Key) == key select item).SingleOrDefault();

            if (index == null)
                throw new RecordNotFoundException(key);

            //If transaction id matches the record, allow read uncommitted data, otherwise
            //only allow reading committed records.
            if (index.TransactionId != null &&
                ((index.TransactionId == tid.Id && index.ActiveFileId == DeletededButNotCommitted) ||
                (index.TransactionId != tid.Id && index.ActiveFileId == InsertedButNotCommitted)))
            {
                throw new RecordNotFoundException("record not found");
            }

            //support reading uncommitted data within a transaction
            lock (this._transactionLogs)
            {
                if (tid != null && this._transactionLogs.ContainsKey(tid))
                {
                    var logs = this._transactionLogs[tid];
                    for(var i = logs.Count - 1; i >= 0 ; i--)
                    {
                        var log = logs[i];

                        if (log.PageIndex == index.PageIndex && log.RowIndex == index.RowIndex)
                            return log.Image;
                    }
                }
            }

            var fileId = index.ActiveFileId < 0 ? index.ShadowFileId : index.ActiveFileId;

            var page = DiskReadPage(tableName, index.PageIndex, fileId);
            return page.GetRow(index.RowIndex);
        }

        public void InsertRecord(Transaction tid, string tableName, string key, Row record)
        {
            Console.WriteLine("DB: InsertRecord {0}, key={1}, value={2}", tableName, key, record.DataString);

            var header = this.ReadDataFileHeader(tableName, 0);
            //aquire insertion lock

            //read active page tableName
            var pageTable = this.DiskReadPageTable(tableName);

            //check if index is full
            if (!(from k in pageTable.RecordIndices where k.Key[0] == 0 select k).Any())
                throw new ApplicationException("index is full");

            //modify page tableName
            if ((from k in pageTable.RecordIndices where Trim(k.Key) == key select k).Any())
                throw new ApplicationException("duplicate key");

            //find an empty page
            var pageIndexTemp = Math.Abs(key.GetHashCode()) % header.TotalPageNum;
            var pageStart = pageIndexTemp;
            Page page = null;

            bool pageInUse = false;
            do
            {
                pageInUse = false;
                foreach (var item in pageTable.RecordIndices)
                {
                    if (item.Key[0] != 0 && item.PageIndex == pageIndexTemp)
                    {
                        pageInUse = true;
                        page = DiskReadPage(tableName, item.PageIndex, item.ActiveFileId);
                        if (page.NextFreeRowIndex <= page.RowsPerPage)
                        {
                            break;
                        }
                        page = null;
                    }
                }
                if (pageInUse)
                {
                    if (page == null)
                        pageIndexTemp = (++pageIndexTemp) % header.TotalPageNum;
                }
                else
                {
                    page = DiskReadPage(tableName, pageIndexTemp, 0);
                    break;
                }
            } while (pageIndexTemp != pageStart);

            if (page == null)
                throw new ApplicationException("out of page space");

            var rowId = page.InsertRow(record);

            if (pageInUse)
            {
                //todo: bugbug
                //check page info
            }
            else
            {
                page.FileId = page.FileId == 0 ? 1 : 0;
            }

            //update page table
            pageTable.InsertIndex(key, page.PageIndex, rowId, page.FileId, tid.Id);

            //write page into the shadow
            DiskWritePage(tableName, page, page.FileId);

            //save page tableName file
            UpdateShadowIdsForPage(pageTable, page.PageIndex, page.FileId, tid.Id);
            DiskWritePageTable(tableName, pageTable);

            if (!UseTwoPhaseCommit)
            {
                CommitPages(tid, new List<Page> { page });
            }

            lock (_transactionLogs)
            {
                var log = new UpdateLog
                              {
                                  OperationType = OperationType.Insert,
                                  Image = record,
                                  PageIndex = page.PageIndex,
                                  RowIndex = rowId,
                                  PageShadowFileId = page.FileId,
                                  TableName = tableName,
                                  TransactionId = tid.Id,
                                  Key = key
                              };
                if (_transactionLogs.ContainsKey(tid))
                {
                    _transactionLogs[tid].Add(log);
                }
                else
                {
                    _transactionLogs.Add(tid, new List<UpdateLog> { log });
                }
            }
        }

        public void UpdateRecord(Transaction tid, string tableName, string key, Row record)
        {
            Console.WriteLine("DB: UpdateRecord {0}, key={1}, value={2}", tableName, key, record.DataString);

            //read active page tableName
            var pageTable = this.DiskReadPageTable(tableName);

            //modify page tableName
            var index = (from item in pageTable.RecordIndices where Trim(item.Key) == key select item).SingleOrDefault();

            if (index == null)
                throw new RecordNotFoundException("record not found");

            if (index.TransactionId != null && index.TransactionId == tid.Id && index.ActiveFileId == DeletededButNotCommitted)
                throw new RecordNotFoundException("record not found");

            var page = DiskReadPage(tableName, index.PageIndex, index.ActiveFileId < 0 ? index.ShadowFileId : index.ActiveFileId);

            page.UpdateRow(record, index.RowIndex);

            if (index.ActiveFileId >= 0)
                page.FileId = page.FileId == 0 ? 1 : 0;

            //write page into the shadow
            DiskWritePage(tableName, page, page.FileId);

            //update page table index
            index.ShadowFileId = page.FileId;
            index.IsDirty = 1;
            index.TransactionId = tid.Id;

            //save page tableName file
            UpdateShadowIdsForPage(pageTable, page.PageIndex, page.FileId, tid.Id);
            DiskWritePageTable(tableName, pageTable);

            if (!UseTwoPhaseCommit)
            {
                CommitPages(tid, new List<Page> { page });
            }

            lock (_transactionLogs)
            {
                var log = new UpdateLog
                {
                    OperationType = OperationType.Update,
                    Image = record,
                    PageIndex = page.PageIndex,
                    RowIndex = index.RowIndex,
                    PageShadowFileId = page.FileId,
                    TableName = tableName,
                    TransactionId = tid.Id,
                    Key = key
                };
                if (_transactionLogs.ContainsKey(tid))
                {
                    _transactionLogs[tid].Add(log);
                }
                else
                {
                    _transactionLogs.Add(tid, new List<UpdateLog> { log });
                }
            }
        }

        public void DeleteRecord(Transaction tid, string tableName, string key)
        {
            //read active page tableName
            var pageTable = this.DiskReadPageTable(tableName);

            var index = (from item in pageTable.RecordIndices where Trim(item.Key) == key select item).SingleOrDefault();

            if (index == null) //||
                //(index.TransactionId != null && index.TransactionId != tid.Id))
                throw new RecordNotFoundException("record not found");

            var page = DiskReadPage(tableName, index.PageIndex, index.ActiveFileId < 0 ? index.ShadowFileId : index.ActiveFileId);
            var beforeImage = page.GetRow(index.RowIndex);

            page.UpdateRow(page.CreateEmptyRow(), index.RowIndex);
            page.FileId = page.FileId == 0 ? 1 : 0;

            //write page into the shadow
            DiskWritePage(tableName, page, page.FileId);

            //update page table index
            pageTable.MarkIndexDeleted(index, tid.Id);

            //save page tableName file
            UpdateShadowIdsForPage(pageTable, page.PageIndex, page.FileId, tid.Id);
            DiskWritePageTable(tableName, pageTable);

            //FIX THE DUPLIATE PAGE
            if (!UseTwoPhaseCommit)
            {
                CommitPages(tid, new List<Page> { page });
            }

            lock (_transactionLogs)
            {
                var log = new UpdateLog
                {
                    OperationType = OperationType.Detete,
                    Image = beforeImage,
                    PageIndex = page.PageIndex,
                    RowIndex = index.RowIndex,
                    PageShadowFileId = page.FileId,
                    TableName = tableName,
                    TransactionId = tid.Id,
                    Key = key
                };
                if (_transactionLogs.ContainsKey(tid))
                {
                    _transactionLogs[tid].Add(log);
                }
                else
                {
                    _transactionLogs.Add(tid, new List<UpdateLog> { log });
                }
            }
        }

        public void Prepare(Transaction tid)
        {
            try
            {
                lock (_transactionLogs)
                {
                    if (_transactionLogs.ContainsKey(tid))
                    {
                        List<UpdateLog> logs = _transactionLogs[tid];

                        foreach (var log in logs)
                        {
                            //TODO: optimize on table, not per log
                            var pageTable = DiskReadPageTable(log.TableName);

                            var index = (from i in pageTable.RecordIndices where Trim(i.Key) == log.Key select i).Single();

                            index.TransactionId = log.TransactionId;
                            index.IsDirty = 1;
                            index.TransactionId = log.TransactionId;
                            Debug.Assert(index.PageIndex == log.PageIndex);
                            Debug.Assert(index.RowIndex == log.RowIndex);

                            if (log.OperationType == OperationType.Insert)
                            {
                                index.ActiveFileId = InsertedButNotCommitted;
                                index.ShadowFileId = log.PageShadowFileId;
                            }
                            else if (log.OperationType == OperationType.Update)
                            {
                                index.ShadowFileId = log.PageShadowFileId;
                            }
                            else if (log.OperationType == OperationType.Detete)
                            {
                                index.ShadowFileId = index.ActiveFileId;
                                index.ActiveFileId = DeletededButNotCommitted;
                            }

                            DiskWritePageTable(log.TableName, pageTable);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                System.Console.WriteLine("SimpleDatabase:Prepare: \n" + e);
            }
        }

        public void Commit(Transaction tid)
        {
            try
            {
                lock (_transactionLogs)
                {
                    if (_transactionLogs.ContainsKey(tid))
                    {
                        List<UpdateLog> logs = _transactionLogs[tid];

                        _transactionLogs.Remove(tid);

                        foreach (var log in logs)
                        {
                            //TODO: optimize on table, not per log
                            var pageTable = DiskReadPageTable(log.TableName);

                            var index =
                                (from i in pageTable.RecordIndices where Trim(i.Key) == log.Key select i).Single();

                            if (log.OperationType == OperationType.Insert)
                            {
                                index.ActiveFileId = index.ShadowFileId;
                            }
                            else if (log.OperationType == OperationType.Update)
                            {
                                index.ActiveFileId = index.ShadowFileId;
                                index.ShadowFileId = 0;
                            }
                            else if (log.OperationType == OperationType.Detete)
                            {
                                index.Key = new string('\0', index.Key.Length);
                                index.ActiveFileId = 0;
                                index.ShadowFileId = 0;
                            }

                            index.IsDirty = 0;
                            index.TransactionId = Guid.Empty;
                            DiskWritePageTable(log.TableName, pageTable);
                        }
                    }
                }

            }
            catch (Exception e)
            {
                System.Console.WriteLine("SimpleDatabase:Commit: \n" + e);
            }
        }

        public void Abort(Transaction tid)
        {
            try
            {
                lock (_transactionLogs)
                {
                    if (_transactionLogs.ContainsKey(tid))
                    {
                        List<UpdateLog> logs = _transactionLogs[tid];

                        _transactionLogs.Remove(tid);

                        foreach (var log in logs)
                        {
                            //TODO: optimize on table, not per log
                            var pageTable = DiskReadPageTable(log.TableName);

                            var index =
                                (from i in pageTable.RecordIndices where Trim(i.Key) == log.Key select i).Single();

                            if (log.OperationType == OperationType.Insert)
                            {
                                index.Key = new string('\0', index.Key.Length);
                                index.ActiveFileId = 0;
                            }
                            else if (log.OperationType == OperationType.Update)
                            {
                            }
                            else if (log.OperationType == OperationType.Detete)
                            {
                                index.ActiveFileId = index.ShadowFileId;
                            }

                            index.ShadowFileId = 0;
                            index.IsDirty = 0;
                            index.TransactionId = Guid.Empty;
                            DiskWritePageTable(log.TableName, pageTable);
                        }
                    }
                }

            }
            catch (Exception e)
            {
                System.Console.WriteLine("SimpleDatabase:Abort: \n" + e);
            }
        }

        private void UpdateShadowIdsForPage(PageTable pageTable, int pageIndex, int shadowId, Guid transactionId)
        {
            foreach (var item in pageTable.RecordIndices)
            {
                if (item.PageIndex == pageIndex)
                {
                    item.IsDirty = 1;
                    item.ShadowFileId = shadowId;
                    item.TransactionId = transactionId;
                }
            }
        }

        private string Trim(string key)
        {
            return key.TrimEnd('\0');
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
            var pageTableIndex = (from k in pageTable.RecordIndices where Trim(k.Key) == key select k).Single();
            pageTableIndex.ShadowFileId = pageTableIndex.ActiveFileId == 0 ? 1 : 0;

            if (pageTableIndex.PageIndex != page.PageIndex)
                throw new ApplicationException("index doesn't match the page");

            //save page into shadow
            DiskWritePage(tableName, page, pageTableIndex.ShadowFileId);

            //save page tableName file
            DiskWritePageTable(tableName, pageTable);
        }

        public Page ReadPage(string tableName, string key)
        {
            //read active page tableName
            var pageTable = this.DiskReadPageTable(tableName);
            var pageTableIndex = (from k in pageTable.RecordIndices where Trim(k.Key) == key select k).SingleOrDefault();

            if (pageTableIndex == null)
                throw new RecordNotFoundException(key);

            return DiskReadPage(tableName, pageTableIndex.PageIndex, pageTableIndex.ActiveFileId);
        }

        private void CommitPages(Transaction tid, List<Page> pages)
        {
            //Copy shadowIds into ActiveIds and save
            //this.DiskWritePageTable(tableName);

            foreach (var page in pages)
            {
                var pageTable = this.DiskReadPageTable(page.TableName);

                foreach (var item in pageTable.RecordIndices)
                {
                    if (item.TransactionId == tid.Id)
                    {
                        if (item.IsDirty == 1)
                        {
                            //Commit delete
                            if (item.ActiveFileId == DeletededButNotCommitted)
                            {
                                item.Key = new string('\0', item.Key.Length);
                                item.ActiveFileId = 0;
                            }

                            item.ActiveFileId = item.ShadowFileId;
                            item.ShadowFileId = 0;
                            item.IsDirty = 0;
                            item.TransactionId = Guid.Empty;
                        }
                    }
                }

                DiskWritePageTable(page.TableName, pageTable);
            }
        }

        //private void AbortPage(Transaction tid, List<Page> pages)
        //{
        //    //Remove shadowIds associated with the records
        //    foreach (var page in pages)
        //    {
        //        var pageTable = this.DiskReadPageTable(page.TableName);

        //        foreach (var item in pageTable.RecordIndices)
        //        {
        //            if (item.TransactionId == tid.Id)
        //            {
        //                //Remove the key inserted, but not commited
        //                if (item.ActiveFileId == InsertedButNotCommitted)
        //                {
        //                    item.Key = new string('\0', item.Key.Length);
        //                    item.ActiveFileId = 0;
        //                }

        //                //Undo delete
        //                if (item.ActiveFileId == DeletededButNotCommitted)
        //                {
        //                    item.ActiveFileId = item.ShadowFileId;
        //                }

        //                item.ShadowFileId = 0;
        //                item.TransactionId = Guid.Empty;
        //            }
        //        }

        //        //this.DiskWritePageTable(tableName);
        //        DiskWritePageTable(page.TableName, pageTable);
        //    }
        //}

        private Page DiskReadPage(string tableName, int pageIndex, int fileId)
        {
            string filename = DatabaseName + "." + tableName + ".data." + fileId;
            DataFileHeader tableDataFileHeader = ReadDataFileHeader(tableName, fileId);

            if (pageIndex >= tableDataFileHeader.TotalPageNum)
                throw new ApplicationException("pageIndex");

            var p = new Page(tableDataFileHeader)
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

            if (page.PageIndex >= tableDataFileHeader.TotalPageNum)
                throw new ApplicationException("pageIndex");

            page.FileId = fileId;
            using (var fileStream = new FileStream(filename, FileMode.Open))
            {
                fileStream.Seek(DataFileHeaderSize + page.PageIndex * page.PageSize, SeekOrigin.Begin);
                byte[] pageData = page.Encode();
                fileStream.Write(pageData, 0, page.PageSize);
            }
        }

        public PageTable DiskReadPageTable(string tableName)
        {
            string filename = DatabaseName + "." + tableName + ".index";

            using (var fileStream = new FileStream(filename, FileMode.Open))
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
                throw new ApplicationException("The simpleDatabase " + DatabaseName + " is not initialized." + DebuggingInfo);
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
                throw new ApplicationException("The manifest of the simpleDatabase " + DatabaseName + " is missing." +
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

        private void CreateTableFile(string tableName, int rowSize, int pageSize = DefaultPageSize, int pageNum = DefaultTotalPages)
        {
            for (int j = 0; j <= 1; j++)
            {
                string filename = DatabaseName + "." + tableName + ".data." + j;

                if (File.Exists(filename))
                    continue;

                var databaseHeader = new DataFileHeader
                {
                    TotalPageNum = pageNum,
                    PageSize = pageSize,
                    RowSize = rowSize,
                    RowsPerPage = (pageSize - Page.PageHeadSize) / rowSize
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

                using (var fileStream = new FileStream(filename, FileMode.OpenOrCreate))
                {
                    for (int i = 0; i < pageNum; i++)
                    {
                        fileStream.Seek(DataFileHeaderSize + i * pageSize, SeekOrigin.Begin);

                        //Mark Page Header - {P$}{Page Index}{NextFreeRowIndex} 
                        var encoder = new UTF8Encoding();

                        byte[] byteArray = encoder.GetBytes("P$");
                        fileStream.Write(byteArray, 0, byteArray.Length);

                        byteArray = BitConverter.GetBytes(i);
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
                var pfh = (DataFileHeader)obj;
                pfh.RowsPerPage = (pfh.PageSize - Page.PageHeadSize) / pfh.RowSize;
                return pfh;
            }
        }
    }
}