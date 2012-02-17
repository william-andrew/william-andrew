using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace MyRM.Storage
{

    //TODO: ADD LOCKS
    //TODO: ADD HASH INDEX 
    //TODO: INTEGRATE WITH CALLER!!
    public class DatabaseFileAccess
    {
        private const int DefaultPageSize = 1024;
        private const int DefaultPageNumber = 1024;
        public const int DefaultPageHeaderSize = 64;
        private const int DataFileHeaderSize = 512;

        private readonly string _databaseName;
        private string _filePath;
        private bool _isInitialized;
        private Dictionary<string, int> _tables = new Dictionary<string, int>();

        public DatabaseFileAccess(string databaseName)
        {
            _databaseName = databaseName;
        }

        public string FilePath
        {
            get { return _filePath; }
        }

        public string DatabaseName
        {
            get { return _databaseName; }
        }

        private string DatabaseManifestFileName
        {
            get { return DatabaseName + @".manifest"; }
        }

        private string DebuggingInfo
        {
            get { return " PID=" + GetProcessId(); }
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
            if (File.Exists(DatabaseManifestFileName))
                ReadDatabaseManifest();
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

        public void ReadDatabaseManifest()
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

        public bool ContainsTable(string tableName)
        {
            return _tables.ContainsKey(tableName);
        }

        public void WriteDatabaseManifest()
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

        public void WritePage(string tableName, Page page)
        {
            var shadowId = _tables[tableName] == 0 ? 1 :0;
            WritePage(tableName, page, shadowId);
        }

        public void WritePage(string tableName, Page page, int shadowId)
        {
            string filename = page.TableName + ".data." + shadowId;
            DataFileHeader tableDataFileHeader = ReadDataFileHeader(tableName, page.ShadowId);

            if (page.PageIndex >= tableDataFileHeader.PageNum)
                throw new ApplicationException("pageIndex");

            page.ShadowId = shadowId;
            using (var fileStream = new FileStream(filename, FileMode.Open))
            {
                fileStream.Seek(DataFileHeaderSize + page.PageIndex*page.PageSize, SeekOrigin.Begin);
                byte[] pageData = page.Encode();
                fileStream.Write(pageData, 0, page.PageSize);
            }
        }

        public Page ReadPage(string tableName, int pageIndex)
        {
            var shadowId = _tables[tableName];
            return ReadPage(tableName, shadowId, pageIndex);
        }

        public Page ReadPage(string tableName, int shadowId, int pageIndex)
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
                fileStream.Seek(DataFileHeaderSize + pageIndex*p.PageSize, SeekOrigin.Begin);
                var pageData = new byte[p.PageSize];
                fileStream.Read(pageData, 0, p.PageSize);
                p.Decode(pageData);
            }

            return p;
        }

        public string CreateTableFile(string tableName, int rowSize, int pageSize = DefaultPageSize,
                                      int pageNum = DefaultPageNumber)
        {
            String activeFileName = null;
            for (int j = 0; j <= 1; j++)
            {
                string filename = tableName + ".data." + j;

                if (j == 0)
                    activeFileName = filename;

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

                int rowsPerPage = (pageSize - DefaultPageHeaderSize)/rowSize;

                using (var fileStream = new FileStream(filename, FileMode.Open))
                {
                    for (int i = 0; i < pageNum; i++)
                    {
                        fileStream.Seek(DataFileHeaderSize + i*pageSize, SeekOrigin.Begin);

                        //Mark Page Header - {P}{.}{Page Index}{RowSize}{rowsPerPage}{NextFreeRowIndex} 
                        var encoder = new UTF8Encoding();

                        byte[] byteArray = encoder.GetBytes("P");
                        fileStream.Write(byteArray, 0, byteArray.Length);

                        byteArray = encoder.GetBytes(".");
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

            return activeFileName;
        }

        public DataFileHeader ReadDataFileHeader(string tableName, int shadowId)
        {
            string filename = tableName + ".data." + shadowId;
            using (var fileStream = new FileStream(filename, FileMode.Open))
            {
                var buffer = new byte[DataFileHeaderSize];
                fileStream.Seek(0, SeekOrigin.Begin);
                fileStream.Read(buffer, 0, DataFileHeaderSize);
                object obj = SerializationHelper.ByteArrayToObject(buffer);
                return (DataFileHeader) obj;
            }
        }

        public void CreateTable(string table, int rowSize)
        {
            EnsureInitialized();

            lock (_tables)
            {
                if (!ContainsTable(table))
                {
                    string f = CreateTableFile(table, rowSize);

                    _tables.Add(table, 0);
                    WriteDatabaseManifest();
                }
            }
        }

        public void CommitPage(string tableName, Page page0)
        {
            if (!page0.IsDirty)
                return;

            _tables[tableName] = page0.ShadowId;

            WriteDatabaseManifest();
        }
    }
}