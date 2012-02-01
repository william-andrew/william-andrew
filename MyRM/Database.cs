using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Xml.Linq;

namespace MyRM
{
    public interface IDatabase
    {
        void CreateTable(string tableName);
        string DatabaseName { get; }
        void Initialize(bool autoRecovery = true);
        bool ContainsTable(string tableName);
        string ReadTable(string tableName);
        void WriteTable(string tableName, string data);
    }

    public class Database : IDatabase
    {
        private readonly string _databaseName;
        private bool _isInitialized;
        private Dictionary<string, string> _tables = new Dictionary<string, string>();

        public Database(string databaseName, bool isIntializationRequired = true)
        {
            _databaseName = databaseName;

            if (isIntializationRequired)
            {
                Initialize();
            }
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

        public string DatabaseName
        {
            get { return _databaseName; }
        }

        private string DebuggingInfo
        {
            get { return " PID=" + this.GetProcessId(); }            
        }

        private string DatabaseManifestFileName
        {
            get { return DatabaseName + @".manifest"; }
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
                        throw new ApplicationException("The database " + DatabaseName + " requires recovery." + DebuggingInfo);
                }
                BuildNewDatabase();
            }
            _isInitialized = true;
        }

        private void BuildNewDatabase()
        {
            WriteDatabaseManifest();
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

        /// <summary>
        /// TODO: support page concept, and on more granular level
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="data"></param>
        public void WriteTable(string tableName, string data)
        {
            EnsureInitialized();

            if (!ContainsTable(tableName))
            {
                throw new ArgumentException("table not exists - " + tableName +  DebuggingInfo , tableName);
            }

            lock (_tables)
            {
                string newTablenName = IncrementTableVersion(tableName);

                File.WriteAllText(newTablenName, data);
                WriteDatabaseManifest();
            }
        }

        public string ReadTable(string tableName)
        {
            EnsureInitialized();

            if (!ContainsTable(tableName))
            {
                throw new ArgumentException("table not exists - " + tableName + DebuggingInfo, tableName);
            }

            string tablenPath;
            lock (_tables)
            {
                tablenPath = _tables[tableName];
            }
            return File.ReadAllText(tablenPath);
        }

        public void CreateTable(string tableName)
        {
            EnsureInitialized();

            lock (_tables)
            {
                if (!ContainsTable(tableName))
                {
                    string newTablenName = this.GetVersionedTableName(tableName);

                    File.WriteAllText(newTablenName, String.Empty);
                    _tables.Add(tableName, newTablenName);
                    WriteDatabaseManifest();
                }
            }
        }

        public void ReadDatabaseManifest()
        {
            if (!File.Exists(DatabaseManifestFileName))
            {
                throw new ApplicationException("The manifest of the database " + DatabaseName + " is missing." + DebuggingInfo);
            }

            XDocument xdoc = XDocument.Load(DatabaseManifestFileName);
            Dictionary<string, string> tables = xdoc.Descendants("Table").ToDictionary(e => e.Attribute("Name").Value,
                                                                                       e => e.Attribute("File").Value);
            _tables = tables;
        }

        /// <summary>
        /// TODO: Support granular page file concept
        /// </summary>
        public void WriteDatabaseManifest()
        {
            var xdoc = new XDocument();
            var root = new XElement("Tables");
            xdoc.Add(root);

            foreach (var t in _tables)
            {
                var e = new XElement("Table", new XAttribute("Name", t.Key), new XAttribute("File", t.Value));
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

        public string GetTableFilePath(string tableName)
        {
            return _tables[tableName];
        }

        private string GetVersionedTableName(string tableName)
        {
            return DatabaseName + "." + tableName + ".table." + Guid.NewGuid();
        }

        private string IncrementTableVersion(string tableName)
        {
            _tables[tableName] = GetVersionedTableName(tableName);
            return _tables[tableName];
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
    }
}