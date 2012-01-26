using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Linq;

namespace MyRM
{
    public class MyDatabase
    {
        private Dictionary<string, string> _tables = new Dictionary<string, string>();

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
            get { return "MyDatabase"; }
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
            if (!ContainsTable(tableName))
            {
                RegisterTable(tableName);
            }

            lock (_tables)
            {
                var newTablenName = IncrementTableVersion(tableName);

                File.WriteAllText(newTablenName, data);
                WriteDatabaseManifest();
            }
        }

        public string ReadTable(string tableName)
        {
            if (!ContainsTable(tableName))
            {
                throw new ArgumentException("table not exists - " + tableName, tableName);
            }

            string tablenPath;
            lock (_tables)
            {
                tablenPath = _tables[tableName];
            }

            //TODO: fix this, add db intialization code
            if (File.Exists(tablenPath))
                return File.ReadAllText(tablenPath);
            else return null;
        }

        public void RegisterTable(string tableName)
        {
            if (!ContainsTable(tableName))
            {
                _tables.Add(tableName, tableName + ".table");
            }            
        }

        public void ReadDatabaseManifest()
        {
            var xdoc = XDocument.Load(DatbaseManifestFileName);
            var tables = xdoc.Descendants("Table").ToDictionary(e => e.Attribute("Name").Value, e => e.Attribute("File").Value);
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
            xdoc.Save(DatbaseManifestFileName + ".tmp");

            if (File.Exists(DatbaseManifestFileName))
            {
                File.Replace(DatbaseManifestFileName + ".tmp", DatbaseManifestFileName, DatbaseManifestFileName + ".bak");
                File.Delete(DatbaseManifestFileName + ".bak");
            }
            else
            {
                File.Move(DatbaseManifestFileName + ".tmp", DatbaseManifestFileName);
            }
        }

        public string GetTableFilePath(string tableName)
        {
            return _tables[tableName];
        }

        private string DatbaseManifestFileName
        {
            get { return DatabaseName + @".manifest"; }
        }

        private string IncrementTableVersion(string tableName)
        {
            this._tables[tableName] = tableName + ".table." + Guid.NewGuid();
            return this._tables[tableName];
        }
    }
}