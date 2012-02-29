using System.Collections.Generic;
using MyRM.Storage;
using TP;

namespace TestProject
{
    public class MockDatabase : IDatabaseFileAccess
    {
        public void CreateTable(string tableName, int rowSize, int keySize = 36)
        {
            throw new System.NotImplementedException();
        }

        public string DatabaseName
        {
            get { throw new System.NotImplementedException(); }
        }

        public void Initialize(bool autoRecovery = true)
        {
            throw new System.NotImplementedException();
        }

        public bool ContainsTable(string tableName)
        {
            throw new System.NotImplementedException();
        }

        public void UpsertRecord(Transaction tid, string tableName, string key, Row record)
        {
            throw new System.NotImplementedException();
        }

        public void DeleteRecord(Transaction tid, string tableName, string key)
        {
            throw new System.NotImplementedException();
        }

        public Row ReadRecord(Transaction tid, string tableName, string key)
        {
            throw new System.NotImplementedException();
        }

        public Dictionary<string, Row> ReadAllRecords(Transaction tid, string tableName)
        {
            throw new System.NotImplementedException();
        }

        public void Commit(Transaction tid)
        {
            throw new System.NotImplementedException();
        }

        public void Abort(Transaction tid)
        {
            throw new System.NotImplementedException();
        }
    }
}