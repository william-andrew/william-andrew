using System.Collections.Generic;
using TP;

namespace MyRM.Storage
{
    public interface ISimpleDatabase
    {
        void CreateTable(string tableName, int rowSize, int keySize = 36);
        string DatabaseName { get; }
        void Initialize(bool autoRecovery = true);
        bool ContainsTable(string tableName);
        void UpsertRecord(Transaction tid, string tableName, string key, Row record);
        void DeleteRecord(Transaction tid, string tableName, string key);
        Row ReadRecord(Transaction tid, string tableName, string key);
        Dictionary<string, Row> ReadAllRecords(Transaction tid, string tableName);
        void Commit(Transaction tid);
        void Abort(Transaction tid);
        void Prepare(Transaction tid);
    }
}