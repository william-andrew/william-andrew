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
}