using MyRM.Storage;

namespace Tools
{
    class DumpDB
    {
        static void Main(string[] args)
        {
            var db = new SimpleDatabase(args[0], true, true);
            var pageTable = db.DiskReadPageTable(args[1]);

            System.Console.WriteLine("Database is {0}, index item size is {1}", args[0], pageTable.RecordIndexEntrySizeInBytes);

            foreach (var r in pageTable.RecordIndices)
            {
                bool isNotFound = false;
                Row row = null;
                try
                {
                    row = db.ReadRecord(null, args[1], r.Key);
                }
                catch (RecordNotFoundException)
                {
                    isNotFound = true;
                }
                System.Console.WriteLine("Key={0}, P={1}, R={2}, F={3}, Data={4}", r.Key, r.PageIndex, r.RowIndex, r.ActiveFileId, isNotFound ? "not found" : row.DataString);
            }
            System.Console.WriteLine("Total rows {0}", pageTable.RecordIndices.Length);
        }
    }
}
