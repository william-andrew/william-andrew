namespace MyRM.Storage
{
    public class Table
    {
        public PageTable PageTable { set; get; }
        public DataFileHeader FileHeader { set; get; }
        public string Name { get; private set; }

        public Table(string name, PageTable pageTable, DataFileHeader fileHeader)
        {
            this.Name = name;
            this.PageTable = pageTable;
            this.FileHeader = fileHeader;
        }
    }
}
