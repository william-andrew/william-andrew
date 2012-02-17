namespace MyRM.Storage
{
    public class Row
    {
        public Row()
        {
        }

        public Row(int rowSize)
        {
            Data = new byte[rowSize];
        }

        public byte[] Data { get; set; }
    }
}