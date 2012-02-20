using System.Text;

namespace MyRM.Storage
{
    public class Row
    {
        readonly UTF8Encoding _encoder = new UTF8Encoding();
        
        public Row()
        {
        }

        public Row(int rowSize)
        {
            Data = new byte[rowSize];
        }

        public byte[] Data { get; set; }

        public string DataString
        {
            get { return _encoder.GetString(Data).TrimEnd(new[] {'\0'}); }
        }
    }
}