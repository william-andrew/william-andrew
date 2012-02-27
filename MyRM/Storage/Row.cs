using System;
using System.Text;

namespace MyRM.Storage
{
    /// <summary>
    /// Represents a table record 
    /// </summary>
    public class Row
    {
        private readonly UTF8Encoding _encoder = new UTF8Encoding();
        private readonly int _rowSize;
        private readonly byte[] _data;

        public Row(int rowSize)
        {
            _rowSize = rowSize;
            _data = new byte[rowSize];
        }

        public byte[] Data
        {
            get
            {
                return _data;
            }
            set
            {
                if (value.Length > _rowSize)
                {
                    throw new ArgumentException("input data size exceeds the row size allowed");
                }
                Array.Copy(value, _data, value.Length < _rowSize ? value.Length : _rowSize);
                Array.Clear(_data, (value.Length < _rowSize ? value.Length : _rowSize), _rowSize - value.Length);
            }
        }

        public string DataString
        {
            get { return _encoder.GetString(Data).TrimEnd(new[] { '\0' }); }
        }
    }
}