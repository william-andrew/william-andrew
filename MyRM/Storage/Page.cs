using System;
using System.Text;

namespace MyRM.Storage
{
    //Mark Page Header - {P$}{Page Index}{NextFreeRowIndex}
    //GetRow               {row 0}
    //GetRow               {row n}
    /// <summary>
    /// Represents a database Page.
    /// </summary>
    public class Page
    {
        public string TableName { get; set; }
        public string DataFileName { get; set; }
        public int FileId { get; set; }
        public int PageIndex { get; set; }
        public int PageSize { get; set; }
        
        public int RowSize { get; private set; }
        public int RowsPerPage { get; private set; }
        public int NextFreeRowIndex { get; private set; }

        private Row[] _rows;
        private readonly UTF8Encoding _encoder = new UTF8Encoding();

        public const int PageHeadSize = DatabaseFileAccess.PageHeadSize; // {P$}{Page Index}{NextFreeRowIndex}

        public Page(DataFileHeader dataFileHeader)
        {
            this.RowSize = dataFileHeader.RowSize;
            this.RowsPerPage = dataFileHeader.RowsPerPage;
        }

        public Row GetRow(int i)
        {
            var r = new Row(this.RowSize);
            Array.Copy(_rows[i].Data, r.Data, RowSize);
            return r;
        }

        public Row CreateEmptyRow()
        {
            var r = new Row(this.RowSize);
            return r;
        }

        public bool IsDirty { get; set; }

        public int InsertRow(Row row)
        {
            var rowId = NextFreeRowIndex++;
            IsDirty = true;
            Array.Copy(row.Data, _rows[rowId].Data, row.Data.Length < RowSize ? row.Data.Length : RowSize);
            return rowId;
        }

        public void UpdateRow(Row row, int index)
        {
            IsDirty = true;
            Array.Copy(row.Data, _rows[index].Data, row.Data.Length < RowSize ? row.Data.Length : RowSize);
        }

        public void Decode(byte[] pageData)
        {
            if ("P$" != _encoder.GetString(pageData, 0, 2))
                throw new ApplicationException("Invalid page - Page Header");

            if (this.PageIndex != BitConverter.ToInt32(pageData, 2))
                throw new ApplicationException("Invalid Page - PageIndex");

            NextFreeRowIndex = BitConverter.ToInt32(pageData, 2 + 4);

            _rows = new Row[this.RowsPerPage];

            for (var i = 0; i < RowsPerPage; i++)
            {
                var buffer = new byte[RowSize];
                Array.Copy(pageData, Page.PageHeadSize + i * RowSize, buffer, 0, this.RowSize);
                _rows[i] = new Row(this.RowSize) { Data = buffer };
            }
        }

        public byte[] Encode()
        {
            var rowData = new byte[this.PageSize];

            var byteArray = _encoder.GetBytes("P$");
            Array.Copy(byteArray, 0, rowData, 0, byteArray.Length);

            byteArray = BitConverter.GetBytes(PageIndex);
            Array.Copy(byteArray, 0, rowData, 2, byteArray.Length);

            byteArray = BitConverter.GetBytes(NextFreeRowIndex);
            Array.Copy(byteArray, 0, rowData, 2 + 4, byteArray.Length);


            for (var i = 0; i < RowsPerPage; i++)
            {
                Array.Copy(_rows[i].Data, 0, rowData, Page.PageHeadSize + i * RowSize, this.RowSize);
            }

            return rowData;
        }
    }
}
