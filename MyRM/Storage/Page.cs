using System;
using System.Text;

namespace MyRM.Storage
{
    //Mark Page Header - {P}{.}{Page Index}{RowSize}{rowsPerPage}{NextFreeRowIndex}
    //Row               {row 0}
    //Row               {row n}
    public class Page
    {
        //TODO:  ADD LOCKS!!!
        public string TableName { get; set; }
        public string DataFileName { get; set; }
        public int ShadowId { get; set; }
        public int PageIndex { get; set; }
        public int PageSize { get; set; }
        public int RowSize { get; set; }
        public int RowsPerPage { get; set; }

        public int NextFreeRowIndex { get; private set; }

        private Row[] _rows;

        public Row Row(int i)
        {
            var r = new Row { Data = new byte[RowSize] };
            Array.Copy(_rows[i].Data, r.Data, RowSize);
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
            var encoder = new UTF8Encoding();
            if ("P." != encoder.GetString(pageData, 0, 2))
                throw new ApplicationException("Invalid page - Page Header");

            if (this.PageIndex != BitConverter.ToInt32(pageData, 2))
                throw new ApplicationException("Invalid Page - PageIndex");

            RowSize = BitConverter.ToInt32(pageData, 6);
            RowsPerPage = BitConverter.ToInt32(pageData, 10);
            NextFreeRowIndex = BitConverter.ToInt32(pageData, 14);

            _rows = new Row[this.RowsPerPage];

            for (var i = 0; i < RowsPerPage; i++)
            {
                var buffer = new byte[RowSize];
                Array.Copy(pageData, DatabaseFileAccess.DefaultPageHeaderSize + i * RowSize, buffer, 0, this.RowSize);
                _rows[i] = new Row { Data = buffer };
            }
        }

        public byte[] Encode()
        {
            var rowData = new byte[this.PageSize];

            var encoder = new UTF8Encoding();

            var byteArray = encoder.GetBytes("P.");
            Array.Copy(byteArray, 0, rowData, 0, byteArray.Length);

            byteArray = BitConverter.GetBytes(PageIndex);
            Array.Copy(byteArray, 0, rowData, 2, byteArray.Length);

            byteArray = BitConverter.GetBytes(RowSize);
            Array.Copy(byteArray, 0, rowData, 6, byteArray.Length);

            byteArray = BitConverter.GetBytes(RowsPerPage);
            Array.Copy(byteArray, 0, rowData, 10, byteArray.Length);

            byteArray = BitConverter.GetBytes(NextFreeRowIndex);
            Array.Copy(byteArray, 0, rowData, 14, byteArray.Length);


            for (var i = 0; i < RowsPerPage; i++)
            {
                Array.Copy(_rows[i].Data, 0, rowData, DatabaseFileAccess.DefaultPageHeaderSize + i * RowSize, this.RowSize);
            }

            return rowData;
        }
    }
}
