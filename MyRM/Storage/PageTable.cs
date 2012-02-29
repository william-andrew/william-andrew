using System;
using System.Text;

namespace MyRM.Storage
{
    public class PageTable
    {
        public RecordIndexEntry[] RecordIndices;

        public int PageTableSize = DatabaseFileAccess.DefaultPageSize;
        public int EntrySize;
        private readonly int _keySize;

        public PageTable(byte[] data, int pageTableSize = DatabaseFileAccess.DefaultPageSize, int keySize = 36)
        {
            if (pageTableSize != data.Length)
            {
                throw new ArgumentException("invalid PageTable data", "data");
            }

            PageTableSize = pageTableSize;
            this._keySize = keySize;
            this.EntrySize = keySize + 5 * 4; //PageIndex, RowIndex, ActiveId, ShadowId, IsDirty
    
            var encoder = new UTF8Encoding();
            RecordIndices = new RecordIndexEntry[PageTableSize/EntrySize];

            for (var i = 0; i < PageTableSize / EntrySize; i++)
            {
                var p = i * EntrySize;

                var keyBuffer = new byte[keySize];
                Array.Copy(data, p, keyBuffer, 0, keySize);

                var pi = new RecordIndexEntry
                             {
                                 Key = encoder.GetString(keyBuffer),
                                 PageIndex = BitConverter.ToInt32(data, p + keySize),
                                 RowIndex = BitConverter.ToInt32(data, p + keySize + 4),
                                 ActiveId = BitConverter.ToInt32(data, p + keySize + 4 + 4),
                                 ShadowId = BitConverter.ToInt32(data, p + keySize + 4 + 4 + 4),
                                 IsDirty = BitConverter.ToInt32(data, p + keySize + 4 + 4 + 4 + 4)
                             };

                RecordIndices[i] = pi;
            }
        }

        public byte[] GetBytes()
        {
            var buffer = new byte[PageTableSize];
            var encoder = new UTF8Encoding();

            for (var i = 0; i < PageTableSize / EntrySize; i++)
            {
                var p = i * EntrySize;
                var byteKey = encoder.GetBytes(RecordIndices[i].Key);
                Array.Copy(byteKey, 0, buffer, p, byteKey.Length < _keySize ? byteKey.Length : _keySize); //TODO: FIX KEY LEN VALIDATION

                var byteArray = BitConverter.GetBytes(RecordIndices[i].PageIndex);
                Array.Copy(byteArray, 0, buffer, p + _keySize, 4);

                byteArray = BitConverter.GetBytes(RecordIndices[i].RowIndex);
                Array.Copy(byteArray, 0, buffer, p + _keySize + 4, 4);

                byteArray = BitConverter.GetBytes(RecordIndices[i].ActiveId);
                Array.Copy(byteArray, 0, buffer, p + _keySize + 4 + 4, 4);

                byteArray = BitConverter.GetBytes(RecordIndices[i].ShadowId);
                Array.Copy(byteArray, 0, buffer, p + _keySize + 4 + 4 + 4, 4);

                byteArray = BitConverter.GetBytes(RecordIndices[i].IsDirty);
                Array.Copy(byteArray, 0, buffer, p + _keySize + 4 + 4 + 4 + 4, 4);
            }

            return buffer;
        }

        public void InsertIndex(string key, int pageId, int rowId, int pageFileId)
        {
            lock (this.RecordIndices)
            {
                bool isDone = false;
                foreach (var item in RecordIndices)
                {
                    if (item.Key[0] == 0)
                    {
                        item.Key = key;
                        item.PageIndex = pageId;
                        item.RowIndex = rowId;
                        item.ActiveId = 0;
                        item.ShadowId = pageFileId;
                        item.IsDirty = 1;
                        isDone = true;
                        break;
                    }
                }

                if (!isDone)
                    throw new ApplicationException("run out of index space");
            }
        }

        public void RemoveIndex(RecordIndexEntry index)
        {
            lock (this.RecordIndices)
            {
                foreach (var item in RecordIndices)
                {
                    if (item.Key == index.Key)
                    {
                        item.Key = new string('\0', _keySize);
                        item.PageIndex = 0;
                        item.RowIndex = 0;
                        item.ActiveId = 0;
                        item.ShadowId = 0;
                        item.IsDirty = 0;
                    }
                }
            }
        }
    }
}