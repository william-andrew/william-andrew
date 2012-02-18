using System;
using System.Text;

namespace MyRM.Storage
{
    public class PageTable
    {
        public PageIndexEntry[] PageIndices;

        public int PageTableSize = 4096;
        public int EntrySize = 36 + 4 + 4 + 4;

        public PageTable(byte[] data)
        {
            var encoder = new UTF8Encoding();
            PageIndices = new PageIndexEntry[PageTableSize/EntrySize];

            for (var i = 0; i < PageTableSize / EntrySize; i++)
            {
                var pi = new PageIndexEntry();
                var p = i * EntrySize;

                var keyBuffer = new byte[36];
                Array.Copy(data, p, keyBuffer,0 , 36);

                pi.Key = encoder.GetString(keyBuffer);
                PageIndices[i] = pi;
            }
        }

        public byte[] GetBytes()
        {
            var buffer = new byte[PageTableSize];
            var encoder = new UTF8Encoding();

            for (var i = 0; i < PageTableSize / EntrySize; i++)
            {
                var p = i * EntrySize;
                Array.Copy(encoder.GetBytes(PageIndices[i].Key), 0, buffer, p, 36);

                var byteArray = BitConverter.GetBytes(PageIndices[i].PageIndex);
                Array.Copy(byteArray, 0, buffer, p + 36, 4);

                byteArray = BitConverter.GetBytes(PageIndices[i].RowIndex);
                Array.Copy(byteArray, 0, buffer, p + 36 + 4, 4);

                byteArray = BitConverter.GetBytes(PageIndices[i].ShadowId);
                Array.Copy(byteArray, 0, buffer, p + 36 + 4 + 4, 4);
            }

            return buffer;
        }
    }

    public class PageIndexEntry
    {
        public string Key;
        public int PageIndex;
        public int RowIndex;
        public int ShadowId;
    }
}