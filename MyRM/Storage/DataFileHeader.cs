using System;

namespace MyRM.Storage
{
    [Serializable]
    public struct DataFileHeader
    {
        public int PageSize;
        public int PageNum;
        public int Version;
    }
}