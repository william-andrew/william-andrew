using System;

namespace MyRM.Storage
{
    /// <summary>
    /// Represents the head structure of a database data file.
    ///
    /// [DataFileHeader]
    /// [Page 0]
    /// [Page 1]
    /// ...
    /// [Page N]
    ///
    /// </summary>
    [Serializable]
    public struct DataFileHeader
    {
        public int PageSize; 
        public int TotalPageNum;
        public int RowSize;

        /// <summary>
        /// Computed field, not saved on disk
        /// </summary>
        [NonSerialized]
        public int RowsPerPage;
    }
}