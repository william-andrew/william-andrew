using System;

namespace MyRM.Storage
{
    /// <summary>
    /// Represents an entry in the index and
    /// points to a record in the data file.
    /// </summary>
    public class RecordIndexEntry
    {
        /// <summary>
        /// Unique key for a record
        /// </summary>
        public string Key;

        /// <summary>
        /// Page index that points to a page which contains the record
        /// </summary>
        public int PageIndex;

        /// <summary>
        /// Row index that points to a record
        /// </summary>
        public int RowIndex;

        /// <summary>
        /// Active file Id
        /// If it is -1, means the record is only in shadow and 
        /// never commited before
        /// </summary>
        public int ActiveId;

        /// <summary>
        /// Shawdow file Id
        /// </summary>
        public int ShadowId;

        /// <summary>
        /// The record has been modified and not committed
        /// </summary>
        public int IsDirty;

        /// <summary>
        /// The transaction owns the record now
        /// </summary>
        public Guid? TransactionId;
    }
}