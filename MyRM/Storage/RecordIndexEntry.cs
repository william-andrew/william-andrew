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
        public int PageIndex;
        public int RowIndex;
        public int ActiveId;
        public int ShadowId;
        public int IsDirty;
    }
}