using System;

namespace MyRM.Storage
{
    //In memory log that records a single change
    public class UpdateLog
    {
        public Guid TransactionId;
        public Row Image;
        public string TableName;
        public int PageIndex;
        public int RowIndex;
        public int PageShadowFileId;
        public OperationType OperationType;
        public string Key;
    }
}
