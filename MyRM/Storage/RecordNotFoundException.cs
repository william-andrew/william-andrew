using System;

namespace MyRM.Storage
{
    public class RecordNotFoundException : Exception
    {
        public RecordNotFoundException(string key)
            : base("record not found, key=" + key)
        {
        }
    }
}