namespace MyRM.Storage
{
    public class PageIndexEntry
    {
        public string Key;
        public int PageIndex;
        public int RowIndex;
        public int ShadowId;
        //Page Index V2
        public int ActiveId;
        public int IsDirty;
    }
}