using System;
using TP;

namespace MyRM
{
    [Serializable]
    public class Resource
    {
        private readonly RID _rId;
        private int _count;
        private int _price;

        public Resource() { }
        public Resource(RID rId)
        {
            this._rId = rId;
        }
        public Resource(RID rId, int c, int p)
        {
            this._rId = rId;
            this._count = c;
            this._price = p;
        }

        public String getName() { return _rId.getName(); }
        public RID getID() { return _rId; }
        public RID.Type getType() { return _rId.getType(); }
        public int getCount() { return _count; }
        public int getPrice() { return _price; }

        public void incrCount() { ++this._count; }
        public void incrCount(int c) { this._count += c; }

        public void decrCount(int c) { this._count -= c; }
        public void decrCount() { --this._count; }

        public void setCount(int count) { this._count = count; }
        public void setPrice(int price) { this._price = price; }

        public override int GetHashCode() { return _rId.GetHashCode(); }

        public override String ToString()
        {
            return _rId.getName() + "," + _count + "," + _price;
        }
    }
}
