using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization.Json;
using System.Text;
using MyRM.Storage;
using TP;

namespace MyRM
{
    public static class SerializationHelper
    {
        public static byte[] ObjectToByteArray(Object obj)
        {
            if (obj == null)
                return null;
            var bf = new BinaryFormatter();
            var ms = new MemoryStream();
            bf.Serialize(ms, obj);
            return ms.ToArray();
        }

        public static Object ByteArrayToObject(byte[] arrBytes)
        {
            var memStream = new MemoryStream();
            var binForm = new BinaryFormatter();
            memStream.Write(arrBytes, 0, arrBytes.Length);
            memStream.Seek(0, SeekOrigin.Begin);
            var obj = binForm.Deserialize(memStream);
            return obj;
        }
       
        public static Row ConvertResourceToRow(Resource resource)
        {
            var encoder = new UTF8Encoding();
            var rowString = resource.getType() + "," + resource.getID().getName() + "," + resource.getCount().ToString(CultureInfo.InvariantCulture) + "," +
                            resource.getPrice();
            //TODO: FIX THE SIZE
            var row = new Row(96) {Data = encoder.GetBytes(rowString)};
            return row;
        }

        public static Resource ConvertRowToResource(Row record)
        {
            var data = record.DataString.Split(',');

            RID.Type t;
            Enum.TryParse(data[0], true, out t);
            var name = data[1];
            var c = int.Parse(data[2]);
            var p = int.Parse(data[3]);

            return new Resource(new RID(t, name), c, p);
        }

        public static HashSet<RID> ConvertRowToReservation(string key, Row record)
        {
            var items = new HashSet<RID>();

            var data = record.DataString.Split('|');

            if (string.IsNullOrEmpty(record.DataString))
                return items;

            foreach (var r in data)
            {
                var rr = r.Split(',');
                TP.RID.Type t;

                Enum.TryParse(rr[1], true, out t);

                var value = rr[0];
                var rid = new RID(t, value);
                items.Add(rid);
            }
            return items;
        }

        public static Row ConvertReservationToRow(string key, HashSet<RID> reserved)
        {
            var encoder = new UTF8Encoding();
            var rowString = new StringBuilder();
            foreach(var r in reserved)
            {
                rowString.AppendFormat("{0},{1}|", r.getName(), r.getType().ToString());
            }
            if (rowString.Length > 0)
                --rowString.Length;
            return new Row(96){Data = encoder.GetBytes(rowString.ToString())};
        }

        public static byte[] ToJson<T>(this T obj)
        {
            var stream = new MemoryStream();
            try
            {
                //serialize data to a stream, then to a JSON string
                var jsSerializer = new DataContractJsonSerializer(typeof(T));
                jsSerializer.WriteObject(stream, obj);

                return stream.ToArray();
            }
            finally
            {
                stream.Close();
                stream.Dispose();
            }
        }

        public static T FromJson<T>(byte[] obj)
        {
            var stream = new MemoryStream(obj);
            try
            {
                var jsSerializer = new DataContractJsonSerializer(typeof(T));
                var r = jsSerializer.ReadObject(stream);

                return (T)r;
            }
            finally
            {
                stream.Close();
                stream.Dispose();
            }
        }
    }
}