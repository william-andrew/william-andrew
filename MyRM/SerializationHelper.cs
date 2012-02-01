using System;
using System.Collections.Generic;
using System.IO;
using System.Xml.Linq;
using TP;

namespace MyRM
{
    public static class SerializationHelper
    {
        public static Dictionary<Customer, HashSet<RID>> DeserializeReservations(string xml)
        {
            var result = new Dictionary<Customer, HashSet<RID>>();
            if (xml == null)
                return result;

            var xdoc = XDocument.Load(new StringReader(xml));
            var root = xdoc.Element("Reservations");
            if (root == null)
                return result;

            foreach (var e in root.Elements())
            {
                var c = new Customer
                {
                    Id = new Guid(e.Attribute("Customer").Value)
                };

                var items = new HashSet<RID>();

                foreach (var i in e.Elements())
                {
                    TP.RID.Type t;
                    Enum.TryParse(i.Attribute("Type").Value, true, out t);

                    var value = i.Attribute("Name").Value;
                    var rid = new RID(t, value);
                    items.Add(rid);
                }
                result.Add(c, items);
            }
            return result;
        }

        public static string SerializeReservations(Dictionary<Customer, HashSet<RID>> reservations)
        {
            var xdoc = new XDocument();
            var root = new XElement("Reservations");
            xdoc.Add(root);

            foreach (var c in reservations)
            {
                var e = new XElement("Reservation", new XAttribute("Customer", c.Key.Id.ToString()));
                root.Add(e);
                foreach (var r in c.Value)
                {
                    e.Add(new XElement("RID", new XAttribute("Type", r.getType()), new XAttribute("Name", r.getName())));
                }
            }
            return xdoc.ToString();
        }

        public static string SerializeResource(Dictionary<RID, Resource> dictionary)
        {
            var xdoc = new XDocument();
            var root = new XElement("Resources");
            xdoc.Add(root);

            foreach (var r in dictionary)
            {
                var e = new XElement("Resource", new XAttribute("Type", r.Key.getType()),
                    new XAttribute("Name", r.Key.getName()),
                    new XAttribute("Price", r.Value.getPrice()),
                    new XAttribute("Count", r.Value.getCount())
                    );
                root.Add(e);
            }
            return xdoc.ToString();
        }

        public static Dictionary<RID, Resource> DeserializeResources(string xml)
        {
            var result = new Dictionary<RID, Resource>();
            if (xml == null)
                return result;

            var xdoc = XDocument.Load(new StringReader(xml));
            var root = xdoc.Element("Resources");

            if (root == null)
                return result;

            foreach (var e in root.Elements())
            {
                TP.RID.Type t;
                Enum.TryParse(e.Attribute("Type").Value, true, out t);
                var name = e.Attribute("Name").Value;
                var c = int.Parse(e.Attribute("Count").Value);
                var p = int.Parse(e.Attribute("Price").Value);

                var key = new RID(t, name);
                var item = new Resource(key, c, p);

                result.Add(key, item);
            }
            return result;
        }
    }
}