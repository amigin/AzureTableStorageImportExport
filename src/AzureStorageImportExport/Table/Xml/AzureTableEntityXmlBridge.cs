using System;
using System.Collections.Generic;
using System.Globalization;
using System.Xml;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageImportExport.Table.Xml
{
    public static class AzureTableEntityXmlBridge
    {

        public static DynamicTableEntity ReadTableStorageEntity(this XmlReader xmlReader)
        {
            xmlReader.Read();
            if (xmlReader.NodeType == XmlNodeType.EndElement)
                return null;

            var result = new DynamicTableEntity
            {
                PartitionKey = xmlReader[XmlConsts.PartitionKeyAttr].FromXmlBase64String(),
                RowKey = xmlReader[XmlConsts.RowKeyAttr].FromXmlBase64String()
            };

            while (xmlReader.Read())
            {
                if (xmlReader.NodeType == XmlNodeType.EndElement && xmlReader.Name == XmlConsts.EntityTag)
                    break;

                var value = xmlReader.DeserializeValue();

                if (value != null)
                    result.Properties.Add(new KeyValuePair<string, EntityProperty>(xmlReader[XmlConsts.EntityKeyAttr],
                        value));
                else
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"Invalid attr {xmlReader[XmlConsts.EntityKeyAttr]} in entity: {result.PartitionKey} {result.RowKey}");
                    Console.ResetColor();
                }

            }

            return result;

        }


        private static EntityProperty DeserializeValue(this XmlReader xmlReader)
        {
            var value = xmlReader[XmlConsts.EntityValueAttr];
            var type = xmlReader[XmlConsts.EntityTypeAttr];
            // this string value, string type

            if (type == TypeSerializer.StrType)
                return new EntityProperty(value.FromXmlBase64String());

            if (type == TypeSerializer.BoolType)
                return new EntityProperty(value == "1");

            if (type == TypeSerializer.BinType)
                return new EntityProperty(Convert.FromBase64String(value));

            if (type == TypeSerializer.Int32Type)
                return new EntityProperty(int.Parse(value, CultureInfo.InvariantCulture));

            if (type == TypeSerializer.Int64Type)
                return new EntityProperty(long.Parse(value, CultureInfo.InvariantCulture));

            if (type == TypeSerializer.DoubleType)
                return new EntityProperty(double.Parse(value, CultureInfo.InvariantCulture));

            if (type == TypeSerializer.DateTimeType)
                return new EntityProperty(value.ParseIsoDateTime());

            return null;

        }


        public static void ReadTillTableTag(this XmlReader xmlReader, string name)
        {
            while (xmlReader.Read())
            {
                if (xmlReader.NodeType == XmlNodeType.Element && xmlReader.Name == name)
                    return;
            }
        }



        public static void WriteEntity(this XmlWriter xmlWriter, DynamicTableEntity entity)
        {
            xmlWriter.WriteStartElement(XmlConsts.EntityTag);
            xmlWriter.WriteAttributeString(XmlConsts.PartitionKeyAttr, entity.PartitionKey.ToXmlString());
            xmlWriter.WriteAttributeString(XmlConsts.RowKeyAttr, entity.RowKey.ToXmlString());

            xmlWriter.WriteEntityProperties(entity);

            xmlWriter.WriteEndElement();
        }


        private static void WriteEntityProperties(this XmlWriter xmlWriter, DynamicTableEntity entity)
        {
            foreach (var property in entity.Properties)
            {
                var value = property.Value.PropertyAsObject;

                xmlWriter.WriteStartElement(XmlConsts.EntityFieldTag);
                xmlWriter.WriteAttributeString(XmlConsts.EntityKeyAttr, property.Key);
                xmlWriter.WriteAttributeString(XmlConsts.EntityTypeAttr, value.EntityProprtyTypeToString());
                xmlWriter.WriteAttributeString(XmlConsts.EntityValueAttr, value.ToXmlString());
                xmlWriter.WriteEndElement();
            }

        }



    }
}
