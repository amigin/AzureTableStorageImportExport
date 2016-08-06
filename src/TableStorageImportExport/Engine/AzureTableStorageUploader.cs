using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Xml;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace TableStorageImportExport.Engine
{
    public static class AzureTableStorageUploader
    {
        private static void ReadTillTableTag(this XmlReader xmlReader, string name)
        {
            while (xmlReader.Read())
            {
                if (xmlReader.NodeType == XmlNodeType.Element && xmlReader.Name == name)
                    return;
            }
        }

        public static void UploadToDb(string connectionString, string fileName)
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();

            var settings = new XmlReaderSettings
            {
                IgnoreComments = true,
                IgnoreWhitespace = true,
            };
            using (var xmlReader = XmlReader.Create(fileName, settings))
            {
                xmlReader.ReadTillTableTag(XmlConsts.RootTag);

                while (xmlReader.Read())
                {
                    if (xmlReader.NodeType == XmlNodeType.Element && xmlReader.Name == XmlConsts.TableTag)
                        xmlReader.SaveTable(xmlReader[XmlConsts.TableNameAttr], cloudTableClient);
                }

          }


        }

        private static void SaveTable(this XmlReader xmlReader, string tableName, CloudTableClient table)
        {
            var tableRef = table.GetTableReference(tableName);
            tableRef.CreateIfNotExistsAsync().Wait();

            var entitiesToSave = new List<DynamicTableEntity>();
            var entity = xmlReader.ReadDynamicEntity();

            while (entity != null)
            {
                if (entitiesToSave.Count > 0)
                    if (entitiesToSave[0].PartitionKey != entity.PartitionKey)
                        entitiesToSave.SaveBatch(tableRef);

                entitiesToSave.Add(entity);

                if (entitiesToSave.Count == 50)
                    entitiesToSave.SaveBatch(tableRef);

                entity = xmlReader.ReadDynamicEntity();
            }


            if (entitiesToSave.Count > 0)
                entitiesToSave.SaveBatch(tableRef);
        }

        private static void SaveBatch(this ICollection<DynamicTableEntity> entities, CloudTable tableRef)
        {
            var c = entities.Count;

            var batch = new TableBatchOperation();
            foreach (var entity in entities)
                batch.InsertOrReplace(entity);
            tableRef.ExecuteBatchAsync(batch).Wait();

            entities.Clear();
            Console.WriteLine($"Saved {c} entities");
        }

        private static DynamicTableEntity ReadDynamicEntity(this XmlReader xmlReader)
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

                var value = ReadValue(xmlReader[XmlConsts.EntityTypeAttr], xmlReader[XmlConsts.EntityValueAttr]);

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


        public static EntityProperty ReadValue(string type, string value)
        {

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


        private static string FromXmlBase64String(this string value)
        {
            var bytes = Convert.FromBase64String(value);
            return Encoding.UTF8.GetString(bytes);
        }
    }
}
