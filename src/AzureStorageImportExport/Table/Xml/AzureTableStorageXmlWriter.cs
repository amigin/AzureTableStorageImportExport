using System;
using System.IO;
using System.Xml;

namespace AzureStorageImportExport.Table.Xml
{

    public static class AzureTableStorageXmlWriter
    {
        public static void WriteToStream(this IAzureTableStorageStream tableStream, Stream outStream)
        {
            var xmlWriter = XmlWriter.Create(outStream);
            xmlWriter.WriteStartDocument();
            xmlWriter.WriteStartElement(XmlConsts.RootTag);

            xmlWriter.WriteTables(tableStream);

            xmlWriter.WriteEndElement();
            xmlWriter.WriteEndDocument();

            xmlWriter.Flush();
        }


        private static void WriteTables(this XmlWriter xmlWriter, IAzureTableStorageStream tableStream)
        {
            foreach (var table in tableStream.GetTables())
            {
                xmlWriter.WriteStartElement(XmlConsts.TableTag);
                xmlWriter.WriteAttributeString(XmlConsts.TableNameAttr, table.TableName);
                Console.WriteLine("Saving table: " + table.TableName);

                foreach (var entity in table.GetEntities())
                    xmlWriter.WriteEntity(entity);

                xmlWriter.WriteEndElement();
            }

        }


    }
}
