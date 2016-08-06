using System;
using System.IO;
using System.Xml;

namespace TableStorageImportExport.Engine.Xml
{

    public static class AzureTableStorageFileWriter
    {
        public static void WriteToFile(this IAzureTableStorageStream stream, string fileName)
        {

            using (var fileStream = new FileStream(fileName, FileMode.Create))
            {
                var xmlWriter = XmlWriter.Create(fileStream);
                xmlWriter.WriteStartDocument();
                xmlWriter.WriteStartElement(XmlConsts.RootTag);

                xmlWriter.WriteTables(stream);

                xmlWriter.WriteEndElement();
                xmlWriter.WriteEndDocument();

                xmlWriter.Flush();

                fileStream.Flush();
            }
        }


        private static void WriteTables(this XmlWriter xmlWriter, IAzureTableStorageStream stream)
        {
            foreach (var table in stream.GetTables())
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
