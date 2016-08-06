using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;

namespace TableStorageImportExport.Engine
{

    public static class AzureTableStorageDownloader
    {

        public static void DownloadFromDb(string connectionString, string fileName)
        {

            using (var stream = new FileStream(fileName, FileMode.Create))
            {
                var xmlWriter = XmlWriter.Create(stream);



                xmlWriter.WriteStartDocument();
                xmlWriter.WriteStartElement(XmlConsts.RootTag);


                var tables = AzureTableStorageScanner.DownloadFromDb(fileName);

                foreach (var table in tables)
                {
                    xmlWriter.WriteStartElement(XmlConsts.TableTag);
                    xmlWriter.WriteAttributeString(XmlConsts.TableNameAttr, table.TableName);
                    Console.WriteLine("Saving table: " + table.TableName);


                    foreach (var entity in table.GetEntities())
                    {
                        xmlWriter.WriteStartElement(XmlConsts.EntityTag);

                        xmlWriter.WriteAttributeString(XmlConsts.PartitionKeyAttr, entity.PartitionKey.ToXmlString());
                        xmlWriter.WriteAttributeString(XmlConsts.RowKeyAttr, entity.RowKey.ToXmlString());

                        foreach (var property in entity.Properties)
                        {
                            var value = property.Value.PropertyAsObject;

                            xmlWriter.WriteStartElement(XmlConsts.EntityFieldTag);
                            xmlWriter.WriteAttributeString(XmlConsts.EntityKeyAttr, property.Key);
                            xmlWriter.WriteAttributeString(XmlConsts.EntityTypeAttr, value.EntityProprtyTypeToString());
                            xmlWriter.WriteAttributeString(XmlConsts.EntityValueAttr, value.ToXmlString());
                            xmlWriter.WriteEndElement();
                        }
                        xmlWriter.WriteEndElement();
                    }

                    xmlWriter.WriteEndElement();
                }

                xmlWriter.WriteEndElement();
                xmlWriter.WriteEndDocument();

                xmlWriter.Flush();

                stream.Flush();
            }


        }


    }
}
