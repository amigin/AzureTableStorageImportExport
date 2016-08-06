using System;
using System.Collections.Generic;
using System.Xml;
using Microsoft.WindowsAzure.Storage.Table;

namespace TableStorageImportExport.Engine.Xml
{



    public class AzureStorageXmlReader : IAzureTableStorageStream
    {

        public class AzureTableXmlStream : IAzureTableStream
        {
            private readonly XmlReader _xmlReader;

            internal AzureTableXmlStream(XmlReader xmlReader, string tableName)
            {
                _xmlReader = xmlReader;
                TableName = tableName;
            }

            public string TableName { get; }


            public IEnumerable<DynamicTableEntity> GetEntities()
            {
                var entity = _xmlReader.ReadTableStorageEntity();
                while (entity != null)
                {
                    yield return entity;
                    entity = _xmlReader.ReadTableStorageEntity();
                }
            }

        }

        private readonly string _fileName;

        public AzureStorageXmlReader(string fileName)
        {
            _fileName = fileName;
        }



        public IEnumerable<IAzureTableStream> GetTables()
        {
            var settings = new XmlReaderSettings
            {
                IgnoreComments = true,
                IgnoreWhitespace = true,
            };
            using (var xmlReader = XmlReader.Create(_fileName, settings))
            {
                xmlReader.ReadTillTableTag(XmlConsts.RootTag);

                while (xmlReader.Read())
                {
                    if (xmlReader.NodeType == XmlNodeType.Element && xmlReader.Name == XmlConsts.TableTag)
                    {
                        var azureTableStream = new AzureTableXmlStream(xmlReader, xmlReader[XmlConsts.TableNameAttr]);
                        yield return azureTableStream;
                    }
                }

            }
        }



        public static IAzureTableStorageStream CreateStream(string fileName)
        {
            return new AzureStorageXmlReader(fileName);
        }



    }
    
}
