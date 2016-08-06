using System.Collections.Generic;
using System.IO;
using System.Xml;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageImportExport.Table.Xml
{

    public class AzureStorageXmlReader : IAzureTableStorageStream
    {
        private readonly Stream _stream;

        private class AzureTableXmlStream : IAzureTableStream
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

        private AzureStorageXmlReader(Stream stream)
        {
            _stream = stream;
        }

        public IEnumerable<IAzureTableStream> GetTables(params string[] tableNames)
        {
            var settings = new XmlReaderSettings
            {
                IgnoreComments = true,
                IgnoreWhitespace = true,
            };
            using (var xmlReader = XmlReader.Create(_stream, settings))
            {
                xmlReader.ReadTillTableTag(XmlConsts.RootTag);

                while (xmlReader.Read())
                {
                    if (xmlReader.NodeType == XmlNodeType.Element && xmlReader.Name == XmlConsts.TableTag && !xmlReader.IsEmptyElement)
                    {
                        var tableName = xmlReader[XmlConsts.TableNameAttr];

                        if (tableNames.CanTableBeYelded(tableName))
                            yield return new AzureTableXmlStream(xmlReader, tableName);
                    }
                }

            }
        }

        public static IAzureTableStorageStream CreateStream(Stream stream)
        {
            return new AzureStorageXmlReader(stream);
        }

    }
    
}
