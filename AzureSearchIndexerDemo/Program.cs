using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Http;

using RedDog.Search;
using RedDog.Search.Http;
using RedDog.Search.Model;

namespace AzureSearchIndexerDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            var schemaName = "SalesLT";
            var tableName = "Product";

            var connection = ApiConnection.Create("shibayan", "F44E01F9C347901528BBBECA32791F40");

            DeleteResource(connection, tableName);

            CreateIndex(connection, schemaName, tableName);

            CreateDataSource(connection, schemaName, tableName);

            CreateIndexer(connection, tableName);

            RunIndexer(connection);

            GetIndexerStatus(connection);
        }

        private static void DeleteResource(ApiConnection connection, string tableName)
        {
            var client = new IndexManagementClient(connection);

            client.DeleteIndexAsync(tableName.ToLower());

            connection.Execute(new ApiRequest("datasources/sampledb", HttpMethod.Delete)).Wait();
            connection.Execute(new ApiRequest("indexers/sampleindexer", HttpMethod.Delete)).Wait();
        }

        private static void CreateIndex(ApiConnection connection, string schemaName, string tableName)
        {
            var sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString);

            sqlConnection.Open();

            var columns = GetColumns(sqlConnection, schemaName, tableName);
            var primaryKey = GetPrimaryKey(sqlConnection, schemaName, tableName);

            var index = new Index(tableName.ToLower());

            foreach (var column in columns.Where(p => CanConvertEdmDataType(p.Item2)))
            {
                if (column.Item1 == primaryKey)
                {
                    index.WithField(column.Item1, FieldType.String, p => p.IsKey().IsRetrievable());
                }
                else
                {
                    index.WithField(column.Item1, SqlDataTypeToEdmDataType(column.Item2), p =>
                    {
                        if (p.Type == FieldType.String)
                        {
                            p.IsSearchable();
                        }
                        else
                        {
                            p.IsFilterable();
                        }

                        p.IsRetrievable();
                    });
                }
            }

            var client = new IndexManagementClient(connection);

            client.CreateIndexAsync(index).Wait();
        }

        private static void CreateDataSource(ApiConnection connection, string schemaName, string tableName)
        {
            connection.Execute(new ApiRequest("datasources", HttpMethod.Post)
            {
                Body = new
                {
                    name = "sampledb",
                    type = "azuresql",
                    credentials = new
                    {
                        connectionString = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString
                    },
                    container = new
                    {
                        name = string.Format("[{0}].[{1}]", schemaName, tableName),
                    }
                }
            }).Wait();
        }

        private static void CreateIndexer(ApiConnection connection, string tableName)
        {
            connection.Execute(new ApiRequest("indexers", HttpMethod.Post)
            {
                Body = new
                {
                    name = "sampleindexer",
                    dataSourceName = "sampledb",
                    targetIndexName = tableName.ToLower()
                }
            }).Wait();
        }

        private static void RunIndexer(ApiConnection connection)
        {
            connection.Execute(new ApiRequest("indexers/sampleindexer/run", HttpMethod.Post)).Wait();
        }

        private static void GetIndexerStatus(ApiConnection connection)
        {
            var result = connection.Execute<IndexerStatusResult>(new ApiRequest("indexers/sampleindexer/status", HttpMethod.Get)).Result;

            Console.WriteLine(result.Body.ToString());
        }

        private static List<Tuple<string, string>> GetColumns(SqlConnection connection, string schemaName, string tableName)
        {
            var command = connection.CreateCommand();

            command.CommandText = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @SchemaName AND TABLE_NAME = @TableName ORDER BY ORDINAL_POSITION";
            command.Parameters.Add(new SqlParameter("@SchemaName", schemaName));
            command.Parameters.Add(new SqlParameter("@TableName", tableName));

            using (var reader = command.ExecuteReader())
            {
                var columns = new List<Tuple<string, string>>();

                while (reader.Read())
                {
                    var columnName = reader.GetString(0);
                    var dataType = reader.GetString(1);

                    columns.Add(Tuple.Create(columnName, dataType));
                }

                return columns;
            }
        }

        private static string GetPrimaryKey(SqlConnection connection, string schemaName, string tableName)
        {
            var command = connection.CreateCommand();

            command.CommandText = "SELECT TOP (1) COLUMN_NAME FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC ON CCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME WHERE TC.TABLE_SCHEMA = @SchemaName AND TC.TABLE_NAME = @TableName AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY'";
            command.Parameters.Add(new SqlParameter("@SchemaName", schemaName));
            command.Parameters.Add(new SqlParameter("@TableName", tableName));

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    return reader.GetString(0);
                }
            }

            return null;
        }

        public static bool CanConvertEdmDataType(string dataType)
        {
            return _dataTypeMapping.ContainsKey(dataType);
        }

        private static string SqlDataTypeToEdmDataType(string dataType)
        {
            string result;

            return _dataTypeMapping.TryGetValue(dataType, out result) ? result : null;
        }

        private static readonly Dictionary<string, string> _dataTypeMapping = new Dictionary<string, string>
        {
            { "bit", "Edm.Boolean" },
            { "int", "Edm.Int32" },
            { "smallint", "Edm.Int32" },
            { "tinyint", "Edm.Int32" },
            { "bigint", "Edm.Int64" },
            { "real", "Edm.Double" },
            { "float", "Edm.Double" },
            { "smallmoney", "Edm.String" },
            { "money", "Edm.String" },
            { "decimal", "Edm.String" },
            { "numeric", "Edm.String" },
            { "char", "Edm.String" },
            { "nchar", "Edm.String" },
            { "varchar", "Edm.String" },
            { "nvarchar", "Edm.String" },
            { "smalldatetime", "Edm.DateTimeOffset" },
            { "datetime", "Edm.DateTimeOffset" },
            { "datetime2", "Edm.DateTimeOffset" },
            { "date", "Edm.DateTimeOffset" },
            { "datetimeoffset", "Edm.DateTimeOffset" },
            { "uniqueidentifer", "Edm.String" }
        };
    }

    public class IndexerStatusResult
    {
        public string Status { get; set; }
        public ExecutionHistory LastResult { get; set; }
        public ExecutionHistory[] ExecutionHistory { get; set; }
    }

    public class ExecutionHistory
    {
        public string Status { get; set; }
        public object ErrorMessage { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public Error[] Errors { get; set; }
        public int ItemsProcessed { get; set; }
        public int ItemsFailed { get; set; }
        public object InitialTrackingState { get; set; }
        public object FinalTrackingState { get; set; }
    }

    public class Error
    {
        public object Key { get; set; }
        public bool Status { get; set; }
        public string ErrorMessage { get; set; }
    }
}
