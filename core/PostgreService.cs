using System.Reflection;
using System.Text;
using Dapper;
using Npgsql;

namespace RepProc4Replenit.Core;

public abstract class PostgreService<T> where T : class
{
    public Type ModelType { get; set; }
    public IEnumerable<PropertyInfo> ModelProperties { get; set; }
    public string TableName { get; set; }
    
    
    public PostgreService()
    {
        this.ModelType = typeof(T);
        this.TableName = this.ModelType.Name + "s";
        this.ModelProperties = this.ModelType.GetProperties().Where(p => p.Name != "Id");
        
        RuntimeControl.PostgreConnection.ExecuteAsync(PostgreBuilder.GenerateTableQuery<T>()).Wait();
    }
    
    
    
    public async Task<IEnumerable<T>> GetData(long DbRowId, int pageSize)
    {
        var query = PostgreBuilder.GenerateSelectQuery<T>(
                conditions: new Dictionary<string, string> { { "Id", "> @DbRowId" } },
                orderBy: "DbRowId",
                limit: pageSize
            );
 
        return await RuntimeControl.PostgreConnection.QueryAsync<T>(query, new { DbRowId = DbRowId });
    }

    public async Task<T?> GetLast()
    {
        var query = PostgreBuilder.GenerateSelectQuery<T>( 
            orderBy: "Id",
            orderByIsDesc: true
        );
        
        return await RuntimeControl.PostgreConnection.QuerySingleOrDefaultAsync<T>(query);
    }
 
    public async Task InsertAsync(T entity)
    { 
        var properties = ModelProperties.Where(p => p.Name != "Id");
        var columnNames = string.Join(", ", properties.Select(p => p.Name));
        var valueParameters = string.Join(", ", properties.Select(p => $"@{p.Name}"));

        var sql = $"INSERT INTO {TableName} ({columnNames}) VALUES ({valueParameters})";

        await RuntimeControl.PostgreConnection.ExecuteAsync(sql, entity);
    }

 
    public async Task UpdateAsync(T entity, string keyColumnName, object keyValue)
    { 
        var properties = ModelProperties.Where(p => p.Name != "Id");
        var setClause = string.Join(", ", properties.Select(p => $"{p.Name} = @{p.Name}"));
        var sql = $"UPDATE {TableName} SET {setClause} WHERE {keyColumnName} = @KeyValue";

        var parameters = new DynamicParameters(entity);
        parameters.Add("KeyValue", keyValue);

        await RuntimeControl.PostgreConnection.ExecuteAsync(sql, parameters);
    }
 
    
    public async Task BulkInsertAsync(IEnumerable<T> entities)
    { 
        var properties = ModelProperties.Where(p => p.Name != "Id").ToList();
        var csvData = new StringBuilder();

        foreach (var entity in entities)
        {
            var values = properties.Select(p => FormatValue(p.GetValue(entity)));
            csvData.AppendLine(string.Join("\t", values));
        }
 
        var copyCommand = $"COPY {TableName} ({string.Join(", ", properties.Select(p => p.Name))}) FROM STDIN (FORMAT csv, DELIMITER '\t')";
        await using var writer = RuntimeControl.PostgreConnection.BeginBinaryImport(copyCommand);
        foreach (var entity in entities)
        {
             writer.StartRow();
            foreach (var property in properties)
            {
                var value = property.GetValue(entity);
                writer.Write(value ?? DBNull.Value, GetNpgsqlDbType(property.PropertyType));
            }
        }

        await writer.CompleteAsync();
    }
    
    
    public string FormatValue(object? value)
    {
        if (value == null)
            return "\\N";
        if (value is string strValue)
            return strValue.Replace("\t", " ").Replace("\n", " ");
        if (value is DateTime dateValue)
            return dateValue.ToString("yyyy-MM-dd HH:mm:ss");
        return value.ToString();
    }
    
    public NpgsqlTypes.NpgsqlDbType GetNpgsqlDbType(Type type)
    {
        return type switch
        {
            _ when type == typeof(int) => NpgsqlTypes.NpgsqlDbType.Integer,
            _ when type == typeof(long) => NpgsqlTypes.NpgsqlDbType.Bigint,
            _ when type == typeof(string) => NpgsqlTypes.NpgsqlDbType.Text,
            _ when type == typeof(DateTime) => NpgsqlTypes.NpgsqlDbType.Timestamp,
            _ when type == typeof(decimal) || type == typeof(double) => NpgsqlTypes.NpgsqlDbType.Numeric,
            _ => throw new NotSupportedException($"Type '{type.Name}' is not supported.")
        };
    }
}