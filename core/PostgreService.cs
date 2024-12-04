using System.Data;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using Dapper;
using Npgsql;

namespace RepProc4Replenit.Core;

public abstract class PostgreService<T> where T : class
{
    public Type ModelType { get; set; }
    public IEnumerable<PropertyInfo> ModelProperties { get; set; }
    public string TableName { get; set; }
    
    
    public PostgreService(string? indexKeyName = null)
    {
        this.ModelType = typeof(T);
        this.TableName = this.ModelType.Name + "s";
        this.ModelProperties = this.ModelType.GetProperties();
        
        RuntimeControl.PostgreConnection.ExecuteAsync(PostgreBuilder.GenerateTableQuery<T>(indexKeyName)).Wait();
    }
    
    
    public async Task<int> GetMaxId()
    { 
        var query = $"SELECT MAX(Id) FROM {this.TableName};";
        await using var command = new NpgsqlCommand(query, RuntimeControl.PostgreConnection);
        var result = await command.ExecuteScalarAsync();
        return result == DBNull.Value ? 0 : Convert.ToInt32(result);
    }
    
    
    public async Task<List<T>> LoadDataModelRange(int startId, int endId, Func<IDataReader, T> mapFunction)
    {
        List<T> results = new List<T>();

        NpgsqlCommand command = new NpgsqlCommand(
            $"SELECT * FROM {this.TableName} WHERE Id BETWEEN @startId AND @endId",
            RuntimeControl.PostgreConnection
        );
        command.Parameters.AddWithValue("@startId", startId);
        command.Parameters.AddWithValue("@endId", endId);

        using (var reader = await command.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                results.Add(mapFunction(reader));

        return results;
    }
    
    
    public async Task<List<string>> LoadDataCsvRange(int startId, int endId, Func<IDataReader, string> mapFunction)
    {
        List<string> results = new List<string>();

        NpgsqlCommand command = new NpgsqlCommand(
            $"SELECT * FROM {this.TableName} WHERE Id BETWEEN @startId AND @endId",
            RuntimeControl.PostgreConnection
        );
        command.Parameters.AddWithValue("@startId", startId);
        command.Parameters.AddWithValue("@endId", endId);

        using (var reader = await command.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                results.Add(mapFunction(reader));

        return results;
    }
    
    
    public async Task TransferDataRange(int startId, int endId, Func<IDataReader, string> transferFunction)
    { 
        NpgsqlCommand command = new NpgsqlCommand(
            $"SELECT * FROM {this.TableName} WHERE Id BETWEEN @startId AND @endId",
            RuntimeControl.PostgreConnection
        );
        command.Parameters.AddWithValue("@startId", startId);
        command.Parameters.AddWithValue("@endId", endId);

        using (var reader = await command.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                transferFunction(reader);
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
 
    
    
    public async Task BulkInsertWithStringBuilder(StringBuilder csvData, string fields)
    {  
        var copyCommand = $"COPY {TableName} ({fields}) FROM STDIN (FORMAT csv, DELIMITER ',')";
        await using (var writer = RuntimeControl.PostgreConnection.BeginTextImport(copyCommand))
            await writer.WriteAsync(csvData.ToString());
    }
    
    public async Task BulkUpdateWithStringBuilder(StringBuilder csvData, string fields)
    {  
        await using var transaction = await RuntimeControl.PostgreConnection.BeginTransactionAsync();

        try
        {  
            string tempTableName = this.TableName + "_" + DateTime.Now.Ticks.ToString();
            var createTempTableQuery = PostgreBuilder.GenerateTempTableQuery<T>(tempTableName);

            await using (var createTableCommand = new NpgsqlCommand(createTempTableQuery, RuntimeControl.PostgreConnection, transaction))
                await createTableCommand.ExecuteNonQueryAsync();
            
            var copyCommand = $"COPY {tempTableName} ({fields}) FROM STDIN (FORMAT csv, DELIMITER ',')";
            await using (var writer = RuntimeControl.PostgreConnection.BeginTextImport(copyCommand))
                await writer.WriteAsync(csvData.ToString());
 
            var updateQuery = $@"
                UPDATE {this.TableName} AS mt
                SET
                    { string.Join(",", fields.Replace("Id,", "").Split(',').Select(p => $"{p}=tt.{p}"))}
                FROM {tempTableName} AS tt
                WHERE mt.Id = tt.Id;
            ";

            await using (var updateCommand = new NpgsqlCommand(updateQuery, RuntimeControl.PostgreConnection, transaction))
                await updateCommand.ExecuteNonQueryAsync();
            
            await transaction.CommitAsync();
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
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