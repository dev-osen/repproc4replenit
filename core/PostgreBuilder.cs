using System.Text;

namespace RepProc4Replenit.Core;

public static class PostgreBuilder
{
    public static string GenerateTableQuery<T>(string? indexKeyName = null) where T : class
    {
        var type = typeof(T);
        var tableName = type.Name + "s";
        var properties = type.GetProperties();

        var queryBuilder = new StringBuilder();
        queryBuilder.AppendLine($"CREATE TABLE IF NOT EXISTS {tableName} (");
        for (int i = 0; i < properties.Length; i++)
        {
            var prop = properties[i];
            var columnName = prop.Name;
            var columnType = GetPostgreSqlType(prop.PropertyType);
            string column = "";

            if (columnName.Equals("Id", StringComparison.OrdinalIgnoreCase))
                column = $"    {columnName} SERIAL PRIMARY KEY";
            else
                column = $"    {columnName} {columnType} NOT NULL";
            
            if(i != properties.Length - 1)
                column += ",";
            
            queryBuilder.AppendLine(column);
        }

        if(!string.IsNullOrEmpty(indexKeyName))
            queryBuilder.AppendLine($",    {indexKeyName} BIGINT UNIQUE NOT NULL");
         
        queryBuilder.AppendLine(");");
 
        if(!string.IsNullOrEmpty(indexKeyName))
        {
            queryBuilder.AppendLine($@"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_{tableName.ToLower()}_{indexKeyName}'
          AND n.nspname = 'public'
    ) THEN
        CREATE INDEX idx_{tableName.ToLower()}_{indexKeyName} ON {tableName} ({indexKeyName});
    END IF;
END $$;
");
        }

        return queryBuilder.ToString();
    }


    public static string GenerateTempTableQuery<T>(string tableName) where T : class
    {
        var type = typeof(T); 
        var properties = type.GetProperties();

        var queryBuilder = new StringBuilder();
        queryBuilder.AppendLine($"CREATE TABLE IF NOT EXISTS {tableName}_{DateTime.Now.Ticks} (");
        foreach (var prop in properties)
        {
            var columnName = prop.Name;
            var columnType = GetPostgreSqlType(prop.PropertyType);

            if (columnName.Equals("Id", StringComparison.OrdinalIgnoreCase))
                queryBuilder.AppendLine($"    {columnName} SERIAL PRIMARY KEY,");
            else if (columnName.Equals("DbRowId", StringComparison.OrdinalIgnoreCase))
                queryBuilder.AppendLine($"    {columnName} BIGINT UNIQUE NOT NULL,");
            else
                queryBuilder.AppendLine($"    {columnName} {columnType} NOT NULL,");
        }

        queryBuilder.Length--;
        queryBuilder.AppendLine(");");
        
        return queryBuilder.ToString();
    }
    
    
    
    
    public static string GenerateSelectQuery<T>(List<string>? columns = null, Dictionary<string, string>? conditions = null, string? orderBy = null, bool? orderByIsDesc = false, int? limit = null) where T : class
    {
        var type = typeof(T);
        var tableName = type.Name;

        var queryBuilder = new StringBuilder();
 
        var selectColumns = columns != null && columns.Any() ? string.Join(", ", columns) : "*";
        queryBuilder.AppendLine($"SELECT {selectColumns}");
        queryBuilder.AppendLine($"FROM {tableName}");
 
        if (conditions != null && conditions.Any())
        {
            var whereClauses = conditions.Select(c => $"{c.Key} {c.Value}");
            queryBuilder.AppendLine("WHERE " + string.Join(" AND ", whereClauses));
        }
 
        if (!string.IsNullOrEmpty(orderBy))
            queryBuilder.AppendLine($"ORDER BY {orderBy}{(orderByIsDesc ?? false ? " DESC" : "")}");
        
        if (limit.HasValue)
            queryBuilder.AppendLine($"LIMIT {limit.Value}");

        return queryBuilder.ToString();
    }
    
    

    private static string GetPostgreSqlType(Type type)
    {
        return type switch
        {
            Type t when t == typeof(int) => "INT",
            Type t when t == typeof(long) => "BIGINT",
            Type t when t == typeof(string) => "VARCHAR(255)",
            Type t when t == typeof(DateTime) => "TIMESTAMP",
            Type t when t == typeof(double) => "DOUBLE PRECISION",
            Type t when t == typeof(decimal) => "NUMERIC",
            _ => throw new NotSupportedException($"Type '{type.Name}' is not supported.")
        };
    }
}