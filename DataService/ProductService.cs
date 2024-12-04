using System.Text;
using Npgsql;
using RepProc4Replenit.Core;
using RepProc4Replenit.DataModels;

namespace RepProc4Replenit.DataService;

public class ProductService : PostgreService<Product>
{
    public ProductService() : base("ProductId") { }
    public async Task BulkSaveOrUpdateWithStringBuilder(StringBuilder csvData, string fields)
    {  
        await using var transaction = await RuntimeControl.PostgreConnection.BeginTransactionAsync();

        try
        {  
            string tempTableName = this.TableName + "_" + DateTime.Now.Ticks.ToString();
            
            var createTempTableQuery = PostgreBuilder.GenerateTempTableQuery<Product>(tempTableName);
            await using (var createTableCommand = new NpgsqlCommand(createTempTableQuery, RuntimeControl.PostgreConnection, transaction))
                await createTableCommand.ExecuteNonQueryAsync();
            
            
            var copyCommand = $"COPY {tempTableName} ({fields}) FROM STDIN (FORMAT csv, DELIMITER ',')";
            await using (var writer = RuntimeControl.PostgreConnection.BeginTextImport(copyCommand))
                await writer.WriteAsync(csvData.ToString());
            
            var updateQuery = $@"
                MERGE INTO {this.TableName} AS Target
                USING {tempTableName} AS Source
                ON Target.ProductId = Source.ProductId
                WHEN MATCHED THEN
                    UPDATE SET
                        AverageCalculatedDuration = Source.AverageCalculatedDuration
                WHEN NOT MATCHED THEN
                    INSERT (AverageCalculatedDuration)
                    VALUES (Source.AverageCalculatedDuration);
            ";

            await using (var updateCommand = new NpgsqlCommand(updateQuery, RuntimeControl.PostgreConnection, transaction))
                await updateCommand.ExecuteNonQueryAsync();
            
            await transaction.CommitAsync();
        }
        catch(Exception ex)
        {
            await transaction.RollbackAsync();
            throw ex;
        }
    }
    
     
    
    
    
    
}