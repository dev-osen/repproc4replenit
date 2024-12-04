using System.Text.RegularExpressions;
using DotNetEnv;
using Npgsql;

namespace RepProc4Replenit.Core;

public static class RuntimeControl
{
    public static RedisClient RedisPreData { get; set; }
    public static RedisClient RedisCustomerProduct { get; set; }
    public static RedisClient RedisCustomerProductChecker { get; set; }
    public static RedisClient RedisTransaction { get; set; }
    public static RedisClient RedisTransactionChecker { get; set; }
    public static RedisClient RedisTransactionWorker { get; set; }
    public static RedisClient RedisTransactionRaw { get; set; }
    public static RedisClient RedisProduct { get; set; }
    public static RedisClient RedisProductList { get; set; }
    public static RedisClient RedisTaskError { get; set; }
    public static RedisClient RedisLog { get; set; }
    
    
    public static NpgsqlConnection PostgreConnection { get; set; }
    
    
    public static string WorkerChannelName { get; set; } 
    
    
    public static string ProgramMode { get; set; }
    

    public static async Task Load(string programMode)
    {
        ProgramMode = programMode;
        
        Env.Load();

        WorkerChannelName = Env.GetString("KAFKA_TASK_CHANNEL"); 
        
        RedisPreData = new RedisClient(RedisDataTypesEnum.PreData); 
        RedisCustomerProduct = new RedisClient(RedisDataTypesEnum.CustomerProduct);
        RedisCustomerProductChecker = new RedisClient(RedisDataTypesEnum.CustomerProductChecker);
        RedisTransaction = new RedisClient(RedisDataTypesEnum.Transaction);
        RedisTransactionChecker = new RedisClient(RedisDataTypesEnum.TransactionChecker);
        RedisTransactionWorker = new RedisClient(RedisDataTypesEnum.TransactionWorker);
        RedisTransactionRaw = new RedisClient(RedisDataTypesEnum.TransactionRaw);
        RedisProduct = new RedisClient(RedisDataTypesEnum.Product);
        RedisProductList = new RedisClient(RedisDataTypesEnum.ProductList);
        RedisTaskError = new RedisClient(RedisDataTypesEnum.TaskError);
        RedisLog = new RedisClient(RedisDataTypesEnum.Log);

        PostgreConnection = await PostgreClient.Connection();

        KafkaClient.TopicChecker(new List<string>() { WorkerChannelName }, 50).Wait();
    }



    public static async Task ErrorLog(long taskId, Exception ex)
    {
        string errorMessage = $"[ERROR]: {ex.Message}"; 
        try
        { 
            if (!string.IsNullOrEmpty(ex.StackTrace))
            { 
                var lineMatch = Regex.Match(ex.StackTrace, @":line (\d+)");
                if (lineMatch.Success)
                    errorMessage += $" / [LINE]: {lineMatch.Groups[1].Value}"; 
            }  
        }
        catch (Exception innerEx) {}
        
        RedisTaskError.HashSetAsync(taskId.ToString(), DateTime.Now.ToString("u"), errorMessage).Wait();

        if (ProgramMode == "consumer")
            await LoggerService.Send(errorMessage);
    }
    
    

    public static string PreDataKey(long taskId) => $"task-predata-{taskId}";  
    public static string CustomerProductKey() => $"customer-product"; 
    public static string TransactionKey(string UserReferenceId, string ProductId, string TransactionDate) => $"{UserReferenceId}.{ProductId}.{TransactionDate.Replace(" ", "")}";
    public static string CustProdKey(string UserReferenceId, string ProductId) => $"{UserReferenceId}.{ProductId}";
    public static string ProductKey() => $"products";
}