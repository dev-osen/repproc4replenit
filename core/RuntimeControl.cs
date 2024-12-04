using System.Data;
using System.Text.RegularExpressions;
using DotNetEnv;
using Microsoft.VisualBasic;
using Npgsql;
using RepProc4Replenit.Objects;

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
    
    
    public static NpgsqlConnection PostgreConnection { get; set; }
    
    
    public static string WorkerChannelName { get; set; } 
    

    public static async Task Load()
    {
        Env.Load();

        RuntimeControl.WorkerChannelName = Env.GetString("KAFKA_TASK_CHANNEL"); 
        
        RuntimeControl.RedisPreData = new RedisClient(RedisDataTypesEnum.PreData); 
        RuntimeControl.RedisCustomerProduct = new RedisClient(RedisDataTypesEnum.CustomerProduct);
        RuntimeControl.RedisCustomerProductChecker = new RedisClient(RedisDataTypesEnum.CustomerProductChecker);
        RuntimeControl.RedisTransaction = new RedisClient(RedisDataTypesEnum.Transaction);
        RuntimeControl.RedisTransactionChecker = new RedisClient(RedisDataTypesEnum.TransactionChecker);
        RuntimeControl.RedisTransactionWorker = new RedisClient(RedisDataTypesEnum.TransactionWorker);
        RuntimeControl.RedisTransactionRaw = new RedisClient(RedisDataTypesEnum.TransactionRaw);
        RuntimeControl.RedisProduct = new RedisClient(RedisDataTypesEnum.Product);
        RuntimeControl.RedisProductList = new RedisClient(RedisDataTypesEnum.ProductList);
        RuntimeControl.RedisTaskError = new RedisClient(RedisDataTypesEnum.TaskError);

        RuntimeControl.PostgreConnection = await PostgreClient.Connection();

        KafkaClient.TopicChecker(new List<string>() { RuntimeControl.WorkerChannelName }, 50).Wait();
    }



    public static async Task ErrorLog(long taskId, Exception ex)
    {
        string errorMessage = $"[MESSAGE]: {ex.Message}"; 
        try
        { 
            if (!string.IsNullOrEmpty(ex.StackTrace))
            { 
                var lineMatch = Regex.Match(ex.StackTrace, @":line (\d+)");
                if (lineMatch.Success)
                    errorMessage += $"/[LINE]: {lineMatch.Groups[1].Value}"; 
            }  
        }
        catch (Exception innerEx) {}
        
        RuntimeControl.RedisTaskError.HashSetAsync(taskId.ToString(), DateTime.Now.ToString("u"), errorMessage).Wait();
    }
    
    

    public static string PreDataKey(long taskId) => $"task-predata-{taskId}";  
    public static string CustomerProductKey() => $"customer-product"; 
    public static string TransactionKey(string UserReferenceId, string ProductId, string TransactionDate) => $"{UserReferenceId}.{ProductId}.{TransactionDate.Replace(" ", "")}";
    public static string CustProdKey(string UserReferenceId, string ProductId) => $"{UserReferenceId}.{ProductId}";
    public static string ProductItemKey(string productId) => $"product.average.{productId}";
    public static string ProductKey() => $"products";
}