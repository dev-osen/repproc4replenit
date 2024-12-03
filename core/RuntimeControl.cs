using System.Data;
using DotNetEnv;
using Npgsql;

namespace RepProc4Replenit.Core;

public static class RuntimeControl
{
    public static RedisClient RedisPreData { get; set; }
    public static RedisClient RedisProduct { get; set; }
    public static RedisClient RedisCustomer { get; set; }
    public static RedisClient RedisCombine { get; set; }
    public static RedisClient RedisDataKey { get; set; }
    
    
    public static NpgsqlConnection PostgreConnection { get; set; }
    
    
    public static string WorkerChannelName { get; set; }
    public static int WorkerMaxLineCount { get; set; }

    public static async Task Load()
    {
        Env.Load();

        RuntimeControl.WorkerChannelName = Env.GetString("KAFKA_TASK_CHANNEL");
        RuntimeControl.WorkerMaxLineCount = Env.GetInt("WORKER_MAX_LINE_COUNT");
        
        RuntimeControl.RedisPreData = new RedisClient(RedisDataTypesEnum.PreData);
        RuntimeControl.RedisProduct = new RedisClient(RedisDataTypesEnum.Product);
        RuntimeControl.RedisCustomer = new RedisClient(RedisDataTypesEnum.Customer);
        RuntimeControl.RedisCombine = new RedisClient(RedisDataTypesEnum.Combine);
        RuntimeControl.RedisDataKey = new RedisClient(RedisDataTypesEnum.DataKey);

        RuntimeControl.PostgreConnection = await PostgreClient.Connection();

        KafkaClient.TopicChecker(new List<string>() { RuntimeControl.WorkerChannelName }, 50).Wait();
    }


    public static string PreDataKey(long taskId) => $"task-predata-{taskId}";
    public static string ProductKey() => $"products";
    public static string CustomerKey() => $"customers";
    public static string CombineKey() => $"combines";
    
}