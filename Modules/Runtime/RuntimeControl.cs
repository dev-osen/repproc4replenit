using DotNetEnv;
using RepProc4Replenit.Core;

namespace RepProc4Replenit.Modules.Runtime;

public static class RuntimeControl
{
    public static RedisClient RedisPreData { get; set; }
    public static RedisClient RedisProduct { get; set; }
    public static RedisClient RedisCustomer { get; set; }
    public static RedisClient RedisCombine { get; set; }
    
    
    public static string WorkerChannelName { get; set; }
    public static int WorkerMaxLineCount { get; set; }

    public static void Load()
    {
        Env.Load();

        RuntimeControl.WorkerChannelName = Env.GetString("KAFKA_TASK_CHANNEL");
        RuntimeControl.WorkerMaxLineCount = Env.GetInt("WORKER_MAX_LINE_COUNT");
        
        RuntimeControl.RedisPreData = new RedisClient(RedisDataTypesEnum.PreData);
        RuntimeControl.RedisProduct = new RedisClient(RedisDataTypesEnum.PreData);
        RuntimeControl.RedisCustomer = new RedisClient(RedisDataTypesEnum.PreData);
        RuntimeControl.RedisCombine = new RedisClient(RedisDataTypesEnum.PreData);

        KafkaClient.TopicChecker(new List<string>() { RuntimeControl.WorkerChannelName }, 50).Wait();
    }


    public static string PreDataKey(long taskId) => $"task-predata-{taskId}";
    public static string ProductKey() => $"products";
    public static string CustomerKey() => $"customers";
    public static string CombineKey() => $"combines";
    
}