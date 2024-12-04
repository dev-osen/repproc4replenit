using StackExchange.Redis;

namespace RepProc4Replenit.Core;

public static class LoggerService
{
    public static void StartServer()
    {
        Task.Run(async () =>
        {
            ISubscriber subscriber = RuntimeControl.RedisLog.RedisConnection.GetSubscriber();
            await subscriber.SubscribeAsync("logs", (channel, message) =>
            {
                Console.WriteLine(message);
            });
        });
    }



    public static string ConsumerKey { get; set; }
    public static ISubscriber LogSender { get; set; }

    public static void StartConsumer(string consumerKey)
    {
        ConsumerKey = consumerKey;
        LogSender = RuntimeControl.RedisLog.RedisConnection.GetSubscriber();
    } 
    public static async Task Send(string log) => await LogSender.PublishAsync("logs", $"[{ConsumerKey}]: {log}");
    
    
    
}