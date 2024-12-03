using System.Globalization;
using System.Text.Json;
using Confluent.Kafka;
using RepProc4Replenit.Core;
using RepProc4Replenit.Enums;
using RepProc4Replenit.Modules.Runtime;
using RepProc4Replenit.Objects;
using StackExchange.Redis;

namespace RepProc4Replenit.Modules.Worker;

public static class WorkerControl
{
    public static void Run()
    {
        var cts = new CancellationTokenSource(); 
        Console.CancelKeyPress += (sender, eventArgs) =>
        { 
            cts.Cancel();
            eventArgs.Cancel = true;
        };
        
        using var consumer = new ConsumerBuilder<Null, string>(KafkaClient.ConsumerConfig()).Build();
        consumer.Subscribe(RuntimeControl.WorkerChannelName);
        
        try
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var result = consumer.Consume(cts.Token);
                
                WorkerTask workerTask = JsonSerializer.Deserialize<WorkerTask>(result.Message.Value);
                
                if(workerTask.TaskType == (int)TaskTypeEnum.RunWithFile)
                    WorkerControl.RunWithFile(workerTask).Wait();
                if(workerTask.TaskType == (int)TaskTypeEnum.RunWithDb)
                    WorkerControl.RunWithDb(workerTask).Wait();
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"Consumer error: {e.Message}");
        }
        finally
        {
            consumer.Close();  
            Console.WriteLine("Consumer stopped!");
        }
    }

    public static async Task RunWithFile(WorkerTask workerTask)
    {
        string redisKey = RuntimeControl.PreDataKey(workerTask.TaskId);
        List<string> lines = await RuntimeControl.RedisPreData.GetList(redisKey, workerTask.PartIndex, workerTask.PartSize);
        
        string productKey = RuntimeControl.ProductKey();
        string customerKey = RuntimeControl.CustomerKey();
        string combineKey = RuntimeControl.CombineKey();
        
        
        IBatch redisProductBatch = RuntimeControl.RedisProduct.RedisDatabase.CreateBatch();
        IBatch redisCustomerBatch = RuntimeControl.RedisCustomer.RedisDatabase.CreateBatch();
        IBatch redisCombineBatch = RuntimeControl.RedisCombine.RedisDatabase.CreateBatch();
        
        
        foreach (string line in lines)
        {
            var columns = line.Split(',');
            // DateTime.Parse(columns[1], CultureInfo.InvariantCulture),
            
            RawItem item = new RawItem
            {
                UserReferenceId = columns[0],
                TransactionDate = columns[1],
                ProductId = long.Parse(columns[2]),
                Quantity = int.Parse(columns[3])
            };
            
            await redisProductBatch.HashSetAsync(productKey, item.ProductId.ToString(), 
                JsonSerializer.Serialize(new ProductItem()
                {
                    UserReferenceId = item.UserReferenceId,
                    TransactionDate = item.TransactionDate,
                    Quantity = item.Quantity,
                }));
             
            await redisCustomerBatch.HashSetAsync(customerKey, item.ProductId.ToString(), 
                JsonSerializer.Serialize(new CustomerItem()
                {
                    ProductId = item.ProductId,
                    TransactionDate = item.TransactionDate,
                    Quantity = item.Quantity,
                }));
            
            await redisCombineBatch.HashSetAsync(combineKey, $"{item.UserReferenceId}:{item.ProductId}", item.TransactionDate);
        }
        
        redisProductBatch.Execute();
        redisCustomerBatch.Execute();
        redisCombineBatch.Execute();
    }

    public static async Task RunWithDb(WorkerTask workerTask)
    {
        
    }
    
    
}