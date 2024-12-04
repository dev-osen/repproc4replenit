using System.Globalization;
using System.Text.Json;
using Confluent.Kafka;
using RepProc4Replenit.Core;
using RepProc4Replenit.DataModels;
using RepProc4Replenit.DataService;
using RepProc4Replenit.Enums; 
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
                
                TaskWorker? taskWorker = JsonSerializer.Deserialize<TaskWorker>(result.Message.Value);
                
                if (taskWorker?.WorkerType == (int)WorkerTypeEnum.DataWorker)
                {
                    using DataWorker fileFileWorker = new DataWorker();
                    fileFileWorker.Run(taskWorker).Wait();
                }
                
                if (taskWorker?.WorkerType == (int)WorkerTypeEnum.FileWorker)
                {
                    using FileWorker fileFileWorker = new FileWorker();
                    fileFileWorker.Run(taskWorker).Wait();
                }
                
                if (taskWorker?.WorkerType == (int)WorkerTypeEnum.CalculationWorker)
                {
                    using CalculationWorker fileFileWorker = new CalculationWorker();
                    fileFileWorker.Run(taskWorker).Wait();
                }
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
    
}