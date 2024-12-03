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
                
                WorkerTask workerTask = JsonSerializer.Deserialize<WorkerTask>(result.Message.Value);

                if (workerTask.TaskType == (int)TaskTypeEnum.RunWithFile)
                {
                    WorkerFileProcessor fileWorker = new WorkerFileProcessor();
                    fileWorker.Run(workerTask).Wait();
                }
                    
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

   

    public static async Task RunWithDb(WorkerTask workerTask)
    {
        
    }
    
    
}