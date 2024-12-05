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
    public static string ConsumerKey = Guid.NewGuid().ToString("N");
    
    public static async Task Run()
    { 
        LoggerService.StartConsumer(ConsumerKey);
        
        try
        {
            RabbitClient.Consume<TaskWorker>(taskWorker =>
            {
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
                
            }, RuntimeControl.ProgramCancellation.Token);
            
            await Task.Delay(Timeout.Infinite, RuntimeControl.ProgramCancellation.Token);
        }
        catch (TaskCanceledException tce){}
        catch (Exception e)
        {
            LoggerService.Send($"Consumer error: {e.Message}");
        }
        finally
        { 
            LoggerService.Send("Consumer stopped!");
        }
    } 
    
}