using System.Text.Json;
using Confluent.Kafka;
using DotNetEnv;
using RepProc4Replenit.Core;
using RepProc4Replenit.Enums;
using RepProc4Replenit.Objects;

namespace RepProc4Replenit.Modules.Server;

public static class TaskWorkerRunner
{ 
    public static async Task Run(long taskId, int lineCount, WorkerTypeEnum workerType)
    { 
        int maxLineCount = MaxLineCount(workerType);
        
        IProducer<Null, string> workerProducer = new ProducerBuilder<Null, string>(KafkaClient.ProducerConfig).Build();
        
        int taskCount = lineCount / maxLineCount;
        int lastTaskSize = 0;
        if (lineCount % maxLineCount > 0)
        {
            lastTaskSize = lineCount % maxLineCount;
            taskCount++;
        }
        
        Task[] produceTasks = new Task[taskCount];
        for (int i = 0; i < taskCount - 1; i++)
        {
            string messageValue = JsonSerializer.Serialize(new Objects.TaskWorker(){ TaskId = taskId, PartIndex = i, PartSize = maxLineCount, WorkerType = (int)workerType });
            produceTasks[i] = workerProducer.ProduceAsync(RuntimeControl.WorkerChannelName, new Message<Null, string> { Value = messageValue })
                .ContinueWith(task =>
                {
                    if (!task.IsCompletedSuccessfully) 
                        Console.WriteLine($"Task Error: {task.Exception?.Message}");
                });
        }
        
        string extraMessage = JsonSerializer.Serialize(new Objects.TaskWorker(){ TaskId = taskId, PartIndex = taskCount - 1, PartSize = lastTaskSize, WorkerType = (int)workerType });
        produceTasks[taskCount - 1] = workerProducer.ProduceAsync(RuntimeControl.WorkerChannelName, new Message<Null, string> { Value = extraMessage })
            .ContinueWith(task =>
            {
                if (!task.IsCompletedSuccessfully) 
                    Console.WriteLine($"Task Error: {task.Exception?.Message}");
            });
        
        await Task.WhenAll(produceTasks);
        
    }


    public static int MaxLineCount(WorkerTypeEnum workerType)
    {
        int result = 0;
        switch (workerType)
        {
            case WorkerTypeEnum.DataWorker: result = Env.GetInt("DATA_WORKER_MAX_LINE_COUNT"); break;
            case WorkerTypeEnum.FileWorker: result = Env.GetInt("FILE_WORKER_MAX_LINE_COUNT"); break;
            case WorkerTypeEnum.CalculationWorker: result = Env.GetInt("CALC_WORKER_MAX_LINE_COUNT"); break;
            case WorkerTypeEnum.ProductWorker: result = Env.GetInt("PROD_WORKER_MAX_LINE_COUNT"); break;
        }
        return result;
    }
    
    
}