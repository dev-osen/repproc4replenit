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
        
        int workerCount = lineCount / maxLineCount;
        int lastTaskSize = 0;
        if (lineCount % maxLineCount > 0)
        {
            lastTaskSize = lineCount % maxLineCount;
            workerCount++;
        }
 
        if (workerCount <= 0)
            return;
         
        Task[] produceTasks = new Task[workerCount];
        for (int i = 0; i < workerCount - 1; i++)
        {
            produceTasks[i] = RabbitClient.ProduceAsync(new TaskWorker(){ TaskId = taskId, PartIndex = i, PartSize = maxLineCount, WorkerType = (int)workerType })
                .ContinueWith(task =>
                {
                    if (!task.IsCompletedSuccessfully)
                        Console.WriteLine($"Task Error: {task.Exception?.Message}");
                });
        }
        
        if(lastTaskSize > 0)
            produceTasks[workerCount - 1] = RabbitClient.ProduceAsync(new TaskWorker(){ TaskId = taskId, PartIndex = workerCount - 1, PartSize = lastTaskSize, WorkerType = (int)workerType })
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