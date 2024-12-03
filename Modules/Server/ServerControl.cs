using System.Text.Json;
using Confluent.Kafka;
using RepProc4Replenit.Core;
using RepProc4Replenit.Enums;
using RepProc4Replenit.Modules.Runtime;
using RepProc4Replenit.Objects;

namespace RepProc4Replenit.Modules.Server;

public static class ServerControl
{ 
    public static async Task RunWithFile(string csvFilePath)
    {
        IProducer<Null, string> workerProducer = new ProducerBuilder<Null, string>(KafkaClient.ProducerConfig).Build();
        
        long taskId = DateTime.Now.Ticks;
        int taskType = (int)TaskTypeEnum.RunWithFile;
        
        using CsvReader csvReader = new CsvReader(taskId, csvFilePath);
        int lineCount = csvReader.Run();
        
        int taskCount = lineCount / RuntimeControl.WorkerMaxLineCount;
        int lastTaskSize = 0;
        if (lineCount % RuntimeControl.WorkerMaxLineCount > 0)
        {
            lastTaskSize = lineCount % RuntimeControl.WorkerMaxLineCount;
            taskCount++;
        }
        
        var produceTasks = new Task[taskCount];
        for (int i = 0; i < taskCount - 1; i++)
        {
            string messageValue = JsonSerializer.Serialize(new WorkerTask(){ TaskId = taskId, PartIndex = i, PartSize = RuntimeControl.WorkerMaxLineCount, TaskType = taskType});
            produceTasks[i] = workerProducer.ProduceAsync(RuntimeControl.WorkerChannelName, new Message<Null, string> { Value = messageValue })
                .ContinueWith(task =>
                {
                    if (!task.IsCompletedSuccessfully) 
                        Console.WriteLine($"Failed to produce message: {task.Exception?.Message}");
                });
        }
        
        string extraMessage = JsonSerializer.Serialize(new WorkerTask(){ TaskId = taskId, PartIndex = taskCount - 1, PartSize = lastTaskSize, TaskType = taskType });
        produceTasks[taskCount - 1] = workerProducer.ProduceAsync(RuntimeControl.WorkerChannelName, new Message<Null, string> { Value = extraMessage })
            .ContinueWith(task =>
            {
                if (!task.IsCompletedSuccessfully) 
                    Console.WriteLine($"Failed to produce message: {task.Exception?.Message}");
            });
        
        await Task.WhenAll(produceTasks);
    }

    public static void RunWithDb(string taskId)
    {
        
    }
    



}