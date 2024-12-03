using System.Text.Json;
using Confluent.Kafka;
using RepProc4Replenit.Core;
using RepProc4Replenit.DataModels;
using RepProc4Replenit.DataService;
using RepProc4Replenit.Enums; 
using RepProc4Replenit.Objects;

namespace RepProc4Replenit.Modules.Server;

public static class ServerControl
{  
    public static async Task RunWithFile(string csvFilePath)
    {
        IProducer<Null, string> workerProducer = new ProducerBuilder<Null, string>(KafkaClient.ProducerConfig).Build();
        
        
        ProcessTaskService taskService = new ProcessTaskService();
        await taskService.Create(csvFilePath);
        long taskId = taskService.CurrentTask.TaskId;
        
        
        using CsvReader csvReader = new CsvReader(taskId, csvFilePath);
        int lineCount = csvReader.Run();
        

        await taskService.UpdateStatus(TaskStatusesEnum.FileReady);
        
        
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
            string messageValue = JsonSerializer.Serialize(new WorkerTask(){ TaskId = taskId, PartIndex = i, PartSize = RuntimeControl.WorkerMaxLineCount, TaskType = (int)TaskTypeEnum.RunWithFile});
            produceTasks[i] = workerProducer.ProduceAsync(RuntimeControl.WorkerChannelName, new Message<Null, string> { Value = messageValue })
                .ContinueWith(task =>
                {
                    if (!task.IsCompletedSuccessfully) 
                        Console.WriteLine($"Task Error: {task.Exception?.Message}");
                });
        }
        
        string extraMessage = JsonSerializer.Serialize(new WorkerTask(){ TaskId = taskId, PartIndex = taskCount - 1, PartSize = lastTaskSize, TaskType = (int)TaskTypeEnum.RunWithFile });
        produceTasks[taskCount - 1] = workerProducer.ProduceAsync(RuntimeControl.WorkerChannelName, new Message<Null, string> { Value = extraMessage })
            .ContinueWith(task =>
            {
                if (!task.IsCompletedSuccessfully) 
                    Console.WriteLine($"Task Error: {task.Exception?.Message}");
            });
        
        
        await taskService.UpdateStatus(TaskStatusesEnum.Processing);
        
        await Task.WhenAll(produceTasks);
    }

    public static void RunWithDb(string taskId)
    {
        
    }
    



}