using RepProc4Replenit.Core; 
using RepProc4Replenit.DataService;
using RepProc4Replenit.Enums;
using StackExchange.Redis;

namespace RepProc4Replenit.Modules.Server;

public static class ServerControl
{
    public static async Task Run(string csvFilePath)
    {
        try
        {
            Console.WriteLine($"[INFO]: Program started!");
            ProcessTaskService taskService = new ProcessTaskService();
            await taskService.Create(csvFilePath);
            long taskId = taskService.CurrentTask.TaskId;
            
            
            Console.WriteLine($"[INFO]: DataCollection started!");
            await taskService.UpdateStatus(TaskStatusesEnum.DataCollection);
            ReplenishmentService replenishmentService = new ReplenishmentService();
            int maxId = await replenishmentService.GetMaxId();
            await TaskWorkerRunner.Run(taskId, maxId, WorkerTypeEnum.DataWorker);
            
            
            Console.WriteLine($"[INFO]: FileReading started!");
            await taskService.UpdateStatus(TaskStatusesEnum.FileReading);
            using CsvDataReader csvDataReader = new CsvDataReader(taskId, csvFilePath);
            int lineCount = csvDataReader.Run();
             
            
            Console.WriteLine($"[INFO]: DataProcessing started!");
            await taskService.UpdateStatus(TaskStatusesEnum.DataProcessing);
            await TaskWorkerRunner.Run(taskId, lineCount, WorkerTypeEnum.FileWorker);
            
            
            Console.WriteLine($"[INFO]: CalculationProcessing started!");
            await taskService.UpdateStatus(TaskStatusesEnum.CalculationProcessing);
            long custProdCount = await RuntimeControl.RedisCustomerProduct.RedisDatabase.ListLengthAsync(RuntimeControl.CustomerProductKey());
            await TaskWorkerRunner.Run(taskId, unchecked((int)custProdCount), WorkerTypeEnum.CalculationWorker);
            
            
            Console.WriteLine($"[INFO]: ProductProcessing started!");
            await taskService.UpdateStatus(TaskStatusesEnum.ProductProcessing);
            long productCount = await RuntimeControl.RedisProduct.RedisDatabase.ListLengthAsync(RuntimeControl.ProductKey());
            await TaskWorkerRunner.Run(taskId, unchecked((int)productCount), WorkerTypeEnum.ProductWorker);


            if (await RuntimeControl.RedisTaskError.KeyExistsAsync(taskId.ToString()))
            {
                Console.WriteLine($"[INFO]: Program completed with error!");
                await taskService.UpdateStatus(TaskStatusesEnum.CompletedWithError);
                HashEntry[]? errors = await RuntimeControl.RedisTransaction.RedisDatabase.HashGetAllAsync(taskId.ToString());
                if(errors?.Length > 0)
                    foreach (HashEntry error in errors)
                        Console.WriteLine($"[ERROR][{error.Name}]: {error.Value}");
            }
            else
            {
                Console.WriteLine($"[INFO]: Program completed successfully!");
                await taskService.UpdateStatus(TaskStatusesEnum.CompletedWithSuccess);
            }
        }
        catch (AggregateException ex)
        {
            foreach (var innerEx in ex.InnerExceptions)
                Console.WriteLine(innerEx.Message);
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }
}