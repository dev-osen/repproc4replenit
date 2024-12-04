using System.Collections.Concurrent;
using System.Text.Json;
using DotNetEnv;
using RepProc4Replenit.Core;
using RepProc4Replenit.DataModels;
using RepProc4Replenit.DataService;
using RepProc4Replenit.Objects;
using StackExchange.Redis;

namespace RepProc4Replenit.Modules.Worker;

public class FileWorker: IDisposable
{
    public ConcurrentBag<string> Lines { get; set; }
     
    public IBatch RedisCustomerProductBatch { get; set; }
    public IBatch RedisCustomerProductCheckerBatch { get; set; }
    public IBatch RedisTransactionBatch { get; set; }
    public IBatch RedisTransactionRawBatch { get; set; } 
    
    
    
    public string CustomerProductKey = RuntimeControl.CustomerProductKey();
    
    
    public async Task Run(TaskWorker taskWorker)
    {
        try
        {
            string redisKey = RuntimeControl.PreDataKey(taskWorker.TaskId);
            Lines = new ConcurrentBag<string>(await RuntimeControl.RedisPreData.GetList(redisKey, taskWorker.PartIndex, taskWorker.PartSize));
        
         
            RedisCustomerProductBatch = RuntimeControl.RedisCustomerProduct.RedisDatabase.CreateBatch();
            RedisCustomerProductCheckerBatch = RuntimeControl.RedisCustomerProductChecker.RedisDatabase.CreateBatch();
            RedisTransactionBatch = RuntimeControl.RedisTransaction.RedisDatabase.CreateBatch();
            RedisTransactionRawBatch = RuntimeControl.RedisTransactionRaw.RedisDatabase.CreateBatch();

        
            List<(int, int)> subParts = new List<(int, int)>();

            int threadMaxRowCount = Env.GetInt("FILE_THREAD_MAX_LINE_COUNT");
            int partCount = Lines.Count / threadMaxRowCount;
            for(int i = 0; i < partCount; i++)
                subParts.Add((i *  threadMaxRowCount, threadMaxRowCount));
        
            if(Lines.Count % threadMaxRowCount > 0)
                subParts.Add((partCount * threadMaxRowCount, Lines.Count % threadMaxRowCount));
        
            Parallel.ForEach(subParts, subPart => this.RunSubTask(subPart.Item1, subPart.Item2).Wait());
        
            RedisCustomerProductBatch.Execute();
            RedisCustomerProductCheckerBatch.Execute();
            RedisTransactionBatch.Execute();
            RedisTransactionRawBatch.Execute();
        }
        catch (AggregateException ex)
        {
            foreach (var innerEx in ex.InnerExceptions)
                await RuntimeControl.ErrorLog(taskWorker.TaskId, innerEx);
        }
        catch (Exception e)
        {
            await RuntimeControl.ErrorLog(taskWorker.TaskId, e);
        }
    }

    public async Task RunSubTask(int startIndex, int size)
    {
        // Id, TransactionKey, CustProdKey, UserReferenceId, ProductId, TransactionDate
        
        string line = string.Empty, dataLine = string.Empty, custProdKey = string.Empty, transactionKey = string.Empty, userReferenceId = string.Empty, transactionDate = string.Empty, productId = string.Empty;
        
        for(int i = startIndex; i < startIndex + size; i++)
        {
            line = Lines.ElementAt(i);
            var columns = line.Split(',');
            userReferenceId = columns[0];
            transactionDate = columns[1].Split('.')[0];
            productId = columns[2]; 
     
            transactionKey = RuntimeControl.TransactionKey(userReferenceId, productId, transactionDate);
            if(await RuntimeControl.RedisTransactionRaw.KeyExistsAsync(transactionKey))
                continue;

            custProdKey = RuntimeControl.CustProdKey(userReferenceId, productId);
            if(!(await RuntimeControl.RedisCustomerProductChecker.KeyExistsAsync(custProdKey)))
            {
                await RedisCustomerProductCheckerBatch.SetAddAsync(custProdKey, "1");
                await RedisCustomerProductBatch.ListRightPushAsync(CustomerProductKey, custProdKey);
            }
            
            dataLine = $"0,{transactionKey},{custProdKey},{userReferenceId},{productId},{transactionDate}";
            
            await RedisTransactionRawBatch.SetAddAsync(transactionKey, dataLine); 
            await RedisTransactionBatch.HashSetAsync(custProdKey, transactionKey, transactionDate);
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }
    
    ~FileWorker() => Dispose();
}