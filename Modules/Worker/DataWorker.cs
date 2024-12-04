using System.Collections.Concurrent;
using System.Data;
using System.Globalization;
using System.Text;
using DotNetEnv;
using RepProc4Replenit.Core;
using RepProc4Replenit.DataModels;
using RepProc4Replenit.DataService;
using RepProc4Replenit.Objects;
using StackExchange.Redis;

namespace RepProc4Replenit.Modules.Worker;

public class DataWorker: IDisposable
{
    public ReplenishmentService ReplenishmentService { get; set; }
    
    
    public ConcurrentBag<string> Lines { get; set; }
    public IBatch RedisProductCustomerBatch { get; set; }
    public IBatch RedisProductCustomerCheckerBatch { get; set; }
    public IBatch RedisTransactionBatch { get; set; }
    public IBatch RedisTransactionRawBatch { get; set; } 
    
    
    
    public string CustomerProductKey = RuntimeControl.CustomerProductKey();
    
    
    public async Task Run(TaskWorker taskWorker)
    {
        try
        {
            int startId = taskWorker.PartIndex * taskWorker.PartSize;
            int endId = (taskWorker.PartIndex + 1) * taskWorker.PartSize;
            
            // Id, TransactionKey, CustProdKey, UserReferenceId, ProductId, TransactionDate
            ReplenishmentService = new ReplenishmentService();
            Lines =  new ConcurrentBag<string>(await ReplenishmentService.LoadDataCsvRange(startId, endId, (IDataReader reader) => 
                $"{reader.GetString(reader.GetOrdinal("Id"))}," +
                $"{reader.GetString(reader.GetOrdinal("TransactionKey"))}," +
                $"{reader.GetString(reader.GetOrdinal("CustProdKey"))}," +
                $"{reader.GetString(reader.GetOrdinal("UserReferenceId"))}," +
                $"{reader.GetString(reader.GetOrdinal("ProductId"))}," +
                $"{reader.GetString(reader.GetOrdinal("TransactionDate"))}"));
            
            
            RedisProductCustomerBatch = RuntimeControl.RedisCustomerProduct.RedisDatabase.CreateBatch();
            RedisProductCustomerCheckerBatch = RuntimeControl.RedisCustomerProductChecker.RedisDatabase.CreateBatch();
            RedisTransactionBatch = RuntimeControl.RedisTransaction.RedisDatabase.CreateBatch();
            RedisTransactionRawBatch = RuntimeControl.RedisTransactionRaw.RedisDatabase.CreateBatch();
            
            List<(int, int)> subParts = new List<(int, int)>();

            int threadMaxRowCount = Env.GetInt("DATA_THREAD_MAX_LINE_COUNT");
            int partCount = Lines.Count / threadMaxRowCount;
            for(int i = 0; i < partCount; i++)
                subParts.Add((i *  threadMaxRowCount, threadMaxRowCount));
            
            if(Lines.Count % threadMaxRowCount > 0)
                subParts.Add((partCount * threadMaxRowCount, Lines.Count % threadMaxRowCount));
            
            Parallel.ForEach(subParts, subPart => this.RunSubTask(subPart.Item1, subPart.Item2).Wait());
            
            RedisProductCustomerBatch.Execute();
            RedisProductCustomerCheckerBatch.Execute();
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
        
        string dataLine = string.Empty, custProdKey = string.Empty, transactionKey = string.Empty, transactionDate = string.Empty;
        
        for(int i = startIndex; i < startIndex + size; i++)
        {
            dataLine = Lines.ElementAt(i);
            var columns = dataLine.Split(',');
            transactionKey = columns[1];
            custProdKey = columns[2]; 
            transactionDate = columns[5].Split('.')[0];
  
            if(await RuntimeControl.RedisTransactionRaw.KeyExistsAsync(transactionKey))
                continue;
  
            if(!(await RuntimeControl.RedisCustomerProductChecker.KeyExistsAsync(custProdKey)))
            {
                await RedisProductCustomerCheckerBatch.SetAddAsync(custProdKey, "1");
                await RedisProductCustomerBatch.ListRightPushAsync(CustomerProductKey, custProdKey);
            }
            
            await RedisTransactionRawBatch.SetAddAsync(transactionKey, dataLine); 
            await RedisTransactionBatch.HashSetAsync(custProdKey, transactionKey, transactionDate);
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }
    
    ~DataWorker() => Dispose();
}