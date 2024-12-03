using System.Collections.Concurrent;
using DotNetEnv;
using RepProc4Replenit.Core;
using RepProc4Replenit.DataModels;
using RepProc4Replenit.DataService;
using RepProc4Replenit.Objects;
using StackExchange.Redis;

namespace RepProc4Replenit.Modules.Worker;

public class WorkerFileProcessor: IDisposable
{
    
    public string WorkerKey { get; set; }
    public ConcurrentBag<string> Lines { get; set; }
    public IBatch RedisProductBatch { get; set; }
    public IBatch RedisCustomerBatch { get; set; }
    public IBatch RedisCombineBatch { get; set; }
    public IBatch RedisDataKey { get; set; }
    public List<DataKey> DataKeys { get; set; }
    public DataKeyService DataKeyService { get; set; }
    
    
    
    
     
    public string CombineKey = RuntimeControl.CombineKey();
    
    
    
    public async Task Run(WorkerTask workerTask)
    { 
        string redisKey = RuntimeControl.PreDataKey(workerTask.TaskId);
        
        DataKeyService = new DataKeyService();
        WorkerKey = Guid.NewGuid().ToString();
        Lines = new ConcurrentBag<string>(await RuntimeControl.RedisPreData.GetList(redisKey, workerTask.PartIndex, workerTask.PartSize));
        DataKeys = new List<DataKey>();
        
        RedisProductBatch = RuntimeControl.RedisProduct.RedisDatabase.CreateBatch();
        RedisCustomerBatch = RuntimeControl.RedisCustomer.RedisDatabase.CreateBatch();
        RedisCombineBatch = RuntimeControl.RedisCombine.RedisDatabase.CreateBatch();
        RedisDataKey = RuntimeControl.RedisDataKey.RedisDatabase.CreateBatch();

        
        List<(int, int)> subParts = new List<(int, int)>();

        int threadMaxRowCount = Env.GetInt("THREAD_MAX_LINE_COUNT");
        int partCount = Lines.Count / threadMaxRowCount;
        for(int i = 0; i < partCount; i++)
            subParts.Add((i *  threadMaxRowCount, threadMaxRowCount));
        
        if(Lines.Count % threadMaxRowCount > 0)
            subParts.Add((partCount * threadMaxRowCount, Lines.Count % threadMaxRowCount));

        
        Parallel.ForEach(subParts, subPart => this.RunSubTask(subPart.Item1, subPart.Item2).Wait());
        
        RedisProductBatch.Execute();
        RedisCustomerBatch.Execute();
        RedisCombineBatch.Execute();
        RedisDataKey.Execute();
        
    }



    public async Task RunSubTask(int startIndex, int size)
    {
        string lineKey = string.Empty;
        string line = string.Empty;
        for(int i = startIndex; i < startIndex + size; i++)
        {
            line = Lines.ElementAt(i);
            var columns = line.Split(',');
            // DateTime.Parse(columns[1], CultureInfo.InvariantCulture),
            
            RawItem item = new RawItem
            {
                UserReferenceId = columns[0],
                TransactionDate = columns[1],
                ProductId = long.Parse(columns[2]),
                Quantity = int.Parse(columns[3])
            };

            lineKey = $"{item.UserReferenceId}:{item.ProductId}:{item.TransactionDate.Split('.')[0].Replace(" ", "")}";
            if(!(await DataKeyService.CheckKey(lineKey)))
                continue;
            
            DataKeys.Add(new DataKey(){ Key = lineKey });
            
            await RedisDataKey.SetAddAsync(lineKey, item.Quantity.ToString());
            
            await RedisCombineBatch.HashSetAsync(CombineKey, $"{item.UserReferenceId}:{item.ProductId}", item.TransactionDate);
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }
    
    ~WorkerFileProcessor() => Dispose();
}