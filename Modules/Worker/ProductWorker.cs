using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using DotNetEnv;
using RepProc4Replenit.Core;
using RepProc4Replenit.DataService;
using RepProc4Replenit.Objects;
using StackExchange.Redis;

namespace RepProc4Replenit.Modules.Worker;

public class ProductWorker: IDisposable
{
     
    public ProductService ProductService { get; set; }
    
    public ConcurrentBag<string> Lines { get; set; }
    public ConcurrentBag<string> DataLines { get; set; } 
    
      
    
    public IBatch RedisProductBatch { get; set; } 
    public IBatch RedisProductListBatch { get; set; } 
    
    
       
    public string ProductKey = RuntimeControl.ProductKey();
    
    
    
    public async Task Run(TaskWorker taskWorker)
    {
        try
        {
            Lines = new ConcurrentBag<string>(await RuntimeControl.RedisCustomerProduct.GetList(ProductKey, taskWorker.PartIndex, taskWorker.PartSize));
            DataLines = new ConcurrentBag<string>();
            RedisProductBatch = RuntimeControl.RedisProduct.RedisDatabase.CreateBatch();
            RedisProductListBatch = RuntimeControl.RedisProductList.RedisDatabase.CreateBatch();
            ProductService = new ProductService();
         
            List<(int, int)> subParts = new List<(int, int)>();

            int threadMaxRowCount = Env.GetInt("PROD_THREAD_MAX_LINE_COUNT");
            int partCount = Lines.Count / threadMaxRowCount;
            for(int i = 0; i < partCount; i++)
                subParts.Add((i *  threadMaxRowCount, threadMaxRowCount));
        
            if(Lines.Count % threadMaxRowCount > 0)
                subParts.Add((partCount * threadMaxRowCount, Lines.Count % threadMaxRowCount));

        
            Parallel.ForEach(subParts, subPart => this.RunSubTask(subPart.Item1, subPart.Item2).Wait());
 
            RedisProductBatch.Execute();
            RedisProductListBatch.Execute();

            if (DataLines.Count > 0)
            {
                StringBuilder saveDataLines = new StringBuilder();
                foreach (string saveDataLine in DataLines)
                    saveDataLines.AppendLine(saveDataLine);
            
                await ProductService.BulkSaveOrUpdateWithStringBuilder(saveDataLines, "ProductId,AverageCalculatedDuration");
            }
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
        double average = 0;
        string productId;
        RedisValue[] values;
        List<double> numericValues;
        
        for(int i = startIndex; i < startIndex + size; i++)
        {
            productId = Lines.ElementAt(i);
            
            values = RuntimeControl.RedisTransaction.RedisDatabase.HashValues(productId); 
            numericValues = values.Select(value => (double.TryParse(value, out var number) ? number : 0)).ToList(); 
            average = numericValues.Any() ? numericValues.Average() : 0;
            await RedisProductListBatch.SetAddAsync($"{productId}:data", average.ToString());   
            
            DataLines.Add($"{productId},{average}");
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }
    
    ~ProductWorker() => Dispose();
}