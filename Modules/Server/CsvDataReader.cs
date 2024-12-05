using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.IO.MemoryMappedFiles;
using System.Text;
using RepProc4Replenit.Core;
using RepProc4Replenit.Objects;
using StackExchange.Redis;

namespace RepProc4Replenit.Modules.Server;

public class CsvDataReader: IDisposable
{
    public ConcurrentBag<FileBufferPart> FileBufferParts { get; } = new();
    public MemoryMappedFile MemoryMappedFile { get; set; }
    
    private string CsvFilePath { get; set; }
    private int ChunkSizeInMb { get; set; }
    private long TaskId { get; set; }
    
    
    
    public CsvDataReader(long taskId, string csvFilePath, int chunkSizeInMb = 10)
    {
        this.TaskId = taskId;
        this.CsvFilePath = csvFilePath; 
        this.ChunkSizeInMb = chunkSizeInMb;
    }
    
    ~CsvDataReader() => this.Dispose();

    public void Dispose()
    {
        this.MemoryMappedFile?.Dispose();
        GC.SuppressFinalize(this);  
    } 
    
    public int Run()
    {
        this.MemoryMappedFile = MemoryMappedFile.CreateFromFile(this.CsvFilePath, FileMode.Open);
        this.SplitChunks();
        Parallel.ForEach(this.FileBufferParts, chunk =>  this.ProcessChunk(chunk));

        return FileBufferParts.Sum(t => t.LineCount);
    }
    
    public void SplitChunks()
    {
        FileInfo fileInfo = new FileInfo(this.CsvFilePath);
        long totalSize = fileInfo.Length;
        long chunkSize = this.ChunkSizeInMb * 1024 * 1024;
 
        int index = 0;
        long currentStart = 0;
        while (currentStart < totalSize)
        {
            long currentEnd = Math.Min(currentStart + chunkSize, totalSize - 1);
            this.FileBufferParts.Add(new FileBufferPart(){ Start = currentStart, End = currentEnd, Index = index, ChunkSize = currentEnd - currentStart + 1});
            currentStart = currentEnd + 1;
            index++;
        }
    }
    
    public void ProcessChunk(FileBufferPart chunk, int bufferToleranceBytesSize = 100)
    {
        using MemoryMappedViewAccessor accessor = this.MemoryMappedFile.CreateViewAccessor(chunk.Start, chunk.ChunkSize, MemoryMappedFileAccess.Read);
         
        int bufferSize = unchecked((int)chunk.ChunkSize);
        if (chunk != this.FileBufferParts.Last())
            bufferSize += bufferToleranceBytesSize;
        byte[] buffer = new byte[bufferSize];
         
        accessor.ReadArray(0, buffer, 0, buffer.Length);
        string content = Encoding.UTF8.GetString(buffer);
        string[] lines = content.Split('\n');
        
        int counter = 0;
        long totalBytesRead = 0;
        bool isCompleted = false;
        string lineTrim;
        
        string redisKey = RuntimeControl.PreDataKey(this.TaskId);
        IBatch redisBatch = RuntimeControl.RedisPreData.RedisDatabase.CreateBatch();
        foreach (string line in lines)
        { 
            if (line == lines.First())
                continue;
          
            lineTrim = line.Trim();
            totalBytesRead += lineTrim.Length + Environment.NewLine.Length; 
            if (totalBytesRead > chunk.ChunkSize)
                isCompleted = true;

            if (!string.IsNullOrEmpty(lineTrim) && !string.IsNullOrWhiteSpace(lineTrim))
            {
                redisBatch.ListRightPushAsync(redisKey, lineTrim);
                counter++;
            }
            
            if(isCompleted)
                break;
        }
        redisBatch.Execute();
        
        chunk.LineCount = counter;
    }
}