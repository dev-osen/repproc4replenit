using RepProc4Replenit.Core;

namespace RepProc4Replenit.DataModels;

public class WorkerPoint
{
    public long Id { get; set; }
    public string WorkerKey { get; set; }
    public string SubWorkerKey { get; set; }
    public int RowCount { get; set; }
}