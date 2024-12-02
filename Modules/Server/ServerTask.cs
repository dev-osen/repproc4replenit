using System.Collections.ObjectModel;
using RepProc4Replenit.Objects;

namespace RepProc4Replenit.Modules.Server;

public class ServerTask: IDisposable
{
    public ObservableCollection<ProcessPartTask> WorkerTaskList { get; set; } = new ObservableCollection<ProcessPartTask>();
    
    
    public ServerTask()
    {
        
    }
    
    
    
    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }
    
    ~ServerTask() => Dispose();
}