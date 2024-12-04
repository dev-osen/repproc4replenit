using RepProc4Replenit.Core;
using RepProc4Replenit.DataModels;
using RepProc4Replenit.Enums;

namespace RepProc4Replenit.DataService;

public class ProcessTaskService: PostgreService<ProcessTask>
{
    public ProcessTask CurrentTask { get; set; }
    
    public async Task<ProcessTask> Create(string filePath)
    { 
        ProcessTask task = new ProcessTask()
        {
            TaskId = DateTime.Now.Ticks,
            FilePath = filePath,
            Status = (int)TaskStatusesEnum.Started
        };

        await InsertAsync(task);

        this.CurrentTask = task;
        
        return task;
    }

    public async Task UpdateStatus(TaskStatusesEnum status)
    {
        this.CurrentTask.Status = (int)status;
        await UpdateAsync(this.CurrentTask, "TaskId", this.CurrentTask.TaskId);
    }
    
}