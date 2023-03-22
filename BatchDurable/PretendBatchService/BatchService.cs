using System;
using System.Linq;
using System.Threading.Tasks;

namespace BatchDurable.PretendBatchService;

class BatchService
{
    public static readonly BatchService Instance = new();
    
    private BatchStatus _batchStatus = BatchStatus.NotRunning;
    
    public bool IsBatchRunning => _batchStatus == BatchStatus.Running;
    public bool IsBatchTerminated => _batchStatus == BatchStatus.Terminated;
    public bool IsBatchCompleted => _batchStatus == BatchStatus.Completed;

    public Task<string[]> GetCustomersForBatch()
    {
        //simulating if the api was async
        return Task.FromResult(Enumerable.Range(1, 2000)
            .Select(x => $"{x}-{Guid.NewGuid().ToString().ToUpperInvariant()}").ToArray());
    }
    
    public Task Enqueuing()
    {
        _batchStatus = BatchStatus.Enqueuing;
        return Task.CompletedTask; //simulating if the api was async
    }

    public Task Enqueued()
    {
        _batchStatus = BatchStatus.Running;
        return Task.CompletedTask; //simulating if the api was async
    }


    enum BatchStatus
    {
        NotRunning,
        Enqueuing,
        Running,
        Terminated,
        Completed
    }
    
}