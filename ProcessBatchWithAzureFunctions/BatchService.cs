using System;
using System.Linq;
using System.Threading.Tasks;

namespace BatchDurable;

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
        return Task.FromResult(Enumerable.Range(1, int.Parse(Environment.GetEnvironmentVariable("BatchSize")!))
            .Select(x => $"{Guid.NewGuid().ToString().ToUpperInvariant().Substring(0, 4)}.{x}").ToArray());
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

    public Task Completed()
    {
        _batchStatus = BatchStatus.Completed;
        return Task.CompletedTask;
    }
}