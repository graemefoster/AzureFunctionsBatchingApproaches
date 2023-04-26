using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;

namespace BatchDurable.ExplicitQueueBasedFunction;

public interface IBatchProcessState
{
    void Initialise(int messagesExpected);
    void ProcessedFile(ProcessedFileResult result);
    void ProcessedCustomer();
}

public class ProcessedFileResult
{
    public int NewBatches { get; set; }
    public int QueuedMessages { get; set; }
}

public class BatchProcessState : IBatchProcessState
{
    private readonly BatchService _batchService;

    public BatchProcessState(BatchService batchService)
    {
        _batchService = batchService;
    }

    public int FilesRemainingCount { get; set; }

    public int MessagesExpectedCount { get; set; }
    public int SentCount { get; set; }
    public int ProcessedCount { get; set; }

    public void Initialise(int messagesExpected)
    {
        FilesRemainingCount = 1;
        MessagesExpectedCount = messagesExpected;
        _batchService.Enqueuing();
    }

    [FunctionName(nameof(BatchProcessState))]
    public static Task
        Run([EntityTrigger] IDurableEntityContext ctx) //can access IoC container here to pass in other registered instances.
        => ctx.DispatchAsync<BatchProcessState>(BatchService.Instance);

    public void ProcessedFile(ProcessedFileResult processedFileResult)
    {
        FilesRemainingCount += processedFileResult.NewBatches - 1;
        SentCount += processedFileResult.QueuedMessages;
        if (FilesRemainingCount == 0)
        {
            _batchService.Enqueued();
        }
    }

    public void ProcessedCustomer()
    {
        ProcessedCount++;
        if (ProcessedCount == SentCount && FilesRemainingCount == 0)
        {
            _batchService.Completed();
        }
    }
}