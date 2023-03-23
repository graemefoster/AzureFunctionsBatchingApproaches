using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;

namespace BatchDurable.DurableFunction;

public static class ProcessLargeBatch
{
    /// <summary>
    /// Initial Durable function breaks the large batch into smaller chunks (in this case, of 200).
    /// Each chunk is executed as a new sub-orchestration.
    /// The outer function then 'waits' for execution of the sub-processes. We don't block a thread or anything when waiting. We just wait to be re-signalled by the
    /// durable functions runtime at some point in the future.
    /// </summary>
    [FunctionName(nameof(ProcessBatch))]
    public static async Task ProcessBatch(
        [OrchestrationTrigger] IDurableOrchestrationContext context,
        ILogger logger)
    {
        //Used so that subsequent invocations do not repeat log statements
        var safeLogger = context.CreateReplaySafeLogger(logger);

        //Only happens the first time the function runs (for this instance of the orchestration). Subsequent runs use an event-sourced result instead of calling the real system.
        safeLogger.LogInformation("Fetching batch");
        var customers = await context.CallActivityAsync<string[]>(nameof(ProcessBatch_FetchCustomers), null);

        safeLogger.LogInformation("Enqueuing batch");
        await context.CallActivityAsync(nameof(ProcessBatch_Enqueuing), null);

        //creates new orchestrations that will each process the messages.
        //Without doing this, huge batches would increase exponentially the number of 'replay' steps when the runtime handles completion events of the sub-activities.
        //Breaking the batch out into a smaller number of smaller batches will reduce this overhead.
        safeLogger.LogInformation("Breaking batch up");
        var allBatches = customers
            .BreakBatch(200)
            .Select(x => context.CallSubOrchestratorAsync(nameof(InnerBatch), x.ToArray())).ToArray();

        safeLogger.LogInformation("Broken batch up into {BatchCount} batches", allBatches.Length);

        safeLogger.LogInformation("Marked batches as enqueued", allBatches.Length);
        await context.CallActivityAsync(nameof(ProcessBatch_EnqueuedAll), null);

        //Wait for all the sub-orchestrations to finish
        await Task.WhenAll(allBatches);

        safeLogger.LogInformation("Processed all batches");

        //Mark as completed.
        await context.CallActivityAsync(nameof(ProcessBatch_CompletedAll), null);
    }

    /// <summary>
    /// Activities are where we interact with downstream systems. An orchestrator has to be deterministic.
    /// </summary>
    /// <param name="log"></param>
    /// <returns></returns>
    [FunctionName(nameof(ProcessBatch_FetchCustomers))]
    public static Task<string[]> ProcessBatch_FetchCustomers([ActivityTrigger] ILogger log)
    {
        var customers = BatchService.Instance.GetCustomersForBatch();
        BatchService.Instance.Enqueuing();
        return customers;
    }

    [FunctionName(nameof(ProcessBatch_Enqueuing))]
    public static Task ProcessBatch_Enqueuing([ActivityTrigger] ILogger log)
    {
        return BatchService.Instance.Enqueuing();
    }

    [FunctionName(nameof(ProcessBatch_EnqueuedAll))]
    public static Task ProcessBatch_EnqueuedAll([ActivityTrigger] ILogger log)
    {
        return BatchService.Instance.Enqueued();
    }

    [FunctionName(nameof(ProcessBatch_CompletedAll))]
    public static Task ProcessBatch_CompletedAll([ActivityTrigger] ILogger log)
    {
        return BatchService.Instance.Completed();
    }

    [FunctionName(nameof(InnerBatch))]
    public static async Task InnerBatch(
        [OrchestrationTrigger] IDurableOrchestrationContext context,
        ILogger logger)
    {
        var safeLogger = context.CreateReplaySafeLogger(logger);
        var customers = context.GetInput<string[]>();
        safeLogger.LogInformation("Running sub-batch of {CustomerLength}", customers.Length);
        await Task.WhenAll(customers.Select(x => context.CallActivityAsync(nameof(InnerBatch_ProcessCustomer), x)));
        safeLogger.LogInformation("Executed sub-batch");
    }

    [FunctionName(nameof(InnerBatch_ProcessCustomer))]
    public static async Task InnerBatch_ProcessCustomer([ActivityTrigger] string customerId, ILogger log)
    {
        await Task.Delay(TimeSpan.FromSeconds(1));
        log.LogInformation("Processed customer {CustomerId}", customerId);
    }
}