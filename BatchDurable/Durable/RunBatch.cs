using System;
using System.Linq;
using System.Threading.Tasks;
using BatchDurable.PretendBatchService;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;

namespace BatchDurable.Durable;

public static class RunBatch
{
    [FunctionName("RunBatch")]
    public static async Task RunOrchestrator(
        [OrchestrationTrigger] IDurableOrchestrationContext context)
    {
        var batchId = context.NewGuid();
        
        var customers = await context.CallActivityAsync<string[]>("RunBatch_FetchCustomers", null);
        await Task.WhenAll(customers.BreakBatch(200).Select(x => context.CallSubOrchestratorAsync("RunBatch_SubBatch", x.ToArray())));
        await context.CallActivityAsync("RunBatch_EnqueuedMessages", null);
    }

    [FunctionName("RunBatch_FetchCustomers")]
    public static Task<string[]> FetchCustomers([ActivityTrigger] ILogger log)
    {
        var customers = BatchService.Instance.GetCustomersForBatch();
        BatchService.Instance.Enqueuing();
        return customers;
    }
    
    [FunctionName("RunBatch_EnqueuedMessages")]
    public static Task EnqueuedMessages([ActivityTrigger] ILogger log)
    {
        return BatchService.Instance.Enqueued();
    }

    [FunctionName("RunBatch_SubBatch")]
    public static async Task RunSubOrchestrator(
        [OrchestrationTrigger] IDurableOrchestrationContext context)
    {
        var customers = context.GetInput<string[]>();
        Console.WriteLine($"Running subbatch of {customers.Length}");
        await Task.WhenAll(customers.Select(x => context.CallActivityAsync("RunBatch_SubBatch_ProcessCustomer", x)));
    }

    [FunctionName("RunBatch_SubBatch_ProcessCustomer")]
    public static Task ProcessCustomer([ActivityTrigger] string customerId, ILogger log)
    {
        Console.WriteLine($"Processing customer {customerId}");
        return Task.CompletedTask;
    }


}