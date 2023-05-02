using System;
using System.Linq;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using BatchDurable.DurableFunction;
using BatchDurable.ExplicitQueueBasedFunction;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;

namespace BatchDurable;

public static class TopLevelHttpTriggers
{
    [FunctionName(nameof(TriggerDurableFunctionBatch))]
    public static async Task<IActionResult> TriggerDurableFunctionBatch(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]
        HttpRequest req,
        ILogger log,
        [DurableClient] IDurableOrchestrationClient durableOrchestrationClient)
    {
        var newRun = await durableOrchestrationClient.StartNewAsync(nameof(ProcessLargeBatch.ProcessBatch), null);
        return new OkObjectResult(new
        {
            durableInstanceId = newRun
        });
    }


    [FunctionName(nameof(TriggerQueueBatch))]
    public static async Task<IActionResult> TriggerQueueBatch(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]
        HttpRequest req,
        ILogger log,
        [Queue(queueName: "batchSplitQueue", Connection = "StorageConnectionString")] IAsyncCollector<string> batchSplitQueue,
        [DurableClient]IDurableEntityClient entityClient)
    {
        //pre-reqs create table
        var tableServiceClient = new TableServiceClient(Environment.GetEnvironmentVariable("StorageConnectionString"));
        var tableClient = tableServiceClient.GetTableClient("BatchProcess");
        await tableClient.CreateIfNotExistsAsync();

        var batchId = Guid.NewGuid().ToString();
        var customers = await BatchService.Instance.GetCustomersForBatch();
        var client = new BlobServiceClient(Environment.GetEnvironmentVariable("StorageConnectionString"));
        var container = client.GetBlobContainerClient("minibatches");
        await container.CreateIfNotExistsAsync();

        await container.UploadBlobAsync($"{batchId}/1.json", new BinaryData(customers.ToArray()));
        await entityClient.SignalEntityAsync<IBatchProcessState>(batchId, s => s.Initialise(customers.Length));
        await batchSplitQueue.AddAsync($"{batchId}/1.json");

        return new OkObjectResult(new
        {
            batchId = batchId
        });
    }
}