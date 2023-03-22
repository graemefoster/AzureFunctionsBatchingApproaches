using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using BatchDurable.Durable;
using BatchDurable.PretendBatchService;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace BatchDurable;

public static class TopLevelHttpTriggers
{
    // [FunctionName("TriggerBatchHttp")]
    // public static async Task<IActionResult> RunAsync(
    //     [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req, 
    //     ILogger log, 
    //     [DurableClient] IDurableOrchestrationClient durableOrchestrationClient)
    // {
    //     var newRun = await durableOrchestrationClient.StartNewAsync(nameof(RunBatch), null);
    //     return new OkObjectResult(new
    //     {
    //         durableInstanceId = newRun
    //     });
    // }
    //
    [FunctionName("TriggerQueueHttp")]
    public static async Task<IActionResult> RunHttpAsync(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req, 
        ILogger log,
        [Queue(queueName:"controlQueue", Connection = "StorageConnectionString")] IAsyncCollector<string> controlQueue)
    {
        var batchId = Guid.NewGuid().ToString();
        var customers = await BatchService.Instance.GetCustomersForBatch();
        await BatchService.Instance.Enqueuing();
        var client = new BlobServiceClient(Environment.GetEnvironmentVariable("StorageConnectionString"));
        var container = client.GetBlobContainerClient("minibatches");
        await container.CreateIfNotExistsAsync();

        await container.UploadBlobAsync($"{batchId}/1.json", new BinaryData(customers.ToArray()));

        await BatchService.Instance.Enqueued();
        return new OkResult();
    }
}

