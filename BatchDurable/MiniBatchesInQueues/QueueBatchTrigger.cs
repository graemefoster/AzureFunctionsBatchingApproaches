using System.Threading.Tasks;
using System;
using System.IO;
using System.Linq;
using System.Text;
using Azure.Storage.Blobs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace BatchDurable.MiniBatchesInQueues;

public static class QueueBatchTrigger
{
    public class MiniBatch
    {
        public string[] Customers { get; set; }
    }
    
    [FunctionName("QueueBatchTrigger")]
    public static async Task RunAsync(
        [QueueTrigger("controlQueue", Connection = "StorageConnectionString")]
        string batchId,
        ILogger log,
        [Queue("miniBatchQueue", Connection = "StorageConnectionString")]
        IAsyncCollector<MiniBatch> miniBatchQueue)
    {
        var client = new BlobServiceClient(Environment.GetEnvironmentVariable("StorageConnectionString"));
        var container = client.GetBlobContainerClient("minibatches");
        
        await Task.WhenAll(container.GetBlobs().Select(async miniBatch =>
        {
            var blobClient = container.GetBlobClient(miniBatch.Name);
            using var stream = new MemoryStream();
            await blobClient.DownloadToAsync(stream);
            await stream.FlushAsync();
            return miniBatchQueue.AddAsync(new MiniBatch  {Customers = JsonConvert.DeserializeObject<string[]>(Encoding.UTF8.GetString(stream.ToArray())) });
        }));
    }

    [FunctionName("MiniBatchTrigger")]
    public static async Task RunMiniBatchAsync(
        [QueueTrigger("miniBatchQueue", Connection = "StorageConnectionString")] MiniBatch miniBatch,
        ILogger log,
        [Queue("processQueue", Connection = "StorageConnectionString")]
        IAsyncCollector<string> processQueue)
    {
        await Task.WhenAll(miniBatch.Customers.Select(x => processQueue.AddAsync(x)));
    }
    
    [FunctionName("CustomerTrigger")]
    public static void RunCustomerAsync(
        [QueueTrigger("processQueue", Connection = "StorageConnectionString")]string customer,
        ILogger log)
    {
        log.LogInformation("Processing Customer {customer}", customer);
    }
}