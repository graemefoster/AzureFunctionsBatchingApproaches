using System.Threading.Tasks;
using System;
using System.IO;
using System.Linq;
using System.Text;
using Azure.Storage.Blobs;
using BatchDurable.Durable;
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
    //
    // [FunctionName("QueueBatchTrigger")]
    // public static async Task RunAsync(
    //     [QueueTrigger("controlQueue", Connection = "StorageConnectionString")]
    //     string batchId,
    //     ILogger log,
    //     [Queue("miniBatchQueue", Connection = "StorageConnectionString")]
    //     IAsyncCollector<MiniBatch> miniBatchQueue)
    // {
    //     var client = new BlobServiceClient(Environment.GetEnvironmentVariable("StorageConnectionString"));
    //     var container = client.GetBlobContainerClient("minibatches");
    //
    //     await Task.WhenAll(container.GetBlobs().Select(async miniBatch =>
    //     {
    //         var blobClient = container.GetBlobClient(miniBatch.Name);
    //         using var stream = new MemoryStream();
    //         await blobClient.DownloadToAsync(stream);
    //         await stream.FlushAsync();
    //         await miniBatchQueue.AddAsync(new MiniBatch
    //             { Customers = JsonConvert.DeserializeObject<string[]>(Encoding.UTF8.GetString(stream.ToArray())) });
    //     }));
    // }
    //
    
    
    [FunctionName("MaxiBatchTrigger")]
    public static async Task RunAsync(
        [BlobTrigger("minibatches/{batchId}/{name}", Connection = "StorageConnectionString")]
        Stream stream,
        ILogger log,
        [Queue("miniBatchQueue", Connection = "StorageConnectionString")]
        IAsyncCollector<MiniBatch> miniBatchQueue,
        string batchId,
        string name)
    {
        using var reader = new StreamReader(stream);
        var customers = JsonConvert.DeserializeObject<string[]>(await reader.ReadToEndAsync());
        if (customers.Length > 50)
        {
            //split batch into 2
            log.LogInformation("Splitting batch of {Length}", customers.Length);
            var client = new BlobServiceClient(Environment.GetEnvironmentVariable("StorageConnectionString"));
            var container = client.GetBlobContainerClient("minibatches");
            await container.CreateIfNotExistsAsync();

            await Task.WhenAll(customers.BreakBatch(customers.Length / 2).Select((batch, idx) =>
                container.UploadBlobAsync($"{batchId}/{Path.GetFileNameWithoutExtension(name)}-{idx}.json",
                    new BinaryData(batch.ToArray()))));
        }
        else
        {
            //write this batch to the queue
            await miniBatchQueue.AddAsync(new MiniBatch { Customers = customers });
        }
    }

    [FunctionName("MiniBatchTrigger")]
    public static async Task RunMiniBatchAsync(
        [QueueTrigger("miniBatchQueue", Connection = "StorageConnectionString")]
        MiniBatch miniBatch,
        ILogger log,
        [Queue("miniBatchQueue", Connection = "StorageConnectionString")]
        IAsyncCollector<MiniBatch> miniBatchQueue,
        [Queue("processQueue", Connection = "StorageConnectionString")]
        IAsyncCollector<string> processQueue)
    {
        if (miniBatch.Customers.Length > 50)
        {
            //split the batch by writing two new queue entries
            log.LogInformation("Large batch {CustomersLength} - splitting into 2", miniBatch.Customers.Length);
            var batch1 = miniBatch.Customers.Take(miniBatch.Customers.Length / 2).ToArray();
            var batch2 = miniBatch.Customers.Except(batch1).ToArray();
            await miniBatchQueue.AddAsync(new MiniBatch { Customers = batch1 });
            await miniBatchQueue.AddAsync(new MiniBatch { Customers = batch2 });
        }
        else
        {
            log.LogInformation("Processing minibatch of {CustomersLength}", miniBatch.Customers.Length);
            await Task.WhenAll(miniBatch.Customers.Select(x => processQueue.AddAsync(x)));
        }
    }

    [FunctionName("CustomerTrigger")]
    public static void RunCustomerAsync(
        [QueueTrigger("processQueue", Connection = "StorageConnectionString")]
        string customer,
        ILogger log)
    {
        log.LogInformation("Processing Customer {customer}", customer);
    }
}