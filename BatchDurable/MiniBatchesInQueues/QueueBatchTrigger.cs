using System.Threading.Tasks;
using System;
using System.IO;
using System.Linq;
using Azure;
using Azure.Data.Tables;
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
        public string[] Customers { get; set; } = default!;
        public string BatchId { get; set; } = default!;
    }
    
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
        var customers = JsonConvert.DeserializeObject<string[]>(await reader.ReadToEndAsync())!;
        if (customers.Length > 100)
        {
            //split batch into 2
            log.LogInformation("Splitting batch of {Length}", customers.Length);
            var client = new BlobServiceClient(Environment.GetEnvironmentVariable("StorageConnectionString"));
            var container = client.GetBlobContainerClient("minibatches");
            await container.CreateIfNotExistsAsync();

            await Task.WhenAll(customers.BreakBatch(customers.Length / 2).Select((batch, idx) =>
                container.UploadBlobAsync($"{batchId}/{Path.GetFileNameWithoutExtension(name)}-{idx}.json",
                    new BinaryData(batch.ToArray()))));
            
            //not hugely important but try and clean up after ourselves
            await container.DeleteBlobIfExistsAsync($"{batchId}/{name}");
        }
        else
        {
            //write this batch to the queue
            await miniBatchQueue.AddAsync(new MiniBatch { BatchId = batchId, Customers = customers });
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
        log.LogInformation("Processing minibatch of {CustomersLength}", miniBatch.Customers.Length);
        await Task.WhenAll(miniBatch.Customers.Select(x => processQueue.AddAsync($"{miniBatch.BatchId}|{x}")));
    }

    [FunctionName("CustomerTrigger")]
    public static async Task RunCustomerAsync(
        [QueueTrigger("processQueue", Connection = "StorageConnectionString")] string customerAndBatch,
        ILogger log)
    {
        var bits = customerAndBatch.Split('|');
        var batchId = bits[0];
        var customerId = bits[1];
        //check for the customer in table storage to reduce duplicate processing. We can handle them
        var tableClient = new TableServiceClient(Environment.GetEnvironmentVariable("StorageConnectionString"));
        var client = tableClient.GetTableClient("BatchProcess");
        await client.CreateIfNotExistsAsync();

        try
        {
            var existing = await client.GetEntityAsync<CustomerProcess>($"{batchId}-{customerId.Substring(0, 3)}", customerId);
            if (!existing.Value.Processed)
            {
                var customer = existing.Value;
                await ProcessCustomer(customer!);
                customer.Processed = true;
                await client.UpdateEntityAsync(customer, customer.ETag);
            }
            
            //only process if not flagged as processed. Should protect against a lot of dupes. 
        }
        catch (RequestFailedException)
        {
            var customer = new CustomerProcess()
            {
                CustomerId = customerId,
                Processed = false,
                PartitionKey = $"{batchId}-{customerId.Substring(5)}",
                RowKey = customerId
            };
            var upsertResult = await client.UpsertEntityAsync(customer)!;
            await ProcessCustomer(customer);
            customer.Processed = true;
            await client.UpdateEntityAsync(customer, upsertResult.Headers.ETag.Value);
        }
    }

    private static async Task ProcessCustomer(CustomerProcess customer)
    {
        customer.Processed = true;
        await Task.Delay(TimeSpan.FromSeconds(2));
    }
    
    public class CustomerProcess : ITableEntity
    {
        public string CustomerId { get; set; } = default!;
        public bool Processed { get; set; }
        public string PartitionKey { get; set; } = default!;
        public string RowKey { get; set; } = default!;
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }
    }
}