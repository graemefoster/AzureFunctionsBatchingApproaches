﻿using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace BatchDurable.ExplicitQueueBasedFunction;

public static class QueueBasedFunctions
{
    /// <summary>
    /// This function is triggered by a new blob arriving that contains the batch information.
    /// If it decides there are too many items in the batch, it splits it into 2 and allows those new smaller batches to retrigger the function.
    /// If it is happy with the batch size, it enqueues the items onto a queue for processing.
    /// This limits the execution time of this function, protecting against any unexpected termination.
    /// </summary>
    [FunctionName("BatchSplitter")]
    public static async Task RunAsync(
        [BlobTrigger("minibatches/{batchId}/{name}", Connection = "StorageConnectionString")]
        Stream stream,
        ILogger log,
        [Queue("processQueue", Connection = "StorageConnectionString")]
        IAsyncCollector<string> processQueue,
        string batchId,
        string name)
    {
        using var reader = new StreamReader(stream);
        var customers = JsonConvert.DeserializeObject<string[]>(await reader.ReadToEndAsync())!;
        if (customers.Length > 100)
        {
            log.LogInformation("Splitting batch of {Length}", customers.Length);
            var client = new BlobServiceClient(Environment.GetEnvironmentVariable("StorageConnectionString"));
            var container = client.GetBlobContainerClient("minibatches");

            await Task.WhenAll(customers.BreakBatch(customers.Length / 2).Select((batch, idx) =>
                container.UploadBlobAsync($"{batchId}/{Path.GetFileNameWithoutExtension(name)}-{idx}.json",
                    new BinaryData(batch.ToArray()))));

            //not hugely important but try and clean up after ourselves
            await container.DeleteBlobIfExistsAsync($"{batchId}/{name}");
        }
        else
        {
            //write this batch to the queue
            log.LogInformation("Processing minibatch of {CustomersLength}", customers.Length);
            await Task.WhenAll(customers.Select(x => processQueue.AddAsync($"{batchId}|{x}")));
        }
    }

    /// <summary>
    /// This function processes an item.
    /// This example uses table-storage to detect an already processed matching request.
    /// If an existing item exists, but it's not marked as complete, then the function will re-execute the processing logic.
    /// This provides at-least once, but with a fairly primitive yet 'good enough' simple de-dupe.
    /// </summary>
    [FunctionName("CustomerTrigger")]
    public static async Task RunCustomerAsync(
        [QueueTrigger("processQueue", Connection = "StorageConnectionString")]
        string customerAndBatch,
        ILogger log)
    {
        var bits = customerAndBatch.Split('|');
        var batchId = bits[0];
        var customerId = bits[1];
        //check for the customer in table storage to reduce duplicate processing. We can handle them
        var tableClient = new TableServiceClient(Environment.GetEnvironmentVariable("StorageConnectionString"));
        var client = tableClient.GetTableClient("BatchProcess");

        try
        {
            var customer = new CustomerProcess()
            {
                CustomerId = customerId,
                Processed = false,
                PartitionKey = $"{batchId}-{customerId.Substring(5)}",
                RowKey = customerId
            };
            var upsertResult = await client.AddEntityAsync(customer)!;
            await ProcessCustomer(customer);
            customer.Processed = true;
            await client.UpdateEntityAsync(customer, upsertResult.Headers.ETag!.Value);
        }
        catch (RequestFailedException re)
        {
            if (re.Status == 409)
            {
                var existing =
                    await client.GetEntityAsync<CustomerProcess>($"{batchId}-{customerId.Substring(0, 3)}", customerId);

                //only process if not flagged as processed. Should protect against a lot of dupes. 
                if (!existing.Value.Processed)
                {
                    var customer = existing.Value;
                    await ProcessCustomer(customer!);
                    customer.Processed = true;
                    await client.UpdateEntityAsync(customer, customer.ETag);
                }
            }
        }
        log.LogInformation("Processed customer {CustomerId}", customerId);
    }

    /// <summary>
    /// Custom processing logic would go here.
    /// </summary>
    private static async Task ProcessCustomer(CustomerProcess customer)
    {
        customer.Processed = true;
        await Task.Delay(TimeSpan.FromSeconds(1));
    }

    /// <summary>
    /// Simple table-storage entity used to capture processed records to help de-dupe.
    /// </summary>
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