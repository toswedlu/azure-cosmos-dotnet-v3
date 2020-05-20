// ----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ----------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Performance.Tests.Benchmarks
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Threading.Tasks;
    using BenchmarkDotNet.Attributes;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Cosmos.CosmosElements;
    using Microsoft.Azure.Cosmos.Fluent;
    using Microsoft.Azure.Cosmos.Json;
    using Microsoft.Azure.Cosmos.Tests;
    using Microsoft.Azure.Cosmos.Tests.Json;

    /// <summary>
    /// Benchmark for Item related operations.
    /// </summary>
    [MemoryDiagnoser]
    public class FeedBenchmarks
    {
        private readonly CosmosClient client;
        private readonly Container container;

        /// <summary>
        /// Initializes a new instance of the <see cref="ItemBenchmark"/> class.
        /// </summary>
        public FeedBenchmarks()
        {
            CosmosClientBuilder clientBuilder = new CosmosClientBuilder(
                accountEndpoint: "https://brandon-test.documents.azure.com:443/",
                authKeyOrResourceToken: "INRWQfxSV2jM9HQ9vUawZtmKXTkK4LUMaXsGqELvEcS12b7AjtNVHQb9Ghf3sSOIhGcMOIXT8EZQzgU2uMzMaw==");

            this.client = clientBuilder.Build();
            Database db = this.client.CreateDatabaseIfNotExistsAsync("BenchmarkDB").Result;
            ContainerResponse containerResponse = db.CreateContainerIfNotExistsAsync(
               id: "BenchmarkContainer",
               partitionKeyPath: "/id",
               throughput: 10000).Result;

            this.container = containerResponse;

            if (containerResponse.StatusCode == HttpStatusCode.Created)
            {
                string path = $"TestJsons/NutritionData.json";
                string json = TextFileConcatenation.ReadMultipartFile(path);
                json = JsonTestUtils.RandomSampleJson(json, seed: 42, maxNumberOfItems: 1000);

                CosmosArray cosmosArray = CosmosArray.Parse(json);
                foreach (CosmosElement document in cosmosArray)
                {
                    ItemResponse<CosmosElement> itemResponse = this.container.CreateItemAsync(document).Result;
                }
            }
        }

        [Benchmark]
        public async Task ReadFeedBaselineAsync()
        {
            FeedIterator feedIterator = this.container.GetItemQueryStreamIterator(
                requestOptions: new QueryRequestOptions()
                {
                    MaxItemCount = -1,
                    MaxConcurrency = -1,
                    MaxBufferedItemCount = -1
                });

            await DrainFeedIterator(feedIterator);
        }

        [Benchmark]
        public async Task ChangeFeedBaselineAsync()
        {
            ChangeFeedIteratorCore feedIterator = ((ContainerCore)this.container)
                .GetChangeFeedStreamIterator(
                    changeFeedRequestOptions: new ChangeFeedRequestOptions()
                    {
                        StartTime = DateTime.MinValue.ToUniversalTime()
                    }) as ChangeFeedIteratorCore;

            await DrainFeedIterator(feedIterator);
        }

        [Benchmark]
        public async Task QueryWithTextAsync()
        {
            QueryRequestOptions queryRequestOptions = new QueryRequestOptions()
            {
                MaxItemCount = -1,
                MaxBufferedItemCount = -1,
                MaxConcurrency = -1,
            };
            SetSerializationFormat(queryRequestOptions, JsonSerializationFormat.Text);

            FeedIterator feedIterator = this.container.GetItemQueryStreamIterator(
                queryText: "SELECT * FROM c",
                requestOptions: queryRequestOptions);

            await DrainFeedIterator(feedIterator);
        }

        [Benchmark]
        public async Task QueryWithBinaryAsync()
        {
            QueryRequestOptions queryRequestOptions = new QueryRequestOptions()
            {
                MaxItemCount = -1,
                MaxBufferedItemCount = -1,
                MaxConcurrency = -1,
            };
            SetSerializationFormat(queryRequestOptions, JsonSerializationFormat.Binary);

            FeedIterator feedIterator = this.container.GetItemQueryStreamIterator(
                queryText: "SELECT * FROM c",
                requestOptions: queryRequestOptions);

            await DrainFeedIterator(feedIterator);
        }

        private static async Task DrainFeedIterator(FeedIterator feedIterator)
        {
            List<Stream> streams = new List<Stream>();
            while (feedIterator.HasMoreResults)
            {
                using (ResponseMessage responseMessage = await feedIterator.ReadNextAsync())
                {
                    streams.Add(responseMessage.Content);
                }
            }
        }

        private static void SetSerializationFormat(
            QueryRequestOptions queryRequestOptions,
            JsonSerializationFormat jsonSerializationFormat)
        {
            string contentSerializationFormat = jsonSerializationFormat switch
            {
                JsonSerializationFormat.Text => "JsonText",
                JsonSerializationFormat.Binary => "CosmosBinary",
                JsonSerializationFormat.HybridRow => "HybridRow",
                _ => throw new Exception(),
            };

            CosmosSerializationFormatOptions formatOptions = new CosmosSerializationFormatOptions(
                contentSerializationFormat,
                (content) => JsonNavigator.Create(content),
                () => JsonWriter.Create(JsonSerializationFormat.Text));

            queryRequestOptions.CosmosSerializationFormatOptions = formatOptions;
        }
    }
}