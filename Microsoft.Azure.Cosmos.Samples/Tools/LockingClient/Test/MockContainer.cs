//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Locking.Test
{
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Cosmos.Locking;
    using Moq;
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;

    public class MockLeaseItem
    {
        public Lock Lock { get; set; }
        public string ETag { get; set; }
        public DateTime TimeAquired { get; set; }
    }

    public class MockContainer
    {
        Mock<Container> _mockContainer = new Mock<Container>();
        Dictionary<string, MockLeaseItem> _locks = new Dictionary<string, MockLeaseItem>();

        static DateTime Now { get { return DateTime.UtcNow; } }

        public Container Container
        {
            get { return this._mockContainer.Object; }
        }

        public Exception ExceptionToThrowOnRelease { get; set; }
        public Exception ExceptionToThrowOnRenew { get; set; }
        public int CreateItemCallCount { get; private set; }
        public int ReplaceItemCallCount { get; private set; }

        public MockContainer()
        {
            this._mockContainer
                .Setup(x => x.CreateItemAsync(It.IsAny<Lock>(), null, null, default))
                .Returns<Lock, PartitionKey, ItemRequestOptions, CancellationToken>(this.CreateItemAsync);
            this._mockContainer
                .Setup(x => x.DeleteItemAsync<Lock>(It.IsAny<string>(), It.IsAny<PartitionKey>(), It.IsAny<ItemRequestOptions>(), default))
                .Returns<string, PartitionKey, ItemRequestOptions, CancellationToken>(this.DeleteItemAsync);
            this._mockContainer
                .Setup(x => x.ReplaceItemAsync(It.IsAny<Lock>(), It.IsAny<string>(), null, It.IsAny<ItemRequestOptions>(), default))
                .Returns<Lock, string, PartitionKey, ItemRequestOptions, CancellationToken>(this.ReplaceItemAsync);
        }

        private Task<ItemResponse<Lock>> CreateItemAsync(Lock l, PartitionKey pk, ItemRequestOptions op, CancellationToken t)
        {
            this.CreateItemCallCount++;
            this.RemoveLockIfExpired(l.Name);
            if (this._locks.ContainsKey(l.Name))
            {
                CosmosException ex = new CosmosException(string.Empty, HttpStatusCode.Conflict, 0, string.Empty, 0);
                throw new AggregateException(string.Empty, ex);
            }
            else
            {
                string etag = Guid.NewGuid().ToString();
                MockLeaseItem lease = new MockLeaseItem() { Lock = l, ETag = etag, TimeAquired = Now };
                this._locks.Add(l.Name, lease);
                ItemResponse<Lock> response = CreateMockItemResponse(HttpStatusCode.Created, etag);
                return Task.FromResult(response);
            }
        }

        private Task<ItemResponse<Lock>> DeleteItemAsync(string s, PartitionKey pk, ItemRequestOptions op, CancellationToken t)
        {
            if (this.ExceptionToThrowOnRelease != null)
            {
                throw this.ExceptionToThrowOnRelease;
            }

            HttpStatusCode statusCode;
            this.RemoveLockIfExpired(s);
            if (this._locks.ContainsKey(s))
            {
                ItemResponse<Lock> response = CreateMockItemResponse(HttpStatusCode.NoContent, this._locks[s].ETag);
                if (!string.IsNullOrWhiteSpace(op.IfMatchEtag) && this._locks[s].ETag == op.IfMatchEtag)
                {
                    this._locks.Remove(s);
                    return Task.FromResult(response);
                }
                else
                {
                    statusCode = HttpStatusCode.PreconditionFailed;
                }
            }
            else
            {
                statusCode = HttpStatusCode.NotFound;
            }

            CosmosException ex = new CosmosException(string.Empty, statusCode, 0, string.Empty, 0);
            throw new AggregateException(string.Empty, ex);
        }

        private Task<ItemResponse<Lock>> ReplaceItemAsync(Lock l, string s, PartitionKey pk, ItemRequestOptions op, CancellationToken t)
        {
            this.ReplaceItemCallCount++;
            if (this.ExceptionToThrowOnRenew != null)
            {
                throw this.ExceptionToThrowOnRenew;
            }

            HttpStatusCode statusCode;
            this.RemoveLockIfExpired(l.Name);
            if (this._locks.ContainsKey(l.Name))
            {
                string etag = Guid.NewGuid().ToString();
                ItemResponse<Lock> response = CreateMockItemResponse(HttpStatusCode.OK, etag);
                if (!string.IsNullOrWhiteSpace(op.IfMatchEtag) && this._locks[s].ETag == op.IfMatchEtag)
                {
                    this._locks[s].Lock = l;
                    this._locks[s].ETag = etag;
                    this._locks[s].TimeAquired = Now;
                    return Task.FromResult(response);
                }
                else
                {
                    statusCode = HttpStatusCode.PreconditionFailed;
                }
            }
            else
            {
                statusCode = HttpStatusCode.NotFound;
            }

            CosmosException ex = new CosmosException(string.Empty, statusCode, 0, string.Empty, 0);
            throw new AggregateException(string.Empty, ex);
        }

        private void RemoveLockIfExpired(string name)
        {
            if (this._locks.ContainsKey(name))
            {
                Lock @lock = this._locks[name].Lock;
                int leaseDurationMS = @lock.LeaseDuration * 1000;
                TimeSpan diff = Now -this._locks[@lock.Name].TimeAquired;
                if (this._locks.ContainsKey(@lock.Name) && diff.TotalMilliseconds >= leaseDurationMS)
                {
                    this._locks.Remove(@lock.Name);
                }
            }
        }

        private static ItemResponse<Lock> CreateMockItemResponse(HttpStatusCode statusCode, string etag)
        {
            Mock<ItemResponse<Lock>> mockResponse = new Mock<ItemResponse<Lock>>();
            mockResponse.Setup(x => x.StatusCode).Returns(statusCode);
            mockResponse.Setup(x => x.ETag).Returns(etag);
            return mockResponse.Object;
        }
    }
}
