//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Locking.Test
{
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Cosmos.Locking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Net;
    using System.Threading.Tasks;

    [TestClass]
    public class ReleaseTests
    {
        [TestMethod]
        public async Task WithAcquiredLock()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient client = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 120
            };
            Lock @lock = await client.AcquireAsync(options);
            await client.ReleaseAsync(@lock);
            try
            {
                await client.AcquireAsync(options);
            }
            catch (LockUnavailableException)
            {
                Assert.Fail("Lock unavailable after release.");
            }
        }

        [TestMethod]
        public async Task WithExpiredLock()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient client = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 1
            };
            Lock @lock = await client.AcquireAsync(options);
            await Task.Delay(options.LeaseDuration * 1500);
            try
            {
                await client.ReleaseAsync(@lock);
            }
            catch
            {
                Assert.Fail();
            }
        }

        [TestMethod]
        public async Task IsAcquired()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 120
            };
            Lock @lock = await lockClient.AcquireAsync(options);
            Assert.IsTrue(@lock.IsAquired);
            await lockClient.ReleaseAsync(@lock);
            Assert.IsFalse(@lock.IsAquired);
        }

        [TestMethod]
        public async Task LockHasValue()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 120
            };
            try
            {
                Lock @lock = await lockClient.AcquireAsync(options);
                await lockClient.ReleaseAsync(@lock);
            }
            catch
            {
                Assert.Fail();
            }
            await Assert.ThrowsExceptionAsync<ArgumentNullException>(() => lockClient.ReleaseAsync(null));
        }

        [TestMethod]
        public async Task CosmosExceptionThrown()
        {
            // Note: for the cosmos exception to be thrown, the status code needs to be anything but PreconditionFailed.
            CosmosException innerEx = new CosmosException(string.Empty, HttpStatusCode.OK, 0, string.Empty, 0);
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            mockCosmosClient.MockContainer.ExceptionToThrowOnRelease = new AggregateException(innerEx);
            LockClient client = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name"
            };
            Lock @lock = await client.AcquireAsync(options);
            await Assert.ThrowsExceptionAsync<AggregateException>(() => client.ReleaseAsync(@lock));
        }

        [TestMethod]
        public async Task OtherExceptionThrown()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            mockCosmosClient.MockContainer.ExceptionToThrowOnRelease = new Exception();
            LockClient client = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name"
            };
            Lock @lock = await client.AcquireAsync(options);
            await Assert.ThrowsExceptionAsync<Exception>(() => client.ReleaseAsync(@lock));
        }
    }
}
