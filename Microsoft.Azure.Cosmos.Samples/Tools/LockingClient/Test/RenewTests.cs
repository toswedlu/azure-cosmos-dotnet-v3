//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Locking.Test
{
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Cosmos.Locking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Threading.Tasks;

    [TestClass]
    public class RenewTests
    {
        [TestMethod]
        public async Task WithAcquiredLock()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock",
                LeaseDuration = 120
            };
            Lock @lock = await lockClient.AcquireAsync(options);
            DateTime origTimeAcquired = @lock.TimeAcquired;
            await lockClient.RenewAsync(@lock);
            Assert.IsTrue(@lock.TimeAcquired > origTimeAcquired);
        }

        [TestMethod]
        public async Task WithReacquiredLock()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock",
                LeaseDuration = 2
            };
            Lock @lock = await lockClient.AcquireAsync(options);
            await Task.Delay(options.LeaseDuration * 1000);
            Lock newLock = await lockClient.AcquireAsync(options);
            await Assert.ThrowsExceptionAsync<LockReleasedException>(() => lockClient.RenewAsync(@lock));
        }

        [TestMethod]
        public async Task WithExpiredLock()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock",
                LeaseDuration = 2
            };
            Lock @lock = await lockClient.AcquireAsync(options);
            await Task.Delay(options.LeaseDuration * 1000);
            await Assert.ThrowsExceptionAsync<LockReleasedException>(() => lockClient.RenewAsync(@lock));
        }

        [TestMethod]
        public async Task LockHasValue()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock",
                LeaseDuration = 120
            };
            try
            {
                Lock @lock = await lockClient.AcquireAsync(options);
                await lockClient.RenewAsync(@lock);
            }
            catch
            {
                Assert.Fail();
            }
            await Assert.ThrowsExceptionAsync<ArgumentNullException>(() => lockClient.RenewAsync(null));
        }

        [TestMethod]
        public async Task CosmosExceptionThrown()
        {
            // Note: for the cosmos exception to be thrown, the status code needs to be anything but PreconditionFailed or NotFound.
            CosmosException innerEx = new CosmosException(string.Empty, System.Net.HttpStatusCode.OK, 0, string.Empty, 0);
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            mockCosmosClient.MockContainer.ExceptionToThrowOnRenew = new AggregateException(innerEx);
            LockClient client = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock"
            };
            Lock @lock = await client.AcquireAsync(options);
            await Assert.ThrowsExceptionAsync<AggregateException>(() => client.RenewAsync(@lock));
        }

        [TestMethod]
        public async Task OtherExceptionThrown()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            mockCosmosClient.MockContainer.ExceptionToThrowOnRenew = new Exception();
            LockClient client = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions acquireOptions = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock"
            };
            Lock @lock = await client.AcquireAsync(acquireOptions);
            await Assert.ThrowsExceptionAsync<Exception>(() => client.RenewAsync(@lock));
        }
    }
}
