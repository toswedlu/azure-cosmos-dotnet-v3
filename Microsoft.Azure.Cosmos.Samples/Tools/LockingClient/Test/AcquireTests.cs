//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Locking.Test
{
    using Microsoft.Azure.Cosmos.Locking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Threading.Tasks;

    [TestClass]
    public class AcquireTests
    {
        [TestMethod]
        public async Task FailAfterTimeout()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                TimeoutMS = 2000,
                RetryWaitMS = 1000
            };
            await lockClient.AcquireAsync(options);
            await Assert.ThrowsExceptionAsync<LockUnavailableException>(() => lockClient.AcquireAsync(options));
            Assert.AreEqual(4, mockCosmosClient.MockContainer.CreateItemCallCount);
        }

        [TestMethod]
        public async Task FailWithZeroTimeout()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                TimeoutMS = 0
            };
            await lockClient.AcquireAsync(options);
            await Assert.ThrowsExceptionAsync<LockUnavailableException>(() => lockClient.AcquireAsync(options));
            Assert.AreEqual(2, mockCosmosClient.MockContainer.CreateItemCallCount);
        }

        [TestMethod]
        public async Task WithExpiredLock()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 1
            };
            await lockClient.AcquireAsync(options);
            await Task.Delay(options.LeaseDuration * 1000);
            try
            {
                await lockClient.AcquireAsync(options);
            }
            catch (LockUnavailableException)
            {
                Assert.Fail("Lock unavailable after expiration.");
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
                LeaseDuration = 2
            };
            Lock @lock = await lockClient.AcquireAsync(options);
            Assert.IsTrue(@lock.IsAquired);
            await Task.Delay(options.LeaseDuration * 1000);
            Assert.IsFalse(@lock.IsAquired);
        }

        [TestMethod]
        public async Task ParitionKeyHasValue()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = string.Empty,
                LockName = "test-name"
            };
            await Assert.ThrowsExceptionAsync<ArgumentException>(() => lockClient.AcquireAsync(options));

            options.PartitionKey = null;
            await Assert.ThrowsExceptionAsync<ArgumentException>(() => lockClient.AcquireAsync(options));

            try
            {
                options.PartitionKey = "test-key";
                await lockClient.AcquireAsync(options);
            }
            catch
            {
                Assert.Fail();
            }
        }

        [TestMethod]
        public async Task LockNameHasValue()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = string.Empty
            };
            await Assert.ThrowsExceptionAsync<ArgumentException>(() => lockClient.AcquireAsync(options));

            options.LockName = null;
            await Assert.ThrowsExceptionAsync<ArgumentException>(() => lockClient.AcquireAsync(options));

            try
            {
                options.LockName = "test-lock";
                await lockClient.AcquireAsync(options);
            }
            catch
            {
                Assert.Fail();
            }
        }

        [TestMethod]
        public async Task OptionsHasValue()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name"
            };
            try
            {
                await lockClient.AcquireAsync(options);
            }
            catch
            {
                Assert.Fail();
            }

            await Assert.ThrowsExceptionAsync<ArgumentNullException>(() => lockClient.AcquireAsync(null));
        }

        [TestMethod]
        public async Task TimeoutMSHasProperValue()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                TimeoutMS = -1
            };
            await Assert.ThrowsExceptionAsync<ArgumentException>(() => lockClient.AcquireAsync(options));

            try
            {
                options.TimeoutMS = 1;
                await lockClient.AcquireAsync(options);
            }
            catch
            {
                Assert.Fail();
            }
        }

        [TestMethod]
        public async Task RetryMSHasValue()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                RetryWaitMS = -1
            };
            await Assert.ThrowsExceptionAsync<ArgumentException>(() => lockClient.AcquireAsync(options));

            try
            {
                options.RetryWaitMS = 1;
                await lockClient.AcquireAsync(options);
            }
            catch
            {
                Assert.Fail();
            }
        }
    }
}