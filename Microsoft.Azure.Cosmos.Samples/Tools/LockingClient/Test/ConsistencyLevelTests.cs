//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Locking.Test
{
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Cosmos.Locking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ConsistencyLevelTests
    {
        [TestMethod]
        public void StrongClientStrongAccount()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            mockCosmosClient.AccountConsistencyLevel = ConsistencyLevel.Strong;
            mockCosmosClient.ClientConsistencyLevel = ConsistencyLevel.Strong;
            try
            {
                new LockClient(mockCosmosClient.Client, "dbname", "containername");
            }
            catch (ConsistencyLevelException)
            {
                Assert.Fail();
            }
        }

        [TestMethod]
        public void StrongClientWeakAccount()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            mockCosmosClient.AccountConsistencyLevel = ConsistencyLevel.Session;
            mockCosmosClient.ClientConsistencyLevel = ConsistencyLevel.Strong;
            Assert.ThrowsException<ConsistencyLevelException>(() => new LockClient(mockCosmosClient.Client, "dbname", "containername"));
        }

        [TestMethod]
        public void WeakClientStrongAccount()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            mockCosmosClient.AccountConsistencyLevel = ConsistencyLevel.Strong;
            mockCosmosClient.ClientConsistencyLevel = ConsistencyLevel.Session;
            Assert.ThrowsException<ConsistencyLevelException>(() => new LockClient(mockCosmosClient.Client, "dbname", "containername"));
        }

        [TestMethod]
        public void NullClientStrongAccount()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            mockCosmosClient.AccountConsistencyLevel = ConsistencyLevel.Strong;
            mockCosmosClient.ClientConsistencyLevel = null;
            try
            {
                new LockClient(mockCosmosClient.Client, "dbname", "containername");
            }
            catch (ConsistencyLevelException)
            {
                Assert.Fail();
            }
        }

        [TestMethod]
        public void NullClientWeakAccount()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            mockCosmosClient.AccountConsistencyLevel = ConsistencyLevel.Session;
            mockCosmosClient.ClientConsistencyLevel = null;
            Assert.ThrowsException<ConsistencyLevelException>(() => new LockClient(mockCosmosClient.Client, "dbname", "containername"));
        }
    }
}
