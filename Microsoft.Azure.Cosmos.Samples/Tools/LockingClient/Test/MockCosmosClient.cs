//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Locking.Test
{
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Cosmos.Locking;
    using Moq;
    using Newtonsoft.Json;
    using System.Threading.Tasks;

    public class MockCosmosClient
    {
        Mock<CosmosClient> _mockClient = new Mock<CosmosClient>();

        public CosmosClient Client
        {
            get { return this._mockClient.Object; }
        }

        public ConsistencyLevel AccountConsistencyLevel { get; set; } = ConsistencyLevel.Strong;
        public ConsistencyLevel? ClientConsistencyLevel { get; set; } = ConsistencyLevel.Strong;
        public MockContainer MockContainer { get; private set; } = new MockContainer();

        public MockCosmosClient()
        {
            this._mockClient
                .Setup(x => x.ClientOptions)
                .Returns(() => new CosmosClientOptions() { ConsistencyLevel = ClientConsistencyLevel });
           this. _mockClient
                .Setup(x => x.GetContainer(It.IsAny<string>(), It.IsAny<string>()))
                .Returns<string, string>((d, c) => this.MockContainer.Container);
            this._mockClient
                .Setup(x => x.ReadAccountAsync())
                .Returns(this.ReadAccountAsync);
        }

        private Task<AccountProperties> ReadAccountAsync()
        {
            if (this.AccountConsistencyLevel == ConsistencyLevel.Strong)
            {
                // Note that Moq can't be used on AccountProperties because its not abstract nor virtual.
                // So fake a mock via JSON deserialization.
                string json = $"{{ \"userConsistencyPolicy\": {{ \"DefaultConsistencyPolicy\": \"Strong\" }} }}";
                return Task.FromResult(JsonConvert.DeserializeObject<AccountProperties>(json));
            }
            throw new ConsistencyLevelException(this.AccountConsistencyLevel);
        }
    }
}
