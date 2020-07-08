//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Locking
{
    using Newtonsoft.Json;
    using System;
    using System.Timers;

    public class Lock
    {
        bool _released = false;

        /**
         * <summary>
         * The partition key used to find the lock in Cosmos DB. This property is serialized
         * into a Cosmos DB item.
         * </summary>
         */
        [JsonProperty(PropertyName = "partitionKey")]
        public string PartitionKey { get; internal set; }

        /**
         * <summary>
         * The unique name of the lock.  This property is serialized into a Cosmos DB
         * item and used as the unique item ID.
         * </summary>
         */
        [JsonProperty(PropertyName = "id")]
        public string Name { get; internal set; }

        /**
         * <summary>
         * The lease duration of the lock in seconds.  This property is serialized
         * into a Cosmos DB item and used as an item's TTL duration.
         * </summary>
         */
        [JsonProperty(PropertyName = "ttl")]
        public int LeaseDuration { get; internal set; }

        /**
         * <summary>
         * The timestamp the lock was aquired.  This timestamp is created local to the client.
         * </summary>
         */
        [JsonIgnore]
        public DateTime TimeAcquired { get; internal set; }

        /**
         * <summary>
         * A flag to determine if the lock is still acquired by the client and has not 
         * been either expired or released.
         * </summary>
         */
        [JsonIgnore]
        public bool IsAquired
        {
            get { return !this._released && (LockUtils.Now - this.TimeAcquired).TotalSeconds < this.LeaseDuration; }
            internal set { this._released = true; }
        }

        /**
         * <summary>
         * The Cosmos DB ETag document identifier used to determine the identity of a lock
         * along with the <c>Name</c> given.
         * </summary>
         */
        [JsonIgnore]
        internal string ETag { get; set; }

        /**
         * <summary>
         * A timer which will automatically renew the lock indefinitely.  This timer
         * will only be instantiated and active if AcquireLockOptions.AutoRenew is true.
         * </summary>
         */
        [JsonIgnore]
        internal Timer AutoRenewTimer { get; set; }

        /**
         * <summary>
         * Instantiates a new Lock.  This is non-public as only the lock client should control lock object creation.
         * </summary>
         */
        internal Lock() { }
    }
}