//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Locking
{
    public class AcquireLockOptions
    {
        /**
         * <summary>
         * The parition key used to find the lock in Cosmos DB.
         * </summary>
         */
        public string PartitionKey { get; set; }

        /**
         * <summary>
         * The unique name of the lock.
         * </summary>
         */
        public string LockName { get; set; }

        /**
         * <summary>
         * The lease duration of the lock in seconds.  This lease duration is used
         * to specify the TTL in the Cosmos DB item, which is limited to a resolution
         * of seconds.
         * </summary>
         */
        public int LeaseDuration { get; set; } = 60;

        /**
         * <summary>
         * The total amount of time to continue to retry lock aquisition in milliseconds.
         * </summary>
         */
        public int TimeoutMS { get; set; } = 0;

        /**
         * <summary>
         * The amount of time to wait in between retries when trying to aquire a lock.
         * </summary>
         */
        public int RetryWaitMS { get; set; } = 1000;

        /**
         * <summary>
         * Whether or not to create an auto-renew timer object which will periodically
         * renew the lock before it expires.
         * </summary>
         */
        public bool AutoRenew { get; set; } = false;
    }
}
