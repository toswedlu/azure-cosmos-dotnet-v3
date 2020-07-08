//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Locking
{
    using System;

    public class LockUnavailableException : Exception
    {
        static string _message = "The lock with partition key: \"{0}\" and name: \"{1}\" is unavailable.";

        public LockUnavailableException(string partitionKey, string name, Exception innerEx = null)
            : base(string.Format(_message, partitionKey, name), innerEx)
        {
        }

        public LockUnavailableException(Lock @lock, Exception innerEx = null)
            : base(string.Format(_message, @lock.PartitionKey, @lock.Name), innerEx)
        {
        }
    }
}