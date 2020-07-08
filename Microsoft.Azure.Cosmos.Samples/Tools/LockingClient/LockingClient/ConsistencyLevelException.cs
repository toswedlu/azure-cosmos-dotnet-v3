//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Locking
{
    using System;

    public class ConsistencyLevelException : Exception
    {
        static string _message = "A consistency level of \"{0}\" is not supported.  Use consistency level Strong.";

        public ConsistencyLevelException(ConsistencyLevel level, Exception innerEx = null)
            : base(string.Format(_message, level), innerEx)
        {
        }
    }
}