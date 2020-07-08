//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Locking
{
    using System;

    internal static class LockUtils
    {
        /**
         * A utility method to standardize which 'now' to use when creating
         * timestamps.  
         */
        public static DateTime Now
        {
            get { return DateTime.UtcNow; }
        }
    }
}
