//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Locking
{
    using System;
    using System.Net;
    using System.Threading.Tasks;
    using System.Timers;

    /**
     * <summary>
     * A simple locking library built on top of Cosmos DB for managing distributed locks relying on
     * Cosmos DB's Strong consitency level.  To avoid clock skew issues, lease duration is defined 
     * using Cosmos DB's item-level TTL functionality.  As long as a lock exists in the database,
     * it has been acquired.  Upon release, the lock is deleted.  Upon expiration, the lock is removed
     * according to its TTL.  During a 'renew' operation, the item is touched, forcing its timestamp to be
     * updated and its TTL timer to be restarted.  Everytime a lock item is created or renewed, its ETag is 
     * updated correspondingly.  The ETag is used to uniquely identify a lock along with the lock's name.
     * </summary>
     */
    public class LockClient
    {
        static string _argumentExceptionMessage = "{0} must have a non-empty, non-null value.";
        static string _argumentNullExceptionMessage = "{0} must be non-null.";
        static string _greaterThanZeroMessage = "{0} must be greater than zero.";

        Container _container;

        /**
         * <summary>
         * Instantiates a new lock client with the given Cosmos DB container.
         * </summary>
         * 
         * <param name="client">The Cosmos DB client object.</param>
         * <param name="databaseName">The name of the database that contains the lease container.</param>
         * <param name="containerName">The name of the lease container.</param>
         * <exception cref="ConsistencyLevelException">If the consistency level is anything other than Strong.</exception>
         */
        public LockClient(CosmosClient client, string databaseName, string containerName)
        {
            if (client == null) throw new ArgumentNullException(string.Format(_argumentExceptionMessage, nameof(client)));
            if (string.IsNullOrWhiteSpace(databaseName)) throw new ArgumentException(string.Format(_argumentExceptionMessage, nameof(databaseName)));
            if (string.IsNullOrWhiteSpace(containerName)) throw new ArgumentException(string.Format(_argumentExceptionMessage, nameof(containerName)));

            // Make sure the client suppports strong consistancy. Cosmos DB doesn't allow the consistency to be higher on a
            // client than what is defined on the subscription, so we can't enforce the consistancy to be strong.  But we can
            // check the consistancy on the account and fail if it isn't Strong.
            this.CheckConsitencyLevel(client);

            this._container = client.GetContainer(databaseName, containerName);
        }

        /**
         * <summary>
         * Attempts to acquire a lock with the given options.  If the lock is unavailable, this will 
         * retry for the specified amount of time until either acquiring the lock or giving up.
         * </summary>
         * 
         * <param name="options">The options used to configure how the lock is acquired.</param>
         * <returns>A <c>Lock</c> object representing the acquired lock.</returns>
         * <exception cref="LockUnavailableException">
         * If the lock is unable to be acquired within the given timeout.
         * </exception>
         */
        public async Task<Lock> AcquireAsync(AcquireLockOptions options)
        {
            if (options == null) throw new ArgumentNullException(string.Format(_argumentNullExceptionMessage, nameof(options)));
            if (string.IsNullOrWhiteSpace(options.PartitionKey)) throw new ArgumentException(string.Format(_argumentExceptionMessage, nameof(options.PartitionKey)));
            if (string.IsNullOrWhiteSpace(options.LockName)) throw new ArgumentException(string.Format(_argumentExceptionMessage, nameof(options.LockName)));
            if (options.TimeoutMS < 0) throw new ArgumentException(string.Format(_greaterThanZeroMessage, nameof(options.TimeoutMS)));
            if (options.RetryWaitMS < 0) throw new ArgumentException(string.Format(_greaterThanZeroMessage, nameof(options.RetryWaitMS)));

            bool done = false;
            Exception innerEx = null;
            DateTime now = LockUtils.Now;
            while (!done)
            {
                try
                {
                    Lock @lock = await this.TryAcquireOnceAsync(options);
                    if (options.AutoRenew)
                    {
                        this.LaunchAutoRenewTimer(@lock);
                    }
                    return @lock;
                }
                catch (LockUnavailableException ex)
                {
                    innerEx = ex;
                }

                if ((LockUtils.Now - now).TotalMilliseconds < options.TimeoutMS)
                {
                    await Task.Delay(options.RetryWaitMS);
                }
                else
                {
                    done = true;
                }
            }

            throw new LockUnavailableException(options.PartitionKey, options.LockName, innerEx);
        }

        /**
         * <summary>
         * Renews the lease on the given lock.  If the lock does not exist in Cosmos DB, then the lock
         * has been released/expired. If the lock exists in Cosmos DB, but the ETags don't match, then 
         * the lock has been released/expired and reacquired and is a different lock.
         * </summary>
         * 
         * <param name="lock">The lock to renew.</param>
         * <exception cref="LockReleasedException">
         * If the lock with the given name and ETag cannot be found in Cosmos DB.
         * </exception>
         */
        public async Task RenewAsync(Lock @lock)
        {
            if (@lock == null) throw new ArgumentNullException(string.Format(_argumentNullExceptionMessage, nameof(@lock)));

            try
            {
                ItemRequestOptions options = new ItemRequestOptions() { IfMatchEtag = @lock.ETag };
                DateTime timeAcquired = LockUtils.Now;
                ItemResponse<Lock> response = await this._container.ReplaceItemAsync(@lock, @lock.Name, null, options);
                @lock.TimeAcquired = timeAcquired;
                @lock.ETag = response.ETag;
            }
            catch (AggregateException ex)
            {
                if (ex.InnerException is CosmosException innerEx && (innerEx.StatusCode == HttpStatusCode.PreconditionFailed || innerEx.StatusCode == HttpStatusCode.NotFound))
                {
                    throw new LockReleasedException(@lock, ex);
                }
                else
                {
                    throw;
                }
            }
        }

        /**
         * <summary>
         * Releases the lock. If the lock does not exist, this will be a no-op.
         * </summary>
         * 
         * <param name="lock">The lock to release.</param>
         */
        public async Task ReleaseAsync(Lock @lock)
        {
            if (@lock == null) throw new ArgumentNullException(string.Format(_argumentNullExceptionMessage, nameof(@lock)));

            try
            {
                // Kill the auto-renew timer before deleting the lock item from Cosmos DB.
                if (@lock.AutoRenewTimer != null)
                {
                    @lock.AutoRenewTimer.Stop();
                    @lock.AutoRenewTimer = null;
                }
                ItemRequestOptions options = new ItemRequestOptions() { IfMatchEtag = @lock.ETag };
                await this._container.DeleteItemAsync<Lock>(@lock.Name, new PartitionKey(@lock.PartitionKey), options);
                @lock.IsAquired = false;
               
            }
            catch (AggregateException ex)
            {
                // If the lock isn't found (NotFound) or the lock is found but the ETag doesn't match (PreconditionFailed),
                // swallow the exception.  The lock is already expired/released.
                if (!(ex.InnerException is CosmosException innerEx) || (innerEx.StatusCode != HttpStatusCode.PreconditionFailed && innerEx.StatusCode != HttpStatusCode.NotFound))
                {
                    throw;
                }
            }
        }

        /**
         * <summary>
         * Trys to acquire a lock only once, without retrying upon failure.
         * </summary>
         * 
         * <param name="options">The options used to configure how the lock is acquired.</param>
         * <returns>A <c>Lock</c> object representing the acquired lock.</returns>
         * <exception cref="LockUnavailableException">If the lock is unable to be acquired.</exception>
         */
        private async Task<Lock> TryAcquireOnceAsync(AcquireLockOptions options)
        {
            Lock @lock = new Lock()
            {
                Name = options.LockName,
                PartitionKey = options.PartitionKey,
                LeaseDuration = options.LeaseDuration,
            };

            try
            {
                @lock.TimeAcquired = LockUtils.Now;
                ItemResponse<Lock> response = await this._container.CreateItemAsync(@lock);
                @lock.ETag = response.ETag;
                return @lock;
            }
            catch (AggregateException ex)
            {
                if (ex.InnerException is CosmosException innerEx && innerEx.StatusCode == HttpStatusCode.Conflict)
                {
                    throw new LockUnavailableException(@lock, ex);
                }
                else
                {
                    throw;
                }
            }
        }

        /**
         * <summary>
         * Checks the consistency level on the client & account level.  For locking to work properly,
         * a consistency level of Strong is needed.  There is no way currently to enforce a Strong consistency
         * level programmatically considering a client can only have an equal or lower level of consistency
         * than what is defined on the account.
         * </summary>
         * 
         * <param name="client">The Cosmos DB client to query account consistency against.</param>
         * <exception cref="ConsistencyLevelException">If the consistency level is anything other than Strong.</exception>
         */
        private void CheckConsitencyLevel(CosmosClient client)
        {
            bool strong = true;
            AccountProperties properties = client.ReadAccountAsync().Result;
            ConsistencyLevel level = client.ClientOptions.ConsistencyLevel ?? properties.Consistency.DefaultConsistencyLevel;
            Exception innerEx = null;
            try
            {
                if (level != ConsistencyLevel.Strong)
                {
                    strong = false;
                }
            }
            catch (AggregateException ex)
            {
                // If the client's consistency level is greater than that of the account's, ReadAccountAsync will 
                // throw an AggregateException.  Unfortuneately, there is no error code to confirm exactly what went
                // wrong so we just have to make an assumption.
                innerEx = ex;
                if (ex.InnerException != null && ex.InnerException is ArgumentException)
                {
                    strong = false;
                }
                else
                {
                    throw;
                }
            }

            if (!strong)
            {
                throw new ConsistencyLevelException(level, innerEx);
            }
        }

        /**
         * <summary>
         * Creates and launches a timer which will periodically renew the lease 
         * on the given lock.
         * </summary>
         * 
         * <param name="lock">The lock to automatically renew.</param>
         */
        private void LaunchAutoRenewTimer(Lock @lock)
        {
            double third = @lock.LeaseDuration * 1000 / 3.0;
            Timer timer = new Timer(third) { AutoReset = false };
            timer.Elapsed += async (sender, args) =>
            {
                DateTime start = LockUtils.Now;
                if (@lock.IsAquired)
                {
                    try
                    {
                        await this.RenewAsync(@lock);
                    }
                    catch
                    {
                        // Try again momentarily.
                    }

                    TimeSpan elapsed = LockUtils.Now - start;
                    timer.Interval = Math.Max(0.1, third - elapsed.TotalMilliseconds);
                    timer.Start();
                }
                else
                {
                    @lock.AutoRenewTimer = null;
                }
            };
            @lock.AutoRenewTimer = timer;
            timer.Start();
        }
    }
}