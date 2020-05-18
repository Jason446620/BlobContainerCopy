//----------------------------------------------------------------------------------
// Microsoft Developer & Platform Evangelism
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, 
// EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES 
// OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
//----------------------------------------------------------------------------------
// The example companies, organizations, products, domain names,
// e-mail addresses, logos, people, places, and events depicted
// herein are fictitious.  No association with any real company,
// organization, product, domain name, email address, logo, person,
// places, or events is intended or should be inferred.
//----------------------------------------------------------------------------------

namespace BlobStorage
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Security.Cryptography;
    using System.ServiceModel.Channels;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.RetryPolicies;
    using Microsoft.Azure.Storage.Shared.Protocol;

    /// <summary>
    /// Advanced samples for Blob storage, including samples demonstrating a variety of client library classes and methods.
    /// </summary>
    public static class Advanced
    {
        // Prefix for containers created by the sample.
        private const string ContainerPrefix = "";

        // Prefix for blob created by the sample.
        private const string BlobPrefix = "";
        public static async Task MyFunc()
        {
            CloudStorageAccount sourcestorageAccount;
            CloudStorageAccount denstinationstorageAccount;

            try
            {
                //source storage
                sourcestorageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("SourceStorageConnectionString"));
                //des storage
                denstinationstorageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DenstinationStorageConnectionString"));
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
            // Create service client for credentialed access to the Blob service.
            CloudBlobClient sblobClient = sourcestorageAccount.CreateCloudBlobClient();
            CloudBlobClient dblobClient = denstinationstorageAccount.CreateCloudBlobClient();
            CloudBlobContainer scontainer = null;
            CloudBlobContainer dcontainer = null;

            try
            {
                try
                {
                    //source container name
                    scontainer = await CreateSampleContainerAsync(sblobClient,"mycontainer");
                    //denstination name
                    dcontainer = await CreateSampleContainerAsync(dblobClient, "jasontest");
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    throw;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
            finally
            {
                // Delete the sample container created by this session.
                if (scontainer != null)
                {
                    IEnumerable<ICloudBlob> sourceBlobRefs;
                    sourceBlobRefs = FindMatchingBlobsAsync(scontainer,string.Empty,10, 100).Result;
                    await MoveMatchingBlobsAsync(sourceBlobRefs, scontainer,dcontainer);
                }

            }
        }

        private static async Task MoveMatchingBlobsAsync(IEnumerable<ICloudBlob> sourceBlobRefs,
           CloudBlobContainer sourceContainer,
           CloudBlobContainer destContainer)
        {
            Console.WriteLine("----------------------------Begin-------------------------------");
            foreach (ICloudBlob sourceBlobRef in sourceBlobRefs)
            {
                if (sourceBlobRef.Properties.ContentType != null)
                {
                    // Copy the source blob
                    CloudBlockBlob destBlob = destContainer.GetBlockBlobReference(sourceBlobRef.Name);

                    try
                    {
                        //exception throwed here  - StartCopyAsync
                        await destBlob.StartCopyAsync(new Uri(GetSharedAccessUri(sourceBlobRef.Name, sourceContainer)));

                        ICloudBlob destBlobRef = await destContainer.GetBlobReferenceFromServerAsync(sourceBlobRef.Name);
                        while (destBlobRef.CopyState.Status == CopyStatus.Pending)
                        {
                            Console.WriteLine($"Blob: {destBlobRef.Name}, Copied: {destBlobRef.CopyState.BytesCopied ?? 0} of  {destBlobRef.CopyState.TotalBytes ?? 0}");
                            await Task.Delay(500);
                            destBlobRef = await destContainer.GetBlobReferenceFromServerAsync(sourceBlobRef.Name);
                        }
                        Console.WriteLine($"Blob: {destBlob.Name} Complete");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Blob: {destBlob.Name} Copy Failed");
                    }
                }
            }
            Console.WriteLine("----------------------------End-------------------------------");
        }
        private static async Task<IEnumerable<ICloudBlob>> FindMatchingBlobsAsync(CloudBlobContainer blobContainer, string prefix, int maxrecords, int total)
        {
            List<ICloudBlob> blobList = new List<ICloudBlob>();
            BlobContinuationToken token = null;
            do
            {
                BlobResultSegment segment = await blobContainer.ListBlobsSegmentedAsync(prefix: prefix, useFlatBlobListing: true, BlobListingDetails.None, maxrecords, token, new BlobRequestOptions(), new OperationContext());
                token = segment.ContinuationToken;
                foreach (var item in segment.Results)
                {
                    blobList.Add((ICloudBlob)item);
                    if (blobList.Count > total) // total record count is configured
                        token = null;
                }
            } while (token != null);
            return blobList;
        }
        private static string GetSharedAccessUri(string blobName, CloudBlobContainer container)
        {
            DateTime toDateTime = DateTime.Now.AddMinutes(60);

            SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy
            {
                Permissions = SharedAccessBlobPermissions.Read,
                SharedAccessStartTime = null,
                SharedAccessExpiryTime = new DateTimeOffset(toDateTime)
            };

            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            string sas = blob.GetSharedAccessSignature(policy);

            return blob.Uri.AbsoluteUri + sas;
        }

        #region source sample code
        
        /// <summary>
        /// Calls samples that demonstrate how to use the Blob service client (CloudBlobClient) object.
        /// </summary>
        /// <param name="blobClient">The Blob service client.</param>
        /// <returns>A Task object.</returns>
        private static async Task CallBlobClientSamples(CloudBlobClient blobClient)
        {
            // Create a buffer manager for the Blob service client. The buffer manager enables the Blob service client to
            // re-use an existing buffer across multiple operations.
            blobClient.BufferManager = new WCFBufferManagerAdapter(
                BufferManager.CreateBufferManager(32 * 1024, 256 * 1024), 256 * 1024);

            // Print out properties for the service client.
            PrintServiceClientProperties(blobClient);

            // Configure storage analytics (metrics and logging) on Blob storage.
            await ConfigureBlobAnalyticsAsync(blobClient);

            // List all containers in the storage account.
            ListAllContainers(blobClient, "sample-");

            // List containers beginning with the specified prefix.
            await ListContainersWithPrefixAsync(blobClient, "sample-");
        }


        #region BlobClientSamples

        /// <summary>
        /// Configures logging and metrics for Blob storage, as well as the default service version.
        /// Note that if you have already enabled analytics for your storage account, running this sample 
        /// will change those settings. For that reason, it's best to run with a test storage account if possible.
        /// The sample saves your settings and resets them after it has completed running.
        /// </summary>
        /// <param name="blobClient">The Blob service client.</param>
        /// <returns>A Task object.</returns>
        private static async Task ConfigureBlobAnalyticsAsync(CloudBlobClient blobClient)
        {
            try
            {
                // Get current service property settings.
                ServiceProperties serviceProperties = await blobClient.GetServicePropertiesAsync();

                // Enable analytics logging and set retention policy to 14 days. 
                serviceProperties.Logging.LoggingOperations = LoggingOperations.All;
                serviceProperties.Logging.RetentionDays = 14;
                serviceProperties.Logging.Version = "1.0";

                // Configure service properties for hourly and minute metrics. 
                // Set retention policy to 7 days.
                serviceProperties.HourMetrics.MetricsLevel = MetricsLevel.ServiceAndApi;
                serviceProperties.HourMetrics.RetentionDays = 7;
                serviceProperties.HourMetrics.Version = "1.0";

                serviceProperties.MinuteMetrics.MetricsLevel = MetricsLevel.ServiceAndApi;
                serviceProperties.MinuteMetrics.RetentionDays = 7;
                serviceProperties.MinuteMetrics.Version = "1.0";

                // Set the default service version to be used for anonymous requests.
                serviceProperties.DefaultServiceVersion = "2018-11-09";

                // Set the service properties.
                await blobClient.SetServicePropertiesAsync(serviceProperties);            
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// Lists all containers in the storage account.
        /// Note that the ListContainers method is called synchronously, for the purposes of the sample. However, in a real-world
        /// application using the async/await pattern, best practices recommend using asynchronous methods consistently.
        /// </summary>
        /// <param name="blobClient">The Blob service client.</param>
        /// <param name="prefix">The container prefix.</param>
        private static void ListAllContainers(CloudBlobClient blobClient, string prefix)
        {
            // List all containers in this storage account.
            Console.WriteLine("List all containers in account:");

            try
            {
                // List containers beginning with the specified prefix, and without returning container metadata.
                foreach (var container in blobClient.ListContainers(prefix, ContainerListingDetails.None, null, null))
                {
                    Console.WriteLine("\tContainer:" + container.Name);
                }

                Console.WriteLine();
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// Lists containers in the storage account whose names begin with the specified prefix, and return container metadata
        /// as part of the listing operation.
        /// </summary>
        /// <param name="blobClient">The Blob service client.</param>
        /// <param name="prefix">The container name prefix.</param>
        /// <returns>A Task object.</returns>
        private static async Task ListContainersWithPrefixAsync(CloudBlobClient blobClient, string prefix)
        {
            Console.WriteLine("List all containers beginning with prefix {0}, plus container metadata:", prefix);

            BlobContinuationToken continuationToken = null;
            ContainerResultSegment resultSegment = null;

            try
            {
                do
                {
                    // List containers beginning with the specified prefix, returning segments of 5 results each. 
                    // Note that passing in null for the maxResults parameter returns the maximum number of results (up to 5000).
                    // Requesting the container's metadata as part of the listing operation populates the metadata, 
                    // so it's not necessary to call FetchAttributes() to read the metadata.
                    resultSegment = await blobClient.ListContainersSegmentedAsync(
                        prefix, ContainerListingDetails.Metadata, 5, continuationToken, null, null);

                    // Enumerate the containers returned.
                    foreach (var container in resultSegment.Results)
                    {
                        Console.WriteLine("\tContainer:" + container.Name);

                        // Write the container's metadata keys and values.
                        foreach (var metadataItem in container.Metadata)
                        {
                            Console.WriteLine("\t\tMetadata key: " + metadataItem.Key);
                            Console.WriteLine("\t\tMetadata value: " + metadataItem.Value);
                        }
                    }

                    // Get the continuation token.
                    continuationToken = resultSegment.ContinuationToken;

                } while (continuationToken != null);

                Console.WriteLine();
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }



        /// <summary>
        /// Prints properties for the Blob service client to the console window.
        /// </summary>
        /// <param name="blobClient">The Blob service client.</param>
        private static void PrintServiceClientProperties(CloudBlobClient blobClient)
        {
            Console.WriteLine("-----Blob Service Client Properties-----");
            Console.WriteLine("Storage account name: {0}", blobClient.Credentials.AccountName);
            Console.WriteLine("Authentication Scheme: {0}", blobClient.AuthenticationScheme);
            Console.WriteLine("Base URI: {0}", blobClient.BaseUri);
            Console.WriteLine("Primary URI: {0}", blobClient.StorageUri.PrimaryUri);
            Console.WriteLine("Secondary URI: {0}", blobClient.StorageUri.SecondaryUri);
            Console.WriteLine("Default buffer size: {0}", blobClient.BufferManager.GetDefaultBufferSize());
            Console.WriteLine("Default delimiter: {0}", blobClient.DefaultDelimiter);
            Console.WriteLine();
        }

        #endregion

        #region BlobContainerSamples

        /// <summary>
        /// Creates a sample container for use in the sample application.
        /// </summary>
        /// <param name="blobClient">The blob service client.</param>
        /// <returns>A CloudBlobContainer object.</returns>
        private static async Task<CloudBlobContainer> CreateSampleContainerAsync(CloudBlobClient blobClient,string containerName)
        {
            // Name the sample container based on new GUID, to ensure uniqueness.
            //string containerName = ContainerPrefix + Guid.NewGuid();

            // Get a reference to a sample container.
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            try
            {
                // Create the container if it does not already exist.
                await container.CreateIfNotExistsAsync();
            }
            catch (StorageException e)
            {
                // Ensure that the storage emulator is running if using emulator connection string.
                Console.WriteLine(e.Message);
                Console.WriteLine("If you are running with the default connection string, please make sure you have started the storage emulator. Press the Windows key and type Azure Storage to select and run it from the list of applications - then restart the sample.");
                Console.ReadLine();
                throw;
            }

            return container;
        }

        /// <summary>
        /// Add some sample metadata to the container.
        /// </summary>
        /// <param name="container">A CloudBlobContainer object.</param>
        /// <returns>A Task object.</returns>
        private static async Task AddContainerMetadataAsync(CloudBlobContainer container)
        {
            try
            {
                // Add some metadata to the container.
                container.Metadata.Add("docType", "textDocuments");
                container.Metadata["category"] = "guidance";

                // Set the container's metadata asynchronously.
                await container.SetMetadataAsync();
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// Sets the anonymous access level.
        /// </summary>
        /// <param name="container">The container.</param>
        /// <param name="accessType">Type of the access.</param>
        /// <returns>A Task object.</returns>
        private static async Task SetAnonymousAccessLevelAsync(CloudBlobContainer container, BlobContainerPublicAccessType accessType)
        {
            try
            {
                // Read the existing permissions first so that we have all container permissions. 
                // This ensures that we do not inadvertently remove any shared access policies while setting the public access level.
                BlobContainerPermissions permissions = await container.GetPermissionsAsync();

                // Set the container's public access level.
                permissions.PublicAccess = BlobContainerPublicAccessType.Container;
                await container.SetPermissionsAsync(permissions);

                Console.WriteLine("Container public access set to {0}", accessType.ToString());
                Console.WriteLine();
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }
        
        /// <summary>
        /// Reads the container's properties.
        /// </summary>
        /// <param name="container">A CloudBlobContainer object.</param>
        private static void PrintContainerPropertiesAndMetadata(CloudBlobContainer container)
        {
            Console.WriteLine("-----Container Properties-----");
            Console.WriteLine("Name: {0}", container.Name);
            Console.WriteLine("URI: {0}", container.Uri);
            Console.WriteLine("ETag: {0}", container.Properties.ETag);
            Console.WriteLine("Last modified: {0}", container.Properties.LastModified);
            PrintContainerLeaseProperties(container);

            // Enumerate the container's metadata.
            Console.WriteLine("Container metadata:");
            foreach (var metadataItem in container.Metadata)
            {
                Console.WriteLine("\tKey: {0}", metadataItem.Key);
                Console.WriteLine("\tValue: {0}", metadataItem.Value);
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Reads the lease properties for the container.
        /// </summary>
        /// <param name="container">A CloudBlobContainer object.</param>
        private static void PrintContainerLeaseProperties(CloudBlobContainer container)
        {
            try
            {
                Console.WriteLine();
                Console.WriteLine("Leasing properties for container: {0}", container.Name);
                Console.WriteLine("\t Lease state: {0}", container.Properties.LeaseState);
                Console.WriteLine("\t Lease duration: {0}", container.Properties.LeaseDuration);
                Console.WriteLine("\t Lease status: {0}", container.Properties.LeaseStatus);
                Console.WriteLine();
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }


        #endregion

        private static byte[] GetRandomBuffer(int size)
        {
            byte[] buffer = new byte[size];
            Random random = new Random();
            random.NextBytes(buffer);
            return buffer;
        }

        #endregion

        
    }
}
