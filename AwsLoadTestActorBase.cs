using System;
using Aamva.Ncs.Core.Extensions;
using Aamva.Ncs.LoadTestFramework.Core.Actors;
using Aamva.Ncs.LoadTestFramework.Core.Extensions;
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;

namespace Aamva.Ncs.LoadTestFramework.DynamoDbActors
{
    public class AwsLoadTestActorBase : LoadTestActorBase
    {
        public string ProfilesLocation => Parameters.GetStringValue("profilesLocation", "credentials.ini");

        public string ProfileName => Parameters.GetProperty("profileName");

        public string Region => Parameters.GetStringValue("region", RegionEndpoint.USEast1.SystemName);
        
        protected T SetClientConfigProperties<T>(T clientConfig) where T : ClientConfig
        {
            clientConfig.RegionEndpoint = RegionEndpoint.GetBySystemName(Region);
            return clientConfig;
        }

        protected AWSCredentials GetAwsCredentials()
        {
            var chain = new CredentialProfileStoreChain(ProfilesLocation);
            AWSCredentials awsCredentials;
            if (chain.TryGetAWSCredentials(ProfileName, out awsCredentials))
            {
                return awsCredentials;
            }

            throw new Exception($"Credentinal file {ProfilesLocation} doesnot contain profile {ProfileName}");
        }
    }
}