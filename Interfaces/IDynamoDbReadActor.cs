using System;
using System.Collections.Generic;
using Aamva.Ncs.LoadTestFramework.DynamoDbActors.Models;

namespace Aamva.Ncs.LoadTestFramework.DynamoDbActors.Interfaces
{
    public interface IDynamoDbReadActor
    {
        List<T1> Read<T1, T2, T3>(string transOrigin, T2 messageProp, out int count) where T1: class
                                                                                    where T2 : IMessageProp;

        List<T1> ReadGsi<T1>(string appId, string messageOrigin, out int count) where T1 : class;

        List<T1> ReadLsi<T1>(string transOrigin, string messageType, out int count) where T1 : class;

        List<T1> ReadLsi2<T1>(string transOrigin, string messageDest, out int count) where T1 : class;

    }
}
