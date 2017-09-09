using System.Threading.Tasks;

namespace Aamva.Ncs.LoadTestFramework.DynamoDbActors.Interfaces
{
    public interface IDynamoDbWriteActor
    {
        void Write<T1> (T1 data) ;

        Task WriteAsync<T1>(T1 data);


    }
}
