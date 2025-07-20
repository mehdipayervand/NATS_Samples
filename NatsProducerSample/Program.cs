using NATS.Net;
using SharedLibrary;

namespace NatsProducerSample;

class Program
{
    static async Task Main(string[] args)
    {
        var cts = new CancellationTokenSource();

        await using var natsClient = new NatsClient(Constants.NatsUrl);

        for (int i = 1; i < 501; i++)
        {
            Console.WriteLine($"Publishing order {i}...");
            await natsClient.PublishAsync($"orders.new.{i}", new Order(OrderId: i), cancellationToken: cts.Token);
            await Task.Delay(50, cts.Token);
        }

        await cts.CancelAsync();

        Console.WriteLine("Bye!");
    }
}

