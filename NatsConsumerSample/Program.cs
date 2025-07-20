using NATS.Net;
using SharedLibrary;

namespace NatsConsumerSample;

class Program
{
    static async Task Main(string[] args)
    {
        await using var natsClient = new NatsClient(Constants.NatsUrl);

        Console.WriteLine("Waiting for messages...");
        var cts = new CancellationTokenSource();
        var subscriptionTask = Task.Run(async () =>
        {
            await foreach (var msg in natsClient.SubscribeAsync<Order>("orders.>", cancellationToken: cts.Token))
            {
                var order = msg.Data;
                Console.WriteLine($"Subscriber received {msg.Subject}: {order}");
            }

            Console.WriteLine("Unsubscribed");
        }, cts.Token);
    }
}