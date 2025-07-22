using System.Text;
using NATS.Client;
using NATS.Client.JetStream;
using NATS.Net;
using SharedLibrary;

namespace NatsConsumerSample;

class Program
{
    static async Task Main(string[] args)
    {
        var cf = new ConnectionFactory();
        using IConnection conn = cf.CreateConnection("nats://localhost:4222");

        string streamName = Constants.StreamName;
        string subject = Constants.Subject;

        IJetStream js = conn.CreateJetStreamContext();

        var consumerConfig = ConsumerConfiguration.Builder()
            .WithDurable(streamName)
            .WithAckPolicy(AckPolicy.Explicit)
            .Build();

        var subOpts = PushSubscribeOptions.Builder()
            .WithDurable(streamName)
            .WithConfiguration(consumerConfig)
            .Build();

        IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(subject, subOpts);

        Console.WriteLine("📡 Waiting for messages...");

        while (true)
        {
            try
            {
                Msg msg = sub.NextMessage(1000);
                if (msg != null)
                {
                    string data = Encoding.UTF8.GetString(msg.Data);
                    Console.WriteLine($"📥 Received: {data}");
                    msg.Ack();
                }
            }
            catch (NATSTimeoutException)
            {
                // No message available during this interval; just continue
            }
        }
    }

    private static async Task subscribe()
    {
        await using var natsClient = new NatsClient(Constants.NatsUrl);

        Console.WriteLine("Waiting for messages...");
        var cts = new CancellationTokenSource();
        var subscriptionTask = Task.Run(async () =>
        {
            await foreach (var msg in natsClient.SubscribeAsync<Order>("orders", cancellationToken: cts.Token))
            {
                var order = msg.Data;
                Console.WriteLine($"Subscriber received {msg.Subject}: {order}");
            }

            Console.WriteLine("Unsubscribed");
        }, cts.Token);
        
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }
}