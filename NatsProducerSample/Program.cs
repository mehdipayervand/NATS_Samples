using System.Text;
using NATS.Net;
using SharedLibrary;
using NATS.Client;
using NATS.Client.JetStream;

namespace NatsProducerSample;

class Program
{
    static async Task Main(string[] args)
    {
        var cf = new ConnectionFactory();
        using IConnection conn = cf.CreateConnection("nats://localhost:4222");

        string streamName = Constants.StreamName;
        string subject = Constants.Subject;

        IJetStreamManagement jsm = conn.CreateJetStreamManagementContext();

        if (!jsm.GetStreamNames().Contains(streamName))
        {
            var config = StreamConfiguration.Builder()
                .WithName(streamName)
                .WithSubjects(subject)
                .Build();

            jsm.AddStream(config);
            Console.WriteLine($"📘 Created stream '{streamName}'.");
        }

        IJetStream js = conn.CreateJetStreamContext();

        int counter = 1;

        Console.WriteLine("🚀 Sending messages every 5 seconds...");

        while (true)
        {
            string message = $"Message #{counter} at {DateTime.Now:HH:mm:ss}";
            PublishAck ack = js.Publish(subject, Encoding.UTF8.GetBytes(message));
            Console.WriteLine($"✅ Sent: {message} (seq {ack.Seq})");

            counter++;
            Thread.Sleep(5 * 1000); // Wait for 5 seconds
        }
    }

    private static async Task publishMessages()
    {
        var cts = new CancellationTokenSource();

        await using var natsClient = new NatsClient(Constants.NatsUrl);

        for (int i = 1; i < 501; i++)
        {
            Console.WriteLine($"Publishing order {i}...");
            await natsClient.PublishAsync($"orders", new Order(OrderId: i), cancellationToken: cts.Token);
            await Task.Delay(50, cts.Token);
        }

        await cts.CancelAsync();

        Console.WriteLine("Bye!");
    }
}