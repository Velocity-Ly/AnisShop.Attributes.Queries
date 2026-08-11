using System.ComponentModel.DataAnnotations;
using KafkaFlow.Configuration;

namespace AnisShop.Attributes.Queries.Infrastructure.KafkaFlowTransport;

// The namespace is KafkaFlowTransport rather than KafkaFlow because the package owns the KafkaFlow
// root namespace, and a folder of the same name would shadow it in every file here.
//
// KafkaFlow builds its whole topology while AddKafka runs, so these values are needed at
// registration and not when something first resolves IOptions. That is why this one validates
// eagerly instead of following the lazy ValidateDataAnnotations pattern the other two listeners
// use: the empty appsettings placeholders never reach it, because nothing registers this transport
// unless Messaging:Transport asks for it by name.
public class KafkaFlowListenerOptions : IValidatableObject
{
    public const string SectionName = "KafkaFlow";

    [Required]
    public string BootstrapServers { get; set; } = string.Empty;

    [Required]
    public string Topic { get; set; } = string.Empty;

    [Required]
    public string ConsumerGroup { get; set; } = string.Empty;

    // The only parallelism knob KafkaFlow has. Every message key is hashed onto one of these
    // workers and stays there, which is what keeps one aggregate in order — and also what makes two
    // unrelated aggregates that hash alike wait for each other. Concurrency is bounded by the
    // worker count, not by how many aggregates are actually in flight.
    [Range(1, 1000)]
    public int WorkersCount { get; set; } = 32;

    // Messages held per worker ahead of processing. Too small and workers idle waiting on a fetch;
    // too large and a rebalance throws away more in-flight work.
    [Range(1, 100_000)]
    public int BufferSize { get; set; } = 100;

    // A worker collects up to BatchSize messages, or waits BatchTimeout, and then hands the lot to
    // the projection middleware in one call.
    [Range(1, 10_000)]
    public int BatchSize { get; set; } = 100;

    [Range(1, 60_000)]
    public int BatchTimeoutMilliseconds { get; set; } = 25;

    // Off by default: on a dedicated event topic a message with no recognised type header is a
    // contract violation and the projector is right to fail loudly on it. Turn this on only when
    // the topic is deliberately shared with other producers (a smoke-test / health-probe topic),
    // where such messages are simply foreign traffic to be skipped. A message that DOES carry a
    // known type but fails to deserialize stays fatal either way — that is still our data going
    // wrong, not someone else's passing through.
    public bool IgnoreUnrecognizedMessages { get; set; }

    // Plaintext keeps the default behaviour for anyone already pointing this at an unsecured broker.
    // Anything else and the credentials below become mandatory.
    public SecurityProtocol SecurityProtocol { get; set; } = SecurityProtocol.Plaintext;

    public SaslMechanism SaslMechanism { get; set; } = SaslMechanism.ScramSha512;

    // Deliberately absent from appsettings.json. Supply them per environment, either as
    // KafkaFlow__SaslUsername / KafkaFlow__SaslPassword environment variables or through user
    // secrets, so a credential can never arrive by way of a committed file.
    public string? SaslUsername { get; set; }

    public string? SaslPassword { get; set; }

    public bool UsesSasl =>
        SecurityProtocol is SecurityProtocol.SaslPlaintext or SecurityProtocol.SaslSsl;

    public static KafkaFlowListenerOptions Read(IConfiguration configuration)
    {
        var options = configuration.GetSection(SectionName).Get<KafkaFlowListenerOptions>() ?? new();

        Validator.ValidateObject(options, new ValidationContext(options), validateAllProperties: true);

        return options;
    }

    // A missing credential would otherwise surface as a broker transport failure several seconds
    // into startup, which reads like a network fault rather than a configuration one.
    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (!UsesSasl)
            yield break;

        if (string.IsNullOrWhiteSpace(SaslUsername))
        {
            yield return new ValidationResult(
                $"{nameof(SaslUsername)} is required when {nameof(SecurityProtocol)} is {SecurityProtocol}.",
                [nameof(SaslUsername)]);
        }

        if (string.IsNullOrWhiteSpace(SaslPassword))
        {
            yield return new ValidationResult(
                $"{nameof(SaslPassword)} is required when {nameof(SecurityProtocol)} is {SecurityProtocol}.",
                [nameof(SaslPassword)]);
        }
    }
}
