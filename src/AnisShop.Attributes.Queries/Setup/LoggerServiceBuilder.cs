using Serilog;
using Serilog.Debugging;

namespace AnisShop.Attributes.Queries.Setup
{
    public class LoggerServiceBuilder
    {
        public static Serilog.ILogger Build()
        {
            var configuration = AppConfiguration.Build();
            var serilogConfiguration = configuration.GetSection("Logger");
            var appName = serilogConfiguration["AppName"];
            var writeToConsole = serilogConfiguration.GetValue<bool>("WriteToConsole");
            var seqUrl = serilogConfiguration["SeqUrl"];
            var logger = new LoggerConfiguration()
            .Enrich.WithProperty("name", appName ?? "local")
            .Enrich.FromLogContext()
            .ReadFrom.Configuration(configuration);

            if (writeToConsole)
                logger.WriteTo.Console();

            if (!string.IsNullOrEmpty(seqUrl))
                logger.WriteTo.Seq(seqUrl);

            SelfLog.Enable(Console.Error);
            return logger.CreateLogger();
        }
    }
}
