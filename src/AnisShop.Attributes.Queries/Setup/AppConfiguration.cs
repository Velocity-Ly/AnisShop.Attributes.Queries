namespace AnisShop.Attributes.Queries.Setup
{
    public static class AppConfiguration
    {
        public static IConfiguration Build()
            => new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json")
                    .AddEnvironmentVariables()
                    .Build();
    }
}
