using Grpc.Core;
using Grpc.Core.Interceptors;
using System.Globalization;

namespace AnisShop.Attributes.Queries.Interceptors
{
    public class CultureInterceptor : Interceptor
    {
        private const string AcceptLanguageHeader = "accept-language";
        private static readonly CultureInfo ArabicCulture = new("ar");
        private static readonly CultureInfo EnglishCulture = new("en");

        public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
            TRequest request,
            ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation)
        {
            var culture = GetCultureFromMetadata(context.RequestHeaders);

            CultureInfo.CurrentCulture = culture;
            CultureInfo.CurrentUICulture = culture;

            return await continuation(request, context);
        }

        private static CultureInfo GetCultureFromMetadata(Metadata headers)
        {
            var acceptLanguage = headers.GetValue(AcceptLanguageHeader);

            return acceptLanguage?.StartsWith("ar", StringComparison.OrdinalIgnoreCase) == true
                ? ArabicCulture
                : EnglishCulture;
        }
    }
}
