using FluentValidation;
using Grpc.Core;
using Grpc.Core.Interceptors;
using System.Text.Json;

namespace AnisShop.Attributes.Queries.Interceptors
{
    public class ValidationInterceptor(IServiceProvider serviceProvider) : Interceptor
    {
        public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
            TRequest request,
            ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation)
        {
            var validator = serviceProvider.GetService<IValidator<TRequest>>();

            if (validator is not null)
            {
                var validationResult = await validator.ValidateAsync(request);

                if (!validationResult.IsValid)
                {
                    var errors = validationResult.Errors
                        .GroupBy(e => e.PropertyName)
                        .ToDictionary(g => g.Key, g => g.Select(e => e.ErrorMessage).ToList());

                    var errorsJson = JsonSerializer.Serialize(errors);

                    var metadata = new Metadata
                    {
                        { "errors", errorsJson }
                    };

                    throw new RpcException(new Status(StatusCode.InvalidArgument, "Validation failed"), metadata);
                }
            }

            return await continuation(request, context);
        }
    }
}
