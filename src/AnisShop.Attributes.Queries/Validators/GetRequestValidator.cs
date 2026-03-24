using AnisShop.Attributes.Queries.QueriesProto;
using AnisShop.Attributes.Queries.Resources;
using FluentValidation;

namespace AnisShop.Attributes.Queries.Validators
{
    public class GetRequestValidator : AbstractValidator<GetRequest>
    {
        public GetRequestValidator()
        {
            RuleFor(x => x.Id)
                .NotEmpty()
                .Must(id => Guid.TryParse(id, out _))
                .WithMessage(Messages.InvalidAttributeId);
        }
    }
}
