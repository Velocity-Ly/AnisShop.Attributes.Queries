using AnisShop.Attributes.Queries.QueriesProto;
using AnisShop.Attributes.Queries.Resources;
using FluentValidation;

namespace AnisShop.Attributes.Queries.Validators
{
    public class GetByTargetRequestValidator : AbstractValidator<GetByTargetRequest>
    {
        public GetByTargetRequestValidator()
        {
            RuleFor(x => x.Scope)
                .NotEqual(AttributeScope.Unspecified)
                .WithMessage(Messages.InvalidScope);

            RuleFor(x => x.TargetId)
                .GreaterThan(0)
                .WithMessage(Messages.InvalidTargetId);

            RuleFor(x => x.CurrentPage)
                .GreaterThan(0);

            RuleFor(x => x.PageSize)
                .GreaterThan(0);
        }
    }
}
