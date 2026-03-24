using AnisShop.Attributes.Queries.QueriesProto;
using AnisShop.Attributes.Queries.Resources;
using FluentValidation;

namespace AnisShop.Attributes.Queries.Validators
{
    public class GetByCategoryRequestValidator : AbstractValidator<GetByCategoryRequest>
    {
        public GetByCategoryRequestValidator()
        {
            RuleFor(x => x.CategoryId)
                .GreaterThan(0)
                .WithMessage(Messages.InvalidCategoryId);

            RuleFor(x => x.CurrentPage)
                .GreaterThan(0);

            RuleFor(x => x.PageSize)
                .GreaterThan(0);
        }
    }
}
