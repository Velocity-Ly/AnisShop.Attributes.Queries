using AnisShop.Attributes.Queries.Infrastructure.Persistence;
using AnisShop.Attributes.Queries.IntegrationTests.Fixtures;
using AnisShop.Attributes.Queries.Tests.Asserts;
using AnisShop.Attributes.Queries.Tests.Fakers.Domain;
using AnisShop.Attributes.Queries.Tests.Helpers;
using AnisShop.Attributes.Queries.Tests.QueriesProto;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;
using SourceDomain = AnisShop.Attributes.Queries.Domain;

namespace AnisShop.Attributes.Queries.IntegrationTests.EventsHandler;

// End-to-end projection tests against a real SQL Server (LocalDB). Unlike the unit suite
// (EF InMemory), these exercise behaviour the InMemory provider cannot honour: the real
// per-batch transaction in the handler, the Version optimistic-concurrency token, and the
// database-level ON DELETE CASCADE foreign keys.
public class EventProjectionIntegrationTest(LocalDbFixture fixture, ITestOutputHelper output)
    : SqlIntegrationTestBase(fixture, output)
{
    [Fact]
    public async Task FullEventSequence_CreateOptionsCategoriesPublish_ProjectsAndIsQueryable()
    {
        // Arrange: Create -> add 2 options -> add 2 categories -> Publish (versions 1..5)
        var builder = new EventHistoryBuilder()
            .Created("Arabic Colors", "Colors", "SingleSelect")
            .OptionAdded("red", "Arabic Red", "Red")
            .OptionAdded("blue", "Arabic Blue", "Blue")
            .CategoriesAdded(10, 20)
            .Published();

        // Act: project the whole history in one batch (one real transaction)
        var success = await MediatorHelper.SendEvents(builder.Build());

        // Assert: handler accepted the batch and the read model is queryable end-to-end via gRPC
        Assert.True(success);

        var response = await GrpcClientHelper.Query(x =>
            x.GetAsync(new GetRequest { Id = builder.AggregateId.ToString() }));

        Assert.Equal(5, response.Attribute.Version);
        Assert.Equal((int)SourceDomain.AttributeStatus.Published, (int)response.Attribute.Status);
        Assert.Equal((int)SourceDomain.AttributeType.SingleSelect, (int)response.Attribute.Type);

        Assert.Equal(2, response.Attribute.Options.Count);
        Assert.Contains(response.Attribute.Options, o => o.Key == "red");
        Assert.Contains(response.Attribute.Options, o => o.Key == "blue");

        Assert.Equal(2, response.Attribute.ApplicableCategoryIds.Count);
        Assert.Contains(10, response.Attribute.ApplicableCategoryIds);
        Assert.Contains(20, response.Attribute.ApplicableCategoryIds);
    }

    [Fact]
    public async Task FullLifecycle_CreateThroughDelete_TransitionsThenRemoves()
    {
        // Arrange: Create -> Publish -> Deprecate -> RemoveDeprecation -> Disable -> Delete (1..6)
        var builder = new EventHistoryBuilder()
            .Created("Arabic Material", "Material", "SingleSelect")
            .Published()
            .MarkedAsDeprecated("Arabic Deprecation Warning", "Deprecation Warning")
            .DeprecationWarningRemoved()
            .Disabled("Arabic Disable Reason", "Disable Reason")
            .Deleted();

        // Act 1: project everything up to and including Disable (V5), as one batch
        var disableSucceeded = await MediatorHelper.SendEvents(builder.BuildUpTo(5));

        // Assert 1: lifecycle transitions survived the real round-trip. The deprecation
        // warning set at V3 must be cleared by RemoveDeprecation (V4); the disable reason
        // set at V5 must be present.
        Assert.True(disableSucceeded);

        var disabled = await AssertAttributeState.Exists(Factory, builder.AggregateId);
        Assert.Equal(5, disabled.Version);
        Assert.Equal(SourceDomain.AttributeStatus.Disabled, disabled.Status);
        Assert.Equal("Arabic Disable Reason", disabled.ArabicDisableReason);
        Assert.Equal("Disable Reason", disabled.EnglishDisableReason);
        Assert.Null(disabled.ArabicDeprecationWarning);
        Assert.Null(disabled.EnglishDeprecationWarning);

        // Act 2: project the Delete (V6) as a follow-up batch
        var deleteSucceeded = await MediatorHelper.SendEvents(builder.BuildFrom(6));

        // Assert 2: the row is gone
        Assert.True(deleteSucceeded);
        await AssertAttributeState.DoesNotExist(Factory, builder.AggregateId);
    }

    [Fact]
    public async Task ConcurrencyToken_StaleVersion_ThrowsDbUpdateConcurrencyException()
    {
        // Arrange: a single attribute at Version 1 in a real DB
        var attribute = await DatabaseHelper.InsertAsync(
            new AttributeFaker().WithStatus(SourceDomain.AttributeStatus.Draft).WithVersion(1));

        // Two independent contexts load the same row (each captures original Version = 1)
        using var scopeA = Factory.Services.CreateScope();
        var dbA = scopeA.ServiceProvider.GetRequiredService<AttributesDbContext>();
        var attributeA = await dbA.Attributes.SingleAsync(a => a.Id == attribute.Id);

        using var scopeB = Factory.Services.CreateScope();
        var dbB = scopeB.ServiceProvider.GetRequiredService<AttributesDbContext>();
        var attributeB = await dbB.Attributes.SingleAsync(a => a.Id == attribute.Id);

        // Act: the first writer wins, bumping the row to Version 2 in the DB
        attributeA.Publish(2);
        await dbA.SaveChangesAsync();

        // Assert: the second writer still holds the stale original Version 1, so the
        // UPDATE ... WHERE Version = 1 matches no rows and EF surfaces the conflict.
        attributeB.Publish(2);
        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => dbB.SaveChangesAsync());
    }

    [Fact]
    public async Task Delete_WithoutLoadingChildren_DatabaseCascadeRemovesOptionsAndCategories()
    {
        // Arrange: an attribute carrying 3 options + 2 categories, materialised via events
        var builder = new EventHistoryBuilder()
            .Created("Arabic Colors", "Colors", "SingleSelect")
            .OptionAdded("red", "Arabic Red", "Red")
            .OptionAdded("green", "Arabic Green", "Green")
            .OptionAdded("blue", "Arabic Blue", "Blue")
            .CategoriesAdded(10, 20);
        Assert.True(await MediatorHelper.SendEvents(builder.Build()));

        // Act: delete ONLY the parent row, without loading its children. EF therefore cannot
        // delete the dependents itself — they can only disappear via the DB's ON DELETE CASCADE.
        using (var scope = Factory.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<AttributesDbContext>();
            var parent = await db.Attributes.SingleAsync(a => a.Id == builder.AggregateId);
            db.Attributes.Remove(parent);
            await db.SaveChangesAsync();
        }

        // Assert: parent and both child tables are empty for this aggregate
        await AssertAttributeState.DoesNotExist(Factory, builder.AggregateId);

        var optionCount = await DatabaseHelper.Query(db =>
            db.AttributeOptions.CountAsync(o => o.AttributeId == builder.AggregateId));
        var categoryCount = await DatabaseHelper.Query(db =>
            db.AttributeCategories.CountAsync(c => c.AttributeId == builder.AggregateId));

        Assert.Equal(0, optionCount);
        Assert.Equal(0, categoryCount);
    }

    [Fact]
    public async Task SortOrder_AddReorderRemove_ProjectsExpectedSortOrders()
    {
        // Arrange: add small/medium/large (SortOrder 0/1/2) -> reorder large,small,medium
        // (large=0, small=1, medium=2) -> remove small. Removal leaves a gap; it does NOT recompact.
        var builder = new EventHistoryBuilder()
            .Created("Arabic Sizes", "Sizes", "SingleSelect")
            .OptionAdded("small", "Arabic Small", "Small")
            .OptionAdded("medium", "Arabic Medium", "Medium")
            .OptionAdded("large", "Arabic Large", "Large")
            .OptionsReordered("large", "small", "medium")
            .OptionRemoved("small");

        // Act
        var success = await MediatorHelper.SendEvents(builder.Build());

        // Assert: final SortOrders reflect the reorder, with the removed key gone (gap kept)
        Assert.True(success);

        var attribute = await AssertAttributeState.Exists(Factory, builder.AggregateId);
        Assert.Equal(6, attribute.Version);
        Assert.Equal(2, attribute.Options.Count);
        Assert.DoesNotContain(attribute.Options, o => o.Key == "small");
        AssertAttributeState.HasOptionWithSortOrder(attribute, "large", 0);
        AssertAttributeState.HasOptionWithSortOrder(attribute, "medium", 2);
    }
}
