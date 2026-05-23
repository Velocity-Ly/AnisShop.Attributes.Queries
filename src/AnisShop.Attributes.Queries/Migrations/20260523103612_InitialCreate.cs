using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace AnisShop.Attributes.Queries.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Attributes",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    ArabicDisplayName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    EnglishDisplayName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    ArabicDescription = table.Column<string>(type: "nvarchar(1000)", maxLength: 1000, nullable: true),
                    EnglishDescription = table.Column<string>(type: "nvarchar(1000)", maxLength: 1000, nullable: true),
                    Type = table.Column<int>(type: "int", nullable: false),
                    Status = table.Column<int>(type: "int", nullable: false),
                    ArabicDeprecationWarning = table.Column<string>(type: "nvarchar(1000)", maxLength: 1000, nullable: true),
                    EnglishDeprecationWarning = table.Column<string>(type: "nvarchar(1000)", maxLength: 1000, nullable: true),
                    ArabicDisableReason = table.Column<string>(type: "nvarchar(1000)", maxLength: 1000, nullable: true),
                    EnglishDisableReason = table.Column<string>(type: "nvarchar(1000)", maxLength: 1000, nullable: true),
                    Version = table.Column<int>(type: "int", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Attributes", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "AttributeCategories",
                columns: table => new
                {
                    AttributeId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    CategoryId = table.Column<int>(type: "int", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AttributeCategories", x => new { x.AttributeId, x.CategoryId });
                    table.ForeignKey(
                        name: "FK_AttributeCategories_Attributes_AttributeId",
                        column: x => x.AttributeId,
                        principalTable: "Attributes",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "AttributeOptions",
                columns: table => new
                {
                    AttributeId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    Key = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    ArabicLabel = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    EnglishLabel = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    IsDisabled = table.Column<bool>(type: "bit", nullable: false),
                    SortOrder = table.Column<int>(type: "int", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AttributeOptions", x => new { x.AttributeId, x.Key });
                    table.ForeignKey(
                        name: "FK_AttributeOptions_Attributes_AttributeId",
                        column: x => x.AttributeId,
                        principalTable: "Attributes",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_AttributeCategories_CategoryId",
                table: "AttributeCategories",
                column: "CategoryId");

            migrationBuilder.CreateIndex(
                name: "IX_AttributeOptions_AttributeId_SortOrder",
                table: "AttributeOptions",
                columns: new[] { "AttributeId", "SortOrder" });

            migrationBuilder.CreateIndex(
                name: "IX_Attributes_ArabicDisplayName",
                table: "Attributes",
                column: "ArabicDisplayName");

            migrationBuilder.CreateIndex(
                name: "IX_Attributes_EnglishDisplayName",
                table: "Attributes",
                column: "EnglishDisplayName");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "AttributeCategories");

            migrationBuilder.DropTable(
                name: "AttributeOptions");

            migrationBuilder.DropTable(
                name: "Attributes");
        }
    }
}
