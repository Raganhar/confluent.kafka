using System;
using Microsoft.EntityFrameworkCore.Migrations;

namespace nup.kafka.Migrations
{
    public partial class InitialCreate : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterDatabase()
                .Annotation("MySql:CharSet", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "KafkaEvents",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "char(36)", nullable: false, collation: "ascii_general_ci"),
                    PartitionKey = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    Partition = table.Column<int>(type: "int", nullable: false),
                    OffSet = table.Column<long>(type: "bigint", nullable: false),
                    ProcessedSuccefully = table.Column<bool>(type: "tinyint(1)", nullable: false),
                    ReasonText = table.Column<string>(type: "varchar(2000)", maxLength: 2000, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    Topic = table.Column<string>(type: "varchar(255)", nullable: false)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    RecievedCreatedAtUtc = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    FinishedProcessingAtUtc = table.Column<DateTime>(type: "datetime(6)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_KafkaEvents", x => x.Id);
                })
                .Annotation("MySql:CharSet", "utf8mb4");

            migrationBuilder.CreateIndex(
                name: "IX_KafkaEvents_Partition_OffSet",
                table: "KafkaEvents",
                columns: new[] { "Partition", "OffSet" });

            migrationBuilder.CreateIndex(
                name: "IX_KafkaEvents_RecievedCreatedAtUtc",
                table: "KafkaEvents",
                column: "RecievedCreatedAtUtc");

            migrationBuilder.CreateIndex(
                name: "IX_KafkaEvents_Topic",
                table: "KafkaEvents",
                column: "Topic");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "KafkaEvents");
        }
    }
}
