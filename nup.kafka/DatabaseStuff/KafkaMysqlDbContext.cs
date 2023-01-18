using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace nup.kafka.DatabaseStuff;

public class KafkaMysqlDbContext:DbContext
{
    // public KafkaMysqlDbContext(DbContextOptions<KafkaMysqlDbContext> options) : base(options) { }
    public DbSet<KafkaMessage> KafkaEvents { get; set; }
    public static string ConnectionString = $"Server=localhost;Database=KafkaMysql;Uid=root;Pwd=root;";

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    { 
        optionsBuilder.UseMySql(ConnectionString,
            ServerVersion.AutoDetect(ConnectionString), mysqlOptions => mysqlOptions.UseNetTopologySuite());
    }
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<KafkaMessage>()
            .HasIndex(p => p.RecievedCreatedAtUtc);
        modelBuilder.Entity<KafkaMessage>()
            .HasIndex(p => p.Topic);
        modelBuilder.Entity<KafkaMessage>()
            .HasIndex(p => new { p.Partition, p.OffSet });
    }
}