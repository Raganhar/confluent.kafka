using nup.kafka.DatabaseStuff;

namespace nup.kafka;

public class DaoLayer
{
    private readonly KafkaMysqlDbContext _db;

    public DaoLayer(KafkaMysqlDbContext db)
    {
        _db = db;
    }

    public void AddEvent(KafkaMessage kafkaMessage)
    {
        throw new NotImplementedException();
    }
}