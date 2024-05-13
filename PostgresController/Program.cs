// See https://aka.ms/new-console-template for more information
using Npgsql;

Console.WriteLine("Hello, World!");
NpgsqlConnection connection = new NpgsqlConnection("Server=192.168.1.32;Port=5432;User Id= postgres;Password= 2;Database=fortest;");
connection.Open();
float sizeStart = 0f;
List<float> values = new List<float>();
while (1 == 1)
{
    using (var cmd = new NpgsqlCommand("SELECT sum(pg_relation_size(pg_catalog.pg_class.oid))/ 1024 / 1024 / 1024 as table_size\r\n   FROM pg_catalog.pg_class\r\n     JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid\r\n    where pg_catalog.pg_namespace.nspname = 'risks'", connection))
    {
        float sizeCurrent = float.Parse(cmd.ExecuteScalar().ToString());
        float diff = sizeCurrent - sizeStart;
        if (sizeStart > 0f)
        {
            values.Add(diff);
            if (values.Count > 10)
            {
                values.RemoveAt(0);
            }
        }
        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.FF")}  |  изменение размера: {diff:F3}G  |  общий размер: {sizeCurrent:F3}G  |  среднее изменение размера (10): {(sizeStart > 0f ? values.Average() : 0f):F3}");
        sizeStart = sizeCurrent;

        Thread.Sleep(10000);
    }
}

