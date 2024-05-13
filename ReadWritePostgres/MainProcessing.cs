using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReadWritePostgres
{
    internal class MainProcessing
    {

        public static void Do()
        {

            var time = DateTime.Now;
            var sizeStart = 0;

            DbProvider db = new DbProvider();
            db.GetOpenedConnection();



            Console.WriteLine("проверяем что есть в blocks");
            using (var maxBlocksCommand = db.Read("select max(id) from risks.blocks"))
            {
                maxBlocksCommand.Read();
                bool noBlocks = maxBlocksCommand.IsDBNull(0);
                if (noBlocks)
                {
                    Console.WriteLine($"Нет записей blocks в БД");
                    return;
                }
                maxBlocksCommand.Close();
                Console.WriteLine();
            }
            
            Console.WriteLine("выбор последнего блока в blocks2");
            long startIndex = 0;
            using (var maxBlocks2Command = db.Read("select max(id) from risks.blocks2")){
                maxBlocks2Command.Read();
                bool noBlocks2 = maxBlocks2Command.IsDBNull(0);
                if (!noBlocks2)
                {
                    startIndex = maxBlocks2Command.GetInt64(0);
                    Console.WriteLine($"Записей blocks2 в БД: {startIndex}");
                }
                else
                {
                    Console.WriteLine($"Нет записей blocks2 в БД");
                }
                maxBlocks2Command.Close();
                Console.WriteLine(); 
            }

            var batch = new NpgsqlBatch(db.Connection);
            int batchSize = 100;
            for (long startBatchBlockId = startIndex + 1; startBatchBlockId <= 1000000000; startBatchBlockId = startBatchBlockId + batchSize)
            {
                long endBatchBlockId = startBatchBlockId + batchSize - 1;
                using (var blockRows = db.Read($"select * from risks.blocks b where b.id between {startBatchBlockId} and {endBatchBlockId}"))
                {

                    while (blockRows.Read())
                    {
                        var attr1 = blockRows.GetString(0);
                        var attr2 = blockRows.GetString(1);
                        var attr3 = blockRows.GetString(2);
                        var attr4 = blockRows.GetString(3);
                        var attr5 = blockRows.GetString(4);
                        var attr6 = blockRows.GetString(5);
                        var attr7 = blockRows.GetString(6);
                        var attrId = blockRows.GetInt64(7);


                        var batchCommandInsertBlock = new NpgsqlBatchCommand("INSERT INTO risks.blocks2(attr1,attr2,attr3,attr4,attr5,attr6,attr7,id) VALUES(@attr1,@attr2,@attr3,@attr4,@attr5,@attr6,@attr7,@block_id)");

                        batchCommandInsertBlock.Parameters.AddWithValue("attr1", attr1);
                        batchCommandInsertBlock.Parameters.AddWithValue("attr2", attr2);
                        batchCommandInsertBlock.Parameters.AddWithValue("attr3", attr3);
                        batchCommandInsertBlock.Parameters.AddWithValue("attr4", attr4);
                        batchCommandInsertBlock.Parameters.AddWithValue("attr5", attr5);
                        batchCommandInsertBlock.Parameters.AddWithValue("attr6", attr6);
                        batchCommandInsertBlock.Parameters.AddWithValue("attr7", attr7);
                        batchCommandInsertBlock.Parameters.AddWithValue("block_id", attrId);
                        batch.BatchCommands.Add(batchCommandInsertBlock);
                    }
                    blockRows.Close();
                }

                using (var transactionRows = db.Read($"select * from risks.transactions b where b.block_id between {startBatchBlockId} and {endBatchBlockId}"))
                {

                    while (transactionRows.Read())
                    {
                        var attr1 = transactionRows.GetString(0);
                        var attr2 = transactionRows.GetString(1);
                        var attr3 = transactionRows.GetString(2);
                        var attr4 = transactionRows.GetString(3);
                        var attr5 = transactionRows.GetString(4);
                        var attr6 = transactionRows.GetString(5);
                        var attr7 = transactionRows.GetString(6);
                        var attrId = transactionRows.GetInt64(7);
                        var attrBlockId = transactionRows.GetInt64(8);

                        var batchCommandInsertTransaction = new NpgsqlBatchCommand("INSERT INTO risks.transactions2(attr1,attr2,attr3,attr4,attr5,attr6,attr7,id, block2_id) VALUES(@attr1,@attr2,@attr3,@attr4,@attr5,@attr6,@attr7,@trans_id,@block_id)");

                        batchCommandInsertTransaction.Parameters.AddWithValue("attr1", attr1);
                        batchCommandInsertTransaction.Parameters.AddWithValue("attr2", attr2);
                        batchCommandInsertTransaction.Parameters.AddWithValue("attr3", attr3);
                        batchCommandInsertTransaction.Parameters.AddWithValue("attr4", attr4);
                        batchCommandInsertTransaction.Parameters.AddWithValue("attr5", attr5);
                        batchCommandInsertTransaction.Parameters.AddWithValue("attr6", attr6);
                        batchCommandInsertTransaction.Parameters.AddWithValue("attr7", attr7);
                        batchCommandInsertTransaction.Parameters.AddWithValue("trans_id", attrId);
                        batchCommandInsertTransaction.Parameters.AddWithValue("block_id", attrBlockId);
                        batch.BatchCommands.Add(batchCommandInsertTransaction);
                    }
                    transactionRows.Close();
                }
                //transfers
                using (var rows = db.Read($"select * from risks.transfers b where b.block_id between {startBatchBlockId} and {endBatchBlockId}"))
                {

                    while (rows.Read())
                    {
                        var attr1 = rows.GetString(0);
                        var attr2 = rows.GetString(1);
                        var attr3 = rows.GetString(2);
                        var attr4 = rows.GetString(3);
                        var attr5 = rows.GetString(4);
                        var attr6 = rows.GetString(5);
                        var attr7 = rows.GetString(6);
                        var attrId = rows.GetInt64(7);
                        var attrTransactionId = rows.GetInt64(8);
                        var attrBlockId = rows.GetInt64(9);

                        var batchCommand = new NpgsqlBatchCommand("INSERT INTO risks.transfers2(attr1,attr2,attr3,attr4,attr5,attr6,attr7,id, transaction2_id,block2_id) VALUES(@attr1,@attr2,@attr3,@attr4,@attr5,@attr6,@attr7,@transfer_id,@transaction_id,@block_id)");

                        batchCommand.Parameters.AddWithValue("attr1", attr1);
                        batchCommand.Parameters.AddWithValue("attr2", attr2);
                        batchCommand.Parameters.AddWithValue("attr3", attr3);
                        batchCommand.Parameters.AddWithValue("attr4", attr4);
                        batchCommand.Parameters.AddWithValue("attr5", attr5);
                        batchCommand.Parameters.AddWithValue("attr6", attr6);
                        batchCommand.Parameters.AddWithValue("attr7", attr7);
                        batchCommand.Parameters.AddWithValue("transfer_id", attrId);
                        batchCommand.Parameters.AddWithValue("transaction_id", attrTransactionId);
                        batchCommand.Parameters.AddWithValue("block_id", attrBlockId);
                        batch.BatchCommands.Add(batchCommand);
                    }
                    rows.Close();
                }


                batch.Prepare();
                batch.ExecuteNonQuery();
                batch = new NpgsqlBatch(db.Connection);

                if ((endBatchBlockId % 100) == 0)
                {
                    //var cmd = new NpgsqlCommand("SELECT cast(sum(pg_relation_size(pg_catalog.pg_class.oid))/ 1024 / 1024 as integer) as table_size\r\n   FROM pg_catalog.pg_class\r\n     JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid\r\n    where pg_catalog.pg_namespace.nspname = 'risks'", db.Connection);
                    //int sizeCurrent = Int32.Parse(cmd.ExecuteScalar().ToString());

                    //Console.WriteLine($"строк в БД: {endBatchBlockId}  |  выполнено за: {DateTime.Now - time}  |  изменение размера: {sizeCurrent - sizeStart}M  |  общий размер: {(((float)sizeCurrent / 1024f)):F3}G ");

                    //sizeStart = sizeCurrent;

                    Console.WriteLine($"строк в БД: {endBatchBlockId}  |  выполнено за: {DateTime.Now - time}");
                    time = DateTime.Now;

                }

            }
            Console.WriteLine("Конец");
            Console.ReadLine();

            db.CloseConnection();

        }
    }
}
