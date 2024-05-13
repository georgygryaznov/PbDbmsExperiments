using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FillPostgres
{
    internal class MainProcessing

    {
        
        public static void Do()
        {

            var time = DateTime.Now;
            var sizeStart = 0;

            DbProvider db = new DbProvider();
            db.GetOpenedConnection();

            long startIndex = 0;

            
            Console.WriteLine("проверяем что есть в blocks");
            var maxBlocksCommand = db.Read("select max(id) from risks.blocks");
            maxBlocksCommand.Read();
            bool noBlocks = maxBlocksCommand.IsDBNull(0);
            if (!noBlocks)
            {
                startIndex = maxBlocksCommand.GetInt64(0);
                Console.WriteLine($"Записей blocks в БД: {startIndex}");
            }
            else {
                Console.WriteLine($"Нет записей blocks в БД");
            }
            maxBlocksCommand.Close();
            Console.WriteLine();

            var batch = new NpgsqlBatch(db.Connection);
            for (long newBlockId = startIndex+1; newBlockId <= 1000000000; newBlockId++) {

                var batchCommandInsertBlock = new NpgsqlBatchCommand("INSERT INTO risks.blocks(attr1,attr2,attr3,attr4,attr5,attr6,attr7,id) VALUES(@attr1,@attr2,@attr3,@attr4,@attr5,@attr6,@attr7,@block_id)");

                batchCommandInsertBlock.Parameters.AddWithValue("attr1", newBlockId);
                batchCommandInsertBlock.Parameters.AddWithValue("attr2", newBlockId);
                batchCommandInsertBlock.Parameters.AddWithValue("attr3", newBlockId);
                batchCommandInsertBlock.Parameters.AddWithValue("attr4", newBlockId);
                batchCommandInsertBlock.Parameters.AddWithValue("attr5", newBlockId);
                batchCommandInsertBlock.Parameters.AddWithValue("attr6", newBlockId);
                batchCommandInsertBlock.Parameters.AddWithValue("attr7", newBlockId);
                batchCommandInsertBlock.Parameters.AddWithValue("block_id", newBlockId);
                batch.BatchCommands.Add(batchCommandInsertBlock);

                for (int i = 1; i <= 200; i++)
                {
                    long newTransactionId = newBlockId * 1000 + i;
                    var batchCommandInsertTransaction = new NpgsqlBatchCommand("INSERT INTO risks.transactions(attr1,attr2,attr3,attr4,attr5,attr6,attr7,id, block_id) VALUES(@attr1,@attr2,@attr3,@attr4,@attr5,@attr6,@attr7,@trans_id,@block_id)");

                    batchCommandInsertTransaction.Parameters.AddWithValue("attr1", newTransactionId);
                    batchCommandInsertTransaction.Parameters.AddWithValue("attr2", newTransactionId);
                    batchCommandInsertTransaction.Parameters.AddWithValue("attr3", newTransactionId);
                    batchCommandInsertTransaction.Parameters.AddWithValue("attr4", newTransactionId);
                    batchCommandInsertTransaction.Parameters.AddWithValue("attr5", newTransactionId);
                    batchCommandInsertTransaction.Parameters.AddWithValue("attr6", newTransactionId);
                    batchCommandInsertTransaction.Parameters.AddWithValue("attr7", newTransactionId);
                    batchCommandInsertTransaction.Parameters.AddWithValue("trans_id", newTransactionId);
                    batchCommandInsertTransaction.Parameters.AddWithValue("block_id", newBlockId);
                    batch.BatchCommands.Add(batchCommandInsertTransaction);

                    int maxTransferCount = new Random().Next(4);
                    for (int j = 1; j <= maxTransferCount; j++)
                    {
                        long newTransferId = newTransactionId * 10 + j;
                        var batchCommandInsertTransfer = new NpgsqlBatchCommand("INSERT INTO risks.transfers(attr1,attr2,attr3,attr4,attr5,attr6,attr7,id, transaction_id, block_id) VALUES(@attr1,@attr2,@attr3,@attr4,@attr5,@attr6,@attr7,@transfer_id,@transaction_id,@block_id)");

                        batchCommandInsertTransfer.Parameters.AddWithValue("attr1", newTransferId);
                        batchCommandInsertTransfer.Parameters.AddWithValue("attr2", newTransferId);
                        batchCommandInsertTransfer.Parameters.AddWithValue("attr3", newTransferId);
                        batchCommandInsertTransfer.Parameters.AddWithValue("attr4", newTransferId);
                        batchCommandInsertTransfer.Parameters.AddWithValue("attr5", newTransferId);
                        batchCommandInsertTransfer.Parameters.AddWithValue("attr6", newTransferId);
                        batchCommandInsertTransfer.Parameters.AddWithValue("attr7", newTransferId);
                        batchCommandInsertTransfer.Parameters.AddWithValue("transfer_id", newTransferId);
                        batchCommandInsertTransfer.Parameters.AddWithValue("transaction_id", newTransactionId);
                        batchCommandInsertTransfer.Parameters.AddWithValue("block_id", newBlockId);
                        batch.BatchCommands.Add(batchCommandInsertTransfer);

                    }
                }
                if ((newBlockId % 1000) == 0)
                {
                    batch.Prepare();
                    batch.ExecuteNonQuery();
                    batch = new NpgsqlBatch(db.Connection);

                }

                if ((newBlockId % 1000) == 0)
                {
                    //var cmd = new NpgsqlCommand("SELECT cast(sum(pg_relation_size(pg_catalog.pg_class.oid))/ 1024 / 1024 as integer) as table_size\r\n   FROM pg_catalog.pg_class\r\n     JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid\r\n    where pg_catalog.pg_namespace.nspname = 'risks'", db.Connection);
                    //int sizeCurrent = Int32.Parse(cmd.ExecuteScalar().ToString());

                    //Console.WriteLine($"строк в БД: {newBlockId}  |  получено за: {DateTime.Now - time}  |  изменение размера: {sizeCurrent-sizeStart}M  |  общий размер: {(((float)sizeCurrent /1024f)):F3}G ");
                    //sizeStart = sizeCurrent;
                    
                    Console.WriteLine($"строк в БД: {newBlockId}  |  получено за: {DateTime.Now - time}");

                    time = DateTime.Now;
                }

            }            /*for (long newBlockId = startIndex+1; newBlockId <= 1000000000; newBlockId++) {
                var cmdInsertBlock = db.GetPreCommand("INSERT INTO risks.blocks(attr1,attr2,attr3,attr4,attr5,attr6,attr7,id) VALUES(@attr1,@attr2,@attr3,@attr4,@attr5,@attr6,@attr7,@block_id)");

                cmdInsertBlock.Parameters.AddWithValue("attr1", newBlockId);
                cmdInsertBlock.Parameters.AddWithValue("attr2", newBlockId);
                cmdInsertBlock.Parameters.AddWithValue("attr3", newBlockId);
                cmdInsertBlock.Parameters.AddWithValue("attr4", newBlockId);
                cmdInsertBlock.Parameters.AddWithValue("attr5", newBlockId);
                cmdInsertBlock.Parameters.AddWithValue("attr6", newBlockId);
                cmdInsertBlock.Parameters.AddWithValue("attr7", newBlockId);
                cmdInsertBlock.Parameters.AddWithValue("block_id", newBlockId);
                int insertedBlockRows = db.PrepareExecuteCommand(cmdInsertBlock);


                for(int i = 1; i<=200;i++)
                {
                    long newTransactionId = newBlockId * 1000 + i;
                    var cmdInsertTransaction = db.GetPreCommand("INSERT INTO risks.transactions(attr1,attr2,attr3,attr4,attr5,attr6,attr7,id, block_id) VALUES(@attr1,@attr2,@attr3,@attr4,@attr5,@attr6,@attr7,@trans_id,@block_id)");

                    cmdInsertTransaction.Parameters.AddWithValue("attr1", newTransactionId);
                    cmdInsertTransaction.Parameters.AddWithValue("attr2", newTransactionId);
                    cmdInsertTransaction.Parameters.AddWithValue("attr3", newTransactionId);
                    cmdInsertTransaction.Parameters.AddWithValue("attr4", newTransactionId);
                    cmdInsertTransaction.Parameters.AddWithValue("attr5", newTransactionId);
                    cmdInsertTransaction.Parameters.AddWithValue("attr6", newTransactionId);
                    cmdInsertTransaction.Parameters.AddWithValue("attr7", newTransactionId);
                    cmdInsertTransaction.Parameters.AddWithValue("trans_id", newTransactionId);
                    cmdInsertTransaction.Parameters.AddWithValue("block_id", newBlockId);
                    int insertedTransactionRows = db.PrepareExecuteCommand(cmdInsertTransaction);

                }


                if ((newBlockId % 10000) == 0)
                {
                    Console.Write("строк в БД: ");
                    Console.WriteLine(newBlockId);
                }

            }*/

            //long? er = rdr.GetInt64(0);
            //if (rdr.GetBoolean(0)) {
            //    
            //}

            //while (rdr.Read())
            //{
            //    Console.WriteLine("{0} {1}", rdr.GetInt32(0), rdr.GetString(1));
            //}


            /*var version = command.ExecuteScalar();
            var strversion = version.ToString();
            Console.WriteLine($"Version: {version}");*/
            /*NpgsqlDataReader reader = command.ExecuteReader();
            reader.NextResult();
            Console.WriteLine(reader.GetString(1));*/
            //Console.WriteLine("command - " + command);
            Console.WriteLine("Конец");
            Console.ReadLine();

            db.CloseConnection();

        }
    }
}
