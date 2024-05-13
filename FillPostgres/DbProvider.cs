using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FillPostgres
{
    internal class DbProvider
    {
        NpgsqlConnection connection = new NpgsqlConnection("Server=192.168.1.32;Port=5432;User Id= postgres;Password= 2;Database=fortest;");
        internal NpgsqlConnection GetOpenedConnection()
        {
            Console.WriteLine("подключаемся к базе ");
            connection.Open();
            return connection;
        }

        internal NpgsqlConnection Connection { get { return connection; } }

        internal void CloseConnection() {
            connection.Close();
        }
        ~DbProvider() {
            connection.Close();
        }

        internal NpgsqlDataReader Read(string commandstring) {
            NpgsqlCommand command = new NpgsqlCommand(commandstring, connection);
            NpgsqlDataReader rdr = command.ExecuteReader();

            return rdr;
        }

        internal NpgsqlCommand GetPreCommand(string commandstring)
        {
            return new NpgsqlCommand(commandstring, connection);
        }
        internal int PrepareExecuteCommand(NpgsqlCommand Command)
        {
            Command.Prepare();
            return Command.ExecuteNonQuery();

        }
    }
}
