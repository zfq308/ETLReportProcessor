using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using ETLDataProcessor.Logger;
using ETLDataProcessor.ConfigMapper;

namespace ETLDataProcessor.ETL.Loader
{
    class DatabaseHelper
    {

        public static String createConnectionString()
        {
            String name = AppConfigHandler.Instance.getDBConfig(DatabaseCredentialsSettings.name.ToString());
            String server = AppConfigHandler.Instance.getDBConfig(DatabaseCredentialsSettings.server.ToString());
            String database = AppConfigHandler.Instance.getDBConfig(DatabaseCredentialsSettings.database.ToString());
            String username = AppConfigHandler.Instance.getDBConfig(DatabaseCredentialsSettings.username.ToString());
            String password = AppConfigHandler.Instance.getDBConfig(DatabaseCredentialsSettings.password.ToString());

            // Construct the connection String here depending on the Provider
            String connectionString = "";

            connectionString = "Data Source=" + server;
            connectionString += ";Initial Catalog=" + database;
            connectionString += ";User ID=" + username;
            connectionString += ";Password=" + password;
            // connectionString += ";Integrated Security=True";
            // connectionString += ";Trusted_Connection=yes";
            // connectionString += ";Provider=System.Data.SqlClient";

            // Console.WriteLine("SQL Connection String : [" + connectionString + "]");
            return connectionString;
        }
    }
}
