using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ETLDataProcessor.ConfigMapper
{
    class DatabaseConfigMapper
    {
        private String name;
        private String server;
        private String database;
        private String username;
        private String password;


        public void setMap(String key, String value)
        {
            switch (key)
            {
                case "name":
                    name = value;
                    break;
                case "server":
                    server = value;
                    break;
                case "database":
                    database = value;
                    break;
                case "username":
                    username = value;
                    break;
                case "password":
                    password = value;
                    break;
            }
        }

        public String getMap(String key)
        {
            String value = "";
            switch (key)
            {
                case "name":
                    value = name;
                    break;
                case "server":
                    value = server;
                    break;
                case "database":
                    value = database;
                    break;
                case "username":
                    value = username;
                    break;
                case "password":
                    value = password;
                    break;
            }
            return value;

        }

    }
}

