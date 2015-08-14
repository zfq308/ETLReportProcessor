using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ETLDataProcessor.ConfigMapper
{
    public class AppConfigHandler
    {
        private static AppConfigHandler instance;
        private static DatabaseConfigMapper dbConfig;
        private static LogConfigMapper logConfig;
        private static DataFilesConfigMapper dataFilesConfig;



        private AppConfigHandler() { }

        ~AppConfigHandler()
        {
            // release any i/o resources here        
        }


        /// <summary>
        /// An AppConfigHandler instance that exposes a single instance
        /// </summary>
        public static AppConfigHandler Instance
        {
            get
            {
                // If the instance is null then create one and init the Queue
                if (instance == null)
                {
                    instance = new AppConfigHandler();
                    dbConfig = new DatabaseConfigMapper();
                    logConfig = new LogConfigMapper();
                    dataFilesConfig = new DataFilesConfigMapper();
                }
                return instance;
            }
        }


        public void setDBConfig(String key, String value)
        {
            dbConfig.setMap(key, value);
        }

        public String getDBConfig(String key)
        {
            return dbConfig.getMap(key);
        }

        public void setLogConfig(String key, String value)
        {
            logConfig.setMap(key, value);
        }

        public String getLogConfig(String key)
        {
            return logConfig.getMap(key);
        }

        public void setDataFilesConfig(String key, String value)
        {
            dataFilesConfig.setMap(key, value);
        }

        public String getDBFilesConfig(String key)
        {
            return dataFilesConfig.getMap(key);
        }


    }


}
