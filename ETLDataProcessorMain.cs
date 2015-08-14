using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.IO;
using System.Xml;
using ETLDataProcessor.ETL.Extractor;
using ETLDataProcessor.ConfigMapper;
using ETLDataProcessor.Logger;
using System.Data.SqlClient;


namespace ETLDataProcessor
{
    enum DataReportType
    {
        // Variant Call Formats
        GENOME_VCF_MAIN,
        GENOME_VCF_EXT,

        // BAM Formats
        GENOME_BAM,

        // BLAST/FASTA Formats
        GENOME_BLAST,
        GENOME_FASTA,
        GENOME_FASTQ
    }

    // Other Formats
    // XML, JSON, RDF, RSS

    enum LogConfigSettings
    {
        folder,
        filetag,
        maxLogAge,
        queueSize,
        enable,
        verboseLevel
    }

    enum DatabaseCredentialsSettings
    {
        name,
        server,
        database,
        username,
        password
    }

    // Pick up the files from the source location, process them according to rules, load it into database and move the processed files.
    enum DatabaseReportFilesSettings
    {
        data_files_folder,
        processed_files_folder
    }



    class ETLDataProcessorMain
    {
        static LogWriter writer;
        static AppConfigHandler appConfig;

        static void Main(string[] args)
        {

            ETLDataProcessorMain dbProcessorObj = new ETLDataProcessorMain();
            appConfig = AppConfigHandler.Instance;
            dbProcessorObj.readXMLAppConfig();

            writer = LogWriter.Instance;
            writer.initializeLogSettings(AppConfigHandler.Instance.getLogConfig(LogConfigSettings.folder.ToString()), AppConfigHandler.Instance.getLogConfig(LogConfigSettings.filetag.ToString()), AppConfigHandler.Instance.getLogConfig(LogConfigSettings.enable.ToString()));

            Console.WriteLine("Starting Database Reporting Processor");
            LogWriter.Instance.WriteToLog("Starting Database Reporting Processor");

            dbProcessorObj.processXMLAppConfigToLogs();


            dbProcessorObj.testDBConnection();
            dbProcessorObj.processDBReportFiles();

            LogWriter.Instance.WriteToLog("Completed Database Reporting Processor");
            LogWriter.Instance.WriteToLog("Exiting");
            Console.WriteLine("Completed Database Reporting Processor");
            Console.WriteLine("Exiting");
            Console.ReadLine();
        }

        private void testDBConnection()
        {
            bool dbConnectStatus = false;
            String sqlconn = ETLDataProcessor.ETL.Loader.DatabaseHelper.createConnectionString();
            SqlConnection con = null;
            try
            {
                con = new SqlConnection(sqlconn);
                con.Open();
                con.Close();
                con.Dispose();
                dbConnectStatus = true;
            }
            catch (Exception ex)
            {
                LogWriter.Instance.WriteToLog(ex.Message);
            }
            finally
            {
                if (con != null)
                {
                    con.Close();
                    con.Dispose();
                }
                if (dbConnectStatus)
                {
                    LogWriter.Instance.WriteToLog("Testing database connection (open/close) - Successful");
                }
                else
                {
                    LogWriter.Instance.WriteToLog("ERROR: Testing database connection (open/close) - Failed (Check Credentials)");

                }
            }


        }



        private void processXMLAppConfigToLogs()
        {
            LogWriter.Instance.WriteToLog("Reading configuration file done");
            writeXMLAppConfigsToLog();
        }




        private void writeXMLAppConfigsToLog()
        {
            String currentSetting = "Database Settings";
            String key = "";
            String value = "";
            String log_mesg = "";
            if (currentSetting.Equals("Database Settings"))
            {
                key = DatabaseCredentialsSettings.name.ToString();
                value = appConfig.getDBConfig(key);
                if (value == null || (value != null && value.Trim().Length == 0))
                {
                    value = "{Not Provided}";
                }
                log_mesg = currentSetting + " : " + key + " --> " + value;



                key = DatabaseCredentialsSettings.server.ToString();
                value = appConfig.getDBConfig(key);
                if (value == null || (value != null && value.Trim().Length == 0))
                {
                    value = "{Not Provided}";
                }
                log_mesg = currentSetting + " : " + key + " --> " + value;
                writer.WriteToLog(log_mesg);



                key = DatabaseCredentialsSettings.database.ToString();
                value = appConfig.getDBConfig(key);
                if (value == null || (value != null && value.Trim().Length == 0))
                {
                    value = "{Not Provided}";
                }
                log_mesg = currentSetting + " : " + key + " --> " + value;
                writer.WriteToLog(log_mesg);

                key = DatabaseCredentialsSettings.username.ToString();
                value = appConfig.getDBConfig(key);
                if (value == null || (value != null && value.Trim().Length == 0))
                {
                    value = "{Not Provided}";
                }
                log_mesg = currentSetting + " : " + key + " --> " + value;

                key = DatabaseCredentialsSettings.password.ToString();
                value = appConfig.getDBConfig(key);
                if (value == null || (value != null && value.Trim().Length == 0))
                {
                    value = "{Not Provided}";
                }
                log_mesg = currentSetting + " : " + key + " --> " + value;


            }

            currentSetting = "Report Files Settings";
            if (currentSetting.Equals("Report Files Settings"))
            {


                key = DatabaseReportFilesSettings.data_files_folder.ToString();
                value = appConfig.getDataFilesConfig(key);
                if (value == null || (value != null && value.Trim().Length == 0))
                {
                    value = "{Not Provided}";
                }
                log_mesg = currentSetting + " : " + key + " --> " + value;
                writer.WriteToLog(log_mesg);



                key = DatabaseReportFilesSettings.processed_files_folder.ToString();
                value = appConfig.getDataFilesConfig(key);
                if (value == null || (value != null && value.Trim().Length == 0))
                {
                    value = "{Not Provided - will continue by using data_files_folder value} ";
                }
                log_mesg = currentSetting + " : " + key + " --> " + value;
                writer.WriteToLog(log_mesg);



            }

            currentSetting = "LOG Settings";
            if (currentSetting.Equals("LOG Settings"))
            {
                key = LogConfigSettings.folder.ToString();
                value = appConfig.getLogConfig(key);
                if (value == null || (value != null && value.Trim().Length == 0))
                {
                    value = "{Not Provided}";
                }
                log_mesg = currentSetting + " : " + key + " --> " + value;
                writer.WriteToLog(log_mesg);

                key = LogConfigSettings.filetag.ToString();
                value = appConfig.getLogConfig(key);
                if (value == null || (value != null && value.Trim().Length == 0))
                {
                    value = "{Not Provided}";
                }
                else
                {
                    value += " ( Log file will be stored as : " + value + "_" + DateTime.Now.ToString("yyyy-MM-dd") + " format )";

                }
                log_mesg = currentSetting + " : " + key + " --> " + value;
                writer.WriteToLog(log_mesg);

                key = LogConfigSettings.enable.ToString();
                value = appConfig.getLogConfig(key);
                if (value == null || (value != null && value.Trim().Length == 0))
                {
                    value = "{Not Provided}";
                }
                log_mesg = currentSetting + " : " + key + " --> " + value;

            }


        }




        private void processDBReportFiles()
        {
            (new DataFilesProcessor()).processDataFile();
        }



        private void readXMLAppConfig()
        {
            String configFilename = "Configs/ETLDataProcessor.config";
            try
            {
                using (XmlTextReader reader = new XmlTextReader(configFilename))
                {
                    String currentSetting = "";
                    while (reader.Read())
                    {
                        if (reader.IsStartElement())
                        {
                            String message = "";
                            String value = "";
                            String key = reader.Name;

                            switch (key)
                            {
                                case "db_credentials_Settings":
                                    currentSetting = "db_credentials_Settings";
                                    break;

                                case "data_files_Settings":
                                    currentSetting = "data_files_Settings";
                                    break;

                                case "log_Settings":
                                    currentSetting = "log_Settings";
                                    break;


                                case "name":
                                    if (currentSetting.Equals("db_credentials_Settings"))
                                    {
                                        value = reader.ReadString();
                                        message = key + " : " + value;
                                        appConfig.setDBConfig(key, value);
                                    }
                                    break;
                                case "server":
                                    if (currentSetting.Equals("db_credentials_Settings"))
                                    {
                                        value = reader.ReadString();
                                        message = key + " : " + value;
                                        appConfig.setDBConfig(key, value);
                                    }
                                    break;
                                case "database":
                                    if (currentSetting.Equals("db_credentials_Settings"))
                                    {
                                        value = reader.ReadString();
                                        message = key + " : " + value;
                                        appConfig.setDBConfig(key, value);
                                    }
                                    break;
                                case "username":
                                    if (currentSetting.Equals("db_credentials_Settings"))
                                    {
                                        value = reader.ReadString();
                                        message = key + " : " + value;
                                        appConfig.setDBConfig(key, value);
                                    }
                                    break;
                                case "password":
                                    if (currentSetting.Equals("db_credentials_Settings"))
                                    {
                                        value = reader.ReadString();
                                        message = key + " : " + value;
                                        appConfig.setDBConfig(key, value);
                                    }
                                    break;

                                case "provider":
                                    if (currentSetting.Equals("db_credentials_Settings"))
                                    {
                                        value = reader.ReadString();
                                        message = key + " : " + value;
                                        appConfig.setDBConfig(key, value);
                                    }
                                    break;

                                case "data_files_folder":
                                    if (currentSetting.Equals("data_files_Settings"))
                                    {
                                        value = reader.ReadString();
                                        message = key + " : " + value;
                                        appConfig.setDataFilesConfig(key, value);
                                    }
                                    break;


                                case "processed_files_folder":
                                    if (currentSetting.Equals("data_files_Settings"))
                                    {
                                        value = reader.ReadString();
                                        message = key + " : " + value;
                                        appConfig.setDataFilesConfig(key, value);
                                    }
                                    break;


                                case "folder":
                                    if (currentSetting.Equals("log_Settings"))
                                    {
                                        value = reader.ReadString();
                                        message = key + " : " + value;
                                        appConfig.setLogConfig(key, value);
                                    }
                                    break;


                                case "filetag":
                                    if (currentSetting.Equals("log_Settings"))
                                    {
                                        value = reader.ReadString();
                                        message = key + " : " + value;
                                        appConfig.setLogConfig(key, value);
                                    }
                                    break;


                                case "enable":
                                    if (currentSetting.Equals("log_Settings"))
                                    {
                                        value = reader.ReadString();
                                        message = key + " : " + value;
                                        appConfig.setLogConfig(key, value);
                                    }
                                    break;




                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Unable to read the Config File at : " + configFilename + "(" + ex.Message + ")");
            }
        }


    }
}
