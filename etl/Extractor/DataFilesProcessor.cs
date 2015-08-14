using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using ETLDataProcessor.ETL.Extractor;
using ETLDataProcessor.Logger;
using ETLDataProcessor.ConfigMapper;


namespace ETLDataProcessor.ETL.Extractor
{


    class DataFilesProcessor
    {


        public void processDataFiles()
        {
            LogWriter.Instance.WriteToLog("Starting database report files processing");
            String dbreportTypesProcessed = "";
            String unProcessedFolders = "";
            foreach (DataReportType dbReportType in Enum.GetValues(typeof(DataReportType)))
            {
                Console.WriteLine("\nProcessing database report files for report type : [" + dbReportType.ToString().ToLower() + "]");
                LogWriter.Instance.WriteToLog("Processing database report files for report type : [" + dbReportType.ToString().ToLower() + "]");
                bool allFilesProcessed = processDataFileFolder(dbReportType);
                dbreportTypesProcessed += "[" + dbReportType.ToString().ToLower() + "] ";
                if (!allFilesProcessed)
                {
                    unProcessedFolders += "[" + dbReportType.ToString().ToLower() + "] ";
                }
            }

            if (unProcessedFolders != null && unProcessedFolders.Trim().Length > 0)
            {
                LogWriter.Instance.WriteToLog("\nSome unprocessed files exists in folders : " + unProcessedFolders);
                Console.WriteLine("\nSome unprocessed files exists in folders : " + unProcessedFolders);

            }
            else
            {
                LogWriter.Instance.WriteToLog("\nAll files processed : " + dbreportTypesProcessed);
                Console.WriteLine("\nAll files processed : " + dbreportTypesProcessed);
            }
            LogWriter.Instance.WriteToLog("Completed database report files processing : " + dbreportTypesProcessed);


        }


        private bool processDataFilesFolder(DataReportType dbReportType)
        {
            // Get Root Path for DB Files [ data_files_folder ] 
            string s_dbFilesFolder = null;
            bool allFilesProcessed = true;


            try
            {
                s_dbFilesFolder = AppConfigHandler.Instance.getDBFilesConfig("data_files_folder");

                if (s_dbFilesFolder == null || (s_dbFilesFolder != null && s_dbFilesFolder.Trim().Length == 0))
                {
                    throw (new IOException("ERROR : The start folder for processing Database Reporting files is not provided in the Config"));
                }


                // Create the DBReport Folder Path
                s_dbFilesFolder += "/" + dbReportType.ToString().ToLower() + "/";

                string[] dbFiles = null;
                dbFiles = Directory.GetFiles(s_dbFilesFolder, "*.*", SearchOption.AllDirectories);

                if (dbFiles != null)
                {
                    if (dbFiles.Length == 0)
                    {
                        Console.WriteLine("No files to process in [" + dbReportType.ToString().ToLower() + "]");
                        LogWriter.Instance.WriteToLog("No files to process in [" + dbReportType.ToString().ToLower() + "]");
                    }
                    else
                    {

                        foreach (string fileName in dbFiles)
                        {
                            String folderFilename = dbReportType.ToString().ToLower() + "/" + Path.GetFileName(fileName);

                            LogWriter.Instance.WriteToLog("Processing : ( " + folderFilename + " )");
                            Console.WriteLine("\t" + Path.GetFileName(fileName));

                            bool DBFileProcessedSuccessfully = false;

                            DBFileProcessedSuccessfully = (new DataFilesReader()).LoadDataReportFileToDatabase(dbReportType, fileName);



                            if (DBFileProcessedSuccessfully)
                            {
                                LogWriter.Instance.WriteToLog("Successfully loaded ( " + folderFilename + " ) into database");
                                string s_dbFilesProcessedFolder = null;
                                s_dbFilesProcessedFolder = AppConfigHandler.Instance.getDBFilesConfig("processed_files_folder");
                                if (s_dbFilesProcessedFolder == null || (s_dbFilesProcessedFolder != null && s_dbFilesProcessedFolder.Trim().Length == 0))
                                {
                                    LogWriter.Instance.WriteToLog("The [processed] folder is not provided in the config-file. Using the database-report files folder (data_files_folder) from config-file to create the [processed] folder");
                                    s_dbFilesProcessedFolder = AppConfigHandler.Instance.getDBFilesConfig("data_files_folder") + "/" + "processed" + "/";
                                }



                                s_dbFilesProcessedFolder += "/" + dbReportType.ToString().ToLower() + "/";
                                try
                                {
                                    Directory.CreateDirectory(@s_dbFilesProcessedFolder);
                                }
                                catch (IOException io_ex)
                                {
                                    throw new IOException("ERROR: Unable to create folder [" + dbReportType.ToString().ToLower() + "] in [processed]");
                                }

                                string moveTargetFilePath = s_dbFilesProcessedFolder + Path.GetFileName(fileName);
                                if (File.Exists(moveTargetFilePath))
                                    File.Delete(moveTargetFilePath);

                                try
                                {
                                    File.Move(@fileName, moveTargetFilePath);
                                    LogWriter.Instance.WriteToLog("Processed and moved the file ( " + Path.GetFileName(fileName) + " ) from [" + dbReportType.ToString().ToLower() + "] to [processed/" + dbReportType.ToString().ToLower() + "]");
                                }
                                catch (IOException io_ex)
                                {
                                    throw (new IOException("ERROR : Processed but unable to move the file ( " + Path.GetFileName(fileName) + " ) from [" + dbReportType.ToString().ToLower() + "] to [processed/" + dbReportType.ToString().ToLower() + "]"));
                                }
                            }
                            else
                            {
                                LogWriter.Instance.WriteToLog("ERROR: ( " + folderFilename + " ) - Unable to load records into database ( Check delimitter )");
                                LogWriter.Instance.WriteToLog("ERROR: ( " + folderFilename + " ) - File not processed and moved from [" + dbReportType.ToString().ToLower() + "] to [processed/" + dbReportType.ToString().ToLower() + "]");
                                allFilesProcessed = false;
                            }


                        }
                    }
                }

            }
            catch (Exception ex)
            {
                LogWriter.Instance.WriteToLog(ex.Message);
            }
            return allFilesProcessed;
        }



    }
}
