using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;
using ETLDataProcessor.ETL.Loader;
using ETLDataProcessor.ETL.Transformer;
using ETLDataProcessor.Logger;

namespace ETLDataProcessor.ETL.Extractor
{
    class DataFilesReader
    {

        public bool LoadDataReportFileToDatabase(DataReportType dbReportType, string dbReportFilename)
        {

            String folderFilename = dbReportType.ToString().ToLower() + "/" + Path.GetFileName(dbReportFilename);
            bool insertStatus = false;

            // Handle various delimitters for files
            char[] delimitters = { '\n' }; // { '\n', '\r' }; 
            string[] delimitters_str = { "\\n" }; // { "\\n", "\\r" };
            String delimitters_sofar = "";

            for (int i = 0; i < delimitters.Length; i++)
            {
                char delimitter = delimitters[i];
                string delimitter_str = delimitters_str[i];

                if (i == 0)
                {
                    delimitters_sofar += delimitter_str;
                }
                else
                {
                    delimitters_sofar += " , " + delimitter_str;
                }

                LogWriter.Instance.WriteToLog("Extracting the file : ( " + folderFilename + " ) with delimitter[" + delimitter_str + "]");
                DataTable tbl = ExtractFileToDataTable(dbReportType, dbReportFilename, delimitter);

                if (tbl != null & tbl.Rows.Count > 0)
                {
                    LogWriter.Instance.WriteToLog("Completed extracting the  file : ( " + folderFilename + " ) with delimitter[" + delimitter_str + "]");
                    insertStatus = (new DatabaseStore()).LoadDataTableToDatabase(dbReportType, tbl);
                    break;
                }
                else
                {
                    if (i < delimitters.Length - 1)
                    {
                        LogWriter.Instance.WriteToLog("WARNING: ( " + folderFilename + " ) - Unable to load records into database with delimitters[" + delimitters_sofar + "]");
                    }
                    if (i == delimitters.Length - 1)
                    {

                        LogWriter.Instance.WriteToLog("ERROR: ( " + folderFilename + " ) - Unable to load records into database with delimitters[" + delimitters_sofar + "]");

                    }

                }
            }
            return insertStatus;
        }

        private bool checkRowIsATitleHeader(DataReportType dbReportType, String fileTag, String Row)
        {
            String[] vcf_main = 
            {
                "#CHROM",
                "POS",
                "ID",
                "REF",
                "ALT",
                "QUAL",
                "Date"
            };

            String[] vcf_ext = 
            {
                "#CHROM",
                "POS",
                "ID",
                "REF",
                "ALT",
                "QUAL",
                "FILTER",
                "INFO",
                "FORMAT",
                "NA00001",
                "NA00002",
                "NA00003",
                "Date"
            };




            bool isRowATitleHeader = false;
            string[] columns = Row.Split(',');

            int num_matches = 0;
            // Fuzzy match
            if (dbReportType == DataReportType.GEONOME_VCF_MAIN)
            {
                for (int i = 0; i < columns.Length; i++)
                {
                    if (columns[i].Replace("\"", "").Trim().ToLower().Equals(server_titles[i].Trim().ToLower()))
                    {
                        num_matches++;
                    }
                }
                double col_pct_match = num_matches / columns.Length;
                if (col_pct_match > 0.85)
                {
                    isRowATitleHeader = true;
                }
            }

            num_matches = 0;
            if (dbReportType == DataReportType.GENOME_VCF_EXT)
            {
                for (int i = 0; i < columns.Length; i++)
                {
                    if (columns[i].Replace("\"", "").Trim().ToLower().Equals(database_titles[i].Trim().ToLower()))
                    {
                        num_matches++;
                    }
                }
                double col_pct_match = num_matches / columns.Length;
                if (col_pct_match > 0.85)
                {
                    isRowATitleHeader = true;
                }
            }


            num_matches = 0;
            if (dbReportType == DataReportType.ACCESS)
            {
                for (int i = 0; i < columns.Length; i++)
                {
                    if (columns[i].Replace("\"", "").Trim().ToLower().Contains(access_titles[i].Trim().ToLower()))
                    {
                        num_matches++;
                    }
                }
                double col_pct_match = num_matches / columns.Length;
                if (col_pct_match > 0.70)
                {
                    isRowATitleHeader = true;
                }
            }
            return isRowATitleHeader;

        }




        private DataTable ExtractFileFormatToDataTable(DataReportType dbReportType, string dbFileFormatName, char delimitter)
        {

            //Creating object of datatable  
            DataTable tbl = new DataTable();
            String folderFilename = dbReportType.ToString().ToLower() + "/" + Path.GetFileName(dbFileFormatName);

            int expected_columns = 0;

            if (dbReportType == DataReportType.GENOME_VCF_MAIN)
            {
                tbl.Columns.Add("#CHROM");
                tbl.Columns.Add("POS");
                tbl.Columns.Add("ID");
                tbl.Columns.Add("REF");
                tbl.Columns.Add("ALT");
                tbl.Columns.Add("QUAL");
                tbl.Columns.Add("Date");
            }

            if (dbReportType == DataReportType.GENOME_VCF_EXT)
            {
                tbl.Columns.Add("#CHROM");
                tbl.Columns.Add("POS");
                tbl.Columns.Add("ID");
                tbl.Columns.Add("REF");
                tbl.Columns.Add("ALT");
                tbl.Columns.Add("QUAL");
                tbl.Columns.Add("FILTER");
                tbl.Columns.Add("INFO");
                tbl.Columns.Add("FORMAT");
                tbl.Columns.Add("NA00001");
                tbl.Columns.Add("NA00002");
                tbl.Columns.Add("NA00003");
                tbl.Columns.Add("Date");
            }


            expected_columns = tbl.Columns.Count;

            try
            {

                //Reading All text  
                string Read = File.ReadAllText(dbFileFormatName);
                String fileName = Path.GetFileName(dbFileFormatName);

                String fileTag = "MSSQL";
                if (fileName.StartsWith("ORACLE"))
                {
                    fileTag = "ORACLE";
                }

                LogWriter.Instance.WriteToLog("Normalizing and checking field values in records...");




                // Avoid inserting null rows.
                //spliting row after new line  
                int rowNum = 0;
                bool processRow = true;
                bool isRowATitleHeader = false;
                foreach (string Row in Read.Split(delimitter))
                {
                    isRowATitleHeader = false;
                    if (rowNum == 0)
                    {
                        isRowATitleHeader = checkRowIsATitleHeader(dbReportType, fileTag, Row);

                        if (isRowATitleHeader)
                        {
                            LogWriter.Instance.WriteToLog("Fuzzy title pattern-recognition match : ( " + folderFilename + " ) - detected a title in the first row ( skipping ) ");
                        }
                        else
                        {
                            LogWriter.Instance.WriteToLog("Fuzzy title pattern-recognition match : ( " + folderFilename + " ) - did not detect a title in the first row ( continuing processing ) ");
                        }


                    }



                    if (!isRowATitleHeader)
                    {
                        if (!string.IsNullOrEmpty(Row) && Row.Replace(",", "").Trim().Length > 0)
                        {
                            string OriginalFileRec;
                            string[] FileRecs = Row.Split(',');
                            string[] ModifiedFileRecs = null;
                            int total_idx = FileRecs.Length;

                            for (int i = 0; i < total_idx; i++)
                            {
                                FileRecs[i] = FileRecs[i].Replace("\n", "");
                                FileRecs[i] = FileRecs[i].Replace("\r", "");
                            }

                            processRow = true;

                            if (dbReportType == DataReportType.GENOME_VCF_MAIN)
                            {
                                if (total_idx != expected_columns)
                                {
                                    processRow = false;
                                }
                                else
                                {
                                    ModifiedFileRecs = new string[expected_columns];
                                }
                            }

                            if (dbReportType == DataReportType.GENOME_VCF_EXT)
                            {

                                if (total_idx != expected_columns)
                                {
                                    processRow = false;
                                }
                                else
                                {
                                    ModifiedFileRecs = new string[expected_columns];
                                }
                            }


                            if (processRow)
                            {
                                int count = 0;
                                int current_idx = 0;
                                while (current_idx < total_idx)
                                {
                                    string columnName = tbl.Columns[current_idx].ColumnName;
                                    OriginalFileRec = FileRecs[current_idx];
                                    string err_prefix = "ERROR: ( " + folderFilename + " ) - Record contains a field (" + columnName + " @ row:" + (rowNum + 1) + " | column:" + (count + 1) + ") with ";
                                    string err_suffix = "(" + OriginalFileRec + ")";
                                    current_idx++;
                                    String modifiedFileRec = (new SchemaFieldNormalizer()).normalizeField(dbReportType, fileTag, OriginalFileRec, count);
                                    // Sanity check for DateTime
                                    if (modifiedFileRec.StartsWith("EMPTY") && columnName.ToLower().Equals("date"))
                                    {
                                        modifiedFileRec = err_prefix + " NULL/EMPTY DateTime"; 
                                    }


                                    // Not a valid column data. Leave the column blank and it will be inserted as NULL in MSSQL Database
                                    if (!modifiedFileRec.Equals("EMPTY") && !modifiedFileRec.StartsWith("ERROR"))
                                    {
                                        ModifiedFileRecs[count] = modifiedFileRec;
                                    }
                                    else
                                    {

                                        if (modifiedFileRec.StartsWith("ERROR"))
                                        {
                                            modifiedFileRec = modifiedFileRec.Replace("ERROR:", err_prefix);
                                            if (OriginalFileRec.Trim().Length > 0)
                                            {
                                                modifiedFileRec += "(" + OriginalFileRec + ")";
                                            }
                                            LogWriter.Instance.WriteToLog(modifiedFileRec + " : [" + Row + "] ( skipping )");
                                            processRow = false;
                                            break;
                                        }
                                    }
                                    count++;

                                }
                                if (processRow)
                                {
                                    tbl.Rows.Add();
                                    for (int mf = 0; mf < ModifiedFileRecs.Length; mf++)
                                    {
                                        tbl.Rows[tbl.Rows.Count - 1][mf] = ModifiedFileRecs[mf];
                                    }
                                }
                                else
                                {

                                }
                            }
                            else
                            {
                                LogWriter.Instance.WriteToLog("ERROR: ( " + folderFilename + " ) - Record (@ row:" + (rowNum + 1) + ") contains incorrect number of columns [found:" + total_idx + " | expected:" + expected_columns + "] : [" + Row + "] ( skipping )");
                            }
                        }
                    }
                    rowNum++;
                }
                LogWriter.Instance.WriteToLog("Completed records normalization and checking stage ( rows:" + rowNum + " | columns:" + expected_columns + " )");

            }
            catch (IOException io_ex)
            {
                LogWriter.Instance.WriteToLog("ERROR : " + io_ex.Message);
            }
            catch (Exception ex)
            {
                LogWriter.Instance.WriteToLog("ERROR : " + ex.Message);

            }

            return tbl;
        }


    }
}
