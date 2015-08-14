using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using ETLDataProcessor.Logger;

namespace ETLDataProcessor.ETL.Loader
{
    class DatabaseStore
    {
        public bool LoadDataTableToDatabase(DataReportType dbReportType, DataTable dt)
        {
            SqlConnection con;
            String sqlconn;
            bool insertStatus = false;
            try
            {

                try
                {
                    sqlconn = DatabaseHelper.createConnectionString();
                    con = new SqlConnection(sqlconn);

                }
                catch (Exception io_ex)
                {
                    throw (new Exception("ERROR : Unable to create Database SQL connection for loading  data with the provided connection string"));
                }

                try
                {
                    con.Open();
                    LogWriter.Instance.WriteToLog("Established database connection successfully for loading  data");
                }
                catch (Exception io_ex)
                {
                    throw (new Exception("ERROR : Unable to open the database connection for loading  data"));

                }

                SqlBulkCopy objbulk;
                try
                {
                    //creating object of SqlBulkCopy    
                    objbulk = new SqlBulkCopy(con);
                }
                catch (Exception io_ex)
                {
                    throw (new Exception("ERROR : Unable to create database SQL bulk-write object for loading  data with the provided database connection"));

                }

                if (objbulk != null)
                {
                    //assigning Destination table name    
                    if (dbReportType == DataReportType.GENOME_VCF_MAIN)
                    {
                        objbulk.DestinationTableName = "DataVCFMain";

                        //Mapping Table column   
                        objbulk.ColumnMappings.Add("#CHROM", "CHROM");
                        objbulk.ColumnMappings.Add("POS","POS");
                        objbulk.ColumnMappings.Add("ID","ID");
                        objbulk.ColumnMappings.Add("REF","REF");
                        objbulk.ColumnMappings.Add("ALT","ALT");
                        objbulk.ColumnMappings.Add("QUAL","QUAL");
                        objbulk.ColumnMappings.Add("Date", "Date");

                    }

                    if (dbReportType == DataReportType.GENOME_VCF_EXT)
                    {
                        objbulk.DestinationTableName = "DataVCFExt";

                        //Mapping Table column   
                        objbulk.ColumnMappings.Add("#CHROM", "CHROM");
                        objbulk.ColumnMappings.Add("POS", "POS");
                        objbulk.ColumnMappings.Add("ID", "ID");
                        objbulk.ColumnMappings.Add("REF", "REF");
                        objbulk.ColumnMappings.Add("ALT", "ALT");
                        objbulk.ColumnMappings.Add("QUAL", "QUAL");
                        objbulk.ColumnMappings.Add("FILTER", "FILTER");
                        objbulk.ColumnMappings.Add("INFO", "INFO");
                        objbulk.ColumnMappings.Add("FORMAT", "FORMAT");
                        objbulk.ColumnMappings.Add("NA00001", "NA00001");
                        objbulk.ColumnMappings.Add("NA00002", "NA00002");
                        objbulk.ColumnMappings.Add("NA00003", "NA00003");
                        objbulk.ColumnMappings.Add("Date", "Date");

                    }
                    /* Pretty print of the table
                    //inserting Datatable Records to DataBase    
                    foreach (DataRow dataRow in dt.Rows)
                    {
                        foreach (var item in dataRow.ItemArray)
                        {
                            Console.Write(item);
                            Console.Write("\t");
                        }
                        Console.WriteLine();
                    }
                    */

                    try
                    {
                        LogWriter.Instance.WriteToLog("Bulk-loading the  data onto the database. Writing ...");
                        objbulk.WriteToServer(dt);
                        objbulk.Close();
                        con.Close();
                        LogWriter.Instance.WriteToLog("Successfully bulk-loaded  data into database. Closed database connection.");
                        insertStatus = true;
                    }
                    catch (Exception io_ex)
                    {
                        con.Close();
                        throw (new Exception("ERROR: Unable to bulk-load the  data onto the Database"));

                    }
                }
            }
            catch (SqlException sql_ex)
            {
                Console.WriteLine(sql_ex.Message);
                LogWriter.Instance.WriteToLog(sql_ex.Message);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                LogWriter.Instance.WriteToLog(ex.Message);
            }
            return insertStatus;
        }




    }
}
