using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ETLDataProcessor.ETL.Transformer
{
    class SchemaFieldNormalizer
    {
        SchemaFieldSanitizer sanityChecker;

        public SchemaFieldNormalizer()
        {
            sanityChecker = new SchemaFieldSanitizer();
        }

        public String normalizeField(DataReportType dbReportType, String fileTag, String rowOriginalField, int columnIdx)
        {
            rowOriginalField = rowOriginalField.Replace("\"", "");

            String rowNormalizedField = rowOriginalField;

            if (dbReportType == DataReportType.GENOME_VCF_MAIN)
            {

                switch (columnIdx)
                {

                    case 1:
                        rowNormalizedField = normalizeFormatField(rowOriginalField);
                        break;

                    case 2:
                        rowNormalizedField = normalizeInfoField(rowOriginalField);
                        break;

                    case 4:
                        rowNormalizedField = normalizeNumericField(rowOriginalField);
                        break;

                    case 5:
                        rowNormalizedField = normalizeNumericField(rowOriginalField);
                        break;
                    case 6:
                        rowNormalizedField = normalizeNumericField(rowOriginalField);
                        break;
                    case 7:
                        rowNormalizedField = normalizeNumericField(rowOriginalField);
                        break;

        

                    case 11:
                        rowNormalizedField = normalizeBooleanField(rowOriginalField);
                        break;


                    case 12:
                        rowNormalizedField = normalizeDateTimeField(rowOriginalField, fileTag);
                        break;




                }

            }

            if (dbReportType == DataReportType.GENOME_VCF_EXT)
            {

                switch (columnIdx)
                {


                    case 1:
                        rowNormalizedField = normalizeFormatField(rowOriginalField);
                        break;

                    case 2:
                        rowNormalizedField = normalizeInfoField(rowOriginalField);
                        break;

                    case 5:
                        rowNormalizedField = normalizeNumericField(rowOriginalField);
                        break;
                    case 6:
                        rowNormalizedField = normalizeBooleanField(rowOriginalField);
                        break;

                    case 8:
                        rowNormalizedField = normalizeBooleanField(rowOriginalField);
                        break;

                    case 9:
                        rowNormalizedField = normalizeNumericField(rowOriginalField);
                        break;


                    case 12:
                        rowNormalizedField = normalizeDateTimeField(rowOriginalField, fileTag);
                        break;




                }


            }

            return rowNormalizedField;

        }


        private String normalizeFormatField(String rowOriginalField)
        {
            String rowNormalizedField = "EMPTY";
            if (sanityChecker.checkStrictlyNonEmptyField(rowOriginalField))
            {



                try
                {

                    rowNormalizedField = rowOriginalField.Trim().ToUpper();

                    if (rowOriginalField.Trim().ToLower().Equals("gt:gq:dp:"))
                    {
                        rowNormalizedField = "GT:GP:GQ";
                    }
                }
                catch (FormatException f_ex)
                {
                    rowNormalizedField = "ERROR: invalid format type";
                }
            }
            return rowNormalizedField.ToString();

        }

        private String normalizeInfoField(String rowOriginalField)
        {
            String rowNormalizedField = "EMPTY";
            if (sanityChecker.checkStrictlyNonEmptyField(rowOriginalField))
            {



                try
                {

                    if (rowOriginalField.Trim().Replace(" ", "").ToLower().StartsWith("ns") ||
                        rowOriginalField.Trim().Replace(" ", "").ToLower().StartsWith("dp") ||
                        rowOriginalField.Trim().Replace(" ", "").ToLower().StartsWith("af"))
                    {

                        rowNormalizedField = rowOriginalField.Trim().ToUpper();
                    }
                    else
                    {
                        rowNormalizedField = "ERROR: invalid info type";
                    }
                }
                catch (FormatException f_ex)
                {
                    rowNormalizedField = "ERROR: invalid info type";

                }
            }
            return rowNormalizedField.ToString();
        }




        private String normalizeBooleanField(String rowOriginalField)
        {
            String rowNormalizedField = "EMPTY";
            if (sanityChecker.checkStrictlyNonEmptyField(rowOriginalField))
            {



                try
                {

                    if (rowOriginalField.ToLower().StartsWith("y") || rowOriginalField.ToLower().StartsWith("yes") ||
                        rowOriginalField.ToLower().StartsWith("n") || rowOriginalField.ToLower().StartsWith("no"))
                    {
                        if (rowOriginalField.ToLower().StartsWith("y") || rowOriginalField.ToLower().StartsWith("yes"))
                        {
                            rowNormalizedField = "YES";
                        }
                        if (rowOriginalField.ToLower().StartsWith("n") || rowOriginalField.ToLower().StartsWith("no"))
                        {
                            rowNormalizedField = "NO";
                        }
                    }
                    else
                    {
                        if (rowOriginalField.Trim().Length > 0 && (rowOriginalField.Trim().ToLower().StartsWith("y") && rowOriginalField.Trim().ToLower().StartsWith("n")))
                        {
                            rowNormalizedField = "ERROR: invalid boolean (YES/NO) type";
                        }

                    }

                }
                catch (FormatException f_ex)
                {
                    rowNormalizedField = "ERROR: invalid boolean (YES/NO) type";
                }
            }
            return rowNormalizedField.ToString();
        }



        private String normalizeNumericField(String rowOriginalField)
        {
            String rowNormalizedField = "EMPTY";
            if (sanityChecker.checkStrictlyNonEmptyField(rowOriginalField))
            {
                try
                {
                    int parsedInt = Int32.Parse(rowOriginalField);

                    // sanity check that the validated numerics must be non-negative
                    if (sanityChecker.checkNonNegative(parsedInt))
                    {
                        rowNormalizedField = parsedInt.ToString();
                    }
                    else
                    {
                        rowNormalizedField = "ERROR: negative numerical value";
                    }

                }
                catch (FormatException f_ex)
                {
                    rowNormalizedField = "ERROR: invalid numerical value";
                }
            }
            return rowNormalizedField.ToString();
        }

        private String normalizeDateTimeField(String rowOriginalField, String fileTag)
        {
            String rowNormalizedField = "EMPTY";

            if (sanityChecker.checkStrictlyNonEmptyField(rowOriginalField))
            {
                try
                {
                        String[] rowDateFields = rowOriginalField.Split('/');
                        // sanity check that the date is in a valid date format
                        if (sanityChecker.checkStrictlyNonEmptyField(rowOriginalField) && rowDateFields.Length >= 3)
                        {
                            rowOriginalField = rowDateFields[2] + "-" + rowDateFields[0] + "-" + rowDateFields[1];
                        }
                        else
                        {
                            rowNormalizedField = "ERROR: invalid DateTime format";
                        }
                    // sanity check that we did have a date formattable string
                    if (sanityChecker.checkStrictlyNonEmptyField(rowOriginalField))
                    {
                        DateTime rowActualDateTime = DateTime.Parse(rowOriginalField);
                        // sanity check on the validated date time that it must be <= current date_time
                        if (sanityChecker.checkDateTimeAgainstCurrent(rowActualDateTime))
                        {
                            rowNormalizedField = rowActualDateTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
                        }
                        else
                        {
                            rowNormalizedField = "ERROR: DateTime > Current DateTime";
                        }

                    }

                }
                catch (FormatException f_ex)
                {
                    rowNormalizedField = "ERROR: invalid DateTime format";
                }
            }
            return rowNormalizedField.ToString();



        }





    }
}
