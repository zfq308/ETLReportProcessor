using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ETLDataProcessor.ETL.Transformer
{
    class SchemaFieldSanitizer
    {

        public bool checkNonNegative(int parsedInt)
        {
            if (parsedInt >= 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }



        public bool checkDateTimeAgainstCurrent(DateTime rowwColumnDateTime)
        {
            if (rowwColumnDateTime <= DateTime.Now)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool checkStrictlyNonEmptyField(String rowOriginalField)
        {
            if (rowOriginalField != null && rowOriginalField.Trim().Length > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
