using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ETLDataProcessor.ConfigMapper
{
    class DataFilesConfigMapper
    {
        private String data_files_folder;
        private String processed_files_folder;


        public void setMap(String key, String value)
        {
            switch (key)
            {
                case "data_files_folder":
                    data_files_folder = value;
                    break;
                case "processed_files_folder":
                    processed_files_folder = value;
                    break;
            }
        }

        public String getMap(String key)
        {
            String value = "";
            switch (key)
            {
                case "data_files_folder":
                    value = data_files_folder;
                    break;
                case "processed_files_folder":
                    value = processed_files_folder;
                    break;
            }
            return value;
        }

    }
}

