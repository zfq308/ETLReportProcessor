using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ETLDataProcessor.ConfigMapper
{
    class LogConfigMapper
    {
        private String folder;
        private String filetag;
        private bool enable;


        public void setMap(String key, String value)
        {
            switch (key)
            {
                case "folder":
                    folder = value;
                    break;
                case "filetag":
                    filetag = value;
                    break;
                case "enable":
                    enable = Boolean.Parse(value);
                    break;
            }
        }

        public String getMap(String key)
        {
            String value = "";
            switch (key)
            {
                case "folder":
                    value = folder;
                    break;
                case "filetag":
                    value = filetag;
                    break;
                case "enable":
                    value = enable.ToString();
                    break;
            }
            return value;
        }

    }
}

