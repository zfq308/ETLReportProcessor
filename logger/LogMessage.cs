using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ETLDataProcessor.Logger
{
    /// <summary>
    /// A Log class to store the message and the Date and Time the log entry was created
    /// </summary>
    public class LogMessage
    {
        public string Message { get; set; }
        public string LogTime { get; set; }
        public string LogDate { get; set; }

        public LogMessage(string message)
        {
            Message = message;
            LogDate = DateTime.Now.ToString("yyyy-MM-dd");
            LogTime = DateTime.Now.ToString("hh:mm:ss.fff tt");
        }

    }

}
