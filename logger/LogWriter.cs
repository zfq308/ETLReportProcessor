using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.IO;

namespace ETLDataProcessor.Logger
{

    /// <summary>
    /// A Logging class implementing the Singleton pattern and an internal Queue to be flushed perdiodically
    /// </summary>
    public class LogWriter
    {
        private static LogWriter instance;
        private static Queue<LogMessage> logQueue;
        private bool enableLogging = false;
        private string logDir = "";
        private string logFile = "";
        private int maxLogAge = 1;
        private int queueSize = 1;
        private static DateTime LastFlushed = DateTime.Now;

        /// <summary>
        /// Private constructor to prevent instance creation
        /// </summary>
        private LogWriter() { }

        ~LogWriter()
        {
            FlushLog();
        }

        public void initializeLogSettings(string logFolder, string logFilename, string log_enable)
        {
            logDir = logFolder;
            logFile = logFilename;
            try
            {
                enableLogging = Boolean.Parse(log_enable);

            }
            catch (FormatException f_ex)
            {
                Console.WriteLine("Log Initialization Error : " + f_ex.Message);
            }
        }


        /// <summary>
        /// An LogWriter instance that exposes a single instance
        /// </summary>
        public static LogWriter Instance
        {
            get
            {
                // If the instance is null then create one and init the Queue
                if (instance == null)
                {
                    instance = new LogWriter();
                    logQueue = new Queue<LogMessage>();
                }
                return instance;
            }
        }

        public void WriteToLog(string message)
        {
            if (enableLogging)
            {
                WriteToEnabledLog(message);
            }
        }

        /// <summary>
        /// The single instance method that writes to the log file
        /// </summary>
        /// <param name="message">The message to write to the log</param>
        private void WriteToEnabledLog(string message)
        {
            // Lock the queue while writing to prevent contention for the log file
            lock (logQueue)
            {
                // Create the entry and push to the Queue
                LogMessage logEntry = new LogMessage(message);
                logQueue.Enqueue(logEntry);

                // If we have reached the Queue Size then flush the Queue
                if (logQueue.Count >= queueSize || DoPeriodicFlush())
                {
                    FlushLog();
                }
            }
        }

        private bool DoPeriodicFlush()
        {
            TimeSpan logAge = DateTime.Now - LastFlushed;
            if (logAge.TotalSeconds >= maxLogAge)
            {
                LastFlushed = DateTime.Now;
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Flushes the Queue to the physical log file
        /// </summary>
        private void FlushLog()
        {
            while (logQueue.Count > 0)
            {
                LogMessage entry = logQueue.Dequeue();
                string logPath = logDir + logFile + "_" + entry.LogDate + ".log";

                using (FileStream fs = File.Open(logPath, FileMode.Append, FileAccess.Write))
                {
                    using (StreamWriter log = new StreamWriter(fs))
                    {
                        log.WriteLine(string.Format("{0}\t{1}", entry.LogTime, entry.Message));
                    }
                }
            }
        }
    }

}
