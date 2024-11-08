using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using ExcelDataReader;
using System.IO;
using System.Runtime.InteropServices;
using Serilog;
using System.Runtime.CompilerServices;
using System.Data.SQLite;

namespace CSVsToSQLServer
{
    internal class Program
    {
        static string DEST_TABLE_NAME = ConfigurationManager.AppSettings["DEST_TABLE_NAME"];
        static string XLS_FOLDER = ConfigurationManager.AppSettings["XLS_FOLDER"];
        static string LOG_FOLDER_PATH = ConfigurationManager.AppSettings["LOG_FOLDER_PATH"];
        static string QUEUE_DB_FOLDER = ConfigurationManager.AppSettings["QUEUE_DB_FOLDER"];
        static string QUEUE_DB_PATH = Path.Combine(QUEUE_DB_FOLDER, "queue.db");


        static void Main(string[] args)
        {
            Log.Logger = InitializeLog();
            ConsoleAndLog("Application started.");
            InitializeQueueDb();
            InitializeXlFiles();

            //get count of files in the xls folder
            string[] files = System.IO.Directory.GetFiles(XLS_FOLDER, "*.xlsx", System.IO.SearchOption.TopDirectoryOnly);
            Console.WriteLine("The file count is: " + files.Length);

            //open up an xlsx file and loop through the rows
            string file = files[0];

            //TODO make is pull multiple xlsx files from the folder
            //TODO make it resilent to repetitive restarts
            //so a sqlite log of which xlsx files it has already processed
            //and a sqlite log of which rows it has already processed

            using (var stream = File.Open(file, FileMode.Open, FileAccess.Read))
            {
                using (var reader = ExcelReaderFactory.CreateReader(stream))
                {
                    do
                    {
                        while (reader.Read())
                        {
                            //TODO put all of this into a try catch block

                            var certTitle =  reader.GetString((int)Col.CertTitle);
                            certTitle = certTitle?.Length > 200 ? certTitle.Substring(0, 200) : certTitle;

                            var fullName = reader.GetString((int)Col.FullName);
                            fullName = fullName?.Length > 100 ? fullName.Substring(0, 100) : fullName;

                            if (certTitle=="CertTitle" && fullName=="FullName")
                            {
                                //we are in a title row, so skip it.
                                continue;
                            }

                            DateTime issueDate = reader.GetDateTime((int)Col.IssueDate);
                            string renewByDate = FormatToRenewByDate(reader.GetString((int)Col.RenewByDate));
                            var eCardCode = reader.GetString((int)Col.ECardCode);
                            var fileName = reader.GetString((int)Col.FileName);

                            if(!IsValidFileName(fileName))
                            {
                                ConsoleAndLog($"File name is invalid: {fileName}", LogLevel.Error);
                                continue;
                            }

                            fileName = Path.GetFileNameWithoutExtension(fileName);
                            int employeeId = extractEmployeeId(fileName);
                            int employeeCertificatesId = extractEmployeeCertificatesId(fileName);
                            var OldCertName = extractOldCertName(fileName);

                            //save the data to the sql server
                            string connectionString = ConfigurationManager.AppSettings["ConnectionString"];
                            using (SqlConnection connection = new SqlConnection(connectionString))
                            {
                                connection.Open();
                                using (SqlCommand command = new SqlCommand(
                                    $@"
IF NOT EXISTS (SELECT 1 FROM {DEST_TABLE_NAME} WHERE EmployeeID = @EmployeeId AND DispositionID = @EmployeeCertificatesId AND OldCertName = @OldCertName)
    BEGIN
        INSERT INTO {DEST_TABLE_NAME} (CertTitle, FullName, IssueDate, RenewByDate, EcardCode, EmployeeID, DispositionID, OldCertName)
        VALUES (@CertTitle, @FullName, @IssueDate, @RenewByDate, @ECardCode, @EmployeeId, @EmployeeCertificatesId, @OldCertName)
    END", connection))
                                {
                                    command.Parameters.AddWithValue("@CertTitle", certTitle);
                                    command.Parameters.AddWithValue("@FullName", fullName);
                                    command.Parameters.AddWithValue("@IssueDate", issueDate);
                                    command.Parameters.AddWithValue("@RenewByDate", renewByDate);
                                    command.Parameters.AddWithValue("@ECardCode", eCardCode);
                                    command.Parameters.AddWithValue("@EmployeeId", employeeId);
                                    command.Parameters.AddWithValue("@EmployeeCertificatesId", employeeCertificatesId);
                                    command.Parameters.AddWithValue("@OldCertName", OldCertName);

                                    command.ExecuteNonQuery();
                                }

                            }
                        }

                    } while (reader.NextResult());
                }
            }

            //readkey to keep the console open  
            Console.ReadKey();
        }

        private static void InitializeQueueDb()
        {
            EnsureFolderExists(QUEUE_DB_FOLDER);

            if (!File.Exists(QUEUE_DB_PATH))
            {
                SQLiteConnection.CreateFile(QUEUE_DB_PATH);
                ConsoleAndLog($"Created SQLite database at: {QUEUE_DB_PATH}");

                using (var connection = new SQLiteConnection($"Data Source={QUEUE_DB_PATH};Version=3;"))
                {
                    connection.Open();

                    string createXLSFileWorkQueueTable = @"
                CREATE TABLE XLSFileWorkQueue (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    FileName TEXT NOT NULL,
                    Status INTEGER NOT NULL,
                    Comments TEXT
                );";

                    string createIndividualRowWorkQueueTable = @"
                CREATE TABLE IndividualRowWorkQueue (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    FileName TEXT NOT NULL,
                    Status INTEGER NOT NULL,
                    Comments TEXT
                );";

                    using (var command = new SQLiteCommand(createXLSFileWorkQueueTable, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    using (var command = new SQLiteCommand(createIndividualRowWorkQueueTable, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    ConsoleAndLog("Created tables: XLSFileWorkQueue and IndividualRowWorkQueue");
                }
            }
            else
            {
                ConsoleAndLog("SQLite database already exists.");
            }
        }

        private static ILogger InitializeLog()
        {
            EnsureFolderExists(LOG_FOLDER_PATH);

            // Configure Serilog
            return new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.File(Path.Combine(LOG_FOLDER_PATH, $"Log{DateTime.Now:MM-dd-yyyy-HH-mm-ss}.txt"))
                .CreateLogger();
        }

        private static void EnsureFolderExists(string folderPath)
        {
            if (!Directory.Exists(folderPath))
            {
                Directory.CreateDirectory(folderPath);
                ConsoleAndLog($"Created directory: {folderPath}");
            }
        }

        /// <summary>
        /// Check for new xl files in the folder, if they exist put a row into the xlsx_files_queue table in a sqlite db
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        private static void InitializeXlFiles()
        {
            List<string> filePaths = new List<string>();
            EnsureFolderExists(XLS_FOLDER);
            filePaths.AddRange(Directory.GetFiles(XLS_FOLDER, "*.xlsx", SearchOption.TopDirectoryOnly));

            if (filePaths.Count == 0)
            {
                ConsoleAndLog("No new files to process.", LogLevel.Information);
            }
            else
            {
                ConsoleAndLog($"Found {filePaths.Count} new files to process.", LogLevel.Information);

            }

        }

        private static void ConsoleAndLog(string message)
        {
            ConsoleAndLog(message, LogLevel.Information);
        }

        private static void ConsoleAndLog(string message, LogLevel level)
        {
            Console.WriteLine(message);

            if (level == LogLevel.Error)
            {
                Log.Error(message);
            }
            else
            {
                Log.Information(message);
            }
        }

        enum LogLevel
        {
            Information,
            Error
        }

        /// <summary>
        /// the file name format is "CertificateName_EmployeeCertificatesId_EmployeeId"
        /// this extracts the CertificateName
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        private static object extractOldCertName(string fileName)
        {
            // Assuming the file name format is "CertificateName_EmployeeCertificatesId_EmployeeId"
            var parts = fileName.Split('_');

            if (parts.Length == 3)
            {
                return parts[0];
            }
            else
            {
                throw new FormatException("Invalid file name format");
            }
        }

        /// <summary>
        /// the file name format is "CertificateName_EmployeeCertificatesId_EmployeeId"
        /// this function extracts the EmployeeCertificatesId
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        private static int extractEmployeeCertificatesId(string fileName)
        {
            // Assuming the file name format is "CertificateName_EmployeeCertificatesId_EmployeeId"
            var parts = fileName.Split('_');

            if (parts.Length == 3 && int.TryParse(parts[1], out int employeeCertificatesId))
            {
                return employeeCertificatesId;
            }
            else
            {
                throw new FormatException("Invalid file name format");
            }
        }

        /// <summary>
        /// takes a random string and formats it to mm/yyyy
        /// </summary>
        /// <param name="timeString"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        private static string FormatToRenewByDate(string timeString)
        {
            if (DateTime.TryParse(timeString, out DateTime date))
            {
                return date.ToString("MM/yyyy");
            }
            else
            {
                throw new FormatException($"Invalid date format. String was {timeString}");
            }
        }

        static int GetRowCount(string query)
        {
            int count = 0;
            string connectionString = ConfigurationManager.AppSettings["ConnectionString"];

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);
                connection.Open();
                count = (int)command.ExecuteScalar();
            }

            return count;
        }

        private static int extractEmployeeId(string fileName)
        {
            // Assuming the file name format is "CertificateName_EmployeeCertificatesId_EmployeeId"
            var parts = fileName.Split('_');

            if (parts.Length == 3 && int.TryParse(parts[2], out int employeeId))
            {
                return employeeId;
            }
            else
            {
                throw new FormatException("Invalid file name format");
            }
        }

        /// <summary>
        /// validates the chars in a filename
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        private static bool IsValidFileName(string fileName)
        {
            if (string.IsNullOrWhiteSpace(fileName) || fileName == String.Empty)
            {
                return false;
            }

            //convert this full filepath to just the filename
            fileName = Path.GetFileNameWithoutExtension(fileName);

            char[] invalidChars = Path.GetInvalidFileNameChars();
            foreach (char c in fileName)
            {
                if (invalidChars.Contains(c))
                {
                    return false;
                }
            }

            return true;
        }

        enum Col
        {
            CertTitle = 0,
            FullName = 1,
            IssueDate = 2,
            RenewByDate = 3,
            ECardCode = 4,
            FileName = 5
        }
    }
}
