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
using static System.Net.WebRequestMethods;

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
            
            var xlFiles = InitializeTheWorkQueue();
            
            foreach (var xlFile in xlFiles)
            {
                ConsoleAndLog($"Processing {xlFile}...");
                ProcessSpreadSheetRows(xlFile);
                UpdateWorkQueueComplete(xlFile);
                ConsoleAndLog($"Finished processing {xlFile}.");
            }

            ConsoleAndLog($"Finished processing ALL files.");

            //readkey to keep the console open  
            Console.ReadKey();
        }

        private static void UpdateWorkQueueComplete(string xlFile)
        {
            using (var connection = new SQLiteConnection($"Data Source={QUEUE_DB_PATH};Version=3;"))
            {
                connection.Open();

                string updateQuery = "UPDATE XLSFileWorkQueue SET Status = 1 WHERE FileName = @xlFile";
                using (var command = new SQLiteCommand(updateQuery, connection))
                {
                    command.Parameters.AddWithValue("@xlFile", xlFile);
                    command.ExecuteNonQuery();
                }
            }
        }

        private static void ProcessSpreadSheetRows(string xlsFile)
        {
            using (var stream = System.IO.File.Open(xlsFile, FileMode.Open, FileAccess.Read))
            {
                using (var reader = ExcelReaderFactory.CreateReader(stream))
                {
                    do
                    {
                        while (reader.Read())
                        {
                            try
                            {

                                // Validate that all required columns are present and not null
                                if (reader.FieldCount < 6 ||
                                    //reader.IsDBNull((int)Col.CertTitle) ||
                                    reader.IsDBNull((int)Col.FullName) ||
                                    reader.IsDBNull((int)Col.IssueDate) ||
                                    reader.IsDBNull((int)Col.RenewByDate) ||
                                    reader.IsDBNull((int)Col.ECardCode) ||
                                    reader.IsDBNull((int)Col.FileName))
                                {
                                    ConsoleAndLog("Invalid or incomplete data row, skipping.", LogLevel.Error);

                                    // Display all fields of the current row
                                    for (int i = 0; i < reader.FieldCount; i++)
                                    {
                                        var value = reader.IsDBNull(i) ? "NULL" : reader.GetValue(i).ToString();
                                        Console.Write($"{value}\t");
                                    }
                                    Console.WriteLine();

                                    continue;
                                }

                                var certTitle = string.Empty;
                                if(!reader.IsDBNull((int)Col.CertTitle))
                                {
                                    certTitle = reader.GetString((int)Col.CertTitle);
                                    certTitle = certTitle?.Length > 200 ? certTitle.Substring(0, 200) : certTitle;
                                }

                                var fullName = reader.GetString((int)Col.FullName);
                                fullName = fullName?.Length > 100 ? fullName.Substring(0, 100) : fullName;

                                if (certTitle == "CertTitle" && fullName == "FullName")
                                {
                                    // we are in a title row, so skip it.
                                    continue;
                                }

                                if (!DateTime.TryParse(reader.GetString((int)Col.IssueDate), out DateTime issueDate))
                                {
                                    ConsoleAndLog($"Invalid IssueDate '{reader.GetString((int)Col.IssueDate)}'. {reader.GetString((int)Col.FileName)}", LogLevel.Error);
                                    continue;
                                }

                                if (!DateTime.TryParse(reader.GetString((int)Col.RenewByDate), out DateTime rbd))
                                {
                                    ConsoleAndLog($"Invalid RenewByDate '{reader.GetString((int)Col.RenewByDate)}'. {reader.GetString((int)Col.FileName)}", LogLevel.Error);
                                    continue;
                                }

                                string renewByDate = rbd.ToString("MM/yyyy");
                                var eCardCode = reader.GetString((int)Col.ECardCode);
                                var fileName = reader.GetString((int)Col.FileName);

                                if (!IsValidFileName(fileName))
                                {
                                    ConsoleAndLog($"File name is invalid: {fileName}", LogLevel.Error);
                                    continue;
                                }

                                fileName = Path.GetFileNameWithoutExtension(fileName);
                                int employeeId = extractEmployeeId(fileName);
                                int employeeCertificatesId = extractEmployeeCertificatesId(fileName);
                                var OldCertName = extractOldCertName(fileName);

                                // save the data to the sql server
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
                            catch (Exception ex)
                            {
                                ConsoleAndLog($"Error processing row: {ex.Message}", LogLevel.Error);
                            }
                        }
                    } while (reader.NextResult());
                }
            }
        }


        private static List<string> InitializeTheWorkQueue()
        {
            List<string> pendingFiles = new List<string>();

            using (var connection = new SQLiteConnection($"Data Source={QUEUE_DB_PATH};Version=3;"))
            {
                connection.Open();

                string query = "SELECT FileName FROM XLSFileWorkQueue WHERE Status = 0 ORDER BY FileName ASC";
                using (var command = new SQLiteCommand(query, connection))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            pendingFiles.Add(reader.GetString(0));
                        }
                    }
                }
            }

            return pendingFiles;
        }

 
        private static void InitializeQueueDb()
        {
            EnsureFolderExists(QUEUE_DB_FOLDER);

            if (!System.IO.File.Exists(QUEUE_DB_PATH))
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
            List<string> handledWorkQueueFiles = new List<string>();

            //get all the rows of the xlsfileworkqueue table
            using (var connection = new SQLiteConnection($"Data Source={QUEUE_DB_PATH};Version=3;"))
            {
                connection.Open();

                string query = "SELECT * FROM XLSFileWorkQueue";
                using (var command = new SQLiteCommand(query, connection))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            // Process each row
                            //var id = reader.GetInt32(0);
                            var fileName = reader.GetString(1);
                            //var status = reader.GetInt32(2);
                            //var comments = reader.IsDBNull(3) ? null : reader.GetString(3);

                            handledWorkQueueFiles.Add(fileName);
                        }
                    }
                }
            }


            List<string> filePaths = new List<string>();
            EnsureFolderExists(XLS_FOLDER);
            filePaths.AddRange(Directory.GetFiles(XLS_FOLDER, "*.xlsx", SearchOption.TopDirectoryOnly));

            //return only the files that are not in the handledWorkQueueFiles list
            filePaths = filePaths.Except(handledWorkQueueFiles).ToList();

            if (filePaths.Count == 0)
            {
                ConsoleAndLog("No new files to process.", LogLevel.Information);
            }
            else
            {
                ConsoleAndLog($"Found {filePaths.Count} new files to process.", LogLevel.Information);
                foreach (var filePath in filePaths)
                {
                    Console.WriteLine(filePath);
                }

                Console.WriteLine("Do you want to add these files to the XLS work queue? (y/n)");
                var response = Console.ReadLine();
                if (response?.ToLower() == "y")
                {
                    using (var connection = new SQLiteConnection($"Data Source={QUEUE_DB_PATH};Version=3;"))
                    {
                        connection.Open();

                        foreach (var filePath in filePaths)
                        {
                            using (var command = new SQLiteCommand(connection))
                            {
                                command.CommandText = "INSERT INTO XLSFileWorkQueue (FileName, Status) VALUES (@FileName, @Status)";
                                command.Parameters.AddWithValue("@FileName", filePath);
                                command.Parameters.AddWithValue("@Status", 0);
                                command.ExecuteNonQuery();
                            }

                            ConsoleAndLog($"Added {filePath} to the XLSFileWorkQueue table.", LogLevel.Information);
                        }
                    }

                }
                else
                {
                    Console.WriteLine("Files not added to the XLS work queue.");
                    return;
                }

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
