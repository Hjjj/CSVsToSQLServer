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

namespace CSVsToSQLServer
{
    internal class Program
    {
        static string DEST_TABLE_NAME = ConfigurationManager.AppSettings["DEST_TABLE_NAME"];
        static string XLS_FOLDER = ConfigurationManager.AppSettings["XLS_FOLDER"];

        static void Main(string[] args)
        {
            //TODO set up logging

            //get a count of the rows in a sql db table
            int count = GetRowCount($"SELECT COUNT(*) FROM {DEST_TABLE_NAME}");

            //print the count to the console
            Console.WriteLine("The row count is: " + count);

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
                // Auto-detect format, supports:
                //  - Binary Excel files (2.0-2003 format; *.xls)
                //  - OpenXml Excel files (2007 format; *.xlsx, *.xlsb)
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
                                //log the error
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
