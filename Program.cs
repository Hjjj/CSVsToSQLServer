using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using ExcelDataReader;
using System.IO;

namespace CSVsToSQLServer
{
    internal class Program
    {
        static string DEST_TABLE_NAME = ConfigurationManager.AppSettings["DEST_TABLE_NAME"];
        static string XLS_FOLDER = ConfigurationManager.AppSettings["XLS_FOLDER"];

        static void Main(string[] args)
        {
            //get a count of the rows in a sql db table
            int count = GetRowCount($"SELECT COUNT(*) FROM {DEST_TABLE_NAME}");

            //print the count to the console
            Console.WriteLine("The row count is: " + count);

            //get count of files in the xls folder
            string[] files = System.IO.Directory.GetFiles(XLS_FOLDER, "*.xlsx", System.IO.SearchOption.TopDirectoryOnly);
            Console.WriteLine("The file count is: " + files.Length);

            //open up an xlsx file and loop through the rows
            string file = files[0];


            using (var stream = File.Open(file, FileMode.Open, FileAccess.Read))
            {
                // Auto-detect format, supports:
                //  - Binary Excel files (2.0-2003 format; *.xls)
                //  - OpenXml Excel files (2007 format; *.xlsx, *.xlsb)
                using (var reader = ExcelReaderFactory.CreateReader(stream))
                {
                    // Choose one of either 1 or 2:

                    // 1. Use the reader methods
                    do
                    {
                        while (reader.Read())
                        {
                            var mystring = reader.GetString(0);
                        }

                    } while (reader.NextResult());

                    // 2. Use the AsDataSet extension method
                    //var result = reader.AsDataSet();
                    // The result of each spreadsheet is in result.Tables
                }
            }


            //readkey to keep the console open  
            Console.ReadKey();
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
    }
}
