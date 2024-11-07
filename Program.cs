using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSVsToSQLServer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            //get a count of the rows in a sql db table
            int count = GetRowCount("SELECT COUNT(*) FROM dbo.Connect_Ecard_Test");

            //print the count to the console
            Console.WriteLine("The row count is: " + count);

            //readkey to keep the console open  
            Console.ReadKey();
        }

        static int GetRowCount(string query)
        {
            int count = 0;
            string connectionString = "Server=LDCAMRDEV1\\SQL2016;Database=Connect_Dev;User Id=webapi;Password=p@ssw0rd;";


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
