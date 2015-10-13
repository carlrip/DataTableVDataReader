using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace DataTableVDataReader
{
    class Program
    {
        static void Main(string[] args)
        {
            // connection string to our test database
            string connectionString = "Data Source=.\\EXPRESS2008;Initial Catalog=NorthwindStart;Integrated Security=true;";

            // test each of the tables that increase in size Customers1, Customers2, ... Customers5
            for (int j = 1; j <= 5; j++)
            {
                // create a stop watch to time the tests
                Stopwatch stopwatch = new Stopwatch();

                // create some lists to hold the time taken
                List<long> dataTableTimeTaken = new List<long>();
                List<long> dataReaderTimeTaken = new List<long>();
                List<long> dataTableSize = new List<long>();

                // construct our SQL statement
                string sql = "SELECT * FROM Customers" + j.ToString();

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    // do the test 5 times to get an average 
                    for (int i = 0; i < 5; i++)
                    {
                        // get data in a DataTable and then convert into the generic object
                        stopwatch.Reset();
                        stopwatch.Start();
                        DataTable dataTable = GetDataTable(connection, sql);
                        List<Dictionary<string, object>> dataFromDataTable = GetData(dataTable);
                        stopwatch.Stop();
                        dataTableTimeTaken.Add(stopwatch.ElapsedMilliseconds);
                        dataTableSize.Add(GetObjectMemorySize(dataTable));  // output the extra memory used for the DataTable

                        // get data using DataReader and then convert into the generic object
                        stopwatch.Reset();
                        stopwatch.Start();
                        List<Dictionary<string, object>> dataFromDataReader = GetData(connection, sql);
                        stopwatch.Stop();
                        dataReaderTimeTaken.Add(stopwatch.ElapsedMilliseconds);
                    }
                }

                // output results
                Console.WriteLine("Customers" + j.ToString() + "...");
                Console.WriteLine("Took " + dataTableTimeTaken.Average().ToString() + "ms to create generic data object using DataTable");
                Console.WriteLine("Took " + dataReaderTimeTaken.Average().ToString() + "ms to create generic data object from DataReader");

                Console.WriteLine("Difference is  " + (dataTableTimeTaken.Average() - dataReaderTimeTaken.Average()).ToString() + "ms");
                Console.WriteLine("DataTable memory size is " + dataTableSize.Average().ToString() + " bytes");
                Console.WriteLine();
            }
            
            Console.ReadLine();
        }

        /// <summary>
        ///  Returns a DataTable for a given connection and SQL statement
        /// </summary>
        /// <param name="connection">Database connection on where to retreive the data</param>
        /// <param name="connection">SELECT SQL statement that gives the DataTable</param>
        private static DataTable GetDataTable(SqlConnection connection, string sql)
        {
            DataTable result = null;
            using (SqlCommand command = new SqlCommand(sql, connection))
            {
                SqlDataAdapter adapter = new SqlDataAdapter();
                adapter.SelectCommand = command;
                DataSet dataSet = new DataSet();
                adapter.Fill(dataSet);
                result = dataSet.Tables[0];
            }
            return result;
        }

        /// <summary>
        ///  Returns a serializable generic data object from a DataTable
        /// </summary>
        /// <param name="dataTable">The data table to create the object from</param>
        private static List<Dictionary<string, object>> GetData(DataTable dataTable)
        {
            List<Dictionary<string, object>> result= new List<Dictionary<string,object>>();
            foreach(DataRow r in dataTable.Rows)
            {
                Dictionary<string, object> newRow = new Dictionary<string, object>();
                foreach (DataColumn c in dataTable.Columns)
                {
                    newRow[c.ColumnName] = r[c.ColumnName];
                }
                result.Add(newRow);
            }
            return result;
        }

        /// <summary>
        ///  Returns a serializable generic data object from given connection and SQL statement to execute
        /// </summary>
        /// <param name="connection">Database connection on where to retreive the data</param>
        /// <param name="connection">SELECT SQL statement to create the generic object from</param>
        private static List<Dictionary<string, object>> GetData(SqlConnection connection, string sql)
        {
            List<Dictionary<string, object>> result = new List<Dictionary<string, object>>();
            using (SqlCommand command = new SqlCommand(sql, connection))
            {
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Dictionary<string, object> newRow = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            newRow[reader.GetName(i)] = reader[i];
                        }
                        result.Add(newRow);
                    }
                }
            }
            return result;
        }

        /// <summary>
        ///  Returns the memory size of the given object
        /// </summary>
        /// <param name="o">The object to size</param>
        private static long GetObjectMemorySize(object o)
        {
            long size = 0;
            using (Stream s = new MemoryStream()) {
                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(s, o);
                size = s.Length;
            }
            return size;
        }
    }
}
