using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nordek.Persistence.Contexts
{
    public static class SqlClient
    {
        public static IConfiguration Configuration { get; set; } = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile($"{Environment.GetEnvironmentVariable("SCHEDULER_ENVIRONMENT") ?? "appsettings"}.json", optional: false, reloadOnChange: true)
        .AddEnvironmentVariables()
        .Build();
        static IConfiguration  _configuration;


        /// <summary> Aman

        //public static IConfiguration Configuration { get; set; }
        //private string GetConnectionString()
        //{
        //    var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.json");
        //    Configuration = builder.Build();
        //    return Configuration.GetConnectionString("DefaultConnection");//GetConnectionString("DefaultConnection");
        //}
        /// </summary>
        private static string str;
         static SqlClient()
        {
           // var i = GetConnectionString();
            //GetConnectionString
            _configuration = Configuration;
             str = _configuration.GetConnectionString("DefaultConnection");
        }

        /// <summary> 
        /// Set the connection, command, and then execute the command with non query. 
        /// </summary> 
        /// 

        // private static string connectionString = str;//"Server=databasssse-1.cjh1zwqeg0kw.eu-north-1.rds.amazonaws.com;Initial Catalog=NordekDB;User ID=admin;Password=Admin1234;MultipleActiveResultSets=true; Integrated Security=False; ";

        public static int  ExecuteNonQuery(string commandText,
            CommandType commandType, params SqlParameter[] parameters)
        {
            using (SqlConnection conn = new SqlConnection(str))
            {
                using (SqlCommand cmd = new SqlCommand(commandText, conn))
                {
                    try
                    {
                        // There're three command types: StoredProcedure, Text, TableDirect. The TableDirect  
                        // type is only for OLE DB.   
                        cmd.CommandType = commandType;
                        cmd.Parameters.AddRange(parameters);
                        cmd.CommandTimeout = 0;
                        conn.Open();
                        int k = cmd.ExecuteNonQuery();

                        return k;
                    }
                    catch (Exception ex)
                    {
                        // LogWriter.ErrorLogWriter.ExceptionLogWrite(ex);
                    }
                    finally
                    {
                        conn.Close();
                    }


                }
            }
            return 0;
        }

        /// <summary> 
        /// Set the connection, command, and then execute the command and only return one value. 
        /// </summary> 
        public static object ExecuteScalar(string commandText,
            CommandType commandType, params SqlParameter[] parameters)
        {
            using (SqlConnection conn = new SqlConnection(str))
            {
                try
                {
                    using (SqlCommand cmd = new SqlCommand(commandText, conn))
                    {
                        cmd.CommandType = commandType;
                        cmd.Parameters.AddRange(parameters);

                        conn.Open();
                        return cmd.ExecuteScalar();
                    }
                }
                catch (Exception ex)
                {
                    // LogWriter.ErrorLogWriter.ExceptionLogWrite(ex);
                }
                finally
                {
                    conn.Close();
                }
                return 0;
            }
        }

        public static object ExecuteScalar(string commandText,
           CommandType commandType, Dictionary<string, string> Params)
        {
            using (SqlConnection conn = new SqlConnection(str))
            {
                try
                {
                    try
                    {
                        using (SqlCommand cmd = new SqlCommand(commandText, conn))
                        {
                            cmd.CommandType = commandType;
                            foreach (var item in Params)
                            {
                                cmd.Parameters.AddWithValue(item.Key, item.Value);
                            }
                            //cmd.Parameters.AddWithValue(para, values);
                            conn.Open();
                            return cmd.ExecuteScalar();
                        }
                    }
                    catch (Exception ex)
                    {
                        // LogWriter.ErrorLogWriter.ExceptionLogWrite(ex);
                    }
                    finally { conn.Close(); }
                }
                catch (Exception ex)
                {
                    // LogWriter.ErrorLogWriter.ExceptionLogWrite(ex);
                }
                finally
                {
                    conn.Close();
                }
                return 0;
            }
        }

        /// <summary> 
        /// Set the connection, command, and then execute the command with query and return the reader. 
        /// </summary> 
        public static SqlDataReader ExecuteReader(string commandText,
            CommandType commandType, params SqlParameter[] parameters)
        {
            SqlConnection conn = new SqlConnection(str);
            try
            {
                using (SqlCommand cmd = new SqlCommand(commandText, conn))
                {
                    cmd.CommandType = commandType;
                    cmd.Parameters.AddRange(parameters);

                    conn.Open();
                    // When using CommandBehavior.CloseConnection, the connection will be closed when the  
                    // IDataReader is closed. 
                    SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                    return reader;
                }
            }
            catch (Exception ex)
            {
                // LogWriter.ErrorLogWriter.ExceptionLogWrite(ex);
                throw ex;
            }
        }

        public static DataTable GetDataTable(string commandText,
           CommandType commandType, params SqlParameter[] parameters)
        {
            SqlConnection conn = new SqlConnection(str);
            try
            {
                using (SqlCommand cmd = new SqlCommand(commandText, conn))
                {
                    cmd.CommandType = commandType;
                    cmd.Parameters.AddRange(parameters);
                    //cmd.CommandTimeout = 0;
                    conn.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    DataTable dt = new DataTable();
                    da.Fill(dt);
                    conn.Close();
                    return dt;
                }
            }
            catch (Exception ex)
            {
                // LogWriter.ErrorLogWriter.ExceptionLogWrite(ex);
            }
            finally
            {
                conn.Close();
            }
            return new DataTable();

        }
        /// <summary>
        /// Set the connection, command, and then execute the command with query and return the dataset.
        /// </summary>
        /// <param name="commandText"></param>
        /// <param name="commandType"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public static DataSet GetDataTableSet(string commandText,
           CommandType commandType, params SqlParameter[] parameters)
        {
            SqlConnection conn = new SqlConnection(str);
            try
            {
                using (SqlCommand cmd = new SqlCommand(commandText, conn))
                {
                    cmd.CommandType = commandType;
                    cmd.Parameters.AddRange(parameters);
                    cmd.CommandTimeout = 0;
                    conn.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    DataSet ds = new DataSet();
                    da.Fill(ds);
                    conn.Close();
                    return ds;
                }
            }
            catch (Exception ex)
            {
                // LogWriter.ErrorLogWriter.ExceptionLogWrite(ex);
            }
            finally
            {
                conn.Close();
            }
            return new DataSet();
        }


        public static DataTable SelectQuery(string query)
        {
            SqlConnection conn = new SqlConnection(str);
            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                DataTable dt = new DataTable();
                da.Fill(dt);
                return dt;
            }

        }
        public static DataTable SelectStored(string StoresProcedure)
        {
            //Dictonary need to added for the parameters
            DataTable dt = new DataTable();

            using (SqlConnection conn = new SqlConnection(str))
            {
                try
                {
                    using (SqlCommand cmd = new SqlCommand(StoresProcedure, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@Email", "");
                        cmd.Parameters.AddWithValue("@Action", 2);
                        conn.Open();
                        SqlDataAdapter da = new SqlDataAdapter(cmd);
                        da.Fill(dt);
                        return dt;
                    }
                }
                catch (Exception ex)
                {
                    // LogWriter.ErrorLogWriter.ExceptionLogWrite(ex);
                }
                finally
                {
                    conn.Close();
                }
                return new DataTable();
            }
        }

        public static DataTable SelectStoredforUpdate(string StoresProcedure, string email)
        {
            DataTable dt = new DataTable();

            using (SqlConnection conn = new SqlConnection(str))
            {
                try
                {
                    using (SqlCommand cmd = new SqlCommand(StoresProcedure, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@Email", email);

                        conn.Open();
                        SqlDataAdapter da = new SqlDataAdapter(cmd);
                        da.Fill(dt);
                        return dt;
                    }
                }
                catch (Exception ex)
                {
                    // LogWriter.ErrorLogWriter.ExceptionLogWrite(ex);
                }
                finally
                {
                    conn.Close();
                }
                return new DataTable();
            }
        }

        public static DataTable SelectStoredSelectUserRole(string StoresProcedure, int UserId)
        {
            DataTable dt = new DataTable();

            using (SqlConnection conn = new SqlConnection(str))
            {
                try
                {
                    using (SqlCommand cmd = new SqlCommand(StoresProcedure, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@UserId", UserId);

                        conn.Open();
                        SqlDataAdapter da = new SqlDataAdapter(cmd);
                        da.Fill(dt);
                        return dt;
                    }
                }
                catch (Exception ex)
                {
                    // LogWriter.ErrorLogWriter.ExceptionLogWrite(ex);
                }
                finally
                {
                    conn.Close();
                }
                return new DataTable();
            }
        }

        public static DataTable SelectStored(string StoresProcedure, string temp)
        {
            DataTable dt = new DataTable();

            using (SqlConnection conn = new SqlConnection(str))
            {
                try
                {
                    using (SqlCommand cmd = new SqlCommand(StoresProcedure, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        conn.Open();
                        SqlDataAdapter da = new SqlDataAdapter(cmd);
                        da.Fill(dt);
                        return dt;
                    }
                }
                catch (Exception ex)
                {
                    // LogWriter.ErrorLogWriter.ExceptionLogWrite(ex);
                }
                finally
                {
                    conn.Close();
                }
                return new DataTable();
            }
        }


        public static Boolean Update(string StoresProcedure, string email)
        {
            using (SqlConnection conn = new SqlConnection(str))
            {
                try
                {
                    using (SqlCommand cmd = new SqlCommand(StoresProcedure, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@Email", email);
                        cmd.Parameters.AddWithValue("@IsDeleted", 0);
                        cmd.Parameters.AddWithValue("@Action", 3);
                        conn.Open();
                        int status = cmd.ExecuteNonQuery();
                        if (status > 0)
                        {
                            return true;
                        }
                        else
                        {
                            return false;
                        }
                    }
                }
                catch (Exception ex)
                {
                    // LogWriter.ErrorLogWriter.ExceptionLogWrite(ex);
                }
                finally
                {
                    conn.Close();
                }
                
                return false;
            }
        }
        public static Boolean Delete(string StoresProcedure, string email)
        {
            using (SqlConnection myConnection = new SqlConnection(str))
            {
                try
                {
                    using (SqlCommand cmd = new SqlCommand(StoresProcedure, myConnection))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@Email", email);
                        myConnection.Open();
                        int status = cmd.ExecuteNonQuery();
                        if (status > 0)
                        {
                            return true;
                        }
                        else
                        {
                            return false;
                        }
                    }
                }
                catch (Exception ex)
                {
                    // LogWriter.ErrorLogWriter.ExceptionLogWrite(ex);
                }
                finally
                {
                    myConnection.Close();
                }
                return false;
            }
        }

        private static void ReadOrderData(string connectionString, string queryString, DataTable dt)
        {

            using (SqlConnection connection =
                       new SqlConnection(str))
            {
                SqlCommand command =
                    new SqlCommand(queryString, connection);
                command.CommandType = CommandType.StoredProcedure;
                connection.Open();
                command.Parameters.AddWithValue("@EmailId", "jitendrak2@chetu.com");
                SqlDataReader reader = command.ExecuteReader();

                // Call Read before accessing data.
                while (reader.Read())
                {
                    ReadSingleRow((IDataRecord)reader, dt);
                }

                // Call Close when done reading.
                reader.Close();
            }
        }

        private static void ReadSingleRow(IDataRecord record, DataTable dt)
        {

            Console.WriteLine(String.Format("{0}, {1}", record[0], record[1]));
        }
        /// <summary>
        /// Bulk insert records in table
        /// </summary>
        /// <typeparam name="P"></typeparam>
        /// <param name="list"></param>
        /// <param name="destinationTableName"></param>
        /// <returns></returns>
        public static bool BulkInsert<P>(IList<P> list, string destinationTableName)
        {
            bool result = false;
            using (SqlConnection conn = new SqlConnection(str))
            {
                try
                {
                    using (var bulkCopy = new SqlBulkCopy(conn))
                    {
                        bulkCopy.BatchSize = list.Count;
                        bulkCopy.DestinationTableName = destinationTableName;

                        var table = new DataTable();
                        var props = TypeDescriptor.GetProperties(typeof(P))
                                                   //Dirty hack to make sure we only have system data types 
                                                   //i.e. filter out the relationships/collections
                                                   .Cast<PropertyDescriptor>()
                                                   .Where(propertyInfo => propertyInfo.PropertyType.Namespace.Equals("System"))
                                                   .ToArray();

                        foreach (var propertyInfo in props)
                        {
                            bulkCopy.ColumnMappings.Add(propertyInfo.Name, propertyInfo.Name);
                            table.Columns.Add(propertyInfo.Name, Nullable.GetUnderlyingType(propertyInfo.PropertyType) ?? propertyInfo.PropertyType);
                        }

                        var values = new object[props.Length];
                        foreach (var item in list)
                        {
                            for (var i = 0; i < values.Length; i++)
                            {
                                values[i] = props[i].GetValue(item);
                            }

                            table.Rows.Add(values);
                        }
                        conn.Open();
                        bulkCopy.BulkCopyTimeout = 0;
                        bulkCopy.WriteToServer(table);
                        result = true;

                    }
                    return result;
                }
                catch (Exception ex)
                {
                    // LogWriter.ErrorLogWriter.ExceptionLogWrite(ex);
                }
                finally
                {
                    conn.Close();
                }
                return false;
            }
        }

        
    }
}
