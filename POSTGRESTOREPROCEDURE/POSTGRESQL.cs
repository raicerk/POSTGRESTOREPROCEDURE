using System;
using System.Collections.Generic;
using System.Data;
using System.Configuration;
using Npgsql;

namespace POSTGRESTOREPROCEDURE
{
    public class POSTGRESQL
    {
        public static DataTable Exec(string NombreConexion, string Procedimiento, Dictionary<string, object> VariableYValores, DataTable dt = null)
        {
            DataTable _dt = dt == null ? new DataTable() : dt;
            NpgsqlConnection conn = new NpgsqlConnection(ConfigurationManager.AppSettings[NombreConexion]);
            try
            {
                conn.Open();
                NpgsqlCommand command = new NpgsqlCommand(Procedimiento, conn);
                command.CommandType = CommandType.StoredProcedure;
                
                if (VariableYValores.Count > 0)
                {
                    foreach (var var in VariableYValores)
                    {
                        command.Parameters.AddWithValue(var.Key, var.Value);
                    }
                }

                NpgsqlDataReader dr = command.ExecuteReader();
                _dt.Load(dr);
                conn.Close();
                return _dt;
            }
            catch (NpgsqlException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        public static DataSet Exec(string NombreConexion, string Procedimiento, Dictionary<string, object> VariableYValores, DataSet ds = null)
        {
            DataSet _ds = ds == null ? new DataSet() : ds;
            NpgsqlConnection conn = new NpgsqlConnection(ConfigurationManager.AppSettings[NombreConexion]);
            try
            {
                conn.Open();
                DataTable dt = new DataTable();
                NpgsqlCommand command = new NpgsqlCommand(Procedimiento, conn);
                command.CommandType = CommandType.StoredProcedure;

                if (VariableYValores.Count > 0)
                {
                    foreach (var var in VariableYValores)
                    {
                        command.Parameters.AddWithValue(var.Key, var.Value);
                    }
                }

                NpgsqlDataReader dr = command.ExecuteReader();
                dt.Load(dr);
                _ds.Tables.Add(dt);
                conn.Close();
                return _ds;
            }
            catch (NpgsqlException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static void Exec(string NombreConexion, string Procedimiento, Dictionary<string, object> VariableYValores)
        {
            NpgsqlConnection conn = new NpgsqlConnection(ConfigurationManager.AppSettings[NombreConexion]);
            try
            {
                conn.Open();
                NpgsqlCommand command = new NpgsqlCommand(Procedimiento, conn);
                command.CommandType = CommandType.StoredProcedure;


                if (VariableYValores.Count > 0)
                {
                    foreach (var var in VariableYValores)
                    {
                        command.Parameters.AddWithValue(var.Key, var.Value);
                    }
                }

                command.ExecuteNonQuery();
                conn.Close();
            }
            catch (NpgsqlException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}
