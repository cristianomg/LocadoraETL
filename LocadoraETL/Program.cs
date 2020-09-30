using System;
using System.Data;
using Oracle.ManagedDataAccess.Client;

namespace LocadoraETL
{
    class Program
    {
        private static readonly string _conexaoBancoOperacional = "";
        private static readonly string _conexaoBancoDw = "";   
        static void Main(string[] args)
        {
            CarregarDMSocio();
            CarregarDMArtista();
            CarregarDMGravadora();
            CarregarDMTitulo();
            CarregarFTLocacao();
        }

        private static void CarregarDMSocio()
        {
            try
            {
                DataTable tabelaSocioOp = new DataTable();
                using (var conexao = new OracleConnection(_conexaoBancoOperacional))
                {
                    conexao.Open();


                    OracleCommand commandSQL = conexao.CreateCommand();

                    commandSQL.CommandText = "SELECT DISTINCT ID_SOC, NOM_SOC, TIPO_SOCIO FROM SOCIOS S JOIN TIPO_SOCIOS TP ON S.COD_TPS = TP.COD_TPS";

                    commandSQL.CommandType = CommandType.Text;

                    OracleDataReader dr = commandSQL.ExecuteReader();

                    tabelaSocioOp.Load(dr);
                }

                var dmSocios = new DataTable();
                dmSocios.Columns.Add("ID_SOC", typeof(int));
                dmSocios.Columns.Add("NOM_SOC", typeof(string));
                dmSocios.Columns.Add("TIPO_SOCIO", typeof(string));

                foreach (DataRow socioRow in tabelaSocioOp.Rows)
                {
                    DataRow row = dmSocios.NewRow();

                    row["ID_SOC"] = socioRow["COD_SOC"];
                    row["NOM_SOC"] = socioRow["NOM_SOC"];
                    row["TIPO_SOCIO"] = socioRow["DSC_TPS"];

                    dmSocios.Rows.Add(row);
                }

                using (var conexao = new OracleConnection(_conexaoBancoDw))
                {
                    conexao.Open();

                    var scriptSQL = "INSERT INTO DM_SOCIO(ID_SOC,NOM_SOC,TIPO_SOCIO) VALUES (:ID_SOC,:NOM_SOC,:TIPO_SOCIO)";


                    foreach (DataRow socioRow in dmSocios.Rows)
                    {
                        using (var commandSQL = new OracleCommand(scriptSQL, conexao))
                        {
                            OracleParameter[] parametros = new OracleParameter[]
                            {
                            new OracleParameter("id",socioRow["ID_SOC"]),
                            new OracleParameter("TPO_ART",socioRow["NOM_SOC"]),
                            new OracleParameter("NAC_BRAS",socioRow["TIPO_SOCIO"]),
                            };

                            commandSQL.Parameters.AddRange(parametros);
                            commandSQL.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch
            {
                Console.WriteLine("Ops... Erro ao carregar DMSocio.");
            }
        }
        private static void CarregarDMArtista()
        {
            try
            {
                DataTable tabelaAristaOp = new DataTable();
                using (var conexao = new OracleConnection(_conexaoBancoOperacional))
                {
                    conexao.Open();


                    OracleCommand commandSQL = conexao.CreateCommand();

                    commandSQL.CommandText = "SELECT DISTINCT ID_ART, TPO_ART, NAC_BRAS, NOM_ART FROM ARTISTA";

                    commandSQL.CommandType = CommandType.Text;

                    OracleDataReader dr = commandSQL.ExecuteReader();

                    tabelaAristaOp.Load(dr);
                }

                var dmSocios = new DataTable();
                dmSocios.Columns.Add("ID_ART", typeof(int));
                dmSocios.Columns.Add("TPO_ART", typeof(string));
                dmSocios.Columns.Add("NAC_BRAS", typeof(string));
                dmSocios.Columns.Add("NOM_ART", typeof(string));

                foreach (DataRow socioRow in tabelaAristaOp.Rows)
                {
                    DataRow row = dmSocios.NewRow();

                    row["ID_ART"] = socioRow["COD_ART"];
                    row["TPO_ART"] = socioRow["TPO_ART"];
                    row["NAC_BRAS"] = socioRow["NAC_BRAS"];
                    row["NOM_ART"] = socioRow["NOM_ART"];

                    dmSocios.Rows.Add(row);
                }

                using (var conexao = new OracleConnection(_conexaoBancoDw))
                {
                    conexao.Open();

                    var scriptSQL = "INSERT INTO DM_ARTISTA(ID_ART,TPO_ART,NAC_BRAS,NOM_ART) VALUES (:ID_ART,:TPO_ART,:NAC_BRAS,:NOM_ART)";


                    foreach (DataRow socioRow in dmSocios.Rows)
                    {
                        using (var commandSQL = new OracleCommand(scriptSQL, conexao))
                        {
                            OracleParameter[] parametros = new OracleParameter[]
                            {
                            new OracleParameter("ID_ART",socioRow["ID_ART"]),
                            new OracleParameter("TPO_ART",socioRow["TPO_ART"]),
                            new OracleParameter("NAC_BRAS",socioRow["NAC_BRAS"]),
                            new OracleParameter("NOM_ART",socioRow["NOM_ART"]),
                            };

                            commandSQL.Parameters.AddRange(parametros);
                            commandSQL.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch
            {
                Console.WriteLine("Ops... Erro ao carregar DMArtista.");

            }
        }
        private static void CarregarDMGravadora()
        {
            try
            {
                DataTable tabelaGravadoraOp = new DataTable();
                using (var conexao = new OracleConnection(_conexaoBancoOperacional))
                {
                    conexao.Open();


                    OracleCommand commandSQL = conexao.CreateCommand();

                    commandSQL.CommandText = "SELECT DISTINCT ID_GRAV, UF_GRAV, NAC_BRAS, NOM_GRAV FROM GRAVADORA";

                    commandSQL.CommandType = CommandType.Text;

                    OracleDataReader dr = commandSQL.ExecuteReader();

                    tabelaGravadoraOp.Load(dr);
                }

                var dmSocios = new DataTable();
                dmSocios.Columns.Add("ID_GRAV", typeof(int));
                dmSocios.Columns.Add("UF_GRAV", typeof(string));
                dmSocios.Columns.Add("NAC_BRAS", typeof(string));
                dmSocios.Columns.Add("NOM_GRAV", typeof(string));

                foreach (DataRow socioRow in tabelaGravadoraOp.Rows)
                {
                    DataRow row = dmSocios.NewRow();

                    row["ID_GRAV"] = socioRow["COD_GRAV"];
                    row["UF_GRAV"] = socioRow["UF_GRAV"];
                    row["NAC_BRAS"] = socioRow["NAC_BRAS"];
                    row["NOM_GRAV"] = socioRow["NOM_GRAV"];

                    dmSocios.Rows.Add(row);
                }

                using (var conexao = new OracleConnection(_conexaoBancoDw))
                {
                    conexao.Open();

                    var scriptSQL = "INSERT INTO DM_GRAVADORA(ID_GRAV,UF_GRAV,NAC_BRAS,NOM_GRAV) VALUES (:ID_GRAV,:UF_GRAV,:NAC_BRAS,:NOM_GRAV)";


                    foreach (DataRow socioRow in dmSocios.Rows)
                    {
                        using (var commandSQL = new OracleCommand(scriptSQL, conexao))
                        {
                            OracleParameter[] parametros = new OracleParameter[]
                            {
                            new OracleParameter("ID_GRAV",socioRow["ID_GRAV"]),
                            new OracleParameter("UF_GRAV",socioRow["UF_GRAV"]),
                            new OracleParameter("NAC_BRAS",socioRow["NAC_BRAS"]),
                            new OracleParameter("NOM_GRAV",socioRow["NOM_GRAV"]),
                            };

                            commandSQL.Parameters.AddRange(parametros);
                            commandSQL.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch
            {
                Console.WriteLine("Ops... Erro ao carregar DMGravadora.");
            }
        }
        private static void CarregarDMTitulo()
        {
            try
            {
                DataTable tabelaTituloOp = new DataTable();
                using (var conexao = new OracleConnection(_conexaoBancoOperacional))
                {
                    conexao.Open();


                    OracleCommand commandSQL = conexao.CreateCommand();

                    commandSQL.CommandText = "SELECT DISTINCT ID_GRAV, UF_GRAV, NAS_BRAS, NOM_GRAV FROM GRAVADORA";

                    commandSQL.CommandType = CommandType.Text;

                    OracleDataReader dr = commandSQL.ExecuteReader();

                    tabelaTituloOp.Load(dr);
                }

                var dmSocios = new DataTable();
                dmSocios.Columns.Add("ID_GRAV", typeof(int));
                dmSocios.Columns.Add("UF_GRAV", typeof(string));
                dmSocios.Columns.Add("NAC_BRAS", typeof(string));
                dmSocios.Columns.Add("NOM_GRAV", typeof(string));

                foreach (DataRow socioRow in tabelaTituloOp.Rows)
                {
                    DataRow row = dmSocios.NewRow();

                    row["ID_GRAV"] = socioRow["COD_GRAV"];
                    row["UF_GRAV"] = socioRow["UF_GRAV"];
                    row["NAC_BRAS"] = socioRow["NAC_BRAS"];
                    row["NOM_GRAV"] = socioRow["NOM_GRAV"];

                    dmSocios.Rows.Add(row);
                }

                using (var conexao = new OracleConnection(_conexaoBancoDw))
                {
                    conexao.Open();

                    var scriptSQL = "INSERT INTO DM_TITULO(ID_TITULO,TPO_TITULO,CLA_TITULO,DSC_TITULO) VALUES (:ID_TITULO,:TPO_TITULO,:CLA_TITULO,:DSC_TITULO)";


                    foreach (DataRow socioRow in dmSocios.Rows)
                    {
                        using (var commandSQL = new OracleCommand(scriptSQL, conexao))
                        {
                            OracleParameter[] parametros = new OracleParameter[]
                            {
                            new OracleParameter("ID_TITULO",socioRow["ID_TITULO"]),
                            new OracleParameter("TPO_TITULO",socioRow["TPO_TITULO"]),
                            new OracleParameter("CLA_TITULO",socioRow["CLA_TITULO"]),
                            new OracleParameter("DSC_TITULO",socioRow["DSC_TITULO"]),
                            };

                            commandSQL.Parameters.AddRange(parametros);
                            commandSQL.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch
            {
                Console.WriteLine("Ops... Erro ao carregar DMTitulo");
            }
            
        }
        private static void CarregarFTLocacao()
        {
            try
            {
                DataTable tabelaOperacional = new DataTable();

                using (var conexao = new OracleConnection(_conexaoBancoOperacional))
                {
                    conexao.Open();

                    OracleCommand commandSQL = conexao.CreateCommand();

                    commandSQL.CommandText = "SELECT  L.COD_SOC, L.DAT_LOC, S.COD_TPS, ITL.COD_TIT, ITL.NUM_COP, GRAV.COD_GRAV, ART.COD_ART," +
                                             "SUM(L.val_loc) VALOR_ARRECADADO," +
                                             "DATEDIFF(day, L.DAT_VENC, L.DAT_PGTO) TEMPO_DEVOLUCAO," +
                                             "0 MULTA_ATRASO" +
                                             "FROM LOCACOES L" +
                                             "JOIN SOCIOS S ON L.COD_SOC = S.COD_SOC" +
                                             "JOIN TIPOS_SOCIOS TPS ON S.COD_TPS = TPS.COD_TPS" +
                                             "JOIN ITENS_LOCACOES ITL ON L.DAT_LOC = ITL.DAT_LOC" +
                                             "JOIN COPIAS COP ON ITL.COD_TIT = COP.COD_TIT AND ITL.NUM_COP = COP.NUM_COP" +
                                             "JOIN TITULOS TIT ON TIT.COD_TIT = COP.COD_TIT" +
                                             "JOIN GRAVADORAS GRAV ON GRAV.COD_GRAV = TIT.COD_GRAV" +
                                             "JOIN ARTISTAS ART ON ART.COD_ART = TIT.COD_ART" +
                                             "WHERE L.STA_PGTO = 'P'" +
                                             "GROUP BY L.COD_SOC, L.DAT_LOC, S.COD_TPS, ITL.COD_TIT, ITL.NUM_COP, GRAV.COD_GRAV, ART.COD_ART, L.DAT_VENC, L.DAT_PGTO";


                    commandSQL.CommandType = CommandType.Text;

                    OracleDataReader dr = commandSQL.ExecuteReader();

                    tabelaOperacional.Load(dr);
                }

                foreach (DataRow row in tabelaOperacional.Rows)
                {
                    using (var conexao = new OracleConnection(_conexaoBancoDw))
                    {
                        conexao.Open();
                        OracleCommand commandSQL = conexao.CreateCommand();

                        //DM_SOCIO

                        commandSQL.CommandText = $"SELECT TOP(1) ID_SOC FROM DM_SOCIO WHERE NOM_SOC = {row["NOM_SOC"]}";

                        commandSQL.CommandType = CommandType.Text;

                        OracleDataReader dr = commandSQL.ExecuteReader();
                        var id_soc = dr.GetString(0);

                        // DM_TITULO

                        commandSQL.CommandText = $"SELECT TOP(1) ID_TITULO FROM DM_TITULO WHERE TPO_TITULO = {row["TPO_TITULO"]} AND CLA_TITULO = {row["CLA_TIT"]} AND DSC_TITULO = {row["DSC_TIT"]}";

                        commandSQL.CommandType = CommandType.Text;

                        dr = commandSQL.ExecuteReader();
                        var id_titulo = dr.GetString(0);

                        //DM ARTISTA

                        commandSQL.CommandText = $"SELECT TOP(1) ID_ART FROM DM_ARTISTA WHERE TPO_ART = {row["TPO_ART"]} AND NAS_BRAS = {row["NAS_BRAS"]} AND NOM_ART = {row["NOM_ART"]}";

                        commandSQL.CommandType = CommandType.Text;

                        dr = commandSQL.ExecuteReader();
                        var id_art = dr.GetString(0);

                        // DM_GRAVADORA

                        commandSQL.CommandText = $"SELECT TOP(1) ID_GRAV FROM DM_GRAVADORA WHERE UF_GRAV = {row["UF_GRAV"]} AND NAS_BRAS = {row["NAS_BRAS"]} AND NOM_GRAV = {row["NOM_GRAV"]}";

                        commandSQL.CommandType = CommandType.Text;

                        dr = commandSQL.ExecuteReader();
                        var id_grav = dr.GetString(0);


                        commandSQL.CommandText = "INSERT INTO FT_LOCACOES(ID_SOC,ID_TITULO,ID_ART,ID_GRAV,VALOR_ARRECADADO,TEMPO_DEVOLUCAO,MULTA_ATRASO) VALUES (:ID_SOC,:ID_TITULO,:ID_ART,:ID_GRAV,VALOR_ARRECADADO)";
                        OracleParameter[] parametros = new OracleParameter[]
                        {
                        new OracleParameter("ID_SOC",id_soc),
                        new OracleParameter("ID_TITULO",id_titulo),
                        new OracleParameter("ID_ART",id_art),
                        new OracleParameter("ID_GRAV",id_grav),
                        new OracleParameter("VALOR_ARRECADADO",row["VAL_LOC"]),
                        new OracleParameter("TEMPO_DEVOLUCAO", row["TEMPO_DEVOLUCAO"]),
                        new OracleParameter("MULTA_ATRASO", row["MULTA_ATRASO"]),
                        };
                        commandSQL.Parameters.AddRange(parametros);
                        commandSQL.ExecuteNonQuery();
                    }
                }
            }
            catch
            {
                Console.WriteLine("Ops... Erro ao carregar FTLocacao");
            }
        }
    }
}
