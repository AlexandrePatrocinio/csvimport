using System.Data.SqlClient;
using System.Data;
using System.Text;
using System.Text.RegularExpressions;
namespace Data.SqlClient;

public class DataAccesObject : IDataAccesObject {

    private void init(string connectionString, string tablename, int bulkcopyboundary) {
        ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        Servername = Servername ?? ConnectionString.Split(';')
            .Where(c => c.ToLower().Contains("source") && c.Split('=')[1].Trim() != String.Empty)
            .Select(c => c.Split('=')[1].Trim())
            .FirstOrDefault() ?? throw new ArgumentException(nameof(Servername));
        Basename = Basename ?? ConnectionString.Split(';')
            .Where(c => c.ToLower().Contains("catalog") && c.Split('=')[1].Trim() != String.Empty)
            .Select(c => c.Split('=')[1].Trim())
            .FirstOrDefault() ?? throw new ArgumentException(nameof(Basename));
        User = User ?? ConnectionString.Split(';')
            .Where(c => c.ToLower().Contains("user") && c.Split('=')[1].Trim() != String.Empty)
            .Select(c => c.Split('=')[1].Trim())
            .FirstOrDefault() ?? throw new ArgumentException(nameof(User));
        Tablename = tablename;
        Bulkcopyboundary = bulkcopyboundary;
    }

    public DataAccesObject(string connectionString, string tablename = "CSVImport", int bulkcopyboundary = 1000) => init(connectionString, tablename, bulkcopyboundary);

    public DataAccesObject(string servername, string basename, string user, string password)
    {   
        Servername = servername ?? throw new ArgumentNullException(nameof(basename));     
        Basename = basename ?? throw new ArgumentNullException(nameof(basename));
        User = user ?? throw new ArgumentNullException(nameof(user));
        if (String.IsNullOrEmpty(password)) throw new ArgumentNullException(nameof(password));
        init(CreateConnectionString(password), "CSVImport", 1000);
    }

    public string ConnectionString { get; private set; }
    public string Servername { get; set; }
    public string Basename { get; set; }
    public string User { get; set; } 
    public string Tablename { get; set; }
    public int Bulkcopyboundary { get; set; }

    public string CreateConnectionString(string password) => $"Data Source={Servername};Initial Catalog={Basename};User ID={User};Password={password}";

    public async Task<bool> BaseCheckAsync(string scriptcreatetable)
    {       

        var created = true;
        try
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                var cmd = new SqlCommand("", connection);
                cmd.Parameters.Add(new SqlParameter("@basename", Basename));

                cmd.CommandText = @$"
                    if not exists(Select 1 from sys.databases where name=@basename)
                        create database {Basename};";

                await cmd.ExecuteNonQueryAsync();

                cmd.Parameters.Add(new SqlParameter("@tablename", Tablename));

                cmd.CommandText = @$"
                    use {Basename}
                    if not exists(Select 1 from sys.objects where name=@tablename and type='U')
                        {scriptcreatetable}";
                
                await cmd.ExecuteNonQueryAsync();

            }
        }
        catch (Exception ex)
        {
            created = false;
            Console.WriteLine(ex.Message);
            Console.WriteLine(scriptcreatetable);
        }

        return created;
    }

    public (DataTable, String) DataTableParse(string[] columns)
    {
        var rgx = new Regex(@"\((\w{1})\)");
        var datatypes = columns.Select((c)=> 
            new KeyValuePair<string, char>(
                rgx.Replace(c,""), 
                rgx.Match(c)?.Captures[0].Value[1] ?? 'C'
            )
        );

        var dt = new DataTable();
        
        StringBuilder scriptcreatetable = new StringBuilder();

        scriptcreatetable.AppendLine($"Create table {Tablename} (");

        foreach (var column in datatypes) {
           switch (column.Value) {
                case 'D':
                    dt.Columns.Add(column.Key, typeof(DateTime));
                    scriptcreatetable.AppendLine($" [{column.Key}] datetime,");
                    break;
                case 'L':
                    dt.Columns.Add(column.Key, typeof(bool));
                    scriptcreatetable.AppendLine($" [{column.Key}] bit,");
                    break;
                case 'N':
                    dt.Columns.Add(column.Key, typeof(decimal));
                    scriptcreatetable.AppendLine($" [{column.Key}] decimal(18,6),");
                    break;
                case 'I':
                    dt.Columns.Add(column.Key, typeof(int));
                    scriptcreatetable.AppendLine($" [{column.Key}] int,");
                    break;
                case 'U':
                    dt.Columns.Add(column.Key, typeof(Guid));
                    scriptcreatetable.AppendLine($" [{column.Key}] uniqueidentifier,");
                    break;                    
                default:
                    dt.Columns.Add(column.Key, typeof(string));
                    scriptcreatetable.AppendLine($" [{column.Key}] nvarchar(255),");
                    break;
           }
        }

        return (dt, scriptcreatetable.ToString().TrimEnd(new char[] {',', '\r', '\n'}) + ")");
    }

    public async Task<int> InsertDataTableAsync(DataTable dataTable)
    {
        try
        {
            using (var connection = new SqlConnection(ConnectionString))
            {                
                await connection.OpenAsync();
                await connection.ChangeDatabaseAsync(Basename);

                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = Tablename;
                    bulkCopy.BatchSize = dataTable?.Rows.Count ?? 0;

                    await bulkCopy.WriteToServerAsync(dataTable);
                    return bulkCopy.BatchSize;
                }
            }                                                                
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            return 0;
        }        
        finally
        {
            dataTable.Clear();
        }
    }
}