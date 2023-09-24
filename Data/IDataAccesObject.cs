using System.Data;

public interface IDataAccesObject {
    string ConnectionString { get; }
    public string Servername { get; set; }
    public string Basename { get; set; }
    public string User { get; set; }
    public string Tablename { get; set; }
    public int Bulkcopyboundary { get; set; }

    string CreateConnectionString(string password);

    Task<bool> BaseCheckAsync(string scriptcreatetable);

    (DataTable, String) DataTableParse(string[] columns);

    Task<int> InsertDataTableAsync(DataTable dataTable);
}