using Microsoft.Extensions.Configuration;
using Data.SqlClient;

var configuration = GetConfiguration();
var options = configuration.GetSection("options");

var dao = new DataAccesObject(
        configuration.GetConnectionString("csvimport") ?? string.Empty,
        bulkcopyboundary: options?.GetValue<int>("BulkCopyBoundary") ?? 1000
    );

dao.Basename = options?.GetValue<string>("Base") ?? string.Empty;

var importcsv = new CsvImporter(
    options?.GetValue<string>("CSVPath") ?? string.Empty,
    dao    
);

await importcsv.ImportCsvAsync();

static IConfiguration GetConfiguration() {
    var builder = new ConfigurationBuilder()
        .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
        .AddJsonFile("appsettings.json");

    return  builder.Build();
}