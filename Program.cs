using Microsoft.Extensions.Configuration;
using Data.SqlClient;

var configuration = GetConfiguration();
var options = configuration.GetSection("options");

var dao = new DataAccesObject(
        configuration.GetConnectionString("csvimport") ?? "",
        bulkcopyboundary: options?.GetValue<int>("BulkCopyBoundary") ?? 1000
    );

dao.Basename = options?.GetValue<string>("Base") ?? "";

var importcsv = new CsvImporter(
    options?.GetValue<string>("CSVPath") ?? "",
    dao    
);

await importcsv.ImportCsvAsync();

static IConfiguration GetConfiguration() {
    var builder = new ConfigurationBuilder()
        .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
        .AddJsonFile("appsettings.json");

    return  builder.Build();
}