using System.Data;
using System.Diagnostics;

public class CsvImporter
{
    private readonly FileInfo _importfile;
    private readonly IDataAccesObject _dao;
    private string[]? _headerline = null!;
    private long _totalreadbytes, _totalinsertedrows, _slices = 1;
    private bool _tablecreated;

    public string[]? Headerline => _headerline;

    public CsvImporter(string filePath, IDataAccesObject dao) {
        if (String.IsNullOrEmpty(filePath)) throw new ArgumentNullException(nameof(filePath));
        _importfile = new FileInfo(filePath);
        _dao = dao ?? throw new ArgumentNullException(nameof(dao));
    }

    public async Task ImportCsvAsync()
    {
        if (File.Exists(_importfile.FullName)) {

            if (_importfile.Length > 0) {
                Stopwatch stopWatch = new Stopwatch();
                stopWatch.Start();
                
                var tasks = new List<Task>();            

                int cores;

                switch (_importfile.Length) {
                    case > 4294967296:
                        cores = Environment.ProcessorCount;
                        break;
                    case > 268435456:
                        cores = 16;
                        break;
                    case > 16777216:
                        cores = 8;
                        break;
                    case > 1048576:
                        cores = 4;
                        break;
                    case > 65536:
                        cores = 2;
                        break;
                    default:
                        cores = 1;
                    break;
                }

                _slices = _importfile.Length / cores;

                await ReadHeader();
                
                (DataTable dt, string scriptcreatetable) = _dao.DataTableParse(_headerline);

                _tablecreated = await _dao.BaseCheckAsync(scriptcreatetable);

                for (var i = 0; i<cores; i++) {                                       
                    
                    if (!(_headerline is null)) {
                        var fsSlices = new FileStream(_importfile.FullName, FileMode.Open, FileAccess.Read);
                        fsSlices.Position = _slices * i;

                        var taskId = (short)(i + 1);                        

                        tasks.Add(Task.Run(()=> ImportSlice(fsSlices, taskId, dt.Clone())));
                        Console.WriteLine($"Start task: {taskId}");
                    }
                    else break;
                }

                Console.WriteLine($"Active tasks numbers : {cores}");

                await Task.WhenAll(tasks);
                
                stopWatch.Stop();
                Console.WriteLine($"Execution times : {stopWatch.ElapsedMilliseconds}");
                Console.WriteLine($"{(long)(_totalreadbytes / 1024)} KB read.");
                Console.WriteLine($"{_totalinsertedrows} rows inserted into the database.");
            }
            else
                Console.WriteLine("Error : empty file");            
        }
        else
            Console.WriteLine("Error : file not find");
    }

    private async Task ReadHeader() {        
        try {
            using (var fileStream = new FileStream(_importfile.FullName, FileMode.Open, FileAccess.Read)) {
                using (var reader = new StreamReader(fileStream)) {
                    var line = await reader.ReadLineAsync();

                    _totalreadbytes += (line?.Length ?? 0) * sizeof(char);

                    _headerline = line?.Split(',');
                    Console.WriteLine($"File : {_importfile.Name}");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            Console.WriteLine("Error : reading header line");
        }
    }

    private async Task ImportSlice(FileStream fileStream, short taskId, DataTable dt)
    {
        long totalBytes = 0;
        var totalinsertedrows = 0;

        using (var reader = new StreamReader(fileStream))
        {
            if (fileStream.Position == 0) 
                await reader.ReadLineAsync();

            var line = String.Empty;

            if (_tablecreated) {
                
                do {
                    if (dt.Rows.Count == _dao.Bulkcopyboundary) {
                        await _dao.InsertDataTableAsync(dt);
                        totalinsertedrows += _dao.Bulkcopyboundary;                                
                    }

                    if (!reader.EndOfStream) {
                        line = await reader.ReadLineAsync();
                        totalBytes += (line?.Length ?? 0) * sizeof(char);
                        LineValuesMap(line, dt);
                    }                                        

                } while(totalBytes < _slices);

                _totalreadbytes += totalBytes;
                
            }
            else {
                Console.WriteLine($"Task ID {taskId}: the table {_dao.Tablename} could not be created.");
            }

            if (!(dt is null) && dt.Rows.Count > 0) {
                totalinsertedrows += dt.Rows.Count;
                await _dao.InsertDataTableAsync(dt);
            }

            _totalinsertedrows += totalinsertedrows;

            Console.WriteLine($"Task ID {taskId}: {totalinsertedrows} rows inserted into the database.");
        }
    }

    private void LineValuesMap(string? line, DataTable dt) {

        try {            

            var values = line?.Split(',') ?? new string[] {};

            if (values.Length == _headerline?.Length && values.Length == dt?.Columns.Count) {
                var row = dt.NewRow();
                string[] trueValues = {"sim", "yes", "oui", "1", "true"};
                string[] falseValues = {"não", "no", "not", "non", "0", "false"};

                for (var i = 0; i < values.Length; i++) {
                    switch (dt.Columns[i].DataType.ToString()) {
                        case "System.Boolean":
                            row[i] = trueValues.Contains(values[i].ToLower()) ? true : falseValues.Contains(values[i].ToLower()) ? false : DBNull.Value;
                            break;
                        case "System.Int16":
                        case "System.Int32":
                        case "System.Int64":
                            int testint;
                            if (int.TryParse(values[i], out testint))
                                row[i] = testint;                        
                            break;
                        case "System.Decimal":
                            decimal testdecimal;
                            if (decimal.TryParse(values[i], out testdecimal))
                                row[i] = testdecimal;
                            break;
                        case "System.DateTime":
                            DateTime testdate;
                            if (DateTime.TryParse(values[i], out testdate))
                                row[i] = testdate;
                            break;
                        case "System.Guid":
                            Guid testguid;
                            if (Guid.TryParse(values[i], out testguid))
                                row[i] = testguid;                            
                            break;
                        default:
                            row[i] = values[i];
                        break;
                    }
                }

                dt.Rows.Add(row);
            }
        }
        catch {}
        
    }
}