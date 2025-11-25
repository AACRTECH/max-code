$server = "IT-5366-MB\SQLEXPRESS" #will need to change to your credentials
$database = "PROPOSAL_CENTRAL"
$tablequery = "SELECT s.name as schemaName, t.name as tableName from sys.tables t inner join sys.schemas s ON t.schema_id = s.schema_id"

# Declare Connection Variables
$connectionTemplate = "Data Source={0};Integrated Security=SSPI;Initial Catalog={1};"
$connectionString = [string]::Format($connectionTemplate, $server, $database)
$connection = New-Object System.Data.SqlClient.SqlConnection
$connection.ConnectionString = $connectionString
$connection.Open()

$command = New-Object System.Data.SqlClient.SqlCommand
$command.CommandText = $tablequery
$command.Connection = $connection
$command.CommandTimeout = 300 # Set the CommandTimeout to 300 seconds (5 minutes)

# Load up the Tables in a dataset
$SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
$SqlAdapter.SelectCommand = $command
$DataSet = New-Object System.Data.DataSet
$SqlAdapter.Fill($DataSet)

# Loop through all tables and export a CSV of the Table Data
foreach ($Row in $DataSet.Tables[0].Rows) {
    $queryData = "SELECT * FROM [$($Row[0])].[$($Row[1])]"

    # Specify the output location of your dump file - CHANGE TO YOUR PATH
    $extractFile = "C:\Users\maxwell.bicking\OneDrive - American Association for Cancer Res\Proposal Central\Files\$($Row[0])_$($Row[1]).csv"

    $command.CommandText = $queryData
    $command.CommandTimeout = 300 # Set the CommandTimeout to 300 seconds (5 minutes) for each table query

    # Reuse SqlDataAdapter and DataSet
    $SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
    $SqlAdapter.SelectCommand = $command
    $TableDataSet = New-Object System.Data.DataSet
    $SqlAdapter.Fill($TableDataSet)

    # Export to CSV
    $TableDataSet.Tables[0] | Export-Csv $extractFile -NoTypeInformation

    # Clear the DataSet for the next table
    $TableDataSet.Tables.Clear()
}

$connection.Close()

# NEW SCRIPT BELOW
<# ---------------------------------------------------------------------------
  Export every table in a database to CSV files (headers included, low memory)
--------------------------------------------------------------------------- #>

# -- UPDATE THESE THREE LINES -------------------------------------------------
$server   = "IT-5366-MB\SQLEXPRESS"              # SQL Server instance
$database = "PROPOSAL_CENTRAL"                  # Database to export
$outRoot  = "C:\Users\maxwell.bicking\OneDrive - American Association for Cancer Res\Proposal Central\Files" # Destination folder
# --------------------------------------------------------------------------- #

# Make sure the output folder exists
if (-not (Test-Path $outRoot)) { New-Item -ItemType Directory -Path $outRoot | Out-Null }

# Re-use one ADO.NET connection for catalog look-ups
$connStr = "Data Source=$server;Initial Catalog=$database;Integrated Security=SSPI;"
$cn  = [Data.SqlClient.SqlConnection]::new($connStr)
$cn.Open()

# Pull the list of [schema] [table] pairs (quoted with [])
$tblSql = @'
SELECT QUOTENAME(s.name) AS SchemaQ,
       QUOTENAME(t.name) AS TableQ,
       s.name            AS SchemaPlain,
       t.name            AS TablePlain
FROM   sys.tables  t
JOIN   sys.schemas s ON s.schema_id = t.schema_id
ORDER  BY s.name, t.name;
'@
$tblCmd = $cn.CreateCommand()
$tblCmd.CommandText = $tblSql
$tblRdr = $tblCmd.ExecuteReader()

# Build an in-memory list (small — just names)
$tables = @()
while ($tblRdr.Read()) {
    $tables += [pscustomobject]@{
        SchemaQ      = $tblRdr.GetString(0)  # [schema]
        TableQ       = $tblRdr.GetString(1)  # [table]
        SchemaPlain  = $tblRdr.GetString(2)  # schema
        TablePlain   = $tblRdr.GetString(3)  # table
    }
}
$tblRdr.Close()

foreach ($t in $tables) {

    # ----- 1.  Get column list for this table --------------------------------
    $colSql = @"
SELECT name, QUOTENAME(name) AS QName
FROM   sys.columns
WHERE  object_id = OBJECT_ID('$($t.SchemaPlain).$($t.TablePlain)')
ORDER  BY column_id;
"@
    $colCmd = $cn.CreateCommand(); $colCmd.CommandText = $colSql
    $colRdr = $colCmd.ExecuteReader()

    $headerParts = @()   # 'Col1','Col2',...
    $dataParts   = @()   # CAST([Col1] AS nvarchar(max)),...
    while ($colRdr.Read()) {
        $namePlain = $colRdr.GetString(0)
        $nameQ     = $colRdr.GetString(1)   # [Col]
        $headerParts += "'$namePlain'"
        $dataParts   += "CAST($nameQ AS nvarchar(max))"
    }
    $colRdr.Close()

    if ($headerParts.Count -eq 0) {          # skip empty tables (unlikely)
        Write-Warning "Table $($t.SchemaQ).$($t.TableQ) has no columns – skipped."
        continue
    }

    $headerSql = $headerParts -join ','
    $dataSql   = $dataParts   -join ','
    $fullQuery = "SELECT $headerSql UNION ALL SELECT $dataSql FROM $($t.SchemaQ).$($t.TableQ)"

    # ----- 2.  Build output path & bcp command -------------------------------
    $filePath = Join-Path $outRoot "$($t.SchemaPlain)_$($t.TablePlain).csv"

    $bcpCmd = @(
        "bcp", "`"$fullQuery`"", "queryout", "`"$filePath`"",
        "-S", "`"$server`"", "-d", "`"$database`"" , "-T",
        "-c",             # char mode
        "-t", ",",        # column delimiter
        "-r", "0x0a"      # row delimiter (LF)
    ) -join ' '

    Write-Host "» Exporting $($t.SchemaQ).$($t.TableQ) → $filePath"
    & cmd /c $bcpCmd
}

$cn.Close()


# ALTERNATE

$ErrorActionPreference = "Stop"
$server   = "IT-5366-MB\SQLEXPRESS"
$database = "PROPOSAL_CENTRAL"
$outRoot  = "C:\Users\maxwell.bicking\OneDrive - American Association for Cancer Res\Proposal Central\Files"

# Build one connection you reuse for the whole run
$connStr = "Data Source=$server;Initial Catalog=$database;Integrated Security=SSPI;"
$cn = [Data.SqlClient.SqlConnection]::new($connStr)
$cn.Open()

# Get list of schema.table names
$tablesCmd = $cn.CreateCommand()
$tablesCmd.CommandText = @'
SELECT QUOTENAME(s.name)  AS SchemaName,
       QUOTENAME(t.name)  AS TableName
FROM   sys.tables AS t
JOIN   sys.schemas AS s ON s.schema_id = t.schema_id
ORDER  BY s.name, t.name;
'@
$tables = $tablesCmd.ExecuteReader()
$tableList = New-Object 'System.Collections.Generic.List[Tuple[string,string]]'
while ($tables.Read()) {
    $tableList.Add([Tuple]::Create($tables.GetString(0), $tables.GetString(1)))
}
$tables.Close()

# Helper that writes one table to CSV without buffering the whole thing
function Export-TableStreaming {
    param(
        [Data.SqlClient.SqlConnection]$Connection,
        [string]$SchemaName,
        [string]$TableName,
        [string]$OutputFolder
    )

    $sql = "SELECT * FROM $SchemaName.$TableName;"
    $cmd = $Connection.CreateCommand()
    $cmd.CommandTimeout = 0          # unlimited – streaming handles long runs
    $cmd.CommandText = $sql

    $reader = $cmd.ExecuteReader()

    $filePath = Join-Path $OutputFolder "$($SchemaName.Trim('[',']'))_$($TableName.Trim('[',']')).csv"
    $sw = [IO.StreamWriter]::new($filePath, $false, [Text.Encoding]::UTF8)

    # Write header row
    for ($i = 0; $i -lt $reader.FieldCount; $i++) {
        $sw.Write('"' + $reader.GetName($i).Replace('"','""') + '"')
        if ($i -lt $reader.FieldCount-1) { $sw.Write(',') }
    }
    $sw.WriteLine()

    # Stream each row
    while ($reader.Read()) {
        for ($i = 0; $i -lt $reader.FieldCount; $i++) {
            $val = if      ($reader.IsDBNull($i)) { '' }
                   elseif  ($reader.GetFieldType($i).Name -eq 'DateTime') {
                       $reader.GetDateTime($i).ToString('yyyy-MM-dd HH:mm:ss')
                   } else { $reader.GetValue($i).ToString() }

            $sw.Write('"' + $val.Replace('"','""') + '"')
            if ($i -lt $reader.FieldCount-1) { $sw.Write(',') }
        }
        $sw.WriteLine()
    }

    $sw.Dispose()
    $reader.Close()

    Write-Host "✔ $SchemaName.$TableName  →  $filePath"
}

foreach ($tbl in $tableList) {
    Export-TableStreaming -Connection $cn `
                          -SchemaName  $tbl.Item1 `
                          -TableName   $tbl.Item2 `
                          -OutputFolder $outRoot
}

$cn.Close()