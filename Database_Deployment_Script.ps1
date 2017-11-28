# Code to ensure SQLPS is loaded
cls
function Import-Module-SQLPS {
    #pushd and popd to avoid import from changing the current directory (ref: http://stackoverflow.com/questions/12915299/sql-server-2012-sqlps-module-changing-current-location-automatically)
    #3>&1 puts warning stream to standard output stream (see https://connect.microsoft.com/PowerShell/feedback/details/297055/capture-warning-verbose-debug-and-host-output-via-alternate-streams)
    #out-null blocks that output, so we don't see the annoying warnings described here: https://www.codykonior.com/2015/05/30/whats-wrong-with-sqlps/
    push-location
    import-module sqlps 3>&1 | out-null
    pop-location
}
 
Import-Module-SQLPS

###########################################################################################################
# SET THESE VARIABLES TO DEFINE THE DB NAMING AND SERVER
###########################################################################################################
# Database name variables
$databaseNameCore = 'DataProfile'
$prodSuffix = ''
$devSuffix = 'Dev'
# Server where the databases will be deployed
$serverName = 'azsdl-vwsqlstg1'
###########################################################################################################

$ns = 'Microsoft.SqlServer.Management.Smo'
$server = New-Object ("$ns.Server") ($serverName)

# Make DBs
$proddb = New-Object ("$ns.Database") ($server, "$databaseNameCore$prodSuffix")
$devdb = New-Object ("$ns.Database") ($server, "$databaseNameCore$devSuffix")
$proddb.Create()
$devdb.Create()

# Make dp Schema
$schemadp = New-Object -TypeName ("$ns.Schema") -argumentlist $proddb, "dp"
$schemadp.Create()
Clear-Variable -Name "schemadp"
$schemadp = New-Object -TypeName ("$ns.Schema") -argumentlist $devdb, "dp"
$schemadp.Create()

# Populate all objects through DDL scripts
Get-ChildItem -Path "$PSScriptRoot\" -Recurse -Filter *.sql -File | sort FullName |
ForEach-Object {
    # read file into variable / run code in SQL
    $sqlQuery = Get-Content $_.FullName -Raw
    $sqlQueryProd = $sqlQuery.Replace("[ProdDB].", "")
    $sqlQueryProd = $sqlQueryProd.Replace("[DevDB]", "$databaseNameCore$devSuffix" )
    Invoke-Sqlcmd -Query $sqlQueryProd -ServerInstance $serverName -Database $databaseNameCore$prodSuffix
    $sqlQueryDev = $sqlQuery.Replace("[ProdDB]", "$databaseNameCore$prodSuffix" )
    $sqlQueryDev = $sqlQueryDev.Replace("[DevDB].", "")
    Invoke-Sqlcmd -Query $sqlQueryDev -ServerInstance $serverName -Database $databaseNameCore$devSuffix
}

# Display all objects created by DB
Invoke-Sqlcmd -Query "SELECT '$databaseNameCore$prodSuffix' as DBName, Type, Name, create_date FROM sys.objects WHERE type IN ( 'U', 'V', 'IF', 'TR', 'P', 'TT' ) ORDER BY Type, name" -ServerInstance $serverName -Database $databaseNameCore$prodSuffix
Invoke-Sqlcmd -Query "use master;"  -ServerInstance $serverName -Database $databaseNameCore$prodSuffix
Invoke-Sqlcmd -Query "SELECT '$databaseNameCore$devSuffix' as DBName, Type, Name, create_date FROM sys.objects WHERE type IN ( 'U', 'V', 'IF', 'TR', 'P', 'TT' ) ORDER BY Type, name" -ServerInstance $serverName -Database $databaseNameCore$devSuffix
Invoke-Sqlcmd -Query "use master;"  -ServerInstance $serverName -Database $databaseNameCore$devSuffix

# Drop DBs, only here for testing
#$proddb.Drop()
#$devdb.Drop()

