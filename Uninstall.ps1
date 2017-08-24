<#
.SYNOPSIS
Uninstall OpenQueryStore on specified instance/database.

.DESCRIPTION
Remove OQS from the supplied instance/database combination, if it is installed.

.PARAMETER SqlInstance
The SQL Server that you're connecting to.

.PARAMETER Database
Specifies the Database where OQS objects will be created

.NOTES
Author: William Durkin (@sql_williamd)

Copyright:
William Durkin (@sql_williamd) / Enrico van de Laar (@evdlaar)

License:
	This script is free to download and use for personal, educational, and internal
	corporate purposes, provided that this header is preserved. Redistribution or sale
	of this script, in whole or in part, is prohibited without the author's express
	written consent.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
	INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
	IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES
	OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

.LINK
https://github.com/OpenQueryStore/OpenQueryStore

.EXAMPLE
.\Uninstall.ps1 -SqlInstance SQL2012 -Database ScooterStore

Will uninstall OQS on instance SQL2012 database named ScooterStore

#>
[CmdletBinding()]
param (
    [parameter(Mandatory = $true)]
    [string]$SqlInstance,
    [parameter(Mandatory = $true)]
    [string]$Database
)
BEGIN {
    $path = Get-Location
    $qOQSExists = "SELECT TOP 1 1 FROM [$Database].[sys].[schemas] WHERE [name] = 'oqs'"
    $null = [Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo")
}
PROCESS {    
    
    # Connect to instance
    try {
        $instance = New-Object Microsoft.SqlServer.Management.Smo.Server $SqlInstance
    }
    catch {
        throw $_.Exception.Message
    }

    # Verify if database exist in the instance
    if (-not ($instance.Databases | Where-Object Name -eq $Database)) {
        Write-Error "Database $Database does not exists on instance $SqlInstance. Uninstallation cancelled."
        return
    }

    # If 'oqs' schema doesn't exists in the target database, we assume that OQS is not there
    if (-not ($instance.ConnectionContext.ExecuteScalar($qOQSExists))) {
        Write-Warning "OpenQueryStore not present in database $database, no action required. Uninstallation cancelled."
        return
    }
    
    # Load the installer files
    $UninstallOQSBase = Get-Content -Path "$path\uninstall_open_query_store.sql" -Raw
    
    if ($UninstallOQSBase -eq "") {
        Write-Warning "OpenQueryStore uninstall file could not be properly loaded from $path. Please check files and permissions and retry the uninstall routine. Uninstallation cancelled."
        return
    }

    # Replace placeholders
    $UninstallOQSBase = $UninstallOQSBase -replace "{DatabaseWhereOQSIsRunning}", "$Database"

    # Ready to install!
    Write-Warning "Uninstalling OQS on $SqlInstance in $database"
    $null = $instance.ConnectionContext.ExecuteNonQuery($UninstallOQSBase)
            
    Write-Warning "Open Query Store uninstallation complete."
}
END {
    $instance.ConnectionContext.Disconnect()
}