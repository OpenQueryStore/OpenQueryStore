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
[CmdletBinding(SupportsShouldProcess = $True)]
param (
    [parameter(Mandatory = $true)]
    [string]$SqlInstance,
    [parameter(Mandatory = $true)]
    [string]$Database
)
BEGIN {
    $path = Get-Location
    $qOQSExists = "SELECT TOP 1 1 FROM [$Database].[sys].[schemas] WHERE [name] = 'oqs'"
    if ($pscmdlet.ShouldProcess("SQL Server SMO", "Loading Assemblies")) {
        try {
            $null = [Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo")
            Write-Verbose "SQL Server Assembly loaded"
        }
        catch {
            Write-Warning "Failed to load SQL Server SMO Assemblies - Quitting"
            break
        }
    }
}
PROCESS {    
    
    # Connect to instance
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Connecting to with SMO")) {
        try {
            $instance = New-Object Microsoft.SqlServer.Management.Smo.Server $SqlInstance
            # Checking if we have actually connected to the instance or not 
            if ($null -eq $instance.Version) {
                Write-Warning "Failed to connect to $SqlInstance - Quitting"
                return
            }
        }
        catch {
            throw $_
        }
    }

    # Verify if database exist in the instance
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking if $database exists")) {
        if (-not ($instance.Databases | Where-Object Name -eq $Database)) {
            Write-Warning "Database [$Database] does not exists on instance $SqlInstance. Installation cancelled."
            return
        }
    }

    try {
       
        if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking for OQS Schema")) {
            # If 'oqs' schema doesn't exists in the target database, we assume that OQS is not there
            if (-not ($instance.ConnectionContext.ExecuteScalar($qOQSExists))) {
                Write-Warning "OpenQueryStore not present in database [$database] on instance '$SqlInstance', no action required. Uninstallation cancelled."
                return
            }
            else {
                Write-Verbose "OQS installation found in database [$database] on instance '$SqlInstance'. Uninstall process can continue."
            }
        }

    }
    catch {
        throw $_
    }
    
    # Load the uninstaller files
    try {
        Write-Verbose "Loading uninstall routine from $path"
        if ($pscmdlet.ShouldProcess("$path\setup\uninstall_open_query_store.sql", "Loading uninstall SQL Query from")) {
            $UninstallOQSBase = Get-Content -Path "$path\setup\uninstall_open_query_store.sql" -Raw
     
            if ($UninstallOQSBase -eq "") {
                Write-Warning "OpenQueryStore uninstall file could not be properly loaded from $path. Please check files and permissions and retry the uninstall routine. Uninstallation cancelled."
                return
            }
        }
        if ($pscmdlet.ShouldProcess("Uninstall Query", "Replacing Database Name with $database")) {
            # Replace placeholders
            $UninstallOQSBase = $UninstallOQSBase -replace "{DatabaseWhereOQSIsRunning}", "$Database"
        }
        Write-Versbose "OQS uninstall routine successfully loaded from $path. Uninstall can continue."
    }
    catch {
        throw $_
    }

    try {
        # Perform the uninstall
        Write-Host "INFO: Uninstalling OQS in [$database] on instance '$SqlInstance'"

        $null = $instance.ConnectionContext.ExecuteNonQuery($UninstallOQSBase)
        
        Write-Host "INFO: Open Query Store uninstallation complete in database [$database] on instance '$SqlInstance'" -ForegroundColor "Green"
    }
    catch {
        throw $_.Exception.Message
    }
}
END {
    $instance.ConnectionContext.Disconnect()
}