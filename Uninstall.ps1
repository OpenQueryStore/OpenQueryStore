<#
.SYNOPSIS
Uninstall OpenQueryStore on specified instance/database.

.DESCRIPTION
Remove OQS from the supplied instance/database combination, if it is installed.

.PARAMETER SqlInstance
The SQL Server that you're connecting to.

.PARAMETER SqlCredential
Credential object used to connect to the SQL Server Instance as a different user. This can be a Windows or SQL Server account. Windows users are determined by the existence of a backslash, so if you are intending to use an alternative Windows connection instead of a SQL login, ensure it contains a backslash.

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

.EXAMPLE
$cred = Get-Credential
.\Uninstall.ps1 -SqlInstance SQL2012 -Database ScooterStore -SqlCredential $cred

Will uninstall OQS on instance SQL2012 database named ScooterStore using SQL Authentication

#>
[CmdletBinding(SupportsShouldProcess = $True)]
param (
    [parameter(Mandatory = $true)]
    [string]$SqlInstance,
	[Alias("SqlCredential")]
    [PSCredential]$Credential,
    [parameter(Mandatory = $true)]
    [string]$Database,
    [ValidateSet("Yes", "No")]
    [string]$InstanceObjects = "No"
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

            try {
				if ($Credential.username -ne $null) {
					$username = ($Credential.username).TrimStart("\")

					if ($username -like "*\*") {
						$username = $username.Split("\")[1]
						$authtype = "Windows Authentication with Credential"
						$instance.ConnectionContext.LoginSecure = $true
						$instance.ConnectionContext.ConnectAsUser = $true
						$instance.ConnectionContext.ConnectAsUserName = $username
						$instance.ConnectionContext.ConnectAsUserPassword = ($Credential).GetNetworkCredential().Password
					}
					else {
						$authtype = "SQL Authentication"
						$instance.ConnectionContext.LoginSecure = $false
						$instance.ConnectionContext.set_Login($username)
						$instance.ConnectionContext.set_SecurePassword($Credential.Password)
					}
				}

                Write-Verbose "Connecting via SMO to $SqlInstance using $authtype"
				$instance.ConnectionContext.Connect()
			}
			catch {
				$message = $_.Exception.InnerException.InnerException
				$message = $message.ToString()
				$message = ($message -Split '-->')[0]
				$message = ($message -Split 'at System.Data.SqlClient')[0]
                $message = ($message -Split 'at System.Data.ProviderBase')[0]
                Write-Warning -Message "Failed to connect to $SqlInstance`: $message "
                Break
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
            Break
        }
    }

    try {
       
        if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking for OQS Schema")) {
            # If 'oqs' schema doesn't exists in the target database, we assume that OQS is not there
            if (-not ($instance.ConnectionContext.ExecuteScalar($qOQSExists))) {
                Write-Warning "OpenQueryStore not present in database [$database] on instance '$SqlInstance', no action required. Uninstallation cancelled."
                Break
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
                Break
            }
        }
        if ($pscmdlet.ShouldProcess("Uninstall Query", "Replacing Database Name with $database")) {
            # Replace placeholders
            $UninstallOQSBase = $UninstallOQSBase -replace "{DatabaseWhereOQSIsRunning}", "$Database"
        }
        Write-Verbose "OQS uninstall routine successfully loaded from $path. Uninstall can continue."
    
        Write-Verbose "Loading OQS uninstall routine from $path for non-db items"
        if ($pscmdlet.ShouldProcess("$path\setup\uninstall_open_query_store_non_db_items.sql", "Loading uninstall SQL Query from")) {
            $UninstallOQSNonDB = Get-Content -Path "$path\setup\uninstall_open_query_store_non_db_items.sql" -Raw
    
           if ($UninstallOQSNonDB -eq "") {
                Write-Warning "OpenQueryStore uninstall file for non-db items could not be properly loaded from $path. Please check files and permissions and retry the uninstall routine. Uninstallation cancelled."		
                Break
            }
        }
        Write-Verbose "OQS uninstall routine successfully loaded from $path for non-db items. Uninstall can continue."
    }
    catch {
        throw $_
    }

    try {
        if ($pscmdlet.ShouldProcess("$SqlInstance - $Database", "UnInstalling Base Query")) {
            # Perform the uninstall
            $null = $instance.ConnectionContext.ExecuteNonQuery($UninstallOQSBase)

            if ($pscmdlet.ShouldProcess("$SqlInstance", "UnInstalling Instance related objects")) {
                if ($InstanceObjects -eq "Yes") {
                    # Perform the uninstall for non db-specific items
                    $null = $instance.ConnectionContext.ExecuteNonQuery($UninstallOQSNonDB)
                    Write-Verbose "Open Query Store uninstallation complete for non-db items on instance '$SqlInstance'" 
                }
            }
        }
        Write-Verbose "Open Query Store uninstallation complete in database [$database] on instance '$SqlInstance'" 
    }
    catch {
        throw $_
    }
}
END {
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Disconnect from ")) {
        $instance.ConnectionContext.Disconnect()
        Write-Verbose "Disconnecting from $SqlInstance"
    }
        Write-Output "Open Query Store has been uninstalled"
}
