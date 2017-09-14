<#
.SYNOPSIS
Install OpenQueryStore on specified instance/database.

.DESCRIPTION
Verify if OQS is not already installed on destination database and if not will run the scripts to install it.

.PARAMETER SqlInstance
The SQL Server that you're connecting to.

.PARAMETER Database
Specifies the Database where OQS objects will be created

.PARAMETER OQSMode
Specifies the mode OQS should operate in. Classic (monitoring a single database) or Centralized (a separate OQS database to monitor multiple databases)

.PARAMETER SchedulerType
Specifies which type of scheduling of data collection should be used. Either "Service Broker" (Default) or "SQL Agent"

.PARAMETER CertificateBackupPath
Specifies the path where certificate backup will be temporarily saved. By default "C:\temp" (the file is deleted immediately after installation)

.NOTES
Author: Cláudio Silva (@ClaudioESSilva)
        William Durkin (@sql_williamd)

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
.\Install.ps1 -SqlInstance SQL2012 -Database ScooterStore -OQSMode "Classic" -SchedulerType "Service Broker" -CertificateBackupPath "C:\temp"

Will install the Classic version on instance SQL2012 database named ScooterStore and use Service Broker for scheduling, storing the certificate in c:\temp.

.EXAMPLE
.\Install.ps1 -SqlInstance "SQL2012" -Database "db3" -OQSMode "Centralized" -SchedulerType "SQL Agent"

Will install the centralized version on instance SQL2012 database named db3 and use the SQL Agent for scheduling.

#>
[CmdletBinding(SupportsShouldProcess = $True)]
param (
    [parameter(Mandatory = $true)]
    [string]$SqlInstance,
    [parameter(Mandatory = $true)]
    [string]$Database,
    [parameter(Mandatory = $true)]
    [ValidateSet("Classic", "Centralized")]
    [string]$OQSMode = "Classic",
    [parameter(Mandatory = $true)]
    [ValidateSet("Service Broker", "SQL Agent")]
    [string]$SchedulerType = "Service Broker",
    [string]$CertificateBackupPath = $ENV:TEMP
)
BEGIN {
    $path = Get-Location
    $qOQSExists = "SELECT TOP 1 1 FROM [$Database].[sys].[schemas] WHERE [name] = 'oqs'"
    $CertificateBackupFullPath = Join-Path -Path $CertificateBackupPath  -ChildPath "open_query_store.cer"
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
            Write-Verbose "Connecting via SMO to $SqlInstance"
            # Checking if we have actually connected to the instance or not 
            if ($null -eq $instance.Version) {
                Write-Warning "Failed to connect to $SqlInstance - Quitting"
                return
            }
        }
        catch {
            throw $_.Exception.Message
        }
    }

    Write-Verbose "Checking SQL Server version"
    # We only support between SQL Server 2008 (v10.X.X) and SQL Server 2014 (v12.X.X)
    if ($instance.Version.Major -lt 10 -or $instance.Version.Major -gt 12) {
        Write-Warning "OQS is only supported between SQL Server 2008 (v10.X.X) to SQL Server 2014 (v12.X.X). Your instance version is $($instance.Version). Installation cancelled."
        return
    }
    Write-Verbose "SQL Server Version Check passed - Version is $($instance.Version)"
    
    Write-Verbose "Checking if Database $Database exists on $SqlInstance"
    # Verify if database exist in the instance
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking if $database exists")) {
        if (-not ($instance.Databases | Where-Object Name -eq $Database)) {
            Write-Warning "Database [$Database] does not exists on instance $SqlInstance. Installation cancelled."
            return
        }
    }
    Write-Verbose "Database $Database exists on $SqlInstance"

    # If we are installing Service Broker for scheduling, we need to do housekeeping for the certificate
    if ($InstallationType -eq "Service Broker") {
        Write-Verbose "Checking Certificate Backup Path $CertificateBackupPath exists"
        #Does the path specified even exist and is it accessible?
        if (-not (Test-Path $CertificateBackupPath -PathType Container)) {
            Write-Warning "The path specified for backing up the service broker certificate ($CertificateBackupPath) doesn't exist or is inaccesible. Installation cancelled."
            return
        }
        Write-Verbose "Certificate Backup Path $CertificateBackupPath exists"

        Write-Verbose "Checking if a oqs Certificate exists at $CertificateBackupPath already"
        #Check if the certificate backup location already has the certificate in it
        if (Test-Path $CertificateBackupFullPath -PathType Leaf) {
            Write-Warning "An OpenQueryStore certificate already exists at the backup location: $CertificateBackupPath. Please choose another path, rename it or remove the file at that location. Installation cancelled."
            return
        }
        Write-Verbose "Certificate existence check completed"
    }

    # SQL Agent mode requires SQL Agent to be present. Express Edition doesn't have that, so we have to stop installation if that is the case.
    Write-Verbose "Checking for Express edition and SQL Agent"
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking edition")) {
        if ($instance.EngineEdition -eq 'Express' -and $SchedulerType -eq 'SQL Agent') {
            Write-Warning "$SqlInstance is an Express Edition instance. OQS installations using $SchedulerType CANNOT be installed on Express Edition (no SQL Agent available). Installation cancelled."
            return
        }
    }
    Write-Verbose "Check for Express edition and SQL Agent passed"

    Write-Verbose "Checking for oqs schema in $database on $SqlInstance"
    # If 'oqs' schema already exists, we assume that OQS is already installed
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking for OQS Schema")) {
        if ($instance.ConnectionContext.ExecuteScalar($qOQSExists)) {
            Write-Warning -Message "OpenQueryStore appears to already be installed on database [$database] on instance '$SqlInstance' (oqs schema already exists). If you want to reinstall please run the Unistall.sql and then re-run this installer. Installation cancelled."
            return
        }
    }
    Write-Verbose "oqs schema does not exist"
    
    # Load the installer files
    try {
        Write-Verbose "Loading install routine from $path"
        if ($pscmdlet.ShouldProcess("$path\setup\install_open_query_store_base.sql", "Loading OQS base SQL Query from")) {
            $InstallOQSBase = Get-Content -Path "$path\setup\install_open_query_store_base.sql" -Raw
        }
        if ($pscmdlet.ShouldProcess("$path\setup\install_gather_statistics.sql", "Loading OQS Gather stats SQL Query from")) {
            $InstallOQSGatherStatistics = Get-Content -Path "$path\setup\install_gather_statistics.sql" -Raw
        }
        if ($pscmdlet.ShouldProcess("$path\setup\install_service_broker.sql", "Loading Service Broker SQL Query from")) {
            $InstallServiceBroker = Get-Content -Path "$path\setup\install_service_broker.sql" -Raw
        }
        if ($pscmdlet.ShouldProcess("$path\setup\install_service_broker_certificate.sql", "Loading Service Broker certificate SQL Query from")) {
            $InstallServiceBrokerCertificate = Get-Content -Path "$path\setup\install_service_broker_certificate.sql" -Raw
        }
        if ($pscmdlet.ShouldProcess("$path\setup\install_sql_agent_job.sql", "Loading Agent Job SQL Query from")) {
            $InstallSQLAgentJob = Get-Content -Path "$path\setup\install_sql_agent_job.sql" -Raw
        }
     
        if ($InstallOQSBase -eq "" -or $InstallOQSGatherStatistics -eq "" -or $InstallServiceBroker -eq "" -or $InstallServiceBrokerCertificate -eq "" -or $InstallSQLAgentJob -eq "") {
            Write-Warning "OpenQueryStore install files could not be properly loaded from $path. Please check files and permissions and retry the install. Installation cancelled."
            return
        }

        # Replace placeholders
        if ($pscmdlet.ShouldProcess("Base Query", "Replacing Database Name with $database")) {
            $InstallOQSBase = $InstallOQSBase -replace "{DatabaseWhereOQSIsRunning}", "$Database"
        }
        if ($pscmdlet.ShouldProcess("Base Query", "Replacing OQS Mode with $OQSMode")) {
            $InstallOQSBase = $InstallOQSBase -replace "{OQSMode}", "$OQSMode"
        }
        if ($pscmdlet.ShouldProcess("Gather Statistics Query", "Replacing Database Name with $database")) {
            $InstallOQSGatherStatistics = $InstallOQSGatherStatistics -replace "{DatabaseWhereOQSIsRunning}", "$Database"
        }
        if ($pscmdlet.ShouldProcess("Service Broker Query", "Replacing Database Name with $database")) {
            $InstallServiceBroker = $InstallServiceBroker -replace "{DatabaseWhereOQSIsRunning}", "$Database"
        }
        if ($pscmdlet.ShouldProcess("Service Broker Certificate Query", "Replacing Database Name with $database")) {
            $InstallServiceBrokerCertificate = $InstallServiceBrokerCertificate -replace "{DatabaseWhereOQSIsRunning}", "$Database"
        }
        if ($pscmdlet.ShouldProcess("Service Broker Certificate Query", "Replacing File Path")) {
            $InstallServiceBrokerCertificate = $InstallServiceBrokerCertificate -replace "{Enter A File Location accessible by the SQL Server Service Account}", "$CertificateBackupFullPath"
        }
        if ($pscmdlet.ShouldProcess("Agent Job Query", "Replacing Database Name with $database")) {
            $InstallSQLAgentJob = $InstallSQLAgentJob -replace "{DatabaseWhereOQSIsRunning}", "$Database"
        }

        Write-Verbose "OQS install routine successfully loaded from $path. Install can continue."
    }
    catch {
        throw $_.Exception.Message
    }


    # Ready to install!
    Write-Verbose "Installing OQS ($OQSMode & $SchedulerType) on $SqlInstance in $database"
     
    # Base object creation
    if ($pscmdlet.ShouldProcess("$SqlInstance - $Database", "Installing Base Query")) {
        try {
            Write-Verbose "Installing OQS base objects"
            $null = $instance.ConnectionContext.ExecuteNonQuery($InstallOQSBase)
            Write-Verbose "Base Query installed in $database on $SqlInstance"
        }
        catch {
            throw $_.Exception.Message
        }
    }
    # Gather statistics stored procedure creation
    if ($pscmdlet.ShouldProcess("$SqlInstance - $Database", "Installing Gather Statistics Query")) {
        try {
            Write-Verbose "Installing OQS gather_statistics stored procedure"
            $null = $instance.ConnectionContext.ExecuteNonQuery($InstallOQSGatherStatistics)
            Write-Verbose "OQS Gather statistics query run on $database in $SqlInstance"
        }
        catch {
            throw $_.Exception.Message
        }
    }
    switch ($SchedulerType) {
        "Service Broker" {
            if ($pscmdlet.ShouldProcess("$SqlInstance - $Database", "Installing Service Broker Query")) {
                try {
                    Write-Verbose "Installing OQS Service Broker objects"
                    $null = $instance.ConnectionContext.ExecuteNonQuery($InstallServiceBroker)
                    Write-Verbose "Service Borker Query run on $Database in $SqlInstance"
                }
                catch {
                    throw $_.Exception.Message
                }
            }
            #We only need to run this script if we don't have any certificate already created (the same certificate can support multiple databases)
            if (-not ($instance.Databases["master"].Certificates | Where-Object Name -eq 'open_query_store')) {
                Write-Verbose "Installing OQS Service Broker certificate"
                if ($pscmdlet.ShouldProcess("$SqlInstance - $Database", "Installing Service Broker Certificate")) {
                    try {
                        $null = $instance.ConnectionContext.ExecuteNonQuery($InstallServiceBrokerCertificate)
                    }
                    catch {
                        throw $_.Exception.Message
                        Write-Warning "Failed to install OQS Service Broker. Please run Uninstall.ps1 to remove partially installed OQS objects."
                    }
                }
            }
            Write-Output "OQS Service Broker installation completed successfully. Collection will start after an instance restart or by running 'EXECUTE [master].[dbo].[open_query_store_startup]'." -ForegroundColor "Yellow"
        }
    
        "SQL Agent" {
            if ($pscmdlet.ShouldProcess("$SqlInstance - $Database", "Installing Agent Job Query")) {
                try {
                    Write-Verbose "Installing OQS SQL Agent scheduling"
                    $null = $instance.ConnectionContext.ExecuteNonQuery($InstallSQLAgentJob)
                    Write-Verbose "OQS Agent Job query run on $database in $SqlInstance"
                }
                catch {
                    throw $_.Exception.Message
                }
            }
            Write-Output "OQS SQL Agent installation completed successfully. A SQL Agent job has been created WITHOUT a schedule. Please create a schedule to begin data collection."
        }
    }
}
END {
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Disconnect from ")) {
        $instance.ConnectionContext.Disconnect()
        Write-Verbose "Disconnecting from $SqlInstance"
    }
        
    if ($OQSMode -eq "centralized") {
        Write-Output "Centralized mode requires databases to be registered for OQS to monitor them. Please add the database names into the table oqs.monitored_databases." 
    }
        
    Write-Output "To avoid data collection causing resource issues, OQS data capture is deactivated. "
    Write-Output "Please update the value in column 'collection_active' in table oqs.collection_metadata as follows: UPDATE [oqs].[collection_metadata] SET [collection_active] = 1" 
        
    if ($SchedulerType -eq "Service Broker") {
        Write-Output "Please remove the file $CertificateBackupFullPath as it is no longer needed and will prevent a fresh install of OQS at a later time." 
    }
            
    Write-Output "Open Query Store installation successfully completed."
}