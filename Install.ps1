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
[CmdletBinding()]
param (
    [parameter(Mandatory = $true)]
    [string]$SqlInstance,
    [parameter(Mandatory = $true)]
    [string]$Database,
    [parameter(Mandatory = $true)]
    [ValidateSet("classic", "centralized")]
    [string]$OQSMode = "classic",
    [parameter(Mandatory = $true)]
    [ValidateSet("Service Broker", "SQL Agent")]
    [string]$SchedulerType = "Service Broker",
    [string]$CertificateBackupPath = "C:\temp"
)
BEGIN {
    $path = Get-Location
    $qOQSExists = "SELECT TOP 1 1 FROM [$Database].[sys].[schemas] WHERE [name] = 'oqs'"
    $CertificateBackupFullPath = Join-Path -Path $CertificateBackupPath  -ChildPath "open_query_store.CER"
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

    # We only support between SQL Server 2008 (v10.X.X) and SQL Server 2014 (v12.X.X)
    if ($instance.Version.Major -lt 10 -or $instance.Version.Major -gt 16) {
        Write-Error "We only support instances from SQL Server 2008 (v10.X.X) to SQL Server 2014 (v12.X.X). Your instance version is $($instance.Version). Installation cancelled."
        return
    }

    # Verify if database exist in the instance
    if (-not ($instance.Databases | Where-Object Name -eq $Database)) {
        Write-Error "Database $Database does not exists on instance $SqlInstance. Installation cancelled."
        return
    }

    # If we are installing Service Broker for scheduling, we need to do housekeeping for the certificate
    if ($InstallationType -eq "Service Broker") {

        #Does the path specified even exist and is it accessible?
        if (-not (Test-Path $CertificateBackupPath -PathType Container)) {
            Write-Error "The path specified for backing up the service broker certificate ($CertificateBackupPath) doesn't exist or is inaccesible. Installation cancelled."
            return
        }

        #Check if the certificate backup location already has the certificate in it
        if (Test-Path $CertificateBackupFullPath -PathType Leaf) {
            Write-Error "An OpenQueryStore certificate already exists at the backup location: $CertificateBackupPath. Please choose another path or remove the file at that location. Installation cancelled."
            return
        }
    }

    # If 'qos' schema already exists, we assume that OQS is already installed
    if ($instance.ConnectionContext.ExecuteScalar($qOQSExists)) {
        Write-Warning "OpenQueryStore is already installed on database $database. If you want to reinstall please run the Unistall.sql and then re-run this installer. Installation cancelled."
        return
    }
    
    # Load the installer files
    $InstallOQSBase = Get-Content -Path "$path\install_open_query_store_base.sql" -Raw
    $InstallOQSGatherStatistics = Get-Content -Path "$path\install_gather_statistics.sql" -Raw
    $InstallServiceBroker = Get-Content -Path "$path\install_service_broker.sql" -Raw
    $InstallServiceBrokerCertificate = Get-Content -Path "$path\install_service_broker_certificate.sql" -Raw
    $InstallSQLAgentJob = Get-Content -Path "$path\install_sql_agent_job.sql" -Raw

    if ($InstallOQSBase -eq "" -or $InstallOQSGatherStatistics -eq "" -or $InstallServiceBroker -eq "" -or $InstallServiceBrokerCertificate -eq "" -or $InstallSQLAgentJob -eq "") {
        Write-Warning "OpenQueryStore install files could not be properly loaded from $path. Please check files and permissions and retry the install. Installation cancelled."
        return
    }

    # Replace placeholders
    $InstallOQSBase = $InstallOQSBase -replace "{DatabaseWhereOQSIsRunning}", "$Database"
    $InstallOQSBase = $InstallOQSBase -replace "{OQSMode}", "$OQSMode"
    $InstallOQSGatherStatistics = $InstallOQSGatherStatistics -replace "{DatabaseWhereOQSIsRunning}", "$Database"
    $InstallServiceBroker = $InstallServiceBroker -replace "{DatabaseWhereOQSIsRunning}", "$Database"
    $InstallServiceBrokerCertificate = $InstallServiceBrokerCertificate -replace "{DatabaseWhereOQSIsRunning}", "$Database"
    $InstallServiceBrokerCertificate = $InstallServiceBrokerCertificate -replace "Enter A File Location accessible by the SQL Server Service Account", "$CertificateBackupFullPath"
    $InstallSQLAgentJob = $InstallSQLAgentJob -replace "{DatabaseWhereOQSIsRunning}", "$Database"

    # Ready to install!
    Write-Warning "Installing OQS ($OQSMode & $SchedulerType) on $SqlInstance in $database"
    
    Write-Output "Installing OQS base objects"
    $null = $instance.ConnectionContext.ExecuteNonQuery($InstallOQSBase)
    Write-Output "Installing OQS gather_statistics stored procedure"
    $null = $instance.ConnectionContext.ExecuteNonQuery($InstallOQSGatherStatistics)

    switch ($SchedulerType) {
        "Service Broker" {
            Write-Output "Installing OQS Service Broker objects"
            $null = $instance.ConnectionContext.ExecuteNonQuery($InstallServiceBroker)

            #We only need to run this script if we don't have any certificate already created (the same certificate can support multiple databases)
            if (-not ($instance.Databases["master"].Certificates | Where-Object Name -eq 'open_query_store')) {
                Write-Output "Installing OQS Service Broker certificate"
                $null = $instance.ConnectionContext.ExecuteNonQuery($InstallServiceBrokerCertificate)
            }
            Write-Warning "OQS Service Broker installation completed successfully. Collection will start after an instance restart or by running 'EXECUTE [master].[dbo].[dbo.open_query_store_startup]'."       
        }

        "SQL Agent" {
            Write-Output "Installing OQS SQL Agent scheduling"
            $null = $instance.ConnectionContext.ExecuteNonQuery($InstallSQLAgentJob)
            Write-Warning "OQS SQL Agent installation completed successfully. A SQL Agent job has been created WITHOUT a schedule. Please create a schedule to begin data collection."                   
        }
    }
    if ($OQSMode -eq "centralized") {
        Write-Warning "Centralized mode requires databases to be registered for OQS to monitor them. Please add the database names into the table oqs.monitored_databases."
    }
    
    Write-Warning "To avoid data collection causing resource issues, OQS data capture is deactivated. "
    Write-Warning "Please update the value in column 'collection_active' in table oqs.collection_metadata as follows: UPDATE [oqs].[collection_metadata] SET [collection_active] = 1"
    
    if ($SchedulerType -eq "Service Broker") {
        Write-Warning "Please remove the file $CertificateBackupFullPath as it is no longer needed and will prevent a fresh install of OQS at a later time."
    }
        
    Write-Warning "Open Query Store installation complete."
}
END {
    $instance.ConnectionContext.Disconnect()
}
