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

.PARAMETER JobOwner
SQL Login for Agent Job Job Owner - Will default to sa if not specified 

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
    [string]$CertificateBackupPath = $ENV:TEMP,
    [string]$JobOwner = 'sa'
)
BEGIN {
    $OQSUninstalled = $false
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
    function Uninstall-OQS {
        [CmdletBinding(SupportsShouldProcess = $True)]
        Param()
        $OQSUninstalled = $True
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
        }
        catch {
            throw $_
        }

        try {
            if ($pscmdlet.ShouldProcess("$SqlInstance - $Database", "UnInstalling Base Query")) {
                # Perform the uninstall
                $null = $instance.ConnectionContext.ExecuteNonQuery($UninstallOQSBase)
            }
            Write-Verbose "Open Query Store uninstallation complete in database [$database] on instance '$SqlInstance'" 
        }
        catch {
            throw $_
        }
        if ($pscmdlet.ShouldProcess("$SqlInstance", "Disconnect from ")) {
            $instance.ConnectionContext.Disconnect()
            Write-Verbose "Disconnecting from $SqlInstance"
        }
        Write-Output "Open Query Store has been uninstalled due to errors in installation - Please review"
        Break
    }
    function Invoke-Catch{
        Param(
            [parameter(Mandatory, ValueFromPipeline)]
            [string]$Message,
            [switch]$Uninstall
        )
        $Script:OQSError = $_.Exception
        if($Uninstall){
            Write-Warning "There was an error at $Message - Running Uninstall then quitting - Error details are in `$OQSError"
            Uninstall-OQS
        }
        else {
            Write-Warning "There was an error at $Message - Installation cancelled - Error details are in `$OQSError"
        }
        Break
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
                Invoke-Catch -Message "Failed to connect to $SqlInstance"
            }
        }
        catch {
            Invoke-Catch -Message "Failed to connect to $SqlInstance"
        }
    }

    Write-Verbose "Checking SQL Server version"
    # We only support between SQL Server 2008 (v10.X.X) and SQL Server 2014 (v12.X.X)
    if ($instance.Version.Major -lt 10 -or $instance.Version.Major -gt 12) {
        Invoke-Catch -Message "OQS is only supported between SQL Server 2008 (v10.X.X) to SQL Server 2014 (v12.X.X). Your instance version is $($instance.Version). Installation cancelled."
    }
    Write-Verbose "SQL Server Version Check passed - Version is $($instance.Version)"
    
    # We know the path we are going to use for the export of the certificate. Lets check it now.
    if ((test-path $CertificateBackupFullPath) -eq $true)
    {
	Invoke-Catch -Message "Old certificate file exists on the path. Please delete is and start the script again."
    }
    
    Write-Verbose "Checking if Database $Database exists on $SqlInstance"
    # Verify if database exist in the instance
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking if $database exists")) {
        if (-not ($instance.Databases | Where-Object Name -eq $Database)) {
            Invoke-Catch -Message "Database [$Database] does not exists on instance $SqlInstance."
        }
    }
    Write-Verbose "Database $Database exists on $SqlInstance"

    # If we are installing Service Broker for scheduling, we need to do housekeeping for the certificate
    if ($InstallationType -eq "Service Broker") {
        Write-Verbose "Checking Certificate Backup Path $CertificateBackupPath exists"
        #Does the path specified even exist and is it accessible?
        if (-not (Test-Path $CertificateBackupPath -PathType Container)) {
            Invoke-Catch -Message  "The path specified for backing up the service broker certificate ($CertificateBackupPath) doesn't exist or is inaccesible."
        }
        Write-Verbose "Certificate Backup Path $CertificateBackupPath exists"

        Write-Verbose "Checking if a oqs Certificate exists at $CertificateBackupPath already"
        #Check if the certificate backup location already has the certificate in it
        if (Test-Path $CertificateBackupFullPath -PathType Leaf) {
            Invoke-Catch -Message  "An OpenQueryStore certificate already exists at the backup location: $CertificateBackupPath. Please choose another path, rename it or remove the file at that location."
        }
        Write-Verbose "Certificate existence check completed"
    }

    # SQL Agent mode requires SQL Agent to be present. Express Edition doesn't have that, so we have to stop installation if that is the case.
    Write-Verbose "Checking for Express edition and SQL Agent"
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking edition")) {
        if ($instance.EngineEdition -eq 'Express' -and $SchedulerType -eq 'SQL Agent') {
            Invoke-Catch -Message  "$SqlInstance is an Express Edition instance. OQS installations using $SchedulerType CANNOT be installed on Express Edition (no SQL Agent available)."
        }
    }
    Write-Verbose "Check for Express edition and SQL Agent passed"

        # Check that we have the JobOwner login
        Write-Verbose "Checking for SQL Agent Job Owner account $JobOwner"
        if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking logins for $JobOwner")) {
            if ($instance.logins.Name.Contains($JobOwner) -eq $false) {
                Invoke-Catch -Message  "$SQLInstance does not have a login named $JobOwner - We cannot create the Agent Job - Quitting"
            }
        }
        Write-Verbose "Checking for SQL Agent Job Owner account $JobOwner passed"

    Write-Verbose "Checking for oqs schema in $database on $SqlInstance"
    # If 'oqs' schema already exists, we assume that OQS is already installed
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking for OQS Schema")) {
        if ($instance.ConnectionContext.ExecuteScalar($qOQSExists)) {
            Invoke-Catch -Message "OpenQueryStore appears to already be installed on database [$database] on instance '$SqlInstance' (oqs schema already exists). If you want to reinstall please run the Unistall.sql and then re-run this installer."
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
            Invoke-Catch -Message "OpenQueryStore install files could not be properly loaded from $path. Please check files and permissions and retry the install."
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
        if ($pscmdlet.ShouldProcess("Agent Job Query", "Replacing Job Owner with $JobOwner")) {
            $InstallSQLAgentJob = $InstallSQLAgentJob -replace "{JobOwner}", "$JobOwner"
        }

        Write-Verbose "OQS install routine successfully loaded from $path. Install can continue."
    }
    catch {
        Invoke-Catch -Message "Failed to load the Install scripts"
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
            Invoke-Catch -Message "Failed to install base SQL query" -Uninstall
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
            Invoke-Catch -Message "Failed to install gather_statistics SQL query" -Uninstall
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
                    Invoke-Catch -Message "Failed to install service broker SQL query" -Uninstall
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
                        Invoke-Catch -Message "Failed to install Service Broker Certificate SQL query" -Uninstall
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
                    Invoke-Catch -Message "Failed to install Agent SQL query" -Uninstall
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
    if($OQSUninstalled = $true){Break}
        
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
