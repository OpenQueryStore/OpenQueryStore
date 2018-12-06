<#
.SYNOPSIS
Install OpenQueryStore on specified instance/database.

.DESCRIPTION
Verify if OQS is not already installed on destination database and if not will run the scripts to install it.

.PARAMETER SqlInstance
The SQL Server that you're connecting to.

.PARAMETER SqlCredential
Credential object used to connect to the SQL Server Instance as a different user. This can be a Windows or SQL Server account. Windows users are determined by the existence of a backslash, so if you are intending to use an alternative Windows connection instead of a SQL login, ensure it contains a backslash.

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

.PARAMETER CreateDatabase
Create a central OQS database if it isn't already there - Will default to "no" if not specified 

.PARAMETER AutoConfigure
Automatically sets up data collection, escecially useful in classic mode to quickly start OQS data collection. Defaults to "No".

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

.EXAMPLE
$cred = Get-Credential
.\Install.ps1 -SqlInstance "SQL2012" -Database "db3" -OQSMode "Centralized" -SchedulerType "SQL Agent" -SqlCredential $cred

Will install the centralized version on instance SQL2012 database named db3 and use the SQL Agent for scheduling using SQL Authentication


#>
[CmdletBinding(SupportsShouldProcess = $True)]
param (
    [parameter(Mandatory = $true)]
    [string]$SqlInstance,
	[Alias("SqlCredential")]
    [PSCredential]$Credential,
    [parameter(Mandatory = $true)]
    [string]$Database,
    [parameter(Mandatory = $true)]
    [ValidateSet("Classic", "Centralized")]
    [string]$OQSMode = "Classic",
    [parameter(Mandatory = $true)]
    [ValidateSet("Service Broker", "SQL Agent")]
    [string]$SchedulerType = "Service Broker",
    [string]$CertificateBackupPath = $ENV:TEMP,
    [string]$JobOwner = "sa",
    [ValidateSet("Yes", "No")]
    [string]$CreateDatabase = "No",
    [ValidateSet("Yes", "No")]
    [string]$AutoConfigure = "No"
)
BEGIN {
    $OQSUninstalled = $false
    $path = Get-Location
    $qOQSExists = "SELECT TOP 1 1 FROM [$Database].[sys].[schemas] WHERE [name] = 'oqs'"
    $qOQSStartupProcExists = "SELECT 1 FROM MASTER.INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'open_query_store_startup'"
    $qOQSNonDBInstalled = $false
    $qOQSCreate = "CREATE DATABASE [$Database]"
    $qOQSAutoConfigCollection = "UPDATE [$Database].[oqs].[collection_metadata] SET [collection_active] = 1"
    $qOQSAutoConfigDatabaseClassic = "INSERT INTO [$Database].[oqs].[monitored_databases] ( [database_name] ) VALUES ( '$Database' );"
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
                # Perform the uninstall for db-specific items
                $null = $instance.ConnectionContext.ExecuteNonQuery($UninstallOQSBase)

                # If 'oqs' startup procedure already exists, we assume that OQS is already installed and the startup procedure and other non-db items should not be removed
                if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking for OQS Startup Procedure")) {
                    if ($qOQSNonDBInstalled -eq $TRUE) {
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
        if ($pscmdlet.ShouldProcess("$SqlInstance", "Disconnect from ")) {
            $instance.ConnectionContext.Disconnect()
            Write-Verbose "Disconnecting from $SqlInstance"
        }
        Write-Output "Open Query Store has been uninstalled due to errors in installation - Please review"
        Break
    }
    function Invoke-Catch {
        Param(
            [parameter(Mandatory, ValueFromPipeline)]
            [string]$Message,
            [switch]$Uninstall
        )
        $Script:OQSError = $_.Exception
        if ($Uninstall) {
            Write-Warning "There was an error at $Message - Running Uninstall then quitting - Error details are in `$OQSError"
            Write-Verbose "`$OQSError: $OQSError"
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
				Invoke-Catch -Message "Failed to connect to $SqlInstance`: $message "
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
    
    Write-Verbose "Checking if Database $Database exists on $SqlInstance"
    # Verify if database exist in the instance
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking if $database exists")) {
        if (-not ($instance.Databases | Where-Object Name -eq $Database) -and ($CreateDatabase -eq "No")) {
            Invoke-Catch -Message "Database [$Database] does not exists on instance $SqlInstance and the CreateDatabase parameter was set to 'No'."
        }
    }
    Write-Verbose "Database $Database exists on $SqlInstance"

    Write-Verbose "Checking the Compatibility Level of Database $Database on $SqlInstance"
    # We only support between SQL Server 2008 (version100) and SQL Server 2014 (version120)
    $CompLevel=$instance.databases|where-Object Name -eq $Database|select CompatibilityLevel
    if ($CompLevel.CompatibilityLevel -replace "Version" -lt 100 -or $CompLevel.CompatibilityLevel -replace "Version" -gt 120 ) {
       Invoke-Catch -Message "OQS is only supported between SQL Server 2008 (version100) to SQL Server 2014 (version120). Your database compatibility level is $($CompLevel.CompatibilityLevel). Installation cancelled."
    }
    Write-Verbose "Database compatibility Check passed - Compatibility is $($CompLevel.CompatibilityLevel)"

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
    
    Write-Verbose "Checking for oqs startup procedure in database MASTER on $SqlInstance"
    # If 'oqs' startup procedure already exists, we assume that OQS is already installed
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking for OQS Startup Procedure")) {
        if ($instance.ConnectionContext.ExecuteScalar($qOQSStartupProcExists)) {
            # Parameter set to TRUE, so we know which objects must be removed, after installation failure
            $qOQSNonDBInstalled = $TRUE
        }
    }
    Write-Verbose "Check for oqs startup procedure passed"

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
    
    # Create OQS database
    if ($pscmdlet.ShouldProcess("$SqlInstance - $Database", "Creating database if not already available.")) {
        try {
            
            Write-Verbose "Creating OQS database [$Database]"
            if (-not ($instance.Databases | Where-Object Name -eq $Database) -and ($CreateDatabase -eq "Yes")) {
                $null = $instance.ConnectionContext.ExecuteNonQuery($qOQSCreate)
            }
            Write-Verbose "OQS database [$Database] created on $SqlInstance"
        }
        catch {
            Invoke-Catch -Message "Failed to create OQS database [$Database]"
        }
    }

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
            Write-Output "OQS Service Broker installation completed successfully."
            Write-Output "Collection will start after an instance restart or by running 'EXECUTE [master].[dbo].[open_query_store_startup]'."
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

    # Automatic configuration to get OQS up and running *immediately*
    if ($AutoConfigure -eq "Yes") {
        Write-Verbose "Autoconfiguring OQS data collection"
        if ($pscmdlet.ShouldProcess("$SqlInstance - $Database", "Autoconfiguring OQS data collection")) {
            try {
                $null = $instance.ConnectionContext.ExecuteNonQuery($qOQSAutoConfigCollection)
            }
            catch {
                Invoke-Catch -Message "Failed to automagically configure OQS data collection, please configure manually"
            }
        }
    }

    # Automatic configuration for classic mode & SQL Agent scheduler type = register the OQS database immediately
    # Service Broker actually constantly re-registers the database when running in classic mode, making this step less important
    if (($AutoConfigure -eq "Yes") -and ($SchedulerType -eq "SQL Agent") -and ($OQSMode -eq "Classic") ) {
        Write-Verbose "Autoconfiguring OQS data collection"
        if ($pscmdlet.ShouldProcess("$SqlInstance - $Database", "Autoconfiguring OQS data collection: Classic Mode & SQL Agent Scheduler Type")) {
            try {
                $null = $instance.ConnectionContext.ExecuteNonQuery($qOQSAutoConfigDatabaseClassic)
            }
            catch {
                Invoke-Catch -Message "Failed to automagically register [$Database] for $OQSMode and $SchedulerType, please configure manually"
            }
        }
    }
}
END {
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Disconnect from ")) {
        $instance.ConnectionContext.Disconnect()
        Write-Verbose "Disconnecting from $SqlInstance"
    }

    if ($OQSUninstalled -eq $true) {Break}
        
    if ($OQSMode -eq "centralized") {
        Write-Output "Centralized mode requires databases to be registered for OQS to monitor them. Please add the database names into the table oqs.monitored_databases." 
    }

    if ($AutoConfigure -eq "No") {
        Write-Output ""
        Write-Output ""
        Write-Output "USER ACTION REQUIRED:"
        Write-Output ""
        Write-Output "To avoid data collection causing resource issues, OQS data capture is deactivated."
        Write-Output "Please update the value in column 'collection_active' in table oqs.collection_metadata."
        Write-Output "UPDATE [oqs].[collection_metadata] SET [collection_active] = 1" 
        Write-Output ""
    }
    if ($SchedulerType -eq "Service Broker") {
        if ($pscmdlet.ShouldProcess("$CertificateBackupFullPath", "Removing OQS Service Broker Certificate file ")) {
            if (Test-Path $CertificateBackupFullPath -PathType Leaf) {
                try {
                    Write-Verbose "Attempting to remove OQS Service Broker certificate"
                    Remove-Item $CertificateBackupFullPath -Force
                    Write-Verbose "Successfully removed OQS Service Broker certificate."
                    return    
                }
                catch {
                    Invoke-Catch -Message "Failed to remove OQS Service Broker certificate, please check and remove the file manually ($CertificateBackupFullPath) "
                }            
            }
        }
    }
    
    Write-Output "Open Query Store installation successfully completed."
}
