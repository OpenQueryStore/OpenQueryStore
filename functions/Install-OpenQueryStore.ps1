function Install-OpenQueryStore {
    [CmdletBinding(SupportsShouldProcess = $True)]
    param (
        [parameter(Mandatory = $true)]
        [string]$SqlInstance,
        [parameter(Mandatory = $true)]
        [string]$DatabaseName,
        [ValidateSet("Classic", "Centralized")]
        [string]$OQSMode = "Classic",    
        [parameter(Mandatory = $true)]
        [ValidateSet("Service Broker", "SQL Agent")]
        [string]$SchedulerType,
        [string]$CertificateBackupPath = $ENV:TEMP,
        [string]$JobOwner = 'sa'
    )
    Begin {
        $qOQSExists = "SELECT TOP 1 1 FROM [$Database].[sys].[schemas] WHERE [name] = 'oqs'"
        $qOQSCreate = "CREATE DATABASE [$Database]"
        ## load dbatools as it will make things easier
        if ((Get-Module dbatools -ListAvailable).Count -eq 0) {
            Write-Warning "OpenQueryStore requires dbatools module (https://dbatools.io) - Please install using Install-module dbatools"
            Break 
        }
        else {
            Import-Module dbatools
        }
        $CertificateBackupFullPath = Join-Path -Path $CertificateBackupPath  -ChildPath "open_query_store.cer"
    }
    
    Process {
        
        # Create a function to go in the Catch Block
        function Invoke-Catch {
            Param(
                [parameter(Mandatory, ValueFromPipeline)]
                [string]$Message,
                [switch]$Uninstall
            )
            $Script:OQSError = $_.Exception
            if ($Uninstall) {
                Write-Warning "There was an error at $Message - Running Uninstall then quitting - Error details are in `$OQSError"
                Uninstall-OQS
            }
            else {
                Write-Warning "There was an error at $Message - Installation cancelled - Error details are in `$OQSError"
            }
            Return
        }

        Function Test-OQSSchema {
            $instance.ConnectionContext.ExecuteScalar($qOQSExists)
        }

        Function New-OQSDatabase {
            $null = $instance.ConnectionContext.ExecuteNonQuery($qOQSCreate)
        }

        Function Install-OQSBase {
            $null = $instance.ConnectionContext.ExecuteNonQuery($InstallOQSBase)
        }
        Function Install-OQSGatherStatistics {
            $null = $instance.ConnectionContext.ExecuteNonQuery($InstallOQSGatherStatistics)
        }

        $Instance = Connect-DbaInstance -SqlInstance $SqlInstance 
    
        # We only support between SQL Server 2008 (v10.X.X) and SQL Server 2014 (v12.X.X)
        if ($instance.Version.Major -lt 10 -or $instance.Version.Major -gt 12) {
            Invoke-Catch -Message "OQS is only supported between SQL Server 2008 (v10.X.X) to SQL Server 2014 (v12.X.X). Your instance version is $($instance.Version). Installation cancelled."
            return
        }
        Write-Verbose "Checking if Database $Database exists on $SqlInstance"
        # Verify if database exist in the instance
        if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking if $database exists")) {
            $Database = Get-DbaDatabase -SqlInstance $SqlInstance -Database $DatabaseName
            if (-not $Database) {
                Invoke-Catch -Message "Database [$Database] does not exists on instance $SqlInstance."
                return
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
        }
        Write-Verbose "Certificate Backup Path $CertificateBackupPath exists"
        
        Write-Verbose "Checking if a oqs Certificate exists at $CertificateBackupPath already"
        #Check if the certificate backup location already has the certificate in it
        if (Test-Path $CertificateBackupFullPath -PathType Leaf) {
            Invoke-Catch -Message  "An OpenQueryStore certificate already exists at the backup location: $CertificateBackupPath. Please choose another path, rename it or remove the file at that location."
        }
        Write-Verbose "Certificate existence check completed"
        # SQL Agent mode requires SQL Agent to be present. Express Edition doesn't have that, so we have to stop installation if that is the case.
        Write-Verbose "Checking for Express edition and SQL Agent"
        if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking edition")) {
            if ($instance.EngineEdition -eq 'Express' -and $SchedulerType -eq 'SQL Agent') {
                Invoke-Catch -Message  "$SqlInstance is an Express Edition instance. OQS installations using $SchedulerType CANNOT be installed on Express Edition (no SQL Agent available)."
                Return
            }
        }
        Write-Verbose "Check for Express edition and SQL Agent passed"

    # Check that we have the JobOwner login
    Write-Verbose "Checking for SQL Agent Job Owner account $JobOwner"
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking logins for $JobOwner")) {
        if ($instance.logins.Name.Contains($JobOwner) -eq $false) {
            Invoke-Catch -Message  "$SQLInstance does not have a login named $JobOwner - We cannot create the Agent Job - Quitting"
            Return
        }
    }
    Write-Verbose "Checking for SQL Agent Job Owner account $JobOwner passed"

    Function Test-OQSSchema {
        $instance.ConnectionContext.ExecuteScalar($qOQSExists)
    }

    Write-Verbose "Checking for oqs schema in $database on $SqlInstance"
    # If 'oqs' schema already exists, we assume that OQS is already installed
    if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking for OQS Schema")) {
        if (Test-OQSSchema) {
            Invoke-Catch -Message "OpenQueryStore appears to already be installed on database [$database] on instance '$SqlInstance' (oqs schema already exists). If you want to reinstall please run the Unistall.sql and then re-run this installer."
            Return
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
            Return
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
        Return
    }


    }
    End {}
}
