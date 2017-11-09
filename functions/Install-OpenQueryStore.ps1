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
        ## load dbatools as it will make things easier
        if ((Get-Module dbatools -ListAvailable).Count -eq 0) {
            Write-Warning "OpenQueryStore requires dbatools module (https://dbatools.io) - Please install using Install-module dbatools"
            break
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
            Break
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

    }
    End {}
}
