function Install-OpenQueryStore {
    [CmdletBinding(SupportsShouldProcess = $True)]
    param (
        [string]$CertificateBackupPath = $ENV:TEMP
    )
    Begin {

    }
    
    Process {
        
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
        function Connect-SMO {
            [CmdletBinding(SupportsShouldProcess = $True)]
            param ()
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
        }

        function Test-SQLVersion {
            # We only support between SQL Server 2008 (v10.X.X) and SQL Server 2014 (v12.X.X)
            if ($instance.Version.Major -lt 10 -or $instance.Version.Major -gt 12) {
                Invoke-Catch -Message "OQS is only supported between SQL Server 2008 (v10.X.X) to SQL Server 2014 (v12.X.X). Your instance version is $($instance.Version). Installation cancelled."
            }
        }
        function Test-OQSDatabase {
            [CmdletBinding(SupportsShouldProcess = $True)]
            param ()
            # Verify if database exist in the instance
            if ($pscmdlet.ShouldProcess("$SqlInstance", "Checking if $database exists")) {
                if (-not ($instance.Databases | Where-Object Name -eq $Database)) {
                    Invoke-Catch -Message "Database [$Database] does not exists on instance $SqlInstance."
                }
            }
        }
        $qOQSExists = "SELECT TOP 1 1 FROM [$Database].[sys].[schemas] WHERE [name] = 'oqs'"
        $CertificateBackupFullPath = Join-Path -Path $CertificateBackupPath  -ChildPath "open_query_store.cer"
        if ($pscmdlet.ShouldProcess("SQL Server SMO", "Loading Assemblies")) {
            try {
                Install-SMO
                Write-Verbose "SQL Server Assembly loaded"
            }
            catch {
                Write-Warning "Failed to load SQL Server SMO Assemblies - Quitting"
                break
            }
        }
        # Connect to instance
        Connect-SMO

        Write-Verbose "Checking SQL Server version"
        
        Test-SQLVersion
        Write-Verbose "SQL Server Version Check passed - Version is $($instance.Version)"
         
        Test-OQSDatabase
        Write-Verbose "Database $Database exists on $SqlInstance"

        Write-Verbose "Checking if Database $Database exists on $SqlInstance"
    }
    End {}
}