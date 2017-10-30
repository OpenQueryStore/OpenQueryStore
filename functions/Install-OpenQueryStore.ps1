function Install-OpenQueryStore {
    [CmdletBinding(SupportsShouldProcess = $True)]
    param (
        [parameter(Mandatory = $true)]
        [string]$SqlInstance,
        [parameter(Mandatory = $true)]
        [string]$DatabaseName,
        [parameter(Mandatory = $true)]
        [ValidateSet("Classic", "Centralized")]
        [string]$OQSMode = "Classic",
        [string]$CertificateBackupPath = $ENV:TEMP
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
    }
    End {}
}
