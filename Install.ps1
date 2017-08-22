<#
.SYNOPSIS
Install OpenQueryStore on specified instance/database

.DESCRIPTION
Verify if OQS is not already installed on destination database and if not will run the scripts to install it.

.PARAMETER SqlInstance
The SQL Server that you're connecting to.

.PARAMETER Database
Specifies the Database where OQS objects will be created

.PARAMETER InstallationType
Specifies the type of installation to be done. Classic (by default) or Centralised (will be available soon)

.PARAMETER CertificateBackupPath
Specifies the path where certificate backup will be saved. By default "C:\temp"

.NOTES
Author: Cláudio Silva (@ClaudioESSilva)

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
.\Install.ps1 -SqlInstance SQL2012 -Database ScooterStore -InstallationType Classic

Will install the Classic version on instance SQL2012 database named ScooterStore.

.EXAMPLE
.\Install.ps1 -SqlInstance "SQL2012" -Database "db3" -InstallationType Classic -CertificateBackupPath "C:\temp"

Will install the Classic version on instance SQL2012 database named ScooterStore and use the folder "C:\temp" to save certificate backup.

#>
[CmdletBinding()]
param (
    [parameter(Mandatory = $true)]
    [string]$SqlInstance,
    [parameter(Mandatory = $true)]
    [string]$Database,
    [ValidateSet("Classic", "Centralised")]
    [string]$InstallationType = "Classic",
    [string]$CertificateBackupPath = "C:\temp"
)

$path = "$HOME\Documents\OpenQueryStore"
$qOQSExists = "SELECT TOP 1 1 FROM [$Database].[sys].[schemas] WHERE [name] = 'oqs'"
$CertificateBackupFullPath = $CertificateBackupPath + "OpenQueryStore.CER"

try {
    $null = [Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo")

    #Connect to instance
    $instance = New-Object Microsoft.SqlServer.Management.Smo.Server $SqlInstance

    #As is today, we only support between SQL Server 2008 (v10.X.X) and SQL Server 2014 (v12.X.X)
    if ($instance.Version.Major -lt 10 -or $instance.Version.Major -gt 12) {
        Write-Warning "We only support instances from SQL Server 2008 (v9.X.X) to SQL Server 2014 (v12.X.X). Your instance version is $($instance.Version). Quitting"
        return
    }

    #Verify if database exist in the instance
    if (-not ($instance.Databases | Where-Object Name -eq $Database)) {
        Write-Error "Database $Database does not exists on instance $SqlInstance"
        return
    }

    #Check if the certificate backup location already has the certificate in it
    $CertExists = Test-Path $CertificateBackupFullPath
    if ($CertExists -eq $true) {
        Write-Warning "An OpenQueryStore certificate already exists at the backup location: $CertificateBackupPath. Please choose another path or remove the file at that location. Quitting."
        return
    }


    #Verify if oqs schema exists
    $AlreadyExists = $instance.ConnectionContext.ExecuteScalar($qOQSExists)

    #if 'qos' schema already exists - We assume that OQS is already installed
    if ($AlreadyExists -eq $true) {
        Write-Warning "OpenQueryStore is already installed on database $database. If you want to reinstall please run the Unistall.sql and then re-run this installer. Quitting."
        return
    }
    else {
        Write-Output "We will install OQS on database $database"
    }

    switch ($InstallationType) {
        "Classic" {
            $qInstallOQSClassic = Get-Content -Path "$path\InstallOQSClassic.sql" -Raw
            $qInstallOQSServiceBrokerCertificate = Get-Content -Path "$path\InstallOQSServiceBrokerCertificate.sql" -Raw
            $qInstallOQSClassicStartupProcedure = Get-Content -Path "$path\InstallOQSClassicStartupProcedure.sql" -Raw
                                        
            #Replace some values
            $qInstallOQSClassic = $qInstallOQSClassic -replace "{DatabaseWhereOQSIsRunning}", "[$Database]"

            $qInstallOQSServiceBrokerCertificate = $qInstallOQSServiceBrokerCertificate -replace "{DatabaseWhereOQSIsRunning}", "[$Database]"
            $qInstallOQSServiceBrokerCertificate = $qInstallOQSServiceBrokerCertificate -replace "Enter A File Location accessible by the SQL Server Service Account", "$CertificateBackupFullPath"

            $qInstallOQSClassicStartupProcedure = $qInstallOQSClassicStartupProcedure -replace "{DatabaseWhereOQSIsRunning}", "[$Database]"

            Write-Output "Running InstallOQSClassic.sql"
            $null = $instance.ConnectionContext.ExecuteNonQuery($qInstallOQSClassic)

            #We only need to run this script if we don't have any certificate already created (the same certificate can support multiple databases)
            if ($instance.Databases["master"].Certificates | Where-Object Name -eq 'OpenQueryStore') {
                Write-Verbose "OpenQueryStore certificate already exists"
            }
            else {
                Write-Output "Running InstallOQSServiceBrokerCertificate.sql"
                $null = $instance.ConnectionContext.ExecuteNonQuery($qInstallOQSServiceBrokerCertificate)
            }

            Write-Output "Running InstallOQSClassicStartupProcedure.sql"
            $null = $instance.ConnectionContext.ExecuteNonQuery($qInstallOQSClassicStartupProcedure)
        }

        "Centralised" {
            Write-Output "This installation type is not available yet."
        }
    }

    #Copy rdl to $mydocuments\SQL Server Management Studio\Custom Reports
    $customReportsPath = [environment]::GetFolderPath([environment+SpecialFolder]::MyDocuments) + "\SQL Server Management Studio\Custom Reports"
    if ((Test-Path $customReportsPath) -eq $false ) { 
        $null = New-Item -Path $customReportsPath -Force -ItemType Directory
    }

    Write-Output "Copying custom report (OpenQueryStoreDashboard.rdl) to $customReportsPath"

    $null = Copy-Item -Path "$path\OpenQueryStoreDashboard.rdl" -Destination $customReportsPath -ErrorAction SilentlyContinue

    if (Test-Path -Path "$path\OpenQueryStoreDashboard.rdl") {
        Write-Output "OpenQueryStoreDashboard.rdl copied with success! You need to go to SSMS and import the report by using - Right click on database -> Reports -> Custom Reports"
    }
    else {
        Write-Output "Unable to copy report to path $customReportsPath"
    }

    $null = Copy-Item -Path "$path\OpenQueryStoreWaitStatsDashboard.rdl" -Destination $customReportsPath -ErrorAction SilentlyContinue
    
    if (Test-Path -Path "$path\OpenQueryStoreWaitStatsDashboard.rdl") {
        Write-Output "OpenQueryStoreWaitStatsDashboard.rdl copied with success! You need to go to SSMS and import the report by using - Right click on database -> Reports -> Custom Reports"
    }
    else {
        Write-Output "Unable to copy report to path $customReportsPath"
    }
    
    #Ask if user want to start collecting data
    Write-Output "Installation complete!"

    $title = "Start collecting data"
    $message = "Do you want to start collecting data for database '$Database'? (Y/N)"
    $yes = New-Object System.Management.Automation.Host.ChoiceDescription "&Yes", "Start collecting"
    $no = New-Object System.Management.Automation.Host.ChoiceDescription "&No", "continue"
    $options = [System.Management.Automation.Host.ChoiceDescription[]]($yes, $no)
    $result = $host.ui.PromptForChoice($title, $message, $options, 0)
    #no
    if ($result -eq 1) {
        Write-Warning "You have decided not to start collecting data. To start collecting data you need to run the following instruction manually 'EXECUTE [master].[dbo].[OpenQueryStoreStartup]'."
    }
    else {
        $null = $instance.ConnectionContext.ExecuteNonQuery("EXECUTE [master].[dbo].[OpenQueryStoreStartup]")

        Write-Output "OpenQueryStore started collecting data"
    }
}
catch {
    Write-Error $_.Exception.Message
}

$instance.ConnectionContext.Disconnect()
