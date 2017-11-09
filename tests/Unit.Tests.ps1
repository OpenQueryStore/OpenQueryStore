$script:ModuleName = 'OpenQueryStore'
# Removes all versions of the module from the session before importing
Get-Module $ModuleName | Remove-Module
$ModuleBase = Split-Path -Parent $MyInvocation.MyCommand.Path
# For tests in .\Tests subdirectory
if ((Split-Path $ModuleBase -Leaf) -eq 'Tests') {
    $ModuleBase = Split-Path $ModuleBase -Parent
}
## this variable is for the VSTS tasks and is to be used for refernecing any mock artifacts
$Env:ModuleBase = $ModuleBase
Import-Module $ModuleBase\$ModuleName.psd1 -PassThru -ErrorAction Stop | Out-Null
Describe "Basic function unit tests" -Tags Build , Unit {

}
InModuleScope -ModuleName $ModuleName -ScriptBlock {
    Describe "Testing Install-OpenQueryStore command" -Tags Install {
        It "Command Install-OpenQueryStore exists" {
            Get-Command Install-OpenQueryStore -ErrorAction SilentlyContinue | Should Not BE NullOrEmpty
        }
        Context "Install-OpenQueryStore input" {
            function Invoke-Catch {}
            Mock Invoke-Catch {}
            $results = (Get-Content $PSScriptRoot\json\SQL2012version.json) -join "`n" | ConvertFrom-Json 
            Mock Connect-DbaInstance {$results}
            Mock Get-DbaDatabase -ParameterFilter { $Database -and $Database -eq "WrongDatabase" } {}
            Mock Get-DbaDatabase -ParameterFilter { $Database -and $Database -eq "OQSDatabase" } {'A Database'}
            It "Should call the Invoke-Catch if there is no database" {
                Install-OpenQueryStore -SqlInstance Dummy -Database WrongDatabase -SchedulerType 'Service Broker'
                $assertMockParams = @{
                    'CommandName' = 'Invoke-Catch'
                    'Times'       = 1
                    'Exactly'     = $true
                }
                Assert-MockCalled @assertMockParams 
            }
            It "Should accept a value for OQSMode of classic" {
                {Install-OpenQueryStore -SqlInstance Dummy -DatabaseName OQSDatabase -OQSMode 'Classic'-SchedulerType 'Service Broker'} | Should Not Throw
            }       
            It "Should accept a value for OQS Mode of centralized" {
                {Install-OpenQueryStore -SqlInstance Dummy -DatabaseName OQSDatabase -OQSMode 'Centralized' -SchedulerType 'Service Broker'} | Should Not Throw
            }            
            It "Should accept a value for SchedulerType of Service Broker" {
                {Install-OpenQueryStore -SqlInstance Dummy -DatabaseName OQSDatabase -OQSMode 'Centralized' -SchedulerType 'Service Broker' } | Should Not Throw
            }            
            It "Should accept a value for SchedulerType of SQL Agent" {
                {Install-OpenQueryStore -SqlInstance Dummy -DatabaseName OQSDatabase -OQSMode 'Centralized' -SchedulerType 'SQL Agent' } | Should Not Throw
            }            
        }
        Context "Version Support" {
            function Invoke-Catch {}
            Mock Invoke-Catch {break}
            #We only support between SQL Server 2008 (v10.X.X) and SQL Server 2014
            It "Should Break for SQL 2017" {
                $results = (Get-Content $PSScriptRoot\json\SQL2017version.json) -join "`n" | ConvertFrom-Json 
                Mock Get-DbaInstance {$results}
                
                Install-OpenQueryStore -SqlInstance Dummy -Database Dummy -SchedulerType 'Service Broker'| Should Be 
            }
            It "Should Break for SQL 2016" {
                $results = (Get-Content $PSScriptRoot\json\SQL2016version.json) -join "`n" | ConvertFrom-Json 
                Mock Connect-DbaInstance {$results}
                
                Install-OpenQueryStore -SqlInstance Dummy -Database Dummy -SchedulerType 'Service Broker' | Should Be 
            }
            It "Should Break for SQL 2005" {
                $results = (Get-Content $PSScriptRoot\json\SQL2005version.json) -join "`n" | ConvertFrom-Json 
                Mock Connect-DbaInstance {$results}
                
                Install-OpenQueryStore -SqlInstance Dummy -Database Dummy -SchedulerType 'Service Broker' | Should Be 
            }
            It "Should Break for SQL 2000" {
                $results = (Get-Content $PSScriptRoot\json\SQL2000version.json) -join "`n" | ConvertFrom-Json 
                Mock Connect-DbaInstance {$results}
                
                Install-OpenQueryStore -SqlInstance Dummy -Database Dummy -SchedulerType 'Service Broker'| Should Be 
            }
            It "Should Break for SQL Agent on Express Edition" {
                $results = (Get-Content $PSScriptRoot\json\ExpressEdition.json) -join "`n" | ConvertFrom-Json 
                Mock Connect-DbaInstance {$results} 
                
                Mock Get-DbaDatabase -ParameterFilter { $Database -and $Database -eq "OQSDatabase" } {'A Database'}
                 Mock Invoke-Catch {'Agent';break}        
                Install-OpenQueryStore -SqlInstance Dummy -Database OQSDatabase -SchedulerType 'SQL Agent'| Should Be 'Agent'
            }
            It "Should Break if JobOwner Does not Exist on the Instance"{
                $results = (Get-Content $PSScriptRoot\json\SQL2012versionLogin.json) -join "`n" | ConvertFrom-Json 
                Mock Connect-DbaInstance {$results}                
                Mock Get-DbaDatabase -ParameterFilter { $Database -and $Database -eq "OQSDatabase" } {'A Database'}
                Mock Invoke-Catch {'Owner';break}        
                Install-OpenQueryStore -SqlInstance Dummy -Database OQSDatabase -SchedulerType 'SQL Agent'| Should Be 'Owner'     
            }
            It "Should Break if OQS Schema exists" {

                $results = (Get-Content $PSScriptRoot\json\SQL2012version.json) -join "`n" | ConvertFrom-Json 
                Mock Connect-DbaInstance {$results}                
                Mock Get-DbaDatabase -ParameterFilter { $Database -and $Database -eq "OQSDatabase" } {'A Database'}
                Mock Invoke-Catch {'Schema';break}        
                Mock Check-OQSSchema {'Schema Exists'}
                Install-OpenQueryStore -SqlInstance Dummy -Database OQSDatabase -SchedulerType 'SQL Agent'| Should Be 'Schema'
            }
            It 'Checks the Mock was called for Connect-DbaInstance' {
                $assertMockParams = @{
                    'CommandName' = 'Connect-DbaInstance'
                    'Times'       = 6 ## now 6 with job Owner
                    'Exactly'     = $true
                }
                Assert-MockCalled @assertMockParams 
            }
            It 'Checks the Mock was called for Invoke-Catch' {
                $assertMockParams = @{
                    'CommandName' = 'Invoke-Catch'
                    'Times'       = 6 ## now 6 with Job Owner
                    'Exactly'     = $true
                }
                Assert-MockCalled @assertMockParams 
            }
        }
        Context "Install-OpenQueryStore Execution" {
            Mock Get-Module {}
            Mock Write-Warning {}
            Mock Test-Path -ParameterFilter {$CertificateBackupPath -and $CertificateBackupPath -eq 'NoCert'} {$false}
            Mock Test-Path -ParameterFilter {$CertificateBackupPath -and $CertificateBackupPath -eq 'NoCert'} {$true}

            It "Should write a warning if dbatools module not available" {      
                Install-OpenQueryStore -SqlInstance Dummy -Database Dummy -SchedulerType 'Service Broker'| Should Not Throw
            }
            It 'Checks the Mock was called for Write-Warning' {
                $assertMockParams = @{
                    'CommandName' = 'Write-Warning'
                    'Times'       = 1
                    'Exactly'     = $true
                }
                Assert-MockCalled @assertMockParams 
            }
            It 'Checks the Mock was called for Get-Module' {
                $assertMockParams = @{
                    'CommandName' = 'Get-Module'
                    'Times'       = 1
                    'Exactly'     = $true
                }
                Assert-MockCalled @assertMockParams 
            }
            It "Should call Invoke catch if no certificate path" {
                Mock Test-Path {$false}
                Install-OpenQueryStore -SqlInstance Dummy -Database Dummy -SchedulerType 'Service Broker'| Should Not Throw
                $assertMockParams = @{
                    'CommandName' = 'Test-Path'
                    'Times'       = 1
                    'Exactly'     = $true
                }
                Assert-MockCalled @assertMockParams 
                $assertMockParams = @{
                    'CommandName' = 'Invoke-Catch'
                    'Times'       = 1
                    'Exactly'     = $true
                }
                Assert-MockCalled @assertMockParams 
            }
            It "Should call Invoke-Catch if certificate exists" {
                Install-OpenQueryStore -SqlInstance dummy -DatabaseName dummy -OQSMode Classic -SchedulerType 'Service Broker' -CertificateBackupPath NoCert 
                $assertMockParams = @{
                    'CommandName' = 'Invoke-Catch'
                    'Times'       = 1
                    'Exactly'     = $true
                }
                Assert-MockCalled @assertMockParams 
            }
        }
    }
    Context "Install-OpenQueryStore Output" {

    }
}

