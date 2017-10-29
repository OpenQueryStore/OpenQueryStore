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
Describe "Basic function unit tests" -Tags Build , Unit{

}
InModuleScope -ModuleName $ModuleName -ScriptBlock {
    Describe "Testing Install-OpenQueryStore command" -Tags Install {
        It "Command Install-OpenQueryStore exists" {
            Get-Command Install-OpenQueryStore -ErrorAction SilentlyContinue | Should Not BE NullOrEmpty
        }
        Context "Install-OpenQueryStore input" {
            
        }
        Context "Install-OpenQueryStore Execution" {
            It "Should exit if SMO will not load" {
               Mock Install-SMO {$false} -Verifiable
                Install-OpenQueryStore | Should Be "Failed to load SQL Server SMO Assemblies - Quitting"
            }

        }
        Context "Install-OpenQueryStore Output" {

        }
    }
}
