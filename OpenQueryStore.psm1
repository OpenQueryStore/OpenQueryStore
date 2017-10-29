[cmdletbinding()]
param()
Write-Verbose $PSScriptRoot

Write-Verbose 'Import everything in sub folders folder'

foreach($folder in @('internal', 'functions'))

{

    $root = Join-Path -Path $PSScriptRoot -ChildPath $folder

    if(Test-Path -Path $root)

    {

        Write-Verbose "processing folder $root"

        $files = Get-ChildItem -Path $root -Filter *.ps1 -Recurse



        # dot source each file

        $files | where-Object{ $_.name -NotLike '*.Tests.ps1'} | 

            ForEach-Object{Write-Verbose $_.basename; . $_.FullName}

    }

}



Export-ModuleMember -function (Get-ChildItem -Path "$PSScriptRoot\functions\*.ps1").basename