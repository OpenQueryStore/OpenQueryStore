<#
.SYNOPSIS
Download OpenQueryStore from GitHub repository

.DESCRIPTION
Download OQS to a temporary folder from GitHub repository.
Unblock the zip file.
Unzip it
Move output to Documents\OpenQueryStore folder

#>
$path = "$HOME\Documents\OpenQueryStore"

$url = 'https://github.com/OpenQueryStore/OpenQueryStore/archive/master.zip'
$branch = "master"

$temp = ([System.IO.Path]::GetTempPath()).TrimEnd("\")
$zipfile = "$temp\OpenQueryStore.zip"

if (!(Test-Path -Path $path)) {
	try {
		Write-Output "Creating directory: $path"
		New-Item -Path $path -ItemType Directory | Out-Null
	}
	catch {
		throw "Can't create $Path. You may need to Run as Administrator"
	}
}
else {
	try {
		Write-Output "Deleting previously installed module"
		Remove-Item -Path "$path\*" -Force -Recurse
	}
	catch {
		throw "Can't delete $Path. You may need to Run as Administrator"
	}
}

Write-Output "Downloading archive from github"
try {
	Invoke-WebRequest $url -OutFile $zipfile
}
catch {
	#try with default proxy and usersettings
	Write-Output "Probably using a proxy for internet access, trying default proxy settings"
	(New-Object System.Net.WebClient).Proxy.Credentials = [System.Net.CredentialCache]::DefaultNetworkCredentials
	Invoke-WebRequest $url -OutFile $zipfile
}

# Unblock if there's a block
Unblock-File $zipfile -ErrorAction SilentlyContinue

Write-Output "Unzipping"

# Keep it backwards compatible
$shell = New-Object -ComObject Shell.Application
$zipPackage = $shell.NameSpace($zipfile)
$destinationFolder = $shell.NameSpace($temp)
$destinationFolder.CopyHere($zipPackage.Items())

Write-Output "Cleaning up"
Move-Item -Path "$temp\OpenQueryStore-$branch\*" $path
Remove-Item -Path "$temp\OpenQueryStore-$branch"
Remove-Item -Path $zipfile


#Change sesison location so we can run the Install.ps1
Set-Location $path