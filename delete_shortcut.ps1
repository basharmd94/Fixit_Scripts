# Define the base path
$basePath = "D:\"

# Define the list of shortcut file names
$shortcutFileNames = @("AV.lnk", "Photo.lnk", "Video.lnk", "AV.scr", "Photo.scr", "Video.scr","IMG001.exe")

# Iterate through each folder recursively
Get-ChildItem -Path $basePath -Recurse | ForEach-Object {
    # Iterate through each shortcut file name and delete if it exists
    foreach ($shortcutFileName in $shortcutFileNames) {
        $shortcutFilePath = Join-Path $_.FullName $shortcutFileName
        Remove-Item -Path $shortcutFilePath -Force -ErrorAction SilentlyContinue
    }
}