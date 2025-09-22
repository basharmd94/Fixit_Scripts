Dim WinScriptHost
Set WinScriptHost = CreateObject("WScript.Shell")
WinScriptHost.Run Chr(34) & "D:\import_products\inv_report.bat" & Chr(34), 0
Set WinScriptHost = Nothing