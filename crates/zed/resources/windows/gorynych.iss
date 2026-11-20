[Setup]
AppId={{EEA10A57-3BF1-4920-BDDD-31ACDBAB55A1}
AppName=Zed Gorynych
AppVersion={#Version}
AppPublisher=Phoenix-
DefaultDirName={localappdata}\Programs\Zed Gorynych
DefaultGroupName=Zed Gorynych
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
OutputDir={#OutputDir}
OutputBaseFilename={#OutputName}
Compression=lzma2
SolidCompression=yes
AppMutex={code:GetAppMutex}
SetupMutex=Zed-Gorynych-Setup-Mutex
CloseApplications=no
UninstallDisplayIcon={app}\Zed.exe

[Files]
Source: "{#Stage}\Zed.exe"; DestDir: "{code:GetInstallDir}"; Flags: ignoreversion
Source: "{#Stage}\conpty.dll"; DestDir: "{code:GetInstallDir}"; Flags: ignoreversion
Source: "{#Stage}\bin\*"; DestDir: "{code:GetInstallDir}\bin"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "{#Stage}\x64\*"; DestDir: "{code:GetInstallDir}\x64"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "{#Stage}\remote-server\*"; DestDir: "{code:GetInstallDir}\remote-server"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "{#Stage}\tools\*"; DestDir: "{app}\tools"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "{#Stage}\gorynych-installed"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\Zed Gorynych"; Filename: "{app}\Zed.exe"

[Run]
Filename: "{app}\Zed.exe"; Description: "Launch Zed Gorynych"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
Type: filesandordirs; Name: "{app}\updates"
Type: filesandordirs; Name: "{app}\install"
Type: filesandordirs; Name: "{app}\remote-server"
Type: filesandordirs; Name: "{app}\tools"

[Code]
function IsUpdating(): Boolean;
begin
  Result := WizardSilent() and (CompareText(ExpandConstant('{param:update}'), 'true') = 0);
end;

function GetAppMutex(Param: string): string;
begin
  if IsUpdating() then Result := '' else Result := 'Zed-Editor-Dev-Instance-Mutex';
end;

function GetInstallDir(Param: string): string;
begin
  if IsUpdating() then Result := ExpandConstant('{app}\install') else Result := ExpandConstant('{app}');
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if (CurStep = ssPostInstall) and IsUpdating() then
    if not SaveStringToFile(ExpandConstant('{app}\updates\versions.txt'), '{#Version}' + #13#10, False) then
      RaiseException('Could not write the update completion flag.');
end;
