program totp;

uses
  Vcl.Forms,
  uMain in 'uMain.pas' {Form1},
  DelphiZXingQRCode in '..\..\Source\DelphiZXingQRCode.pas',
  uB32Encoding in '..\..\Source\uB32Encoding.pas',
  uTOTP in '..\..\Source\uTOTP.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
