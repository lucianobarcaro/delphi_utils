program totp;

uses
  Vcl.Forms,
  uMain in 'uMain.pas' {Form1},
  uTOTP in 'uTOTP.pas',
  DelphiZXingQRCode in 'DelphiZXingQRCode.pas',
  uB32Encoding in 'uB32Encoding.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
