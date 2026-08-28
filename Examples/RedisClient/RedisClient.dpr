program RedisClient;

uses
  Vcl.Forms,
  uMain in 'uMain.pas' {Form1},
  Redis.Client in '..\..\Source\Redis.Client.pas',
  Redis.Commands.Str in '..\..\Source\Redis.Commands.Str.pas',
  Redis.Base in '..\..\Source\Redis.Base.pas',
  Redis.Commands.Hash in '..\..\Source\Redis.Commands.Hash.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
