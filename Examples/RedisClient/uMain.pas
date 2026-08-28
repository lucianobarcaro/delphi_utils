unit uMain;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.StdCtrls;

type
  TForm1 = class(TForm)
    btn1: TButton;
    procedure btn1Click(Sender: TObject);
  private
    { Private declarations }
  public
    { Public declarations }
  end;

var
  Form1: TForm1;

implementation

{$R *.dfm}

uses Redis.Client;

procedure TForm1.btn1Click(Sender: TObject);
var
  redisClient: TRedisClient;
  r: string;
begin
  redisClient := tRedisClient.create;
  if not redisClient.connect('192.168.4.6', 6379, '123123') then
    exit;

  redisClient.Strings.&set('teste', 'abobora');
  r:= redisClient.Strings.get('teste');
  showMessage(r);
end;

end.
