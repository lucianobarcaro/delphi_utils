unit uMain;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.StdCtrls, Vcl.ExtCtrls, Vcl.ComCtrls,
  Vcl.Samples.Spin;

type
  TForm1 = class(TForm)
    Memo1: TMemo;
    Panel1: TPanel;
    Label1: TLabel;
    Timer1: TTimer;
    Label2: TLabel;
    edSecret: TLabeledEdit;
    edIssuer: TLabeledEdit;
    edName: TLabeledEdit;
    edURI: TLabeledEdit;
    pb: TProgressBar;
    Button1: TButton;
    Panel2: TPanel;
    Label5: TLabel;
    seIntervalo: TSpinEdit;
    seDigitos: TSpinEdit;
    Label4: TLabel;
    cbAlgoritmo: TComboBox;
    Label3: TLabel;
    procedure Timer1Timer(Sender: TObject);
    procedure FormShow(Sender: TObject);
    procedure edSecretExit(Sender: TObject);
    procedure edIssuerChange(Sender: TObject);
    procedure Button1Click(Sender: TObject);
  private
    { Private declarations }
    fLastDue: integer;
  public
    { Public declarations }
  end;

var
  Form1: TForm1;

implementation

{$R *.dfm}

uses uTOTP;

var obj: TTotp;

procedure TForm1.Button1Click(Sender: TObject);
begin
  fLastDue := -99;
  edSecret.text := obj.newToken;
  edSecretExit(nil);
end;

procedure TForm1.edIssuerChange(Sender: TObject);
begin
  if not assigned(obj) then
    exit;
  edURI.Text := obj.generate_uri(edIssuer.Text, edName.text);
  memo1.Lines.text := obj.genQRCode(edIssuer.Text, edName.text);
end;

procedure TForm1.edSecretExit(Sender: TObject);
begin
  obj := TTotp.create(
    edSecret.text,
    seDigitos.value,
    seIntervalo.value,
    TTOTPAlgorithms(cbAlgoritmo.ItemIndex));

  pb.Max := obj.interval;
  Timer1Timer(nil);
  edIssuerChange(nil);
  label1.Caption := obj.getToken();
end;

procedure TForm1.FormShow(Sender: TObject);
begin
  edSecret.setfocus;
end;

procedure TForm1.Timer1Timer(Sender: TObject);
var
  hora: string;
  H, M, S, MS: Word;
  due: integer;
begin
  if not assigned(obj) then
    exit;

  DecodeTime(Now, H, M, S, MS);
  due := s mod obj.interval;

  // Refresh de tela somente se necessário.
  if fLastDue = due then
    exit;

  hora := FormatDateTime('hh:nn:ss', Now);
  label2.Caption := hora;

  fLastDue := due;
  if due = 0 then
  begin
    label1.Caption := obj.getToken();
    pb.Position := obj.interval;
    pb.State := pbsNormal;
  end
  else
  begin
    if due >= (obj.interval - obj.interval div 8) then
      pb.State := pbsError;
    pb.position := obj.interval - due;
  end;
end;

end.
