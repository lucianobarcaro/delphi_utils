unit Redis.Client;

interface

uses
  System.SysUtils,
  IdTCPClient,
  System.Generics.Collections,
  Redis.Base,
  Redis.Commands.Str,
  Redis.Commands.Hash;

type
  TRedisClient = class(TInterfacedObject, IRedisClient)
  private
    fConn: TIdTCPClient;
    fString: TRedisCommandsStr;
    fHash: TRedisCommandsHash;

    function Serialize(sl: TArray<string>): string;
    function readFromServer: tarray<string>;
  protected
    function AsBool(aResult: tArray<string>): boolean;
    function AsString(aResult: tArray<string>): string;
    function AsInteger(aResult: tArray<string>): integer;
    function AsFloat(aResult: tArray<string>): Real;
    function SendCommand(cmd: tArray<string>): tArray<string>;
  public
    constructor Create;
    destructor Destroy; override;

    function Connect(aHost: string; aPort: UInt16; aPassword:string=''): Boolean;

    // ACL
    function auth(const aPassword: string): Boolean;

    // Comandos publicados
    property Strings: TRedisCommandsStr read fString;
    property Hashes: TRedisCommandsHash read fHash;
  end;

implementation

{ TRedisClient }

function TRedisClient.auth(const aPassword: string): Boolean;
begin
  result := AsBool(sendCommand(['AUTH', aPassword]));
end;

function TRedisClient.connect(aHost: string; aPort: UInt16; aPassword: string): Boolean;
begin
  if assigned(fConn) then
    fConn.Disconnect;

  fConn := TIdTCPClient.create(nil);
  fConn.host := aHost;
  fConn.port := aPort;
  fConn.ConnectTimeout := 3000;
  fConn.ReadTimeout := 3000;
  fConn.Connect;

  if aPassword <> '' then
    if not Auth(aPassword) then
    begin
      fConn.Disconnect;
      raise Exception.create('Falha na autenticação');
    end;
  result := true;
end;

constructor TRedisClient.create;
begin
  fHash := TRedisCommandsHash.create(self);
  fString := TRedisCommandsStr.create(self);
end;

destructor TRedisClient.Destroy;
begin
  if assigned(fConn) then begin
    if fConn.connected then
      fConn.disconnect;
    fConn.Free;
  end;
  fHash.Free;
  fString.Free;

  inherited;
end;

function TRedisClient.readFromServer: tarray<string>;
var
  s: String;
  sz, i: integer;
  rr: TArray<string>;
begin
  s := fConn.IOHandler.readln;
  if CharInSet(s[1], ['+', '-']) then
  begin
    setLength(result, 1);
    result[0] := copy(s, 2, MaxInt);
  end
  else if s[1] = ':' then
  begin
    setLength(result, 1);
    result[0] := copy(s, 2, MaxInt);
  end
  else if s[1] = '$' then
  begin
    setLength(result, 1);
    delete(s, 1, 1);
    sz := StrToInt(s);
    result[0] := fConn.IOHandler.readstring(sz);
    fConn.IOHandler.readstring(2);
  end
  else if s[1] = '*' then
  begin
    delete(s, 1, 1);
    sz := StrToInt(s);
    SetLength(result, sz);
    i:=0;
    while sz > 0 do
    begin
      rr := readFromServer;
      Result[i] := rr[0];
      inc(i);
    end;
  end;
end;

function TRedisClient.AsBool(aResult: tArray<string>): boolean;
begin
  if length(aResult) <> 1 then
    raise Exception.Create('?!?');

  result := Copy(aResult[0], 1, 2) = 'OK';
end;

function TRedisClient.AsFloat(aResult: tArray<string>): Real;
begin
  if length(aResult) <> 1 then
    raise Exception.Create('?!?');

  result := aResult[0].ToDouble;
end;

function TRedisClient.AsInteger(aResult: tArray<string>): integer;
begin
  if length(aResult) <> 1 then
    raise Exception.Create('?!?');

  result := aResult[0].ToInteger;
end;

function TRedisClient.AsString(aResult: tArray<string>): string;
begin
  if length(aResult) <> 1 then
    raise Exception.Create('?!?');

  result := aResult[0];
end;

function TRedisClient.sendCommand(cmd: tArray<string>): tArray<string>;
var
  s: string;
begin
  s := serialize(cmd);
  fConn.IOHandler.Write(s);

  result := readFromServer;
end;

function TRedisClient.serialize(sl: TArray<string>): string;
const
  crlf = #13#10;
var
  r: TStringBuilder;
  v: UTF8String;
  i: integer;
begin
  r := TStringBuilder.Create;
  r.Append('*' + Length(sl).ToString + crlf);

  for i := 0 to length(sl)-1 do
  begin
    v := UTF8ToString(sl[i]);
    r.append('$' + length(v).ToString + crlf + sl[i] + crlf);
  end;

  try
    result := r.ToString;
  finally
    r.free;
  end;
end;

end.
