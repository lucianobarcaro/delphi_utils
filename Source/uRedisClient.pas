unit uRedisClient;

interface

uses
  System.SysUtils,
  IdTCPClient;

type
  TRedisClient = class
  private
    fConn: TIdTCPClient;

    function serialize(sl: TArray<string>): string;
    function sendCommand(cmd: tArray<string>): tArray<string>;
    function readFromServer: tarray<string>;
    function AsBool(aResult: tArray<string>): boolean;
    function AsString(aResult: tArray<string>): string;
  protected
  public
    constructor create;
    destructor Destroy; override;

    function connect(aHost: string; aPort: UInt16; aPassword:string=''): Boolean;

    function get(const aKey: string): string;
    function &set(const aKey, aValue: string): boolean;
    function auth(const aPassword: string): Boolean;
  end;

implementation

{ TRedisClient }

const
  crlf = #13#10;

function TRedisClient.&set(const aKey, aValue: string): boolean;
begin
  result := AsBool(sendCommand(['SET', aKey, aValue]));
end;

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
      raise Exception.Create('Falha na autenticação');
    end;
  result := true;
end;

constructor TRedisClient.create;
begin

end;

destructor TRedisClient.Destroy;
begin
  if assigned(fConn) then begin
    if fConn.connected then
      fConn.disconnect;
    fConn.Free;
  end;
  inherited;
end;

function TRedisClient.get(const aKey: string): string;
begin
  result := AsString(sendCommand(['GET', aKey]));
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
  result := (length(aResult) = 1) and
            (Copy(aResult[0], 1, 2) = 'OK');
end;

function TRedisClient.AsString(aResult: tArray<string>): string;
begin
  if length(aResult) = 1 then
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
