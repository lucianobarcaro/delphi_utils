unit Redis.Client;

interface

uses
  System.SysUtils,
  IdTCPClient,
  System.Generics.Collections;

type
  TRedisClient = class
  private
    fConn: TIdTCPClient;

    function Serialize(sl: TArray<string>): string;
    function readFromServer: tarray<string>;
  protected
    function AsBool(aResult: tArray<string>): boolean;
    function AsString(aResult: tArray<string>): string;
    function AsInteger(aResult: tArray<string>): integer;
    function AsFloat(aResult: tArray<string>): Real;
    function SendCommand(cmd: tArray<string>): tArray<string>;
  public
    destructor Destroy; override;

    function Connect(aHost: string; aPort: UInt16; aPassword:string=''): Boolean;

    // ACL
    function auth(const aPassword: string): Boolean;

    // Str
    function Append(const aKey, aValue: string): boolean;
    function Decr(const aKey: string): boolean;
    function DecrBy(const aKey: string; aValue: integer): boolean;
    function Get(const aKey: string): string;
    function GetDel(const aKey: string): string;
    function GetEx(const aKey: string; aEx: integer=0; aPx:integer=0; aExAt: uint32=0; aPxAt: uint64=0; aPersist:Boolean=False): Boolean;
    function GetRange(const aKey: string; aStart, aEnd:Integer): String;
    function GetSet(const aKey, aValue: string): string;
    function Incr(const akey: String): integer;
    function IncrBy(const aKey: string; aValue: integer): integer;
    function IncrByFloat(const aKey: string; aValue: Real): Real;
    function MGet(const aKeys: tarray<string>): tArray<String>;
    function MSet(const aKeyValues: tarray<string>): Boolean;
    function MSetNX(const aKeyValues: tarray<string>): Boolean;
    function PSetEx(const aKey: string; aMs: uint32; aValue: String): Boolean;
    function &Set(const aKey, aValue: string): boolean;
    function SetEx(const aKey: string; aTTL: uint32; aValue: string): Boolean;
    function SetNX(const aKey, aValue: string): boolean;
    function SetRange(const aKey: string; aOffset: uint32; aValue: string): Boolean;
    function StrLen(const aKey: string): uint32;
    function SubStr(const aKey: string; aStart, aEnd:Integer): string;
	
	// Hash
    function HDel(const aKey: string; AFields: tArray<string>): Boolean;
    function HExists(const aKey, aField: String): Boolean;
    function HGet(const aKey, aField: String): String;
    function HGetAll(const aKey: String): TPair<String, String>;
    function HIncrBy(const aKey, aField: string; aValue: integer): integer;
    function HIncrByFloat(const aKey, aField: string; aValue: Real): Real;
    function HKeys(const aKey: String): TArray<string>;
    function HLen(const aKey: String): integer;
    function HMGet(const aKey: String; aFields: TArray<string>): TPair<String, String>;
    function HMSet(const aKey: String; aPairs: TArray<TPair<String, String>>): TPair<String, String>;
    function HRandField(const aKey: String; aCount:Integer=0; aWithValues:Boolean=False): string;
    // function HScan
    function HSet(const aKey: String; aFields: TArray<string>): TPair<String, String>;
    function HSetNX(const aKey, aField, aValue: String): Boolean;
    function HStrLen(const aKey, aField: string): uint32;
    function HVals(const aKey: String): TArray<TPair<String, String>>;
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

destructor TRedisClient.Destroy;
begin
  if assigned(fConn) then
  begin
    if fConn.connected then
      fConn.disconnect;
    fConn.Free;
  end;

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

function TRedisClient.&Set(const aKey, aValue: string): boolean;
begin
  result := AsBool(sendCommand(['SET', aKey, aValue]));
end;

function TRedisClient.Append(const aKey, aValue: string): boolean;
begin
  result := Asbool(sendCommand(['APPEND', aKey, aValue]));
end;

function TRedisClient.Decr(const aKey: string): boolean;
begin
  result := AsBool(SendCommand(['DECR', aKey]));
end;

function TRedisClient.DecrBy(const aKey: string; aValue: integer): boolean;
begin
  result := AsBool(SendCommand(['DECRBY', aKey, aValue.toString]));
end;

function TRedisClient.Get(const aKey: string): string;
begin
  result := AsString(SendCommand(['GET', aKey]));
end;

function TRedisClient.GetDel(const aKey: string): string;
begin
  result := AsString(SendCommand(['GETDEL', aKey]));
end;

function TRedisClient.GetEx(const aKey: string; aEx, aPx: integer; aExAt: uint32; aPxAt: uint64; aPersist: Boolean): Boolean;
var
  cmd:tArray<string>;
  i: integer;
begin
  i := 1;
  setLength(cmd, i);
  cmd[0] := 'GETEX';
  if aEx > 0 then
  begin
    inc(i, 2);
    setLength(cmd, i);
    cmd[i-2] := 'EX';
    cmd[i-1] := aEx.ToString;
  end;

  if aPx > 0 then
  begin
    if i > 1 then
      raise Exception.Create('Mais de um parâmetro de tempo setado');
    inc(i, 2);
    setLength(cmd, i);
    cmd[i-2] := 'PX';
    cmd[i-1] := aPx.ToString;
  end;

  if aExAt > 0 then
  begin
    if i > 1 then
      raise Exception.Create('Mais de um parâmetro de tempo setado');
    inc(i, 2);
    setLength(cmd, i);
    cmd[i-2] := 'EXAT';
    cmd[i-1] := aEXAT.ToString;
  end;

  if aPxAt > 0 then
  begin
    if i > 1 then
      raise Exception.Create('Mais de um parâmetro de tempo setado');
    inc(i, 2);
    setLength(cmd, i);
    cmd[i-2] := 'PXAT';
    cmd[i-1] := aPXAT.ToString;
  end;

  if aPersist then
  begin
    if i > 1 then
      raise Exception.Create('Mais de um parâmetro de tempo setado');
    inc(i);
    setLength(cmd, i);
    cmd[i-1] := 'PERSIST';
  end;

  result := AsBool(SendCommand(cmd));
end;

function TRedisClient.GetRange(const aKey: string; aStart, aEnd: Integer): String;
begin
  result := AsString(SendCommand(['GETRANGE', aKey, aStart.ToString, aEnd.toString]));
end;

function TRedisClient.GetSet(const aKey, aValue: string): string;
begin
  result := AsString(SendCommand(['GETSET', aKey, aValue]));
end;

function TRedisClient.Incr(const akey: String): integer;
begin
  result := AsInteger(SendCommand(['INCR', aKey]));
end;

function TRedisClient.IncrBy(const aKey: string; aValue: integer): integer;
begin
  result := AsInteger(SendCommand(['INCRBY', aKey, aValue.ToString]));
end;

function TRedisClient.IncrByFloat(const aKey: string; aValue: Real): Real;
begin
  result := AsFloat(SendCommand(['INCRBYFLOAT', aKey, floatToStr(aValue, tFormatSettings.Invariant)]));
end;

function TRedisClient.MGet(const aKeys: tarray<string>): tArray<String>;
begin

end;

function TRedisClient.MSet(const aKeyValues: tarray<string>): Boolean;
begin
  if length(aKeyValues) mod 2 = 1 then
    raise Exception.Create('Numero errado de parâmetros');

  result := AsBool(SendCommand(['MSET'] + aKeyValues));
end;

function TRedisClient.MSetNX(const aKeyValues: tarray<string>): Boolean;
begin
  if length(aKeyValues) mod 2 = 1 then
    raise Exception.Create('Numero errado de parâmetros');

  result := AsBool(SendCommand(['MSETNX'] + aKeyValues));
end;

function TRedisClient.PSetEx(const aKey: string; aMs: uint32; aValue: String): Boolean;
begin
  result := AsBool(SendCommand(['PSETEX', aMs.ToString, aValue]));
end;

function TRedisClient.SetEx(const aKey: string; aTTL: uint32; aValue: string): Boolean;
begin
  result := AsBool(SendCommand(['SETEX', aTTL.ToString, aValue]));
end;

function TRedisClient.SetNX(const aKey, aValue: string): boolean;
begin
  result := AsBool(SendCommand(['SETNX', aValue]));
end;

function TRedisClient.SetRange(const aKey: string; aOffset: uint32; aValue: string): Boolean;
begin
  result := AsBool(SendCommand(['SETRANGE', aOffset.toString, aValue]));
end;

function TRedisClient.StrLen(const aKey: string): uint32;
begin
  result := AsInteger(SendCommand(['STRLEN', aKey]));
end;

function TRedisClient.SubStr(const aKey: string; aStart, aEnd: Integer): string;
begin
  result := AsString(SendCommand(['SUBSTR', aKey, aStart.ToString, aEnd.ToString]));
end;

function TRedisClient.HDel(const aKey: string; AFields: tArray<string>): Boolean;
begin

end;

function TRedisClient.HExists(const aKey, aField: String): Boolean;
begin

end;

function TRedisClient.HGet(const aKey, aField: String): String;
begin

end;

function TRedisClient.HGetAll(const aKey: String): TPair<String, String>;
begin

end;

function TRedisClient.HIncrBy(const aKey, aField: string; aValue: integer): integer;
begin

end;

function TRedisClient.HIncrByFloat(const aKey, aField: string; aValue: Real): Real;
begin

end;

function TRedisClient.HKeys(const aKey: String): TArray<string>;
begin

end;

function TRedisClient.HLen(const aKey: String): integer;
begin

end;

function TRedisClient.HMGet(const aKey: String; aFields: TArray<string>): TPair<String, String>;
begin

end;

function TRedisClient.HMSet(const aKey: String; aPairs: TArray<TPair<String, String>>): TPair<String, String>;
begin

end;

function TRedisClient.HRandField(const aKey: String; aCount: Integer; aWithValues: Boolean): string;
begin

end;

function TRedisClient.HSet(const aKey: String; aFields: TArray<string>): TPair<String, String>;
begin

end;

function TRedisClient.HSetNX(const aKey, aField, aValue: String): Boolean;
begin

end;

function TRedisClient.HStrLen(const aKey, aField: string): uint32;
begin

end;

function TRedisClient.HVals(const aKey: String): TArray<TPair<String, String>>;
begin

end;

end.
