unit Redis.Client;

interface

uses
  System.SysUtils, IdTCPClient, System.Generics.Collections;

type
  TRedisKeyValue = TPair<string, string>;
  TListPoint = (LEFT, RIGHT);
  TListRelative = (BEFORE, AFTER);

  IRedisClient = interface
    ['{556EE5AA-61FA-401D-A270-3FC07421955F}']
    // Str
    function Append(const aKey, aValue: string): boolean;
    function Decr(const aKey: string): boolean;
    function DecrBy(const aKey: string; aValue: integer): boolean;
    function Get(const aKey: string): string;
    function GetDel(const aKey: string): string;
    function GetEx(const aKey: string; aEx: integer = 0; aPx: integer = 0; aExAt: uint32 = 0; aPxAt: uint64 = 0; aPersist: Boolean = False): Boolean;
    function GetRange(const aKey: string; aStart, aEnd: Integer): string;
    function GetSet(const aKey, aValue: string): string;
    function Incr(const akey: string): integer;
    function IncrBy(const aKey: string; aValue: integer): integer;
    function IncrByFloat(const aKey: string; aValue: Real): Real;
    function MGet(const aKeys: tarray<string>): tArray<string>;
    function MSet(const aKeyValues: TArray<TRedisKeyValue>): Boolean;
    function MSetNX(const aKeyValues: TArray<TRedisKeyValue>): Boolean;
    function PSetEx(const aKey: string; aMs: uint32; aValue: string): Boolean;
    function  &Set(const aKey, aValue: string): boolean;
    function SetEx(const aKey: string; aTTL: uint32; aValue: string): Boolean;
    function SetNX(const aKey, aValue: string): boolean;
    function SetRange(const aKey: string; aOffset: uint32; aValue: string): Boolean;
    function StrLen(const aKey: string): uint32;
    function SubStr(const aKey: string; aStart, aEnd: Integer): string;

    // Hash
    function HDel(const aKey: string; AFields: TArray<string>): Boolean;
    function HExists(const aKey, aField: string): Boolean;
    function HGet(const aKey, aField: string): string;
    function HGetAll(const aKey: string): TArray<TRedisKeyValue>;
    function HIncrBy(const aKey, aField: string; aValue: integer): integer;
    function HIncrByFloat(const aKey, aField: string; aValue: Real): Real;
    function HKeys(const aKey: string): TArray<string>;
    function HLen(const aKey: string): integer;
    function HMGet(const aKey: string; aFields: TArray<string>): TArray<TRedisKeyValue>;
    function HMSet(const aKey: string; aPairs: TArray<TRedisKeyValue>): Boolean;
    function HRandField(const aKey: string; aCount: Integer = 0; aWithValues: Boolean = False): string;
    // function HScan
    function HSet(const aKey: string; aFields: TArray<TRedisKeyValue>): Boolean;
    function HSetNX(const aKey, aField, aValue: string): Boolean;
    function HStrLen(const aKey, aField: string): uint32;
    function HVals(const aKey: string): TArray<TRedisKeyValue>;
  end;

  IRedisTransaction = interface(IredisClient)
    procedure Watch(aKeyList: TArray<string>);
    procedure UnWatch;
    procedure Exec;
    procedure Discard;
  end;

  // Implementar PubSub

  TRedisClient = class(TInterfacedObject, IRedisClient)
  private
    fConn: TIdTCPClient;
    Function Serialize(sl: TArray<string>): string;
    Function ReadFromServer(aTimes: integer = 1): TArray<string>;
    Function MPairToArray(aPairs: tArray<TRedisKeyValue>): tArray<string>;
  protected
    Function AsBool(aResult: TArray<string>): Boolean; virtual;
    Function AsString(aResult: TArray<string>): string; virtual;
    Function AsInteger(aResult: TArray<string>): Integer; virtual;
    Function AsFloat(aResult: TArray<string>): Real; virtual;
    Function SendCommand(cmd: TArray<string>): TArray<string>; virtual;
  public
    destructor Destroy; override;

    Function Connect(aHost: string; aPort: UInt16; aPassword: string = ''): Boolean;

    // Transaction
    Function Pipeline: IRedisTransaction;

    // ACL
    Function auth(const aPassword: string): Boolean;

    // String ------------------------------------------------------------------
    Function Append(const aKey, aValue: string): boolean;
    Function Decr(const aKey: string): boolean;
    Function DecrBy(const aKey: string; aValue: integer): boolean;
    Function Get(const aKey: string): string;
    Function GetDel(const aKey: string): string;
    Function GetEx(const aKey: string; aEx: integer = 0; aPx: integer = 0; aExAt: uint32 = 0; aPxAt: uint64 = 0; aPersist: Boolean = False): Boolean;
    Function GetRange(const aKey: string; aStart, aEnd: Integer): string;
    Function GetSet(const aKey, aValue: string): string;
    Function Incr(const akey: string): integer;
    Function IncrBy(const aKey: string; aValue: integer): integer;
    Function IncrByFloat(const aKey: string; aValue: Real): Real;
    Function MGet(const aKeys: tarray<string>): tArray<string>;
    Function MSet(const aKeyValues: TArray<TRedisKeyValue>): Boolean;
    Function MSetNX(const aKeyValues: TArray<TRedisKeyValue>): Boolean;
    Function PSetEx(const aKey: string; aMs: uint32; aValue: string): Boolean;
    Function &Set(const aKey, aValue: string): boolean;
    Function SetEx(const aKey: string; aTTL: uint32; aValue: string): Boolean;
    Function SetNX(const aKey, aValue: string): boolean;
    Function SetRange(const aKey: string; aOffset: uint32; aValue: string): Boolean;
    Function StrLen(const aKey: string): uint32;
    Function SubStr(const aKey: string; aStart, aEnd: Integer): string;

    // Hash --------------------------------------------------------------------
    Function HDel(const aKey: string; AFields: TArray<string>): Boolean;
    Function HExists(const aKey, aField: string): Boolean;
    Function HGet(const aKey, aField: string): string;
    Function HGetAll(const aKey: string): TArray<TRedisKeyValue>;
    Function HIncrBy(const aKey, aField: string; aValue: integer): integer;
    Function HIncrByFloat(const aKey, aField: string; aValue: Real): Real;
    Function HKeys(const aKey: string): TArray<string>;
    Function HLen(const aKey: string): integer;
    Function HMGet(const aKey: string; aFields: TArray<string>): TArray<TRedisKeyValue>;
    Function HMSet(const aKey: string; aPairs: TArray<TRedisKeyValue>): Boolean;
    Function HRandField(const aKey: string; aCount: Integer = 0; aWithValues: Boolean = False): string;
    // function HScan
    Function HSet(const aKey: string; aFields: TArray<TRedisKeyValue>): Boolean;
    Function HSetNX(const aKey, aField, aValue: string): Boolean;
    Function HStrLen(const aKey, aField: string): uint32;
    Function HVals(const aKey: string): TArray<TRedisKeyValue>;

    // List --------------------------------------------------------------------
    Function BLMove(aSource, aDestination: string; popFrom, pushTo: TListPoint; aTimeout: integer): String;
    Function BLPop(aKeys: TArray<string>; aTimeout:Integer): string;
    Function BRPop(aKeys: TArray<string>; aTimeout:Integer): string;
    Function BRPoplPush(aSource, aDestination: string; aTimeout: integer): String;
    Function LIndex(aKey: string; aIndex: integer): String;
    Procedure LInsert(aKey: string; aRelative: TListRelative; aPivot, aItem: string);
    Function LLen(aKey: string): integer;
    Procedure LMove(aSource, aDestination: string; popFrom, pushTo: TListPoint);
    Function LPop(aKey: string; aCount:Integer=1): TArray<String>;
    Function LPos(aKey, aElement: string; aRank:integer=1; aCount: integer=0; aMaxLen:integer=0): TArray<integer>;
    Procedure LPush(aKey: String; aElements: tArray<string>); overload;
    Procedure LPush(aKey, aElement: String); overload;
    Procedure LPushX(aKey: String; aElements: tArray<string>); overload;
    Procedure LPushX(aKey, aElement: String); overload;
    Function LRange(aKey: String; aStart, aStop: integer): TArray<String>;
    Procedure LRem(aKey: string; aCount: integer; aElement:string);
    Procedure LSet(aKey: string; aIndex: integer; aElement: string);
    Procedure LTrim(aKey: String; aStart, aStop: integer);
    Function RPop(aKey: string; aCount:Integer=1): TArray<String>;
    Function RPoplPush(aSource, aDestination: string): String; // Unificar com BRPoplPush (se houver timeout é BRPoplPush)
    Procedure RPush(aKey: String; aElements: tArray<string>); overload;
    Procedure RPush(aKey, aElement: String); overload;
    Procedure RPushX(aKey: String; aElements: tArray<string>); overload;
    Procedure RPushX(aKey, aElement: String); overload;

    // Set ---------------------------------------------------------------------
    Procedure SAdd(aKey, aMember: String); overload;
    Procedure SAdd(aKey: String; aMembers: tArray<string>); overload;
    Function SCard(aKey: String): Integer;
    Function SDiff(aKey: String; aKeys: TArray<string>): TArray<String>;
    Procedure SDiffStore(aDestiny: String; aKeys: TArray<string>);
    Function SInter(aKey: String; aKeys: TArray<string>): TArray<String>;
    Procedure SInterStore(aDestiny: String; aKeys: TArray<string>);
    Function SIsMember(aKey, aMember: String): Boolean;
    Function SMembers(aKey: String): TArray<String>;
    Function SMIsMember(aKey: String; aMembers: TArray<String>): TArray<Boolean>;
    Procedure SMove(aSource, aDestiny, aMember: String);
    Function SPop(aKey: String; aCount:integer=1): TArray<String>;
    Function SRandMember(aKey: String; aCount:integer=1): TArray<String>;
    Procedure SRem(aKey: String; aMembers: TArray<string>);
    // SScan
    Function SUnion(aKeys: TArray<string>): TArray<String>;
    Procedure SUnionStore(aDestiny: String; aKeys: TArray<string>);
  end;

  TRedisTransaction = class(TRedisClient, IRedisTransaction)
  private
    fStack: TArray<string>;
    fWatchStack: TArray<string>;
  protected
    function AsBool(aResult: TArray<string>): Boolean; override;
    function AsString(aResult: TArray<string>): string; override;
    function AsInteger(aResult: TArray<string>): Integer; override;
    function AsFloat(aResult: TArray<string>): Real; override;
    function SendCommand(cmd: TArray<string>): TArray<string>; override;
    // Acumular todos os comandos e enviar de uma única vez.
    // Exceto Serialize e ReadFromServer
    // Sobreescrever todos os métodos privados e protegidos
    // Implementar WATCH, UNWATCH, MULTI, DISCARD, EXEC
  public
    constructor create(AConnection: TIdTCPClient);
    procedure Watch(aKeyList: TArray<string>);
    procedure Unwatch;
    procedure Exec;
    procedure Discard;
  end;

implementation

uses System.TypInfo;

{ TRedisClient }

function TRedisClient.auth(const aPassword: string): Boolean;
begin
  result := AsBool(sendCommand(['AUTH', aPassword]));
end;

function TRedisClient.BLMove(aSource, aDestination: string; popFrom, pushTo: TListPoint; aTimeout: integer): String;
begin
  result := AsString(SendCommand(['BLMOVE', aSource, aDestination,
      GetEnumName(TypeInfo(TListPoint), ord(popFrom)),
      GetEnumName(TypeInfo(TListPoint), ord(pushTo)),
      aTimeout.toString]));
end;

function TRedisClient.BLPop(aKeys: TArray<string>; aTimeout: Integer): string;
begin

end;

function TRedisClient.BRPop(aKeys: TArray<string>; aTimeout: Integer): string;
begin

end;

function TRedisClient.BRPoplPush(aSource, aDestination: string;
  aTimeout: integer): String;
begin

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

function TRedisClient.readFromServer(aTimes: integer): tarray<string>;
var
  s: string;
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
    i := 0;
    while sz > 0 do
    begin
      rr := readFromServer;
      Result[i] := rr[0];
      inc(i);
    end;
  end;

  if aTimes > 1 then
    ReadFromServer(aTimes - 1);
end;

function TRedisClient.RPop(aKey: string; aCount: Integer): TArray<String>;
begin

end;

function TRedisClient.RPoplPush(aSource, aDestination: string): String;
begin

end;

procedure TRedisClient.RPush(aKey, aElement: String);
begin

end;

procedure TRedisClient.RPush(aKey: String; aElements: tArray<string>);
begin

end;

procedure TRedisClient.RPushX(aKey, aElement: String);
begin

end;

procedure TRedisClient.RPushX(aKey: String; aElements: tArray<string>);
begin

end;

function TRedisClient.AsBool(aResult: TArray<string>): boolean;
begin
  if length(aResult) <> 1 then
    raise Exception.Create('?!?');

  result := Copy(aResult[0], 1, 2) = 'OK';
end;

function TRedisClient.AsFloat(aResult: TArray<string>): Real;
begin
  if length(aResult) <> 1 then
    raise Exception.Create('?!?');

  result := aResult[0].ToDouble;
end;

function TRedisClient.AsInteger(aResult: TArray<string>): integer;
begin
  if length(aResult) <> 1 then
    raise Exception.Create('?!?');

  result := aResult[0].ToInteger;
end;

function TRedisClient.AsString(aResult: TArray<string>): string;
begin
  if length(aResult) <> 1 then
    raise Exception.Create('?!?');

  result := aResult[0];
end;

procedure TRedisClient.SAdd(aKey: String; aMembers: tArray<string>);
begin

end;

procedure TRedisClient.SAdd(aKey, aMember: String);
begin

end;

function TRedisClient.SCard(aKey: String): Integer;
begin

end;

function TRedisClient.SDiff(aKey: String;
  aKeys: TArray<string>): TArray<String>;
begin

end;

procedure TRedisClient.SDiffStore(aDestiny: String; aKeys: TArray<string>);
begin

end;

function TRedisClient.sendCommand(cmd: TArray<string>): TArray<string>;
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

  for i := 0 to length(sl) - 1 do
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

// String
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
  cmd: TArray<string>;
  i: integer;
begin
  i := 1;
  setLength(cmd, i);
  cmd[0] := 'GETEX';
  if aEx > 0 then
  begin
    inc(i, 2);
    setLength(cmd, i);
    cmd[i - 2] := 'EX';
    cmd[i - 1] := aEx.ToString;
  end;

  if aPx > 0 then
  begin
    if i > 1 then
      raise Exception.Create('Mais de um parâmetro de tempo setado');
    inc(i, 2);
    setLength(cmd, i);
    cmd[i - 2] := 'PX';
    cmd[i - 1] := aPx.ToString;
  end;

  if aExAt > 0 then
  begin
    if i > 1 then
      raise Exception.Create('Mais de um parâmetro de tempo setado');
    inc(i, 2);
    setLength(cmd, i);
    cmd[i - 2] := 'EXAT';
    cmd[i - 1] := aExAt.ToString;
  end;

  if aPxAt > 0 then
  begin
    if i > 1 then
      raise Exception.Create('Mais de um parâmetro de tempo setado');
    inc(i, 2);
    setLength(cmd, i);
    cmd[i - 2] := 'PXAT';
    cmd[i - 1] := aPxAt.ToString;
  end;

  if aPersist then
  begin
    if i > 1 then
      raise Exception.Create('Mais de um parâmetro de tempo setado');
    inc(i);
    setLength(cmd, i);
    cmd[i - 1] := 'PERSIST';
  end;

  result := AsBool(SendCommand(cmd));
end;

function TRedisClient.GetRange(const aKey: string; aStart, aEnd: Integer): string;
begin
  result := AsString(SendCommand(['GETRANGE', aKey, aStart.ToString, aEnd.toString]));
end;

function TRedisClient.GetSet(const aKey, aValue: string): string;
begin
  result := AsString(SendCommand(['GETSET', aKey, aValue]));
end;

function TRedisClient.Incr(const akey: string): integer;
begin
  result := AsInteger(SendCommand(['INCR', akey]));
end;

function TRedisClient.IncrBy(const aKey: string; aValue: integer): integer;
begin
  result := AsInteger(SendCommand(['INCRBY', aKey, aValue.ToString]));
end;

function TRedisClient.IncrByFloat(const aKey: string; aValue: Real): Real;
begin
  result := AsFloat(SendCommand(['INCRBYFLOAT', aKey, floatToStr(aValue, tFormatSettings.Invariant)]));
end;

function TRedisClient.LIndex(aKey: string; aIndex: integer): String;
begin

end;

procedure TRedisClient.LInsert(aKey: string; aRelative: TListRelative; aPivot,
  aItem: string);
begin

end;

function TRedisClient.LLen(aKey: string): integer;
begin

end;

procedure TRedisClient.LMove(aSource, aDestination: string; popFrom,
  pushTo: TListPoint);
begin

end;

function TRedisClient.LPop(aKey: string; aCount: Integer): TArray<String>;
begin

end;

function TRedisClient.LPos(aKey, aElement: string; aRank, aCount,
  aMaxLen: integer): TArray<integer>;
begin

end;

procedure TRedisClient.LPush(aKey: String; aElements: tArray<string>);
begin

end;

procedure TRedisClient.LPush(aKey, aElement: String);
begin

end;

procedure TRedisClient.LPushX(aKey, aElement: String);
begin

end;

procedure TRedisClient.LPushX(aKey: String; aElements: tArray<string>);
begin

end;

function TRedisClient.LRange(aKey: String; aStart,
  aStop: integer): TArray<String>;
begin

end;

procedure TRedisClient.LRem(aKey: string; aCount: integer; aElement: string);
begin

end;

procedure TRedisClient.LSet(aKey: string; aIndex: integer; aElement: string);
begin

end;

procedure TRedisClient.LTrim(aKey: String; aStart, aStop: integer);
begin

end;

function TRedisClient.MGet(const aKeys: tarray<string>): tArray<string>;
begin

end;

function TRedisClient.MPairToArray(aPairs: tArray<TPair<string, string>>): tArray<string>;
var
  i: integer;
begin
  if Length(aPairs) = 0 then
    raise Exception.Create('Número errado de parâmetros');
  setLength(result, length(aPairs) * 2);
  for i := 0 to length(aPairs) - 1 do
  begin
    result[i * 2] := aPairs[i].Key;
    result[i * 2 + 1] := aPairs[i].Value;
  end;
end;

function TRedisClient.MSet(const aKeyValues: tarray<TPair<string, string>>): Boolean;
begin
  result := AsBool(SendCommand(['MSET'] + MPairToArray(aKeyValues)));
end;

function TRedisClient.MSetNX(const aKeyValues: tArray<TRedisKeyValue>): Boolean;
begin
  result := AsBool(SendCommand(['MSETNX'] + MPairToArray(aKeyValues)));
end;

function TRedisClient.Pipeline: IRedisTransaction;
begin
  result := TRedisTransaction.create(fConn);
end;

function TRedisClient.PSetEx(const aKey: string; aMs: uint32; aValue: string): Boolean;
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

function TRedisClient.SInter(aKey: String;
  aKeys: TArray<string>): TArray<String>;
begin

end;

procedure TRedisClient.SInterStore(aDestiny: String; aKeys: TArray<string>);
begin

end;

function TRedisClient.SIsMember(aKey, aMember: String): Boolean;
begin

end;

function TRedisClient.SMembers(aKey: String): TArray<String>;
begin

end;

function TRedisClient.SMIsMember(aKey: String;
  aMembers: TArray<String>): TArray<Boolean>;
begin

end;

procedure TRedisClient.SMove(aSource, aDestiny, aMember: String);
begin

end;

function TRedisClient.SPop(aKey: String; aCount: integer): TArray<String>;
begin

end;

function TRedisClient.SRandMember(aKey: String;
  aCount: integer): TArray<String>;
begin

end;

procedure TRedisClient.SRem(aKey: String; aMembers: TArray<string>);
begin

end;

function TRedisClient.StrLen(const aKey: string): uint32;
begin
  result := AsInteger(SendCommand(['STRLEN', aKey]));
end;

function TRedisClient.SubStr(const aKey: string; aStart, aEnd: Integer): string;
begin
  result := AsString(SendCommand(['SUBSTR', aKey, aStart.ToString, aEnd.ToString]));
end;

function TRedisClient.SUnion(aKeys: TArray<string>): TArray<String>;
begin

end;

procedure TRedisClient.SUnionStore(aDestiny: String; aKeys: TArray<string>);
begin

end;

// Hash
function TRedisClient.HDel(const aKey: string; AFields: TArray<string>): Boolean;
begin
  result := AsBool(SendCommand(['HDEL', aKey] + AFields));
end;

function TRedisClient.HExists(const aKey, aField: string): Boolean;
begin
  result := AsBool(SendCommand(['HEXISTS', aKey, aField]));
end;

function TRedisClient.HGet(const aKey, aField: string): string;
begin
  result := AsString(SendCommand(['HGET', aKey, aField]));
end;

function TRedisClient.HGetAll(const aKey: string): TArray<TRedisKeyValue>;
begin
  // Como vem?
end;

function TRedisClient.HIncrBy(const aKey, aField: string; aValue: integer): integer;
begin
  result := AsInteger(SendCommand(['HINCRBY', aKey, aField, aValue.toString]));
end;

function TRedisClient.HIncrByFloat(const aKey, aField: string; aValue: Real): Real;
begin
  result := AsFloat(SendCommand(['HINCRBY', aKey, aField, floatToStr(aValue, tFormatSettings.Invariant)]));
end;

function TRedisClient.HKeys(const aKey: string): TArray<string>;
begin
  // Como vem?
end;

function TRedisClient.HLen(const aKey: string): integer;
begin
  result := AsInteger(SendCommand(['HLEN', aKey]));
end;

function TRedisClient.HMGet(const aKey: string; aFields: TArray<string>): TArray<TRedisKeyValue>;
begin
  // Como vem?
end;

function TRedisClient.HMSet(const aKey: string; aPairs: TArray<TRedisKeyValue>): Boolean;
begin
  result := AsBool(SendCommand(['HMSET'] + MPairToArray(aPairs)));
end;

function TRedisClient.HRandField(const aKey: string; aCount: Integer; aWithValues: Boolean): string;
begin
  // Como vem?
end;

function TRedisClient.HSet(const aKey: string; aFields: TArray<TRedisKeyValue>): Boolean;
begin
  result := AsBool(SendCommand(['HSET'] + MPairToArray(aFields)));

end;

function TRedisClient.HSetNX(const aKey, aField, aValue: string): Boolean;
begin
  result := AsBool(SendCommand(['HSETNX', aKey, aField, aValue]));
end;

function TRedisClient.HStrLen(const aKey, aField: string): uint32;
begin
  result := AsInteger(SendCommand(['HSTRLEN', aKey, aField]));
end;

function TRedisClient.HVals(const aKey: string): TArray<TPair<string, string>>;
begin
  // Como vem?
end;



{ TRedisTransaction }

function TRedisTransaction.AsBool(aResult: TArray<string>): Boolean;
begin
  result := True;
end;

function TRedisTransaction.AsFloat(aResult: TArray<string>): Real;
begin
  result := 0;
end;

function TRedisTransaction.AsInteger(aResult: TArray<string>): Integer;
begin
  result := 0;
end;

function TRedisTransaction.AsString(aResult: TArray<string>): string;
begin
  result := '';
end;

constructor TRedisTransaction.create(AConnection: TIdTCPClient);
begin
  fConn := AConnection;
end;

procedure TRedisTransaction.Discard;
begin
  SetLength(fWatchStack, 0);
  SetLength(fStack, 0);
end;

procedure TRedisTransaction.Exec;
begin
  if Length(fStack) < 1 then
    exit;

  if Length(fWatchStack) > 0 then
  begin
    fConn.IOHandler.Write(Serialize(['WATCH'] + fWatchStack));
    ReadFromServer;
  end;

  fConn.IOHandler.Write(string.join('', FStack));
  ReadFromServer(length(fStack));  // Lê múltiplas vezes? TESTAR

  Discard;
end;

function TRedisTransaction.SendCommand(cmd: TArray<string>): TArray<string>;
var
  s: string;
  i: Integer;
begin
  s := Serialize(cmd);
  i := length(fStack);

  if i = 0 then
  begin
    fStack := [Serialize(['MULTI']), ''];
    i := 1;
  end
  else
    setLength(fStack, i + 1);

  fStack[i] := s;
end;

procedure TRedisTransaction.Unwatch;
begin
  setLength(fWatchStack, 0);
end;

procedure TRedisTransaction.Watch(aKeyList: TArray<string>);
var
  i, j: integer;
begin
  if length(fStack) > 0 then
    raise Exception.Create('Watch não pode ser usado dentro de transação');
  i := length(fWatchStack);
  setLength(fWatchStack, i + length(aKeyList));
  for j := 0 to length(aKeyList) do
    fWatchStack[i + j] := aKeyList[j];
end;

end.

