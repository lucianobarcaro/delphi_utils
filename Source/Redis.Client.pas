unit Redis.Client;

interface

uses
  System.SysUtils,
  System.Generics.Collections,
  System.SyncObjs,
  System.Classes,
  IdTCPClient;

type
  TRedisKeyValue = TPair<string, string>;
  TRedisScoreMember = TPair<Integer, string>;
  TListPoint = (LEFT, RIGHT);
  TListRelative = (BEFORE, AFTER);
  TListenerCallBack = Procedure (aChannel, aMessage: String);

  TRedisTransaction = class;
  TRedisListener = class;

  TRedisBase = class
  private
    fConn: TIdTCPClient;
    fHost, fPassword: string;
    Function Serialize(sl: TArray<string>): string;
    Function ReadFromServer(aTimes: integer = 1): TArray<string>;
    Function MPairToArray(aPairs: tArray<TRedisKeyValue>): tArray<string>; overload;
    Function MPairToArray(aPairs: tArray<TRedisScoreMember>): tArray<string>; overload;
    Function ArrayToMPair(aElements: tArray<string>): TArray<TRedisKeyValue>;
    Function ArrayToMScoreMembers(aElements: TArray<string>): TArray<TRedisScoreMember>;
  protected
    Function AsArray(aResult: TArray<string>): TArray<String>; virtual;
    Function AsArrayBool(aResult: TArray<string>): TArray<Boolean>; virtual;
    Function AsArrayInt(aResult: TArray<string>): TArray<Integer>; virtual;
    Function AsBool(aResult: TArray<string>): Boolean; virtual;
    Function AsString(aResult: TArray<string>): string; virtual;
    Function AsInteger(aResult: TArray<string>): Integer; virtual;
    Function AsFloat(aResult: TArray<string>): Real; virtual;
    Function SendCommand(cmd: TArray<string>): TArray<string>; virtual;
  end;

  TRedisClient = class(TRedisBase)
  protected
    function ZInterMakeCmd(aCMD, aDestiny: string; aNumKeys: integer; aKeys: TArray<string>; aWeights: TArray<single>; aAggregate: string; aWithScore: Boolean): TArray<string>;
  public
    destructor Destroy; override;

    Function Connect(aHost: string; aPort: UInt16; aPassword: string = ''): Boolean;

    // Transaction
    Function Pipeline: TRedisTransaction;
    // Listener
    Function Listen(aChannel: String=''): TRedisListener;

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
    Procedure MSet(const aKeyValues: TArray<TRedisKeyValue>);
    Procedure MSetNX(const aKeyValues: TArray<TRedisKeyValue>);
    Procedure PSetEx(const aKey: string; aMs: uint32; aValue: string);
    Procedure &Set(const aKey, aValue: string);
    Procedure SetEx(const aKey: string; aTTL: uint32; aValue: string);
    Procedure SetNX(const aKey, aValue: string);
    Procedure SetRange(const aKey: string; aOffset: uint32; aValue: string);
    Function StrLen(const aKey: string): uint32;
    Function SubStr(const aKey: string; aStart, aEnd: Integer): string;

    // Hash --------------------------------------------------------------------
    Procedure HDel(const aKey: string; AFields: TArray<string>);
    Function HExists(const aKey, aField: string): Boolean;
    Function HGet(const aKey, aField: string): string;
    Function HGetAll(const aKey: string): TArray<TRedisKeyValue>;
    Function HIncrBy(const aKey, aField: string; aValue: integer): integer;
    Function HIncrByFloat(const aKey, aField: string; aValue: Real): Real;
    Function HKeys(const aKey: string): TArray<string>;
    Function HLen(const aKey: string): integer;
    Function HMGet(const aKey: string; aFields: TArray<string>): TArray<TRedisKeyValue>;
    Procedure HMSet(const aKey: string; aPairs: TArray<TRedisKeyValue>);
    Function HRandField(const aKey: string; aCount: Integer = 0; aWithValues: Boolean = False): TArray<String>;
    // function HScan
    procedure HSet(const aKey: string; aFields: TArray<TRedisKeyValue>);
    Procedure HSetNX(const aKey, aField, aValue: string);
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
    Function LPos(aKey, aElement: string; aRank:integer=1; aCount: integer=0; aMaxLen:integer=0): TArray<Integer>;
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

    // Sorted Sets -------------------------------------------------------------
    Function BZPopMax(aKey: String; aTimeout: integer): String; overload;
    Function BZPopMax(aKeys: TArray<String>; aTimeout: integer): String; overload;
    Function BZPopMin(aKey: String; aTimeout: integer): String; overload;
    Function BZPopMin(aKeys: TArray<String>; aTimeout: integer): String; overload;
    Procedure ZAdd(aKey: String; aScoreMember: TRedisScoreMember; aNxXx: String=''; aGtLt: String=''; aCh: boolean=False; aIncr:Boolean=False); overload;
    Procedure ZAdd(aKey: String; aScoreMembers: TArray<TRedisScoreMember>; aNxXx: String=''; aGtLt: String=''; aCh: boolean=False; aIncr:Boolean=False); overload;
    Function ZCard(akey: String): Integer;
    Function ZCount(aKey: String; aMin, aMax: Integer): Integer;
    Function ZDiff(aNumKeys: Integer; aKey: String): TArray<string>; overload;
    Function ZDiff(aNumKeys: Integer; aKeys: tArray<String>): TArray<string>; overload;
    Function ZDiffWithScores(aNumKeys: Integer; aKey: String): TArray<TRedisScoreMember>; overload;
    Function ZDiffWithScores(aNumKeys: Integer; aKeys: tArray<String>): TArray<TRedisScoreMember>; overload;
    Procedure ZDiffStore(aDestiny: String; aNumKeys: Integer; aKey: String); overload;
    Procedure ZDiffStore(aDestiny: String; aNumKeys: Integer; aKeys: tArray<String>); overload;
    Function ZIncrBy(aKey: string; aIncrement: integer; aMember: string): Integer;
    Function ZInter(aNumKeys: integer; aKey: string; aWeights: TArray<Single>=[]; aAggregate: string=''): tArray<string>; overload;
    Function ZInter(aNumKeys: integer; aKeys: TArray<string>; aWeights: TArray<Single>=[]; aAggregate: string=''): tArray<string>; overload;
    Function ZInterWithScores(aNumKeys: integer; aKey: string; aWeights: TArray<Single>=[]; aAggregate: string=''): tArray<TRedisScoreMember>; overload;
    Function ZInterWithScores(aNumKeys: integer; aKeys: TArray<string>; aWeights: TArray<Single>=[]; aAggregate: string=''): tArray<TRedisScoreMember>; overload;
    Procedure ZInterStore(aDestiny: string; aNumKeys: integer; aKey: string; aWeights: TArray<Single>=[]; aAggregate: string=''); overload;
    Procedure ZInterStore(aDestiny: string; aNumKeys: integer; aKeys: TArray<string>; aWeights: TArray<Single>=[]; aAggregate: string=''); overload;
    Function ZLexCount(aKey: string; aMin, aMax: integer): integer;
    Function ZMScore(akey: string; aMember: string): Integer; overload;
    Function ZMScore(akey: string; aMembers: TArray<string>): TArray<Integer>; overload;
    Function ZMPopMax(aKey: string; aCount:integer=1): TArray<TRedisScoreMember>;
  end;

  TRedisTransaction = class(TRedisClient)
  private
    fStack: TArray<string>;
    fWatchStack: TArray<string>;
  protected
    function AsArray(aResult: TArray<string>): TArray<String>; override;
    function AsArrayBool(aResult: TArray<string>): TArray<Boolean>; override;
    function AsArrayInt(aResult: TArray<string>): TArray<Integer>; override;
    function AsBool(aResult: TArray<string>): Boolean; override;
    function AsString(aResult: TArray<string>): string; override;
    function AsInteger(aResult: TArray<string>): Integer; override;
    function AsFloat(aResult: TArray<string>): Real; override;
    function SendCommand(cmd: TArray<string>): TArray<string>; override;
  public
    constructor create(AConnection: TIdTCPClient);
    procedure Watch(aKeyList: TArray<string>);
    procedure Unwatch;
    procedure Exec;
    procedure Discard;
  end;

  TRedisListener = class(TRedisBase)
  private
    fThread: TThread;
    fCS: TCriticalSection;
    fCallback: TListenerCallBack;
    Procedure ThreadReader;
  public
    constructor create(aHost, aPassword:String);
    destructor destroy; override;
    Function Subscribe(aChannel: String): Integer; overload;
    Function Subscribe(aChannel: TArray<String>): Integer; overload;
    Procedure UnSubscribe(aChannel: String); overload;
    Procedure UnSubscribe(aChannel: TArray<String>); overload;

    property Callback: TListenerCallBack read fCallback write fCallback;
  end;

  implementation

uses System.TypInfo;

{ TRedisBase }
function TRedisBase.ArrayToMPair(aElements: tArray<string>): TArray<TRedisKeyValue>;
var
  i, sz: integer;
begin
  sz := Length(aElements);
  if sz mod 2 <> 0 then
    raise Exception.Create('Número errado de parâmetros');
  setLength(result, sz div 2);
  i := 0;
  while i < sz do
  begin
    result[i div 2] := TRedisKeyValue.create(aElements[i], aElements[i+1]);
    inc(i, 2);
  end;
end;

function TRedisBase.ArrayToMScoreMembers(aElements: TArray<string>): TArray<TRedisScoreMember>;
var
  i, sz: integer;
begin
  sz := Length(aElements);
  if sz mod 2 <> 0 then
    raise Exception.Create('Número errado de parâmetros');
  setLength(result, sz div 2);
  i := 0;
  while i < sz do
  begin
    result[i div 2] := TRedisScoreMember.create(StrToInt(aElements[i+1]), aElements[i]);
    inc(i, 2);
  end;
end;

function TRedisBase.AsArray(aResult: TArray<string>): TArray<String>;
begin

end;

function TRedisBase.AsArrayBool(aResult: TArray<string>): TArray<Boolean>;
begin

end;

function TRedisBase.AsArrayInt(aResult: TArray<string>): TArray<Integer>;
begin

end;

function TRedisBase.AsBool(aResult: TArray<string>): boolean;
begin
  if length(aResult) <> 1 then
    raise Exception.Create('?!?');

  result := Copy(aResult[0], 1, 2) = 'OK';
end;

function TRedisBase.AsFloat(aResult: TArray<string>): Real;
begin
  if length(aResult) <> 1 then
    raise Exception.Create('?!?');

  result := aResult[0].ToDouble;
end;

function TRedisBase.AsInteger(aResult: TArray<string>): integer;
begin
  if length(aResult) <> 1 then
    raise Exception.Create('?!?');

  result := aResult[0].ToInteger;
end;

function TRedisBase.AsString(aResult: TArray<string>): string;
begin
  if length(aResult) <> 1 then
    raise Exception.Create('?!?');

  result := aResult[0];
end;

function TRedisBase.MPairToArray(aPairs: tArray<TRedisScoreMember>): tArray<string>;
var
  i: integer;
begin
  if Length(aPairs) = 0 then
    raise Exception.Create('Número errado de parâmetros');
  setLength(result, length(aPairs) * 2);
  for i := 0 to length(aPairs) - 1 do
  begin
    result[i * 2] := aPairs[i].Key.ToString;
    result[i * 2 + 1] := aPairs[i].Value;
  end;
end;

function TRedisBase.MPairToArray(aPairs: tArray<TRedisKeyValue>): tArray<string>;
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

function TRedisBase.ReadFromServer(aTimes: integer): tarray<string>;
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

function TRedisBase.SendCommand(cmd: TArray<string>): TArray<string>;
var
  s: string;
begin
  s := serialize(cmd);
  fConn.IOHandler.Write(s);

  result := readFromServer;
end;

function TRedisBase.Serialize(sl: TArray<string>): string;
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

{ TRedisClient }
function TRedisClient.auth(const aPassword: string): Boolean;
begin
  fPassword := '';
  result := AsBool(sendCommand(['AUTH', aPassword]));
  if result then
    fPassword := aPassword;
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
  result := AsString(SendCommand(['BLPOP'] + aKeys + [aTimeout.ToString]));
end;

function TRedisClient.BRPop(aKeys: TArray<string>; aTimeout: Integer): string;
begin
  result := AsString(SendCommand(['BRPOP'] + aKeys + [aTimeout.ToString]));
end;

function TRedisClient.BRPoplPush(aSource, aDestination: string; aTimeout: integer): String;
begin
  result := AsString(SendCommand(['BRPOPLPUSH', aSource, aDestination, aTimeout.ToString]));
end;

function TRedisClient.BZPopMax(aKey: String; aTimeout: integer): String;
begin
  result := BZPopMax([aKey], aTimeout);
end;

function TRedisClient.BZPopMax(aKeys: TArray<String>; aTimeout: integer): String;
begin
  result := AsString(SendCommand(['BZPOPMAX'] + aKeys + [aTimeout.ToString]));
end;

function TRedisClient.BZPopMin(aKey: String; aTimeout: integer): String;
begin
  result := BZPopMin([aKey], aTimeout);
end;

function TRedisClient.BZPopMin(aKeys: TArray<String>; aTimeout: integer): String;
begin
  result := AsString(SendCommand(['BZPOPMIN'] + aKeys + [aTimeout.ToString]));
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

  fHost := aHost;

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

function TRedisClient.RPop(aKey: string; aCount: Integer): TArray<String>;
begin
  result := AsArray(SendCommand(['RPOP', aKey, aCount.ToString]));
end;

function TRedisClient.RPoplPush(aSource, aDestination: string): String;
begin
  result := AsString(SendCommand(['RPOPLPUSH', aSource, aDestination]));
end;

procedure TRedisClient.RPush(aKey, aElement: String);
begin
  RPush(aKey, [aElement]);
end;

procedure TRedisClient.RPush(aKey: String; aElements: tArray<string>);
begin
  SendCommand(['RPUSH'] + aElements);
end;

procedure TRedisClient.RPushX(aKey, aElement: String);
begin
  RPushX(aKey, [aElement]);
end;

procedure TRedisClient.RPushX(aKey: String; aElements: tArray<string>);
begin
  SendCommand(['RPUSHX'] + aElements);
end;

procedure TRedisClient.SAdd(aKey: String; aMembers: tArray<string>);
begin
  SendCommand(['SADD'] + aMembers);
end;

procedure TRedisClient.SAdd(aKey, aMember: String);
begin
  SAdd(aKey, [aMember]);
end;

function TRedisClient.SCard(aKey: String): Integer;
begin
  result := AsInteger(SendCommand(['SCARD', aKey]));
end;

function TRedisClient.SDiff(aKey: String; aKeys: TArray<string>): TArray<String>;
begin
  result := AsArray(SendCommand(['SDIFF'] + akeys));
end;

procedure TRedisClient.SDiffStore(aDestiny: String; aKeys: TArray<string>);
begin
  SendCommand(['SDIFFSTORE'] + akeys);
end;

Procedure TRedisClient.&Set(const aKey, aValue: string);
begin
  sendCommand(['SET', aKey, aValue]);
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
  result := AsString(SendCommand(['LINDEX', aKey, aIndex.ToString]));
end;

procedure TRedisClient.LInsert(aKey: string; aRelative: TListRelative; aPivot, aItem: string);
begin
  SendCommand(['LINSERT', aKey, GetEnumName(TypeInfo(TListRelative), ord(aRelative)), aPivot, aItem]);
end;

function TRedisClient.Listen(aChannel: String): TRedisListener;
begin
  result := TRedisListener.create(fHost, fPassword);
  if aChannel <> '' then
    result.Subscribe(aChannel);
end;

function TRedisClient.LLen(aKey: string): integer;
begin
  SendCommand(['LLEN', aKey]);
end;

procedure TRedisClient.LMove(aSource, aDestination: string; popFrom, pushTo: TListPoint);
begin
  SendCommand(['LMOVE', aSource, aDestination,
    GetEnumName(TypeInfo(TListPoint), ord(popFrom)),
    GetEnumName(TypeInfo(TListPoint), ord(pushTo))
  ]);
end;

function TRedisClient.LPop(aKey: string; aCount: Integer): TArray<String>;
begin
  result := AsArray(SendCommand(['LPOP', aCount.ToString]));
end;

function TRedisClient.LPos(aKey, aElement: string; aRank, aCount, aMaxLen: integer): TArray<Integer>;
begin
  result := AsArrayInt(SendCommand(['LPOS', aKey, aElement, aRank.ToString, aMaxLen.ToString]));
end;

procedure TRedisClient.LPush(aKey: String; aElements: tArray<string>);
begin
  SendCommand(['LPUSH'] + aElements);
end;

procedure TRedisClient.LPush(aKey, aElement: String);
begin
  LPush(aKey, [aElement]);
end;

procedure TRedisClient.LPushX(aKey, aElement: String);
begin
  LPushX(aKey, [aElement]);
end;

procedure TRedisClient.LPushX(aKey: String; aElements: tArray<string>);
begin
  SendCommand(['LPUSHX'] + aElements);
end;

function TRedisClient.LRange(aKey: String; aStart, aStop: integer): TArray<String>;
begin
  result := asArray(sendCommand(['LRANGE', aKey, aStart.ToString, aStop.ToString]));
end;

procedure TRedisClient.LRem(aKey: string; aCount: integer; aElement: string);
begin
  SendCommand(['LREM', aKey, aCount.ToString, aElement]);
end;

procedure TRedisClient.LSet(aKey: string; aIndex: integer; aElement: string);
begin
  SendCommand(['LSET', aKey, aIndex.ToString, aElement]);
end;

procedure TRedisClient.LTrim(aKey: String; aStart, aStop: integer);
begin
  SendCommand(['LTRIM', aKey, aStart.ToString, aStop.ToString]);
end;

function TRedisClient.MGet(const aKeys: tarray<string>): tArray<string>;
begin
  result := AsArray(SendCommand(['MGET']+ aKeys));
end;

Procedure TRedisClient.MSet(const aKeyValues: tarray<TPair<string, string>>);
begin
  SendCommand(['MSET'] + MPairToArray(aKeyValues));
end;

Procedure TRedisClient.MSetNX(const aKeyValues: tArray<TRedisKeyValue>);
begin
  SendCommand(['MSETNX'] + MPairToArray(aKeyValues));
end;

function TRedisClient.Pipeline: TRedisTransaction;
begin
  result := TRedisTransaction.create(fConn);
end;

Procedure TRedisClient.PSetEx(const aKey: string; aMs: uint32; aValue: string);
begin
  SendCommand(['PSETEX', aMs.ToString, aValue]);
end;

Procedure TRedisClient.SetEx(const aKey: string; aTTL: uint32; aValue: string);
begin
  SendCommand(['SETEX', aTTL.ToString, aValue]);
end;

Procedure TRedisClient.SetNX(const aKey, aValue: string);
begin
  SendCommand(['SETNX', aValue]);
end;

Procedure TRedisClient.SetRange(const aKey: string; aOffset: uint32; aValue: string);
begin
  SendCommand(['SETRANGE', aOffset.toString, aValue]);
end;

function TRedisClient.SInter(aKey: String; aKeys: TArray<string>): TArray<String>;
begin
  result := AsArray(SendCommand(['SINTER'] + aKeys));
end;

procedure TRedisClient.SInterStore(aDestiny: String; aKeys: TArray<string>);
begin
  SendCommand(['SINTERSTORE'] + aKeys)
end;

function TRedisClient.SIsMember(aKey, aMember: String): Boolean;
begin
  Result := asBool(SendCommand(['SISMEMBER', aKey, aMember]));
end;

function TRedisClient.SMembers(aKey: String): TArray<String>;
begin
  result := AsArray(SendCommand(['SMEMBERS', akey]));
end;

function TRedisClient.SMIsMember(aKey: String; aMembers: TArray<String>): TArray<Boolean>;
begin
  result := AsArrayBool(SendCommand(['SMISMEMBER'] + aMembers));
end;

procedure TRedisClient.SMove(aSource, aDestiny, aMember: String);
begin
  SendCommand(['SMOVE', aSource, aDestiny, aMember]);
end;

function TRedisClient.SPop(aKey: String; aCount: integer): TArray<String>;
begin
  result := AsArray(SendCommand(['SPOP', aKey, aCount.ToString]));
end;

function TRedisClient.SRandMember(aKey: String; aCount: integer): TArray<String>;
begin
  result := AsArray(SendCommand(['SRANDMEMBER', aKey, aCount.ToString]));
end;

procedure TRedisClient.SRem(aKey: String; aMembers: TArray<string>);
begin
  SendCommand(['SREM'] + aMembers);
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
  result := AsArray(SendCommand(['SUNION'] + aKeys));
end;

procedure TRedisClient.SUnionStore(aDestiny: String; aKeys: TArray<string>);
begin
  SendCommand(['SUNIONSTORE'] + aKeys)
end;

procedure TRedisClient.ZAdd(aKey: String; aScoreMember: TRedisScoreMember; aNxXx, aGtLt: String; aCh, aIncr: Boolean);
begin
  ZAdd(aKey, [aScoreMember], aNxXx, aGtLt, aCh, aIncr);
end;

procedure TRedisClient.ZAdd(aKey: String; aScoreMembers: TArray<TRedisScoreMember>; aNxXx, aGtLt: String; aCh, aIncr: Boolean);
var
  cmd: tArray<String>;
begin
  cmd := ['ZADD', aKey] + MPairToArray(aScoreMembers);

  aNxXx := UpperCase(aNxXx);
  if aNxXx <> '' then
    if (aNxXx <> 'NX') and (aNxXx <> 'XX') then
      raise Exception.Create('Error Message')
    else
      cmd := cmd + [aNxXx];

  aGtLt := UpperCase(aGtLt);
  if aGtLt <> '' then
    if (aGtLt <> 'GT') and (aGtLt <> 'LT') then
      raise Exception.Create('Error Message')
    else
      cmd := cmd + [aGtLt];

  if aCh then
    cmd := cmd + ['CH'];

  if aIncr then
    cmd := cmd + ['INCR'];

  SendCommand(cmd);
end;

function TRedisClient.ZCard(aKey: String): Integer;
begin
  result := AsInteger(SendCommand(['ZCARD', aKey]));
end;

function TRedisClient.ZCount(aKey: String; aMin, aMax: Integer): Integer;
begin
  result := AsInteger(SendCommand(['ZCOUNT', aKey, aMin.ToString, aMax.ToString]));
end;

function TRedisClient.ZDiff(aNumKeys: Integer; aKey: String): TArray<string>;
begin
  result := ZDiff(aNumKeys, [aKey]);
end;

function TRedisClient.ZDiff(aNumKeys: Integer; aKeys: tArray<String>): TArray<string>;
begin
  result := AsArray(SendCommand(['ZDIFF', aNumKeys.ToString] + aKeys));

end;

procedure TRedisClient.ZDiffStore(aDestiny: String; aNumKeys: Integer; aKeys: tArray<String>);
begin
  SendCommand(['ZDIFFSTORE', aDestiny, aNumKeys.ToString] + aKeys);
end;

procedure TRedisClient.ZDiffStore(aDestiny: String; aNumKeys: Integer; aKey: String);
begin
  ZDiffStore(aDestiny, aNumKeys, [aKey]);
end;

function TRedisClient.ZDiffWithScores(aNumKeys: Integer; aKey: String): TArray<TRedisScoreMember>;
begin
  result := ZDiffWithScores(aNumKeys, [aKey]);
end;

function TRedisClient.ZDiffWithScores(aNumKeys: Integer; aKeys: tArray<String>): TArray<TRedisScoreMember>;
begin
  result := ArrayToMScoreMembers(SendCommand(['ZDIFF', aNumKeys.ToString] + aKeys + ['WITHSCORES']));
end;

function TRedisClient.ZIncrBy(aKey: string; aIncrement: integer; aMember: string): Integer;
begin
  result := AsInteger(SendCommand(['ZINCRBY', aKey, aIncrement.ToString, aMember]));
end;

function TRedisClient.ZInterMakeCmd(aCMD, aDestiny: string; aNumKeys: integer; aKeys: TArray<string>; aWeights: TArray<single>; aAggregate: string; aWithScore: Boolean): TArray<string>;
var
  i: integer;
begin
  result := [aCmd];
  if aDestiny <> '' then
    result := result + [aDestiny];
  result := result + [aNumKeys.toString] + aKeys;

  for i := 0 to length(aWeights)-1 do
  begin
    if i=0 then
      result := result + ['WEIGHTS'];
    result := result + [aWeights[i].ToString];
  end;
  if aAggregate<>'' then
    result := result + ['AGGREGATE', aAggregate];
  if aWithScore then
    result := result + ['WITHSCORES'];
end;

function TRedisClient.ZInter(aNumKeys: integer; aKey: string; aWeights: TArray<Single>; aAggregate: string): tArray<string>;
begin
  result := ZInter(aNumKeys, [aKey], aWeights, aAggregate);
end;

function TRedisClient.ZInter(aNumKeys: integer; aKeys: TArray<string>; aWeights: TArray<Single>; aAggregate: string): tArray<string>;
begin
  result := AsArray(SendCommand(ZInterMakeCmd('ZINTER', '', aNumKeys, aKeys, aWeights, aAggregate, False)));
end;

procedure TRedisClient.ZInterStore(aDestiny: string; aNumKeys: integer; aKeys: TArray<string>; aWeights: TArray<Single>; aAggregate: string);
begin
  SendCommand(ZInterMakeCmd('ZINTERSTORE', aDestiny, aNumKeys, aKeys, aWeights, aAggregate, False));
end;

procedure TRedisClient.ZInterStore(aDestiny: string; aNumKeys: integer; aKey: string; aWeights: TArray<Single>; aAggregate: string);
begin
  ZinterStore(aDestiny, aNumKeys, [akey], aWeights, aAggregate);
end;

function TRedisClient.ZInterWithScores(aNumKeys: integer; aKey: string; aWeights: TArray<Single>; aAggregate: string): tArray<TRedisScoreMember>;
begin
  result := ZInterWithScores(aNumKeys, [aKey], aWeights, aAggregate);
end;

function TRedisClient.ZInterWithScores(aNumKeys: integer; aKeys: TArray<string>; aWeights: TArray<Single>; aAggregate: string): tArray<TRedisScoreMember>;
begin
  result := ArrayToMScoreMembers(SendCommand(ZInterMakeCmd('ZINTER', '', aNumKeys, aKeys, aWeights, aAggregate, True)));
end;

function TRedisClient.ZLexCount(aKey: string; aMin, aMax: integer): integer;
begin
  result := AsInteger(SendCommand(['ZLEXCOUNT', akey, aMin.ToString, aMax.ToString]));
end;

function TRedisClient.ZMPopMax(aKey: string; aCount: integer): TArray<TRedisScoreMember>;
begin
  result := ArrayToMScoreMembers(SendCommand(['ZMPOPMAX', akey, aCount.ToString]));
end;

function TRedisClient.ZMScore(akey, aMember: string): Integer;
begin
  Result := ZMScore(aKey, [aMember])[0];
end;

function TRedisClient.ZMScore(akey: string; aMembers: TArray<string>): TArray<Integer>;
begin
  result := AsArrayInt(SendCommand(['ZMSCORE', akey] + aMembers));
end;

// Hash
Procedure TRedisClient.HDel(const aKey: string; AFields: TArray<string>);
begin
  SendCommand(['HDEL', aKey] + AFields);
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
  result := ArrayToMPair(SendCommand(['HGETALL', aKey]));
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
  result := AsArray(SendCommand(['HKEYS', aKey]));
end;

function TRedisClient.HLen(const aKey: string): integer;
begin
  result := AsInteger(SendCommand(['HLEN', aKey]));
end;

function TRedisClient.HMGet(const aKey: string; aFields: TArray<string>): TArray<TRedisKeyValue>;
begin
  result := ArrayToMPair(SendCommand(['HMGET', aKey] + aFields));
end;

Procedure TRedisClient.HMSet(const aKey: string; aPairs: TArray<TRedisKeyValue>);
begin
  SendCommand(['HMSET'] + MPairToArray(aPairs));
end;

function TRedisClient.HRandField(const aKey: string; aCount: Integer; aWithValues: Boolean): TArray<String>;
var
  cmd: tArray<String>;
begin
  cmd := ['HRANDFIELD', aKey, aCount.ToString];
  if aWithValues then
    cmd := cmd + ['WITHVALUES'];
  result := AsArray(SendCommand(cmd));
end;

procedure TRedisClient.HSet(const aKey: string; aFields: TArray<TRedisKeyValue>);
begin
  SendCommand(['HSET'] + MPairToArray(aFields));
end;

Procedure TRedisClient.HSetNX(const aKey, aField, aValue: string);
begin
  SendCommand(['HSETNX', aKey, aField, aValue]);
end;

function TRedisClient.HStrLen(const aKey, aField: string): uint32;
begin
  result := AsInteger(SendCommand(['HSTRLEN', aKey, aField]));
end;

function TRedisClient.HVals(const aKey: string): TArray<TRedisKeyValue>;
begin
  result := ArrayToMPair(SendCommand(['HVALS', aKey]));
end;



{ TRedisTransaction }
function TRedisTransaction.AsArray(aResult: TArray<string>): TArray<string>;
begin
  result := [];
end;

function TRedisTransaction.AsArrayBool(aResult: TArray<string>): TArray<Boolean>;
begin
  result := [];
end;

function TRedisTransaction.AsArrayInt(aResult: TArray<string>): TArray<Integer>;
begin
  result := [];
end;

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

{ TRedisListener }
function TRedisListener.Subscribe(aChannel: String): Integer;
begin
  Subscribe([aChannel]);
end;

constructor TRedisListener.create(aHost, aPassword: String);
begin
  fCS := TCriticalSection.create;
  fThread := TThread.CreateAnonymousThread(
  procedure
  begin
    repeat
      Sleep(10);
      fCS.Acquire;
      try
        ThreadReader;
      finally
        fCs.Release;
      end;
    until TThread.CheckTerminated;
  end
  );
end;

destructor TRedisListener.destroy;
begin
  fThread.terminate;
  fThread.WaitFor;
  fThread.Free;
  fCS.Free;
  inherited;
end;

function TRedisListener.Subscribe(aChannel: TArray<String>): integer;
begin
  fCS.Acquire;
  try
    SendCommand(['SUBSCRIBE'] + aChannel);
  finally
    fCS.Release;
  end;
end;

procedure TRedisListener.ThreadReader;
var
  ret: TArray<String>;
begin
  if not fConn.IOHandler.CheckForDataOnSource(10) then
    exit;
  ret := ReadFromServer;
  // Lê e interpreta o retorno.
  // TODO: Continuar aqui
end;

procedure TRedisListener.UnSubscribe(aChannel: String);
begin
  UnSubscribe([aChannel]);
end;

procedure TRedisListener.UnSubscribe(aChannel: TArray<String>);
begin
  fCS.Acquire;
  try
    SendCommand(['UNSUBSCRIBE'] + aChannel);
  finally
    fCS.Release;
  end;
end;

end.

