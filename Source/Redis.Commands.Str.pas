unit Redis.Commands.Str;

interface

uses
  Redis.Base,
  System.Generics.Collections;

type
  TRedisCommandsStr = class
  private
    fClient: IRedisClient;
  public
    constructor Create(AClient: IRedisClient);

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
  end;

implementation

uses
  System.SysUtils;

function TRedisCommandsStr.&Set(const aKey, aValue: string): boolean;
begin
  result := fClient.AsBool(fClient.sendCommand(['SET', aKey, aValue]));
end;

function TRedisCommandsStr.Append(const aKey, aValue: string): boolean;
begin
  result := fClient.Asbool(fClient.sendCommand(['APPEND', aKey, aValue]));
end;

constructor TRedisCommandsStr.Create(AClient: IRedisClient);
begin
  fClient := AClient;
end;

function TRedisCommandsStr.Decr(const aKey: string): boolean;
begin
  result := fClient.AsBool(fClient.SendCommand(['DECR', aKey]));
end;

function TRedisCommandsStr.DecrBy(const aKey: string; aValue: integer): boolean;
begin
  result := fClient.AsBool(fClient.SendCommand(['DECRBY', aKey, aValue.toString]));
end;

function TRedisCommandsStr.Get(const aKey: string): string;
begin
  result := fClient.AsString(fClient.SendCommand(['GET', aKey]));
end;

function TRedisCommandsStr.GetDel(const aKey: string): string;
begin
  result := fClient.AsString(fClient.SendCommand(['GETDEL', aKey]));
end;

function TRedisCommandsStr.GetEx(const aKey: string; aEx, aPx: integer;
  aExAt: uint32; aPxAt: uint64; aPersist: Boolean): Boolean;
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

  result := fClient.AsBool(fClient.SendCommand(cmd));
end;

function TRedisCommandsStr.GetRange(const aKey: string; aStart,
  aEnd: Integer): String;
begin
  result := fClient.AsString(fClient.SendCommand(['GETRANGE', aKey, aStart.ToString, aEnd.toString]));
end;

function TRedisCommandsStr.GetSet(const aKey, aValue: string): string;
begin
  result := fClient.AsString(fClient.SendCommand(['GETSET', aKey, aValue]));
end;

function TRedisCommandsStr.Incr(const akey: String): integer;
begin
  result := fClient.AsInteger(fClient.SendCommand(['INCR', aKey]));
end;

function TRedisCommandsStr.IncrBy(const aKey: string; aValue: integer): integer;
begin
  result := fClient.AsInteger(fClient.SendCommand(['INCRBY', aKey, aValue.ToString]));
end;

function TRedisCommandsStr.IncrByFloat(const aKey: string; aValue: Real): Real;
begin
  result := fClient.AsFloat(fClient.SendCommand(['INCRBYFLOAT', aKey, floatToStr(aValue, tFormatSettings.Invariant)]));
end;

function TRedisCommandsStr.MGet(const aKeys: tarray<string>): tArray<String>;
begin

end;

function TRedisCommandsStr.MSet(const aKeyValues: tarray<string>): Boolean;
begin
  if length(aKeyValues) mod 2 = 1 then
    raise Exception.Create('Numero errado de parâmetros');

  result := fClient.AsBool(fClient.SendCommand(['MSET'] + aKeyValues));
end;

function TRedisCommandsStr.MSetNX(const aKeyValues: tarray<string>): Boolean;
begin
  if length(aKeyValues) mod 2 = 1 then
    raise Exception.Create('Numero errado de parâmetros');

  result := fClient.AsBool(fClient.SendCommand(['MSETNX'] + aKeyValues));
end;

function TRedisCommandsStr.PSetEx(const aKey: string; aMs: uint32;
  aValue: String): Boolean;
begin
  result := fClient.AsBool(fClient.SendCommand(['PSETEX', aMs.ToString, aValue]));
end;

function TRedisCommandsStr.SetEx(const aKey: string; aTTL: uint32;
  aValue: string): Boolean;
begin
  result := fClient.AsBool(fClient.SendCommand(['SETEX', aTTL.ToString, aValue]));
end;

function TRedisCommandsStr.SetNX(const aKey, aValue: string): boolean;
begin
  result := fClient.AsBool(fClient.SendCommand(['SETNX', aValue]));
end;

function TRedisCommandsStr.SetRange(const aKey: string; aOffset: uint32;
  aValue: string): Boolean;
begin
  result := fClient.AsBool(fClient.SendCommand(['SETRANGE', aOffset.toString, aValue]));
end;

function TRedisCommandsStr.StrLen(const aKey: string): uint32;
begin
  result := fClient.AsInteger(fClient.SendCommand(['STRLEN', aKey]));
end;

function TRedisCommandsStr.SubStr(const aKey: string; aStart,
  aEnd: Integer): string;
begin
  result := fClient.AsString(fClient.SendCommand(['SUBSTR', aKey, aStart.ToString, aEnd.ToString]));
end;

end.
