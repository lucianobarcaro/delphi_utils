unit Redis.Commands.Hash;

interface

uses
  Redis.Base,
  System.Generics.Collections;

type
  TRedisCommandsHash = class
  private
    fClient: IRedisClient;
  public
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

    constructor Create(AClient: IRedisClient);
  end;

implementation

uses
  System.SysUtils;

constructor TRedisCommandsHash.Create(AClient: IRedisClient);
begin
  fClient := AClient;
end;

function TRedisCommandsHash.HDel(const aKey: string; AFields: tArray<string>): Boolean;
begin

end;

function TRedisCommandsHash.HExists(const aKey, aField: String): Boolean;
begin

end;

function TRedisCommandsHash.HGet(const aKey, aField: String): String;
begin

end;

function TRedisCommandsHash.HGetAll(const aKey: String): TPair<String, String>;
begin

end;

function TRedisCommandsHash.HIncrBy(const aKey, aField: string; aValue: integer): integer;
begin

end;

function TRedisCommandsHash.HIncrByFloat(const aKey, aField: string; aValue: Real): Real;
begin

end;

function TRedisCommandsHash.HKeys(const aKey: String): TArray<string>;
begin

end;

function TRedisCommandsHash.HLen(const aKey: String): integer;
begin

end;

function TRedisCommandsHash.HMGet(const aKey: String; aFields: TArray<string>): TPair<String, String>;
begin

end;

function TRedisCommandsHash.HMSet(const aKey: String; aPairs: TArray<TPair<String, String>>): TPair<String, String>;
begin

end;

function TRedisCommandsHash.HRandField(const aKey: String; aCount: Integer; aWithValues: Boolean): string;
begin

end;

function TRedisCommandsHash.HSet(const aKey: String; aFields: TArray<string>): TPair<String, String>;
begin

end;

function TRedisCommandsHash.HSetNX(const aKey, aField, aValue: String): Boolean;
begin

end;

function TRedisCommandsHash.HStrLen(const aKey, aField: string): uint32;
begin

end;

function TRedisCommandsHash.HVals(const aKey: String): TArray<TPair<String, String>>;
begin

end;

end.
