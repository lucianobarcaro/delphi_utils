unit Redis.Base;

interface

type
  IRedisClient = interface
    ['{C9BCB9D0-17F5-4F6B-904D-9044C85173F5}']
    function Serialize(sl: TArray<string>): string;
    function readFromServer: tarray<string>;
    function AsBool(aResult: tArray<string>): boolean;
    function AsString(aResult: tArray<string>): string;
    function AsInteger(aResult: tArray<string>): integer;
    function AsFloat(aResult: tArray<string>): Real;
    function SendCommand(cmd: tArray<string>): tArray<string>;
  end;

implementation

end.
