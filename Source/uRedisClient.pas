unit uRedisClient;

interface

uses
  System.SysUtils;

type
  IRedisClient = interface
    ['{36ABA87E-9BE2-4F23-AAAE-9284A617111C}']
    // String CMDs
    function append(const aKey, aValue: string): UInt64; overload;
    function append(const aKey, aValue: TBytes): UInt64; overload;
    function decr(const aKey: string): Int64;
    function decrBy(const aKey: string; aDecrement:UInt64): Int64;
    function delEx(const aKey: string; ifeq: string=''; ifne: string=''; ifdeq:string=''; ifnde:string=''):UInt64;
    function digest(const aKey: string): string;
    function get(const aKey: string): string;
    function getDel(const aKey: string): string;
    function getEx(const aKey: string; ms:UInt64=0; ts:uInt64=0): uint64;
    function getRange(const aKey: string; aStart,aEnd:UInt64): string;
    function getSet(const akey, aValue: string):string;
    function incr(const aKey: string): Int64;
    function incrBy(const aKey: string; aIncrement: UInt64): Int64; overload;
    function incrBy(const aKey: string; aIncrement: Double): Double; overload;  // IncrByFloat


  end;

  TRedis = class(TObject)
  private
    fHost: string;
  protected
  public
  published
  end;

implementation

end.
