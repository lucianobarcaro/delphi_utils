unit uTOTP;

interface

uses
  System.SysUtils,
  System.Hash,
  System.DateUtils,
  System.Classes;

type
  TTOTPAlgorithms = (SHA1, SHA256, SHA512);

  TTotp = class
  private
    fSecretB32: string;
    fSecret: TBytes;
    fInterval: integer;
    fDigits: integer;
    fAlgorithm: TTOTPAlgorithms;
  protected
    function getAlgorithm: string;
  public
    constructor create(
      secret: string;
      digits: integer = 6;
      interval: integer = 30;
      algorithm: TTOTPAlgorithms = SHA1
    );
    function newToken(size: integer = 12): string;
    function getToken(when: int64 = 0): string;
    function generate_uri(issuer, name: string): string;
    function genQRCode(issuer, name: string): string;

    property digits: integer read fDigits;
    property interval: integer read fInterval;
    property algorithm: string read getAlgorithm;
  end;

implementation

{ TTotp }

uses
  System.NetEncoding,
  System.math,
  uB32Encoding,
  DelphiZXingQRCode,
  System.Typinfo;

constructor TTotp.create(secret: string; digits, interval: integer; algorithm: TTOTPAlgorithms);
begin
  fSecretB32 := secret;
  fSecret := tB32Encoding.Decode(secret);
  fDigits := digits;
  fInterval := interval;
  fAlgorithm := algorithm;
end;

function TTotp.generate_uri(issuer, name: string): string;
var
  rr: tStringBuilder;
begin
  issuer := TNetEncoding.URL.Encode(issuer).replace('+', '%20');
  name := TNetEncoding.URL.Encode(name).replace('+', '%20');

  rr := tStringBuilder.Create;
  try
    rr.append('secret=' + TNetEncoding.URL.Encode(fSecretB32) +
              '&issuer=' + issuer);
    if fDigits <> 6 then
      rr.append('&digits=' + fDigits.ToString);
    if fInterval <> 30 then
      rr.append('&period=' + fInterval.ToString);
    if fAlgorithm <> SHA1 then
      rr.append('&algorithm=' + getAlgorithm);

    result := Format('otpauth://totp/%s:%s?%s', [issuer, name, rr.ToString]);
  finally
    rr.Free;
  end;
end;

function TTotp.genQRCode(issuer, name: string): string;
var
  qr: TDelphiZXingQRCode;
  c, l: integer;
begin
  qr := TDelphiZXingQRCode.create;
  qr.data := generate_uri(issuer, name);
  qr.encoding := TQRCodeEncoding.qrAuto;
  qr.QuietZone := 0;
  qr.update;
  for l:=0 to qr.rows-1 do
  begin
    for c:=0 to qr.Columns-1 do
    begin
      if qr.IsBlack[l, c] then
        result := result + '#'
      else
        result := result + ' ';
    end;
    result := result + sLineBreak;
  end;
  qr.Free;
end;

function TTotp.getAlgorithm: string;
begin
  result := GetEnumName(TypeInfo(TTotpAlgorithms), ord(fAlgorithm));
end;

function TTotp.getToken(when: int64 = 0): string;
var
  TimeWindow: Int64;
  TimeBytes: TBytes;
  HmacBytes: TBytes;
  Offset, Binary: Integer;
  Token: Cardinal;
  I: Integer;
begin
  if when = 0 then
    when := DateTimeToUnix(Now, false);

  TimeWindow := when div fInterval;

  SetLength(TimeBytes, 8);
  for I := 7 downto 0 do
  begin
    TimeBytes[I] := TimeWindow and $FF;
    TimeWindow := TimeWindow shr 8;
  end;

  case fAlgorithm of
    SHA1:
      HmacBytes := THashSHA1.GetHMACAsBytes(TimeBytes, fSecret);
    SHA256:
      hmacBytes := tHashSha2.getHMACAsBytes(TimeBytes, fSecret, THashSHA2.TSHA2Version.SHA256);
    SHA512:
      HmacBytes := THashSha2.GetHMACAsBytes(TimeBytes, fSecret, THashSHA2.TSHA2Version.SHA512);
  end;

  Offset := HmacBytes[High(HmacBytes)] and $0F;
  Binary := ((HmacBytes[Offset] and $7F) shl 24) or
            ((HmacBytes[Offset + 1] and $FF) shl 16) or
            ((HmacBytes[Offset + 2] and $FF) shl 8) or
            (HmacBytes[Offset + 3] and $FF);

  Token := Binary mod trunc(IntPower(10, fDigits));

  result := Format('%.*d', [fDigits, Token]);

  i := fDigits - 3;
  while i > 0 do
  begin
    insert(' ', result, i + 1);
    dec(i, 3);
  end;
end;

function TTotp.newToken(size: integer): string;
var
  tmp: tBytes;
  i: integer;
begin
  if (size < 1) or (size > 32) then
    raise Exception.Create('Tamanho inválido');

  setLength(tmp, size);
  for I := 0 to size-1 do
    tmp[i] := random(255);

  result := tB32Encoding.Encode(tmp);
end;

end.
