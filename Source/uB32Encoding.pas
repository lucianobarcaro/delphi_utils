unit uB32Encoding;

interface

uses
  System.SysUtils;

type
  tB32Encoding = class
  const
    Alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  public
    class function Encode(const AInput: TBytes): string;
    class function Decode(const AInput: string): TBytes;
  end;

implementation

{ tB32Encoding }

class function tB32Encoding.Decode(const AInput: string): TBytes;
var
  CleanInput: string;
  C: Char;
  Val, Bits, Buffer, Idx: Integer;
begin
  CleanInput := AInput.ToUpper.Replace(' ', '', [rfReplaceAll])
                              .Replace('-', '', [rfReplaceAll])
                              .Replace('=', '', [rfReplaceAll]);

  SetLength(Result, (CleanInput.Length * 5) div 8);

  Buffer := 0;
  Bits := 0;
  Idx := 0;

  for C in CleanInput do
  begin
    Val := Pos(C, Alphabet) - 1;
    if Val < 0 then Continue;

    Buffer := (Buffer shl 5) or Val;
    Bits := Bits + 5;

    if Bits >= 8 then
    begin
      Bits := Bits - 8;
      if Idx < Length(Result) then
      begin
        Result[Idx] := (Buffer shr Bits) and $FF;
        Inc(Idx);
      end;
    end;
  end;
  SetLength(Result, Idx);
end;

class function tB32Encoding.Encode(const AInput: TBytes): string;
var
  I, Index, BitCount, NextByte: Integer;
  Output: tStringBuilder;
begin
  Output := tStringBuilder.Create;
  try
    Index := 0;
    BitCount := 0;
    NextByte := 0;

    for I := 0 to Length(AInput) - 1 do
    begin
      NextByte := (NextByte shl 8) or (AInput[I] and $FF);
      BitCount := BitCount + 8;

      while BitCount >= 5 do
      begin
        BitCount := BitCount - 5;
        Index := (NextByte shr BitCount) and $1F;
        Output.Append(Alphabet[Index + 1]);
      end;
    end;

    // Processa os bits restantes, se houver
    if BitCount > 0 then
    begin
      NextByte := NextByte shl (5 - BitCount);
      Index := NextByte and $1F;
      Output.Append(Alphabet[Index + 1]);
    end;

    // Adiciona o preenchimento (padding com '=') conforme a RFC 4648
    while (Output.Length mod 8) <> 0 do
      Output.Append('=');

    Result := Output.ToString;
  finally
    Output.Free;
  end;
end;

end.
