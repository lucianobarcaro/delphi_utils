object Form1: TForm1
  Left = 0
  Top = 0
  Caption = 'TOTP - Exemplo de uso'
  ClientHeight = 382
  ClientWidth = 749
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -11
  Font.Name = 'Tahoma'
  Font.Style = []
  OldCreateOrder = False
  OnShow = FormShow
  PixelsPerInch = 96
  TextHeight = 13
  object Memo1: TMemo
    Left = 353
    Top = 0
    Width = 396
    Height = 382
    Align = alClient
    Font.Charset = DEFAULT_CHARSET
    Font.Color = clWindowText
    Font.Height = -8
    Font.Name = 'Courier New'
    Font.Style = []
    Lines.Strings = (
      '#######  #     ## ## #      # #######'
      '#     # ## ##    #      # ### #     #'
      '# ### #  ###  # # # ###   #   # ### #'
      '# ### # ## #    ##   ##  #### # ### #'
      '# ### #   # #### ## #       # # ### #'
      '#     # # #  #### # ### ## ## #     #'
      '####### # # # # # # # # # # # #######'
      '            ## #   # #### # #        '
      '##### ##### # ## #  #### ## ## # # # '
      '  #            ##  # ##   #  # #   # '
      '##  # ### ###    ## #    # ##   ## ##'
      '  ##       #  # #   ##  #   # ###  ##'
      '##   ##  # #     #   ######   # #####'
      '  #    # ## ####   # ##     #  # ### '
      ' # #####  #  ######  ##   ##   # # ##'
      '    #  # #  ##  #   ## #  # ##  ## ##'
      '# ####### # # #  #   ## ###  ## # # #'
      '#####           #   ##    # # ## ##  '
      '   #  # # ###  ##   # # # ### #  ####'
      ' ###      ##  #    # ####  # # ###  #'
      '##  # # # ##   # #  ###  # # ##  ## #'
      '# # #       ##### ## #   ## #  #  ## '
      '    ### ###  ####   ### # #### ## ###'
      '####    #   ##      ##### ## # ###  #'
      '## ## ##### # #  #   ## # #   # #####'
      '# ## #  ###        #  #     ##    ## '
      '#  ## #### ##  # ##     #####     ###'
      '#  ##    ###  ##  # ## #  #        # '
      '# ##  #### #    ###  #### ####### ## '
      '        # # ### ##  ##     ##   #    '
      '####### #    ###    ###   # # # ## ##'
      '#     #     ##  #  # # #  # #   #  # '
      '# ### # #   # #     ### # ######### #'
      '# ### # # #     #### #   ##  #### ###'
      '# ### # #####  ##   # # ##    ##  ###'
      '#     # ## #  ##  # ###     ##      #'
      '####### ####     #      # #   ##  ###')
    ParentFont = False
    TabOrder = 0
  end
  object Panel1: TPanel
    Left = 0
    Top = 0
    Width = 353
    Height = 382
    Align = alLeft
    TabOrder = 1
    ExplicitLeft = -6
    object Label1: TLabel
      Left = 8
      Top = 192
      Width = 182
      Height = 46
      Caption = 'C'#243'd igo'
      Font.Charset = DEFAULT_CHARSET
      Font.Color = clWindowText
      Font.Height = -43
      Font.Name = 'Courier New'
      Font.Style = []
      ParentFont = False
    end
    object Label2: TLabel
      Left = 306
      Top = 3
      Width = 31
      Height = 13
      Caption = 'Label2'
    end
    object edSecret: TLabeledEdit
      Left = 8
      Top = 152
      Width = 281
      Height = 21
      EditLabel.Width = 86
      EditLabel.Height = 13
      EditLabel.Caption = 'Segredo (base32)'
      TabOrder = 0
      Text = 'GEZDGNBRGIZTCMRTGEZDG==='
      OnExit = edSecretExit
    end
    object edIssuer: TLabeledEdit
      Left = 8
      Top = 280
      Width = 121
      Height = 21
      EditLabel.Width = 30
      EditLabel.Height = 13
      EditLabel.Caption = 'Issuer'
      TabOrder = 1
      Text = 'Linx Co'
      OnChange = edIssuerChange
    end
    object edName: TLabeledEdit
      Left = 152
      Top = 280
      Width = 121
      Height = 21
      EditLabel.Width = 27
      EditLabel.Height = 13
      EditLabel.Caption = 'Name'
      TabOrder = 2
      Text = 'Luciano'
      OnChange = edIssuerChange
    end
    object edURI: TLabeledEdit
      Left = 8
      Top = 328
      Width = 329
      Height = 21
      BiDiMode = bdLeftToRight
      EditLabel.Width = 18
      EditLabel.Height = 13
      EditLabel.Caption = 'URI'
      ParentBiDiMode = False
      ReadOnly = True
      TabOrder = 3
    end
    object pb: TProgressBar
      Left = 8
      Top = 179
      Width = 281
      Height = 7
      Max = 30
      Position = 22
      Step = 1
      TabOrder = 4
    end
    object Button1: TButton
      Left = 190
      Top = 114
      Width = 99
      Height = 32
      Caption = 'Novo Segredo'
      TabOrder = 5
      OnClick = Button1Click
    end
    object Panel2: TPanel
      Left = 0
      Top = 2
      Width = 201
      Height = 103
      Caption = 'Configura'#231#227'o'
      TabOrder = 6
      VerticalAlignment = taAlignTop
      object Label5: TLabel
        Left = 112
        Top = 51
        Width = 44
        Height = 13
        Caption = 'Intervalo'
      end
      object Label4: TLabel
        Left = 8
        Top = 51
        Width = 32
        Height = 13
        Caption = 'D'#237'gitos'
      end
      object Label3: TLabel
        Left = 8
        Top = 5
        Width = 45
        Height = 13
        Caption = 'Algoritmo'
      end
      object seIntervalo: TSpinEdit
        Left = 112
        Top = 70
        Width = 81
        Height = 22
        Increment = 10
        MaxValue = 180
        MinValue = 10
        TabOrder = 0
        Value = 30
        OnChange = edSecretExit
      end
      object seDigitos: TSpinEdit
        Left = 8
        Top = 70
        Width = 81
        Height = 22
        MaxValue = 9
        MinValue = 1
        TabOrder = 1
        Value = 6
        OnChange = edSecretExit
      end
      object cbAlgoritmo: TComboBox
        Left = 11
        Top = 24
        Width = 145
        Height = 21
        ItemIndex = 0
        TabOrder = 2
        Text = 'SHA1'
        OnChange = edSecretExit
        Items.Strings = (
          'SHA1'
          'SHA256'
          'SHA512')
      end
    end
  end
  object Timer1: TTimer
    Interval = 200
    OnTimer = Timer1Timer
    Left = 304
    Top = 24
  end
end
