from pandas import DataFrame, to_datetime


def process_conciliacao_emprestimo(pfunc: DataFrame, ppessoa: DataFrame, psecao: DataFrame, pffinanc: DataFrame, pparamadicionais: DataFrame, emprestimo: DataFrame, funcionarios: DataFrame, gerou_folha: DataFrame) -> DataFrame:
    """
        TODO: Doc String
    """

    _pffinanc_valor_averbado = (pffinanc
                            .merge(pparamadicionais, left_on=['codcoligada', 'anocomp', 'mescomp'], right_on=['codcoligada', 'anocompcarolpffinanc', 'mescompcarolpffinanc'], how='inner')
                            [['chapa', 'codcoligada', 'anocomp', 'mescomp', 'valor']]
                            .assign(valor=lambda df: df['valor'].astype(float))
                            .groupby(by=['codcoligada', 'chapa', 'anocomp', 'mescomp'])['valor'].sum().reset_index()
                            .rename({'valor': 'valoraverbado'}, axis=1))
    
    _emprestimo_periodo = (emprestimo
                           .assign(anocomp=lambda df: to_datetime(df['vencimento_parcela'], format='%Y-%m-%d %H:%M:%S').dt.year)
                           .assign(mescomp=lambda df: to_datetime(df['vencimento_parcela'], format='%Y-%m-%d %H:%M:%S').dt.month))
    
    df = (pfunc
            .merge(ppessoa, left_on=['codpessoa'], right_on=['codigo'], how='inner')
            .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
            .assign(cnpj=lambda df: df['cgc'].str.replace(r'\.|\/|\-', ''))
            .merge(_pffinanc_valor_averbado, left_on=['codcoligada', 'chapa'], right_on=['codcoligada', 'chapa'], how='left')
            .merge(_emprestimo_periodo, left_on=['cnpj', 'cpf', 'anocomp', 'mescomp'], right_on=['cnpj', 'cpf', 'anocomp', 'mescomp'], how='left')
            .merge(gerou_folha, left_on=['cnpj', 'cpf', 'anocomp', 'mescomp'], right_on=['cnpj', 'cpf', 'ano', 'mes'], how='left')
            .merge(funcionarios, left_on=['cnpj', 'cpf'], right_on=['cnpj', 'cpffuncionario'], how='left')
            .assign(valornaoaverbado=lambda df: df['valoraverbado'] - df['valor_parcela'])
            .assign(motivo=lambda df: '')
            .assign(status_parcela=lambda df: '')
            .assign(periodo=lambda df: df['anocomp'].astype(str) + df['mescomp'].astype(str).str.pad(2, side='left', fillchar='0'))
            .fillna({'gerou_folha': True, 'consignavel': 0.0}))

    if len(df) > 0:
        df.loc[df['valor_parcela'] > df['consignavel'], 'motivo'] = 'Valor da parcela superior a margem consignavel'
        df.loc[df['gerou_folha'] == False, 'motivo'] = 'Não houve geração de folha para esse funcionário nesta data'
    
    df['status_parcela'] = df['motivo'].apply(lambda x: 'Aberta' if x == '' else 'Erro')

    return df[['cnpj', 'cpf', 'periodo', 'valoraverbado', 'valornaoaverbado', 'motivo', 'status_parcela', 'numero_da_parcela', 'codigo_emprestimo']]

    # df = (funcionarios
    #       .assign(cnpj=lambda df: df['cnpj'].str.replace(r'\.|\/|\-', ''))
    #       .merge(_emprestimo_periodo, left_on=['cnpj', 'cpf'], right_on=['cnpj', 'cpf'], how='inner')
    #       .assign(_anocomp=lambda df: to_datetime(df['vencimento_parcela'], format='%Y-%m-%d %H:%M:%S').dt.year)
    #       .assign(_mescomp=lambda df: to_datetime(df['vencimento_parcela'], format='%Y-%m-%d %H:%M:%S').dt.month)
    #       .merge(gerou_folha, left_on=['cnpj', 'cpf', '_anocomp', '_mescomp'], right_on=['cnpj', 'cpf', 'ano', 'mes'], how='left')
    #       .assign(_periodo=lambda df: df['_anocomp'].astype(str) + df['_mescomp'].astype(str).str.pad(2, side='left', fillchar='0'))
    #       .merge(_pffinanc_valor_averbado, left_on=['cnpj', 'cpf'], right_on=['cnpj', 'cpf'], how='inner')
    #       .fillna({'gerou_folha': True, 'margemconsignavel': 0.0})
    #       .assign(motivo=lambda df: '')
    #     )

        
    # df.loc[df['valor_parcela'] > df['margemconsignavel'], 'motivo'] = 'Valor da parcela superior a margem consignavel'
    # df.loc[df['gerou_folha'] == False, 'motivo'] = 'Não houve geração de folha para esse funcionário nesta data'
    # df['status_parcela'] = df['motivo'].apply(lambda x: 'Aberta' if x == '' else 'Erro')

def process_geracao_arquivo(pfunc: DataFrame, ppessoa: DataFrame, psecao: DataFrame, emprestimo: DataFrame) -> DataFrame:
    """
        TODO: Doc String
    """

    _emprestimo_periodo = (emprestimo
                           .assign(anocomp=lambda df: to_datetime(df['vencimento_parcela'], format='%Y-%m-%d %H:%M:%S').dt.year)
                           .assign(mescomp=lambda df: to_datetime(df['vencimento_parcela'], format='%Y-%m-%d %H:%M:%S').dt.month))
    
    return (pfunc
            .merge(ppessoa, left_on=['codpessoa'], right_on=['codigo'], how='inner')
            .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
            .assign(cnpj=lambda df: df['cgc'].str.replace(r'\.|\/|\-', ''))
            .merge(_emprestimo_periodo, left_on=['cnpj', 'cpf'], right_on=['cnpj', 'cpf'], how='left')
            .assign(horapagamento=lambda df: to_datetime(df['vencimento_parcela']).dt.strftime('%H:%M'))
            .assign(datapagamento=lambda df: to_datetime(df['vencimento_parcela']).dt.strftime('%Y-%m-%d'))
            .rename({ 'valor_parcela': 'valor', 'numero_da_parcela': 'referencia' }, axis=1)
            [[ 'chapa', 'datapagamento', 'horapagamento', 'valor', 'referencia' ]]
    )

    # return (funcionarios
    #       .assign(cnpj=lambda df: df['cnpj'].str.replace(r'\.|\/|\-', ''))
    #       .merge(emprestimo, left_on=['cnpj', 'cpf'], right_on=['cnpj', 'cpf'], how='inner')
    #       .assign(_numero_parcela=lambda df: df['numero_da_parcela'])
    #       .assign(_vencimento_parcela=lambda df: to_datetime(df['vencimento_parcela']))
    #       .assign(_anocomp=lambda df: df['_vencimento_parcela'].dt.year)
    #       .assign(_mescomp=lambda df: df['_vencimento_parcela'].dt.month)
    #       .merge(gerou_folha, left_on=['cnpj', 'cpf', '_anocomp', '_mescomp'], right_on=['cnpj', 'cpf', 'ano', 'mes'], how='left')
    #       .assign(_periodo=lambda df: df['_anocomp'].astype(str) + df['_mescomp'].astype(str).str.pad(2, side='left', fillchar='0'))
    #       .assign(horapagamento=lambda df: df['_vencimento_parcela'].dt.strftime('%H:%M'))
    #       .rename({ 'matriculafuncionario': 'chapa', 'valor_parcela': 'valor', '_numero_parcela': 'referencia' }, axis=1)
    #       [[ 'chapa', 'datapagamento', 'horapagamento', 'valor', 'referencia' ]]
    #     )