from pandas import DataFrame, to_datetime

def process_conciliacao_emprestimo(pfunc: DataFrame, ppessoa: DataFrame, psecao: DataFrame, pparam: DataFrame, emprestimo: DataFrame, funcionarios: DataFrame, gerou_folha: DataFrame) -> DataFrame:
    """
        TODO: Doc String
    """
    
    _emprestimo_periodo = (emprestimo
                           .assign(_vencimento_parcela=lambda df: to_datetime(df['vencimento_parcela'], format='%Y-%m-%dT%H:%M:%S.%f'))
                           .assign(anocomp=lambda df: df['_vencimento_parcela'].dt.year)
                           .assign(mescomp=lambda df: df['_vencimento_parcela'].dt.month)
                           .assign(periodo=lambda df: df['anocomp'].astype(str) + df['mescomp'].astype(str).str.pad(2, side='left', fillchar='0'))
                        )
    
    df = (pfunc
            .merge(ppessoa, left_on=['codpessoa'], right_on=['codigo'], how='inner')
            .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
            .assign(cnpj=lambda df: df['cgc'].str.replace(r'\.|\/|\-', ''))
            .merge(pparam, left_on=['codcoligada'], right_on=['codcoligada'], how='inner')
            .merge(_emprestimo_periodo, left_on=['cnpj', 'cpf', 'anocomp', 'mescomp'], right_on=['cnpj', 'cpf', 'anocomp', 'mescomp'], how='inner')
            .assign(valoraverbado=lambda df: df['valor_parcela'])
            .assign(valornaoaverbado=lambda df: 0.0)
            .assign(motivo=lambda df: '')
            .assign(status_parcela=lambda df: '')
    )

    if len(df) > 0 and len(gerou_folha) > 0 and len(funcionarios) > 0:
        df = (df
                .merge(gerou_folha, left_on=['cnpj', 'cpf', 'anocomp', 'mescomp'], right_on=['cnpj', 'cpf', 'ano', 'mes'], how='left')
                .merge(funcionarios, left_on=['cnpj', 'cpf'], right_on=['cnpj', 'cpffuncionario'], how='left')
                .fillna({'gerou_folha': True, 'consignavel': 0.0})
            )
        df.loc[df['valor_parcela'] > df['consignavel'], 'motivo'] = 'Valor da parcela superior a margem consignavel'
        df.loc[df['gerou_folha'] == False, 'motivo'] = 'Não houve geração de folha para esse funcionário nesta data'
    
    df['status_parcela'] = df['motivo'].apply(lambda x: 'Aberta' if x == '' else 'Erro')

    return (df[['cnpj', 'cpf', 'periodo', 'valoraverbado', 'valornaoaverbado', 'motivo', 'status_parcela', 'numero_da_parcela', 'codigo_emprestimo']]
            .rename({
                'cpf': 'cpffuncionario',
                'valornaoaverbado': 'valor_nao_averbado',
                'valoraverbado': 'valor_averbado'
            }, axis=1))

def process_geracao_arquivo(pfunc: DataFrame, ppessoa: DataFrame, psecao: DataFrame, pparam: DataFrame, pparamadicionais: DataFrame, emprestimo: DataFrame) -> DataFrame:
    """
        TODO: Doc String
    """
    
    _emprestimo_periodo = (emprestimo
                           .assign(_vencimento_parcela=lambda df: to_datetime(df['vencimento_parcela'], format='%Y-%m-%dT%H:%M:%S.%f'))
                           .assign(datapagamento=lambda df: df['_vencimento_parcela'].dt.strftime('%d%m%Y'))
                           .assign(horapagamento=lambda df: df['_vencimento_parcela'].dt.strftime('%H:%M'))
                           .assign(anocomp=lambda df: df['_vencimento_parcela'].dt.year)
                           .assign(mescomp=lambda df: df['_vencimento_parcela'].dt.month))

    
    df = (pfunc
            .merge(ppessoa, left_on=['codpessoa'], right_on=['codigo'], how='inner')
            .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
            .assign(cnpj=lambda df: df['cgc'].str.replace(r'\.|\/|\-', ''))
            .merge(pparam, left_on=['codcoligada'], right_on=['codcoligada'], how='inner')
            .merge(_emprestimo_periodo, left_on=['cnpj', 'cpf', 'anocomp', 'mescomp'], right_on=['cnpj', 'cpf', 'anocomp', 'mescomp'], how='inner')
            .merge(pparamadicionais, left_on=['codcoligada', 'anocomp', 'mescomp'], right_on=['codcoligada', 'anocompcarolpffinanc', 'mescompcarolpffinanc'], how='inner')
            .assign(referencia=lambda df: df['numero_da_parcela'].astype(float))
            .rename({ 'valor_parcela': 'valor' }, axis=1)
        )

    df.loc[df['integradocreditas'] == True, 'evento'] = df['eventobasecreditas'].astype(str)
    df.loc[df['integradobv'] == True, 'evento'] = df['eventobasebv'].astype(str)

    return df[[ 'chapa', 'datapagamento', 'horapagamento', 'referencia', 'valor', 'evento' ]]
