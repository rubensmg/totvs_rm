from pandas import DataFrame, IntervalIndex, DateOffset, to_datetime, to_numeric
from datetime import datetime, timedelta


def process_funcionario(pfunc: DataFrame, ppessoa: DataFrame, psecao: DataFrame, pfperff: DataFrame, pfemprt: DataFrame, pparam: DataFrame, pffinanc: DataFrame) -> DataFrame:
    """
        TODO: doc string
    """

    # Incluir o ano comp e mes comp
    _pfperff_group_by_codcoligada_chapa_ano_mes_comp = (pfperff
                                                        .assign(liquido=lambda df: df['liquido'].astype(float))
                                                        .groupby(by=['codcoligada', 'chapa', 'anocomp', 'mescomp'])['liquido'].sum()
                                                        .reset_index())

    _pfemprt_group_by_codcoligada_chapa = (pfemprt
                                           .assign(saldodevedor=lambda df: df['saldodevedor'].astype(float))
                                           .groupby(by=['codcoligada', 'chapa'])['saldodevedor'].sum()
                                           .reset_index())

    _pffinanc_group_by_ano_mes_comp = pffinanc.groupby(by=['anocomp', 'mescomp',
                                                           'chapa', 'codcoligada'])['valor'].count().reset_index()

    return (pfunc
            .merge(ppessoa, left_on=['codpessoa'], right_on=['codigo'], how='inner')
            .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
            .merge(pparam, left_on=['codcoligada'], right_on=['codcoligada'], how='inner')
            .assign(_datacompanterior=lambda df: to_datetime(df['mescomp'].astype(str) + '-' + df['anocomp'].astype(str), format='%m-%Y') - DateOffset(months=1))
            .assign(_anocompanterior=lambda df: df['_datacompanterior'].dt.year)
            .assign(_mescompanterior=lambda df: df['_datacompanterior'].dt.month)
            .merge(_pffinanc_group_by_ano_mes_comp, left_on=['codcoligada', 'chapa', '_anocompanterior', '_mescompanterior'], right_on=['codcoligada', 'chapa', 'anocomp', 'mescomp'], how='inner')
            .merge(_pfperff_group_by_codcoligada_chapa_ano_mes_comp, left_on=['codcoligada', 'chapa', '_anocompanterior', '_mescompanterior'], right_on=['codcoligada', 'chapa', 'anocomp', 'mescomp'], how='left')
            .merge(_pfemprt_group_by_codcoligada_chapa, left_on=['codcoligada', 'chapa'], right_on=['codcoligada', 'chapa'], how='left')
            .query('codsituacao != "D"')
            .assign(emprestimoexterno=lambda df: ~df['saldodevedor'].isna() & df['saldodevedor'] > 0.0)
            .assign(cnpj=lambda df: df['cgc'].str.replace(r'\.|\/|\-', ''))
            .assign(codrecisaorais=lambda df: None)
            .assign(consignavel=lambda df: df['liquido'] * 0.3)
            [[
                'cnpj', 'chapa', 'dataadmissao', 'cpf', 'datademissao', 'consignavel',
                'emprestimoexterno', 'nome', 'salario', 'codsituacao', 'telefone1',
                'codrecisaorais'
            ]]
            .rename({
                'dataadmissao': 'admissao',
                'datademissao': 'demissao',
                'codpessoa': 'chavefuncionario',
                'codsituacao': 'situacaofuncionario',
                'telefone1': 'telefone',
                'chapa': 'matriculafuncionario',
                'cpf': 'cpffuncionario'
            }, axis=1))


def process_recisao(pfunc: DataFrame, ppessoa: DataFrame, psecao: DataFrame, pffinanc: DataFrame, trecisao: DataFrame, pparamadicionais: DataFrame) -> DataFrame:
    """
        TODO: doc string
    """

    _pffinanc_valor_averbado = (pffinanc
                                .merge(pparamadicionais, left_on=['codcoligada', 'anocomp', 'mescomp'], right_on=['codcoligada', 'anocompcarolpffinanc', 'mescompcarolpffinanc'], how='inner')
                                [['chapa', 'codcoligada', 'anocomp', 'mescomp', 'valor']]
                                .assign(periodo=lambda df: df['anocomp'].astype(str) + df['mescomp'].astype(str).str.pad(2, side='left', fillchar='0'))
                                .assign(valor=lambda df: df['valor'].astype(float))
                                .groupby(by=['codcoligada', 'chapa', 'periodo'])['valor'].sum().reset_index()
                                .rename({'valor': 'valoraverbado'}, axis=1)
                            )

    return (pfunc
            .merge(ppessoa, left_on=['codpessoa'], right_on=['codigo'], how='inner')
            .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
            .merge(trecisao, left_on=['tipodemissao'], right_on=['tipodemissao_rm'], how='inner')
            .query('codsituacao == "D" & tipodemissao != 5 & tipodemissao != 6')
            .assign(periodo=lambda df: to_datetime(df['datademissao'], format='%Y-%m-%dT%H:%M:%S.%f').dt.strftime('%Y%m'))
            .merge(_pffinanc_valor_averbado,  left_on=['codcoligada', 'chapa', 'periodo'], right_on=['codcoligada', 'chapa', 'periodo'], how='left')
            .assign(valor_descontado=lambda df: df['valoraverbado'])
            .assign(desc_recisao=lambda df: '')
            .assign(cnpj=lambda df: df['cgc'].str.replace(r'\.|\/|\-', ''))
            [[
                'cnpj', 'cpf', 'datademissao', 'dtpagtorescisao', 'desc_recisao', 'periodo', 'tiporecisao', 'valor_descontado'
            ]]
            .rename({
                'datademissao': 'data_demissao',
                'dtpagtorescisao': 'data_pagamento',
                'tiporecisao': 'tipo_recisao'
            }, axis=1))

def process_conciliacao_emprestimo(pfunc: DataFrame, ppessoa: DataFrame, psecao: DataFrame, pffinanc: DataFrame, pparamadicionais: DataFrame, emprestimo: DataFrame):
    """
        TODO: doc string
    """

    _pffinanc_valor_averbado = (pffinanc
                                .merge(pparamadicionais, left_on=['codcoligada', 'codevento'], right_on=['codcoligada', 'valorstr'], how='inner')
                                .query('nomecoluna == "EventoDescontoCreditas" | nomecoluna == "EventoDescontoBV"')
                                [['chapa', 'codcoligada', 'anocomp', 'mescomp', 'valor']]
                                .drop_duplicates()
                                .rename({'valor': 'valoraverbado'}, axis=1))

    return (pfunc
            .merge(ppessoa, left_on=['codpessoa'], right_on=['codigo'], how='inner')
            .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
            .merge(_pffinanc_valor_averbado, left_on=['codcoligada', 'chapa'], right_on=['codcoligada', 'chapa'], how='inner')
            .assign(cnpj=lambda df: df['cgc'].str.replace(r'\.|\/|\-', ''))
            .merge(_emprestimo_periodo, left_on=['cpf', 'cnpj', 'anocomp', 'mescomp'], right_on=['cpf', 'cnpj', 'anocomp', 'mescomp'], how='inner')
            .assign(periodo=lambda df: df['anocomp'].astype(str) + df['mescomp'].astype(str).str.pad(2, side='left', fillchar='0'))
            .assign(valornaoaverbado=lambda df: df['valoraverbado'] - df['valor_parcela'])
            .assign(motivo=lambda df: '')
            .assign(status_parcela=lambda df: '')
            [['cnpj', 'cpf', 'periodo', 'valoraverbado', 'valornaoaverbado', 'motivo', 'status_parcela', 'numero_da_parcela', 'codigo_emprestimo']])


def process_geracao_folha(pfunc: DataFrame, ppessoa: DataFrame, pparam: DataFrame, psecao: DataFrame, pfhstaft: DataFrame) -> DataFrame:
    """
        TODO: doc string
    """

    return (pfunc
            .merge(ppessoa, left_on=['codpessoa'], right_on=['codigo'], how='inner')
            .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
            .query('codsituacao != "E" & codsituacao != "W" & codsituacao != "R"')
            .merge(pparam, left_on=['codcoligada'], right_on=['codcoligada'], how='inner')
            .merge(pfhstaft, left_on=['codcoligada', 'chapa'], right_on=['codcoligada', 'chapa'], how='inner')
            .assign(_dtinicio=lambda df: df['dtinicio'])
            .assign(_dtfinal=lambda df: df['dtfinal'])
            .replace({ '_dtfinal': '0001-01-01T00:00:00.000Z', '_dtinicial': '0001-01-01T00:00:00.000Z' }, '2099-12-31T23:59:59.000Z')
            .assign(_dtinicio=lambda df: to_datetime(df['_dtinicio'], format='%Y-%m-%dT%H:%M:%S.%f'))
            .assign(_dtfinal=lambda df: to_datetime(df['_dtfinal'], format='%Y-%m-%dT%H:%M:%S.%f'))
            .query('(_dtinicio - _dtfinal).dt.days > 15')
            .assign(gerou_folha=lambda df: ((df['_dtinicio'].dt.month == df['mescomp']) & (df['_dtinicio'].dt.year == df['anocomp'])) | ((df['_dtfinal'].dt.month == df['mescomp']) & (df['_dtfinal'].dt.year == df['anocomp'])))
            .assign(cnpj=lambda df: df['cgc'].str.replace(r'\.|\/|\-', ''))
            [[
                'anocomp', 'mescomp', 'cpf', 'cnpj', 'gerou_folha'
            ]]
            .rename({
                'anocomp': 'ano',
                'mescomp': 'mes'
            }, axis=1))


def process_count_faixa(pfunc: DataFrame, psecao: DataFrame, tsalarycount: DataFrame) -> DataFrame:
    """
        TODO: doc string
    """

    tsalarycount.index = IntervalIndex.from_arrays(
        tsalarycount['init'], tsalarycount['end'], closed='both')

    _pfunc = pfunc.copy()
    _pfunc['faixa'] = _pfunc['salario'].apply(lambda element: tsalarycount.iloc[tsalarycount.index.get_loc(float(element))]['identifier'])

    return (_pfunc
            .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
            .query('codsituacao != "D"')
            .groupby(by=['cgc', 'faixa'])['chapa'].count().reset_index()
            [[
                'cgc', 'faixa', 'chapa'
            ]]
            .rename({
                'cgc': 'cnpj',
                'chapa': 'total'
            }, axis=1)
            .pivot_table('total', ['cnpj'], 'faixa').reset_index())


def process_media_salarial(pfunc: DataFrame, psecao: DataFrame) -> DataFrame:
    """
        TODO: doc string
    """

    return (pfunc
            .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
            .query('codsituacao != "D"')
            .assign(cnpj=lambda df: df['cgc'].str.replace(r'\.|\/|\-', ''))
            .assign(salario=lambda df: df['salario'].astype(float))
            .groupby(by=['cnpj'])['salario'].mean().reset_index()
            [[
                'cnpj', 'salario'
            ]]
            .rename({
                'salario': 'media_salarial'
            }, axis=1))


def process_turnover(pfunc: DataFrame, psecao: DataFrame) -> DataFrame:
    """
        TODO: doc string
    """

    pfunc_groupby_admissao = (pfunc
                              .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
                              .query('dataadmissao.between("2019-01-01", "2019-12-31")')
                              .groupby(by=['cgc'])['dataadmissao'].count()
                              .reset_index()
                              [['cgc', 'dataadmissao']]
                              .rename({'dataadmissao': 'count_dataadmissao'}, axis=1))

    pfunc_groupby_demissao = (pfunc
                              .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
                              .query('datademissao.between("2019-01-01", "2019-12-31")')
                              .groupby(by=['cgc'])['datademissao'].count()
                              .reset_index()
                              [['cgc', 'datademissao']]
                              .rename({'datademissao': 'count_datademissao'}, axis=1))

    pfunc_groupby_codpessoa = (pfunc
                               .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
                               .query('dataadmissao <= "2019-12-31" & codsituacao != "D"')
                               .groupby(by=['cgc'])['codpessoa'].count()
                               .reset_index()
                               [['cgc', 'codpessoa']]
                               .rename({'codpessoa': 'count_total'}, axis=1))

    return (pfunc_groupby_codpessoa
            .merge(pfunc_groupby_admissao, left_on=['cgc'], right_on=['cgc'], how='left')
            .merge(pfunc_groupby_demissao, left_on=['cgc'], right_on=['cgc'], how='left')
            .fillna(0)
            .assign(value=lambda df: round((((df['count_dataadmissao'] + df['count_datademissao']) / 2) / df['count_total']) * 100, 2))
            .assign(cnpj=lambda df: df['cgc'].str.replace(r'\.|\/|\-', ''))
            [[
                'cnpj', 'value'
            ]])
