from pandas import DataFrame, IntervalIndex, DateOffset, to_datetime, to_numeric
from pandas.tseries.offsets import MonthEnd
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

def process_conciliacao_emprestimo(pfunc: DataFrame, ppessoa: DataFrame, psecao: DataFrame, pffinanc: DataFrame, pparam: DataFrame, pparamadicionais: DataFrame, emprestimo: DataFrame, conciliacao_emprestimo: DataFrame):
    """
        TODO: doc string
    """

    _pffinanc_valor_averbado = (pffinanc
                                .merge(pparamadicionais, left_on=['codcoligada', 'anocomp', 'mescomp'], right_on=['codcoligada', 'anocompcarolpffinanc', 'mescompcarolpffinanc'], how='inner')
                                [['chapa', 'codcoligada', 'anocomp', 'mescomp', 'valor']]
                                .assign(valor=lambda df: df['valor'].astype(float))
                                .groupby(by=['codcoligada', 'chapa', 'anocomp', 'mescomp'])['valor'].sum().reset_index()
                                .rename({'valor': 'pffinac_valoraverbado'}, axis=1)
                            )
    
    _emprestimo_periodo = (emprestimo
                           .assign(_vencimento_parcela=lambda df: to_datetime(df['vencimento_parcela'], format='%Y-%m-%dT%H:%M:%S.%f'))
                           .assign(anocomp=lambda df: df['_vencimento_parcela'].dt.year)
                           .assign(mescomp=lambda df: df['_vencimento_parcela'].dt.month)
                        )

    _pparam_last_comp = (pparam
                        .assign(_datelastcomp=lambda df: to_datetime(df['mescomp'].astype(str) + '-' + df['anocomp'].astype(str), format='%m-%Y') - DateOffset(months=1))
                        .assign(anocomp=lambda df: df['_datelastcomp'].dt.year)
                        .assign(mescomp=lambda df: df['_datelastcomp'].dt.month)
                    )


    df = (pfunc
        .merge(ppessoa, left_on=['codpessoa'], right_on=['codigo'], how='inner')
        .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
        .merge(_pparam_last_comp, left_on=['codcoligada'], right_on=['codcoligada'], how='inner')
        .merge(_pffinanc_valor_averbado, left_on=['codcoligada', 'chapa', 'anocomp', 'mescomp'], right_on=['codcoligada', 'chapa', 'anocomp', 'mescomp'], how='left')
        .assign(cnpj=lambda df: df['cgc'].str.replace(r'\.|\/|\-', ''))
        .merge(_emprestimo_periodo, left_on=['cpf', 'cnpj', 'anocomp', 'mescomp'], right_on=['cpf', 'cnpj', 'anocomp', 'mescomp'], how='inner')
        .assign(periodo=lambda df: df['anocomp'].astype(str) + df['mescomp'].astype(str).str.pad(2, side='left', fillchar='0'))
        .merge(conciliacao_emprestimo, left_on=['cpf', 'cnpj', 'codigo_emprestimo', 'numero_da_parcela', 'periodo'], right_on=['cpffuncionario', 'cnpj', 'codigo_emprestimo', 'numero_da_parcela', 'periodo'], how='inner')
        .query('status_parcela == "Aberta"')
    )

    if len(df) > 0:
        df.loc[df['pffinac_valoraverbado'].notnull(), 'valor_averbado'] = df['pffinac_valoraverbado']
        df.loc[df['valor_averbado'] > 0, 'status_parcela'] = 'Paga'
        df = df.assign(valor_nao_averbado=lambda df: df['valor_averbado'] - df['valor_parcela'])

    return df[['cnpj', 'cpffuncionario', 'periodo', 'valor_averbado', 'valor_nao_averbado', 'motivo', 'status_parcela', 'numero_da_parcela', 'codigo_emprestimo']]

def process_geracao_folha(pfunc: DataFrame, ppessoa: DataFrame, pparam: DataFrame, psecao: DataFrame, pfhstaft: DataFrame) -> DataFrame:
    """
        TODO: doc string

        Afastamento menor ou igual a 15 dias, não deve ser levado para a staging Gerou Folha.
	        Exemplo 1: Início em 25/06 e término em 30/06 - 5 dias(Não deve constar na Gerou Folha)
	        Exemplo 2: Início em 25/06 e término em 10/07 - 15 dias(Não deve constar na Gerou Folha)
        Afastamento maior que 15 dias deve verificar se contempla o mês inteiro, caso positivo deve ser levado para o Gerou Folha. Caso negativo o mês não deve ser considerado no Gerou Folha.
	        Exemplo 1: Início em 15/05 e término em 10/07 - 56 dias(Deve levar para o gerou folha, apenas a informação referente ao mês 6)
	        Exemplo 2: Início em 15/05 e término em 15/06 - 30 dias(Não deve levar o funcionário para o Gerou Folha)
        Afastamento sem data final, deve verificar se contempla o mês inteiro, caso positivo deve ser levado para o Gerou Folha
	        Exemplo 1: Início em 20/04 e sem datafim(Deve levar para o mês 6 e para os meses seguintes até existir uma datafim)
	        Exemplo 2: Início em 15/04 e sem datafim(Deve levar para o mês 5, 6 e para os meses seguintes até existir uma datafim)
    """
    #  .query('(_dtinicio - _dtfinal).dt.days > 15') 

    return (pfunc
            .merge(ppessoa, left_on=['codpessoa'], right_on=['codigo'], how='inner')
            .merge(psecao, left_on=['codcoligada', 'codsecao'], right_on=['codcoligada', 'codigo'], how='inner')
            .query('codsituacao != "E" & codsituacao != "W" & codsituacao != "R"')
            .merge(pparam, left_on=['codcoligada'], right_on=['codcoligada'], how='inner')
            .merge(pfhstaft, left_on=['codcoligada', 'chapa'], right_on=['codcoligada', 'chapa'], how='inner')
            .assign(_dtinicio=lambda df: df['dtinicio'])
            .assign(_dtfinal=lambda df: df['dtfinal'])
            .replace({ '_dtfinal': '0001-01-01T00:00:00.000Z', '_dtinicial': '0001-01-01T00:00:00.000Z' }, '2099-12-31T23:59:59.000Z')
            .assign(_dtinicio=lambda df: to_datetime(df['_dtinicio'], format='%Y-%m-%dT%H:%M:%S.%f').dt.tz_convert(None))
            .assign(_dtfinal=lambda df: to_datetime(df['_dtfinal'], format='%Y-%m-%dT%H:%M:%S.%f').dt.tz_convert(None))
            .assign(_dtcomp_endmonth=lambda df: to_datetime(df['mescomp'].astype(str) + '-' + df['anocomp'].astype(str), format='%m-%Y') + MonthEnd(0))
            .query('(_dtinicio.dt.month <= mescomp & _dtinicio.dt.year <= anocomp) & (_dtfinal.dt.month >= mescomp & _dtfinal.dt.year >= anocomp) & (_dtfinal - _dtinicio).dt.days > 15 & abs((_dtcomp_endmonth - _dtinicio).dt.days) > 15 & _dtcomp_endmonth <= _dtfinal')
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
