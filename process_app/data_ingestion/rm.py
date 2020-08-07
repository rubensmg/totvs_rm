from pycarol.staging import Staging
from pycarol import Carol
from pandas import DataFrame
from .common import ingestion_development_cache


class DataIngestion:
    connector_name = 'rm_carol'

    def __init__(self, login: Carol):
        self.stag = Staging(login)

    def pfunc(self) -> DataFrame:
        staging = 'pfunc'
        columns = ['DataAdmissao', 'DataDemissao', 'CodPessoa',
                   'Salario', 'Chapa', 'CodSecao', 'CodColigada',
                   'CodSituacao', 'DtPagtoRescisao', 'TipoDemissao']

        return (self.stag.fetch_parquet(staging_name=staging,
                                        connector_name=self.connector_name,
                                        max_workers=None,
                                        backend='pandas',
                                        return_dask_graph=False,
                                        columns=columns,
                                        merge_records=True,
                                        return_metadata=False,
                                        max_hits=None,
                                        callback=None,
                                        cds=True)
                .rename({
                    'DataAdmissao': 'dataadmissao',
                    'DataDemissao': 'datademissao',
                    'CodPessoa': 'codpessoa',
                    'Salario': 'salario',
                    'Chapa': 'chapa',
                    'CodSecao': 'codsecao',
                    'CodColigada': 'codcoligada',
                    'CodSituacao': 'codsituacao',
                    'DtPagtoRescisao': 'dtpagtorescisao',
                    'TipoDemissao': 'tipodemissao'
                }, axis=1))

    def ppessoa(self) -> DataFrame:
        staging = 'ppessoa'
        columns = ['CPF', 'Nome', 'Telefone1', 'Telefone2', 'Codigo']

        return (self.stag.fetch_parquet(staging_name=staging,
                                        connector_name=self.connector_name,
                                        max_workers=None,
                                        backend='pandas',
                                        return_dask_graph=False,
                                        columns=columns,
                                        merge_records=True,
                                        return_metadata=False,
                                        max_hits=None,
                                        callback=None,
                                        cds=True)
                .rename({
                    'CPF': 'cpf',
                    'Nome': 'nome',
                    'Telefone1': 'telefone1',
                    'Telefon2': 'telefone2',
                    'Codigo': 'codigo'
                }, axis=1))

    def psecao(self) -> DataFrame:
        staging = 'psecao'
        columns = ['Codigo', 'CodColigada', 'CGC']

        return (self.stag.fetch_parquet(staging_name=staging,
                                        connector_name=self.connector_name,
                                        max_workers=None,
                                        backend='pandas',
                                        return_dask_graph=False,
                                        columns=columns,
                                        merge_records=True,
                                        return_metadata=False,
                                        max_hits=None,
                                        callback=None,
                                        cds=True)
                .rename({
                    'Codigo': 'codigo',
                    'CodColigada': 'codcoligada',
                    'CGC': 'cgc'
                }, axis=1))

    def pfperff(self) -> DataFrame:
        staging = 'pfperff'
        columns = ['CodColigada', 'Chapa', 'Liquido', 'AnoComp', 'MesComp']

        return (self.stag.fetch_parquet(staging_name=staging,
                                        connector_name=self.connector_name,
                                        max_workers=None,
                                        backend='pandas',
                                        return_dask_graph=False,
                                        columns=columns,
                                        merge_records=True,
                                        return_metadata=False,
                                        max_hits=None,
                                        callback=None,
                                        cds=True)
                .rename({
                    'CodColigada': 'codcoligada',
                    'Chapa': 'chapa',
                    'Liquido': 'liquido',
                    'AnoComp': 'anocomp',
                    'MesComp': 'mescomp'
                }, axis=1))

    def pfemprt(self) -> DataFrame:
        staging = 'pfemprt'
        columns = ['CodColigada', 'Chapa', 'SaldoDevedor']

        return (self.stag.fetch_parquet(staging_name=staging,
                                        connector_name=self.connector_name,
                                        max_workers=None,
                                        backend='pandas',
                                        return_dask_graph=False,
                                        columns=columns,
                                        merge_records=True,
                                        return_metadata=False,
                                        max_hits=None,
                                        callback=None,
                                        cds=True)
                .rename({
                    'CodColigada': 'codcoligada',
                    'Chapa': 'chapa',
                    'SaldoDevedor': 'saldodevedor'
                }, axis=1))

    def pffinanc(self) -> DataFrame:
        staging = 'pffinanc'
        columns = ['Chapa', 'CodColigada', 'Valor',
                   'CodEvento', 'AnoComp', 'MesComp']

        return (self.stag.fetch_parquet(staging_name=staging,
                                        connector_name=self.connector_name,
                                        max_workers=None,
                                        backend='pandas',
                                        return_dask_graph=False,
                                        columns=columns,
                                        merge_records=True,
                                        return_metadata=False,
                                        max_hits=None,
                                        callback=None,
                                        cds=True)
                .rename({
                    'Chapa': 'chapa',
                    'CodColigada': 'codcoligada',
                    'Valor': 'valor',
                    'CodEvento': 'codeventos',
                    'AnoComp': 'anocomp',
                    'MesComp': 'mescomp'
                }, axis=1))

    def trecisao(self) -> DataFrame:
        return DataFrame(data=list({
            '1': 'Dispensa por justa causa',
            '2': 'Dispensa sem justa causa',
            '3': 'Pedido de demissão',
            '4': 'Pedido de demissão',
            '6': 'Dispensa sem justa causa',
            '7': 'Dispensa sem justa causa',
            '8': 'Falecimento',
            'A': 'Aposentadoria',
            'B': 'Dispensa sem justa causa',
            'C': 'Dispensa sem justa causa',
            'D': 'Aposentadoria',
            'E': 'Aposentadoria',
            'F': 'Falecimento',
            'G': 'Dispensa sem justa causa',
            'H': 'Dispensa sem justa causa'
        }.items()), columns=['tipodemissao_rm', 'tiporecisao'])

    @ingestion_development_cache
    def pparam(self) -> DataFrame:
        staging = 'pparam'
        columns = ['AnoComp', 'MesComp', 'CodColigada']

        return (self.stag.fetch_parquet(staging_name=staging,
                                        connector_name=self.connector_name,
                                        max_workers=None,
                                        backend='pandas',
                                        return_dask_graph=False,
                                        columns=columns,
                                        merge_records=True,
                                        return_metadata=False,
                                        max_hits=None,
                                        callback=None,
                                        cds=True)
                .rename({
                    'AnoComp': 'anocomp',
                    'MesComp': 'mescomp',
                    'CodColigada': 'codcoligada'
                }, axis=1))

    def pfhstaft(self) -> DataFrame:
        staging = 'pfhstaft'
        columns = ['CodColigada', 'Chapa', 'DtInicio', 'DtFinal']

        return (self.stag.fetch_parquet(staging_name=staging,
                                        connector_name=self.connector_name,
                                        max_workers=None,
                                        backend='pandas',
                                        return_dask_graph=False,
                                        columns=columns,
                                        merge_records=True,
                                        return_metadata=False,
                                        max_hits=None,
                                        callback=None,
                                        cds=True)
                .rename({
                    'CodColigada': 'codcoligada',
                    'Chapa': 'chapa',
                    'DtInicio': 'dtinicio',
                    'DtFinal': 'dtfinal'
                }, axis=1))

    def tsalarycount(self) -> DataFrame:
        step = 1000
        max_range = 15
        range_salary = [(f'de{(index*step)+1}a{(index+1)*step}',
                         (index*step)+1.0, (index+1.0)*step) for index in range(max_range)]
        range_salary.append(
            ('de15001ainf', float(max_range*step+1.0), float('inf')))
        return DataFrame(data=range_salary, columns=['identifier', 'init', 'end'])

    @ingestion_development_cache
    def pparamadicionais(self) -> DataFrame:
        staging = 'pparamadicionais'
        columns = ['CodColigada', 'AnoCompCarolPFFINANC', 'AnoCompCarolPFPERFF', 'EventoBaseBV', 'EventoBaseCreditas',
                   'EventoDescontoBV', 'EventoDescontoCreditas', 'MesCompCarolPFFINANC', 'MesCompCarolPFPERFF']

        return (self.stag.fetch_parquet(staging_name=staging,
                                        connector_name=self.connector_name,
                                        max_workers=None,
                                        backend='pandas',
                                        return_dask_graph=False,
                                        columns=columns,
                                        merge_records=True,
                                        return_metadata=False,
                                        max_hits=None,
                                        callback=None,
                                        cds=True)
                .rename({
                    'CodColigada': 'codcoligada',
                    'AnoCompCarolPFFINANC': 'anocompcarolpffinanc',
                    'AnoCompCarolPFPERFF': 'anocompcarolpfperff',
                    'EventoBaseBV': 'eventobasebv',
                    'EventoBaseCreditas': 'eventobasecreditas',
                    'EventoDescontoBV': 'eventodescontobv',
                    'EventoDescontoCreditas': 'eventodescontocreditas',
                    'MesCompCarolPFFINANC': 'mescompcarolpffinanc',
                    'MesCompCarolPFPERFF': 'mescompcarolpfperff'
                }, axis=1))
