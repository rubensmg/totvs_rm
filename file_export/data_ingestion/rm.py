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
        columns = ['cpf', 'nome', 'telefone1', 'telefone2', 'codigo']

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
                    'cpf': 'cpf',
                    'nome': 'nome',
                    'telefone1': 'telefone1',
                    'telefon2': 'telefone2',
                    'codigo': 'codigo'
                }, axis=1))

    def psecao(self) -> DataFrame:
        staging = 'psecao'
        columns = ['CODIGO', 'CODCOLIGADA', 'CGC']

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
                    'CODIGO': 'codigo',
                    'CODCOLIGADA': 'codcoligada',
                    'CGC': 'cgc'
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