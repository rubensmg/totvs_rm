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
        columns = ['DATAADMISSAO', 'DATADEMISSAO', 'CODPESSOA',
                   'SALARIO', 'CHAPA', 'CODSECAO', 'CODCOLIGADA',
                   'CODSITUACAO', 'DTPAGTORESCISAO', 'TIPODEMISSAO']

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
                    'DATAADMISSAO': 'dataadmissao',
                    'DATADEMISSAO': 'datademissao',
                    'CODPESSOA': 'codpessoa',
                    'SALARIO': 'salario',
                    'CHAPA': 'chapa',
                    'CODSECAO': 'codsecao',
                    'CODCOLIGADA': 'codcoligada',
                    'CODSITUACAO': 'codsituacao',
                    'DTPAGTORESCISAO': 'dtpagtorescisao',
                    'TIPODEMISSAO': 'tipodemissao'
                }, axis=1))

    def ppessoa(self) -> DataFrame:
        staging = 'ppessoa'
        columns = ['CPF', 'NOME', 'TELEFONE1', 'TELEFONE2', 'CODIGO']

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
                    'NOME': 'nome',
                    'TELEFONE1': 'telefone1',
                    'TELEFONE2': 'telefone2',
                    'CODIGO': 'codigo'
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
