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

    @ingestion_development_cache
    def pparamadicionais(self) -> DataFrame:
        staging = 'pparamadicionais'
        columns = ['CodColigada', 'AnoCompCarolPFFINANC', 'AnoCompCarolPFPERFF', 'EventoBaseBV', 'EventoBaseCreditas',
                    'IntegradoBV',  'IntegradoCreditas', 'EventoDescontoBV', 'EventoDescontoCreditas', 'MesCompCarolPFFINANC',
                    'MesCompCarolPFPERFF']

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
                    'IntegradoBV': 'integradobv',
                    'IntegradoCreditas': 'integradocreditas',
                    'EventoBaseCreditas': 'eventobasecreditas',
                    'EventoDescontoBV': 'eventodescontobv',
                    'EventoDescontoCreditas': 'eventodescontocreditas',
                    'MesCompCarolPFFINANC': 'mescompcarolpffinanc',
                    'MesCompCarolPFPERFF': 'mescompcarolpfperff'
                }, axis=1))