from pycarol.staging import Staging
from pycarol import Carol
from pandas import DataFrame
from .common import ingestion_development_cache


class DataIngestion:
    connector_name = 'consignado_appresult'

    def __init__(self, login: Carol):
        self.stag = Staging(login)

    def gerou_folha(self) -> DataFrame:
        staging = 'gerou_folha'
        columns = ['cpf', 'cnpj', 'ano', 'mes', 'gerou_folha']

        return self.stag.fetch_parquet(staging_name=staging,
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

    def funcionarios(self):
        staging = 'funcionarios'
        columns = ['cnpj', 'cpffuncionario', 'consignavel']

        return self.stag.fetch_parquet(staging_name=staging,
                                       connector_name=self.connector_name,
                                       max_workers=None,
                                       backend='pandas',
                                       return_dask_graph=False,
                                       columns=None,
                                       merge_records=True,
                                       return_metadata=False,
                                       max_hits=None,
                                       callback=None,
                                       cds=True)