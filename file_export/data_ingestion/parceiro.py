from pycarol.staging import Staging
from pycarol import Carol
from pandas import DataFrame
from .common import ingestion_development_cache

class DataIngestion:
    connector_name = 'parceiro'

    def __init__(self, login: Carol):
        self.stag = Staging(login)

    def emprestimo(self) -> DataFrame:
        staging = 'emprestimo'
        columns = ['cnpj', 'cpf', 'numero_da_parcela', 'valor_parcela', 'vencimento_parcela']

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