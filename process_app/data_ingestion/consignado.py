from pycarol.staging import Staging
from pycarol import Carol
from pandas import DataFrame
from .common import ingestion_development_cache


class DataIngestion:
    connector_name = 'consignado_appresult'

    def __init__(self, login: Carol):
        self.stag = Staging(login)

    def conciliacao_emprestimo(self) -> DataFrame:
        staging = 'conciliacao_emprestimo'
        columns = ['cnpj', 'cpffuncionario', 'periodo', 'valor_averbado', 'valor_nao_averbado', 'motivo', 'status_parcela', 'numero_da_parcela', 'codigo_emprestimo']

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