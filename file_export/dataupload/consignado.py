from pycarol import Carol, Staging
from pandas import DataFrame
from .common import upload_development_generate_csv

class DataUpload:
    connector_name = 'consignado_appresult'

    def __init__(self, login: Carol):
        self.stag = Staging(login)

    def conciliacao_emprestimo(self, df: DataFrame):
        staging = 'conciliacao_emprestimo'
        crosswalk = ['cnpj', 'matriculafuncionario']

        if len(df) > 0:
            self.stag.send_data(data=df,
                                staging_name=staging,
                                connector_name=self.connector_name,
                                step_size=1000,
                                crosswalk_auto_create=crosswalk,
                                auto_create_schema=True,
                                storage_only=True,
                                async_send=False,
                                force=True,
                                max_workers=5)
