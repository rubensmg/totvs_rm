from pycarol import Carol, Staging
from pandas import DataFrame
from .common import upload_development_generate_csv


class DataUpload:
    connector_name = 'consignado_appresult'

    def __init__(self, login: Carol):
        self.stag = Staging(login)

    def funcionarios(self, df: DataFrame):
        staging = 'funcionarios'
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

    def recisao(self, df: DataFrame):
        staging = 'recisao_consignado'
        crosswalk = ['cnpj', 'cpf', 'data_demissao']

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

    @upload_development_generate_csv
    def conciliacao_emprestimo(self, df: DataFrame):
        # Validate Size
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

    def gerou_folha(self, df: DataFrame):
        staging = 'gerou_folha'
        crosswalk = ['ano', 'mes', 'cnpj', 'cpf']

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

    def count_faixa(self, df: DataFrame):
        staging = 'countporfaixa'
        crosswalk = ['cnpj']

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

    def media_salarial(self, df: DataFrame):
        staging = 'mediasalarial'
        crosswalk = ['cnpj']

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

    def turnover(self, df: DataFrame):
        staging = 'turnover'
        crosswalk = ['cnpj']

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
