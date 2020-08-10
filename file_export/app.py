#!env/bin/python

from pycarol import Carol, Tasks, ApiKeyAuth
from data_ingestion import DataIngestionConsignado, DataIngestionParceiro, DataIngestionRM
from dataupload import DataUploadConsignado, DataUploadStorage
from process import process_conciliacao_emprestimo, process_geracao_arquivo
from os import getenv

login = Carol()

task = None

LONGTASKID = getenv('LONGTASKID', '')

if LONGTASKID != "":
    tasks = Tasks(login)
    task = tasks.get_task(LONGTASKID)
else:
    task = None

def print_carol(msg, log_level="INFO"):
    print(msg)
        
    if not task is None:
        task.add_log(msg, log_level)

di_parceiro = DataIngestionParceiro(login)
di_consignado = DataIngestionConsignado(login)
di_rm = DataIngestionRM(login)
du_consignado = DataUploadConsignado(login)
du_storage = DataUploadStorage(login)

print_carol('Downloading pfunc')
pfunc = di_rm.pfunc()

print_carol('Downloading ppessoa')
ppessoa = di_rm.ppessoa()

print_carol('Downloading pescao')
psecao = di_rm.psecao()

print_carol('Downloading pparam')
pparam = di_rm.pparam()

print_carol('Download pparamadicionais')
pparamadicionais = di_rm.pparamadicionais()

print_carol('Downloading emprestimo')
emprestimo = di_parceiro.emprestimo()

print_carol('Downloading funcionarios')
funcionarios = di_consignado.funcionarios()

print_carol('Downloading gerou_folha')
gerou_folha = di_consignado.gerou_folha()

print_carol('Processing conciliacao_emprestimo')
conciliacao_emprestimo = process_conciliacao_emprestimo(pfunc,
                                                        ppessoa,
                                                        psecao,
                                                        pparam,
                                                        emprestimo,
                                                        funcionarios,
                                                        gerou_folha)

print_carol('Processing file')
conciliacao_emprestimo_geracao_arquivo = process_geracao_arquivo(pfunc,
                                                                ppessoa,
                                                                psecao,
                                                                pparam,
                                                                pparamadicionais,
                                                                emprestimo)

print_carol('Uploading conciliacao_emprestimo')
du_consignado.conciliacao_emprestimo(conciliacao_emprestimo)

print_carol('Generating file')
du_storage.conciliacao_emprestimo(conciliacao_emprestimo_geracao_arquivo)