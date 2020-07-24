#!env/bin/python

from dotenv import load_dotenv
from pycarol import Carol, Tasks
from data_ingestion import DataIngestionRM, DataIngestionParceiro
from data_upload import DataUploadConsignado
from process import process_funcionario, process_recisao, process_geracao_folha, process_count_faixa, process_media_salarial, process_turnover, process_conciliacao_emprestimo

login = Carol()


task = None

LONGTASKID = os.getenv('LONGTASKID', '')

if LONGTASKID != "":
    tasks = Tasks(login)
    task = tasks.get_task(LONGTASKID)
else:
    task = None

def print_carol(msg, log_level="INFO"):
    print(msg)
        
    if not task is None:
        task.add_log(msg, log_level)


di_rm = DataIngestionRM(login)
di_parceiro = DataIngestionParceiro(login)
du = DataUploadConsignado(login)

print_carol('Downloading pfunc')
pfunc = di_rm.pfunc()

print_carol('Downloading ppessoa')
ppessoa = di_rm.ppessoa()

print_carol('Downloading pescao')
psecao = di_rm.psecao()

print_carol('Downloading pfperff')
pfperff = di_rm.pfperff()

print_carol('Downloading pfemprt')
pfemprt = di_rm.pfemprt()

print_carol('Downloading pffinanc')
pffinanc = di_rm.pffinanc()

print_carol('Download trecisao')
trecisao = di_rm.trecisao()

print_carol('Download pparam')
pparam = di_rm.pparam()

print_carol('Download pfhstaft')
pfhstaft = di_rm.pfhstaft()

print_carol('Download tsalarycount')
tsalarycount = di_rm.tsalarycount()

print_carol('Download pparamadicionais')
pparamadicionais = di_rm.pparamadicionais()

print_carol('Download emprestimo')
emprestimo = di_parceiro.emprestimo()

print_carol('Processing funcionario')
funcionarios = process_funcionario(pfunc,
                                   ppessoa,
                                   psecao,
                                   pfperff,
                                   pfemprt,
                                   pparam,
                                   pffinanc)

print_carol('Processing recisao')
recisao = process_recisao(pfunc,
                          ppessoa,
                          psecao,
                          pffinanc,
                          trecisao,
                          pparamadicionais)

# print_carol('Processing conciliacao_emprestimo')
# conciliacao_emprestimo = process_conciliacao_emprestimo(pfunc,
#                                                         ppessoa,
#                                                         psecao,
#                                                         pffinanc,
#                                                         pparamadicionais,
#                                                         emprestimo)

print_carol('Processing geracao_folha')
geracao_folha = process_geracao_folha(pfunc,
                                      ppessoa,
                                      pparam,
                                      psecao,
                                      pfhstaft)

print_carol('Processing count_faixa')
count_faixa = process_count_faixa(pfunc,
                                  psecao,
                                  tsalarycount)

print_carol('Processing media_salarial')
media_salarial = process_media_salarial(pfunc,
                                        psecao)

print_carol('Processing turnover')
turnover = process_turnover(pfunc,
                            psecao)

print_carol('Upload funcionarios')
du.funcionarios(funcionarios)

print_carol('Upload recisao')
du.recisao(recisao)

print_carol('Upload conciliacao_emprestimo')
du.conciliacao_emprestimo(conciliacao_emprestimo)

print_carol('Upload geraco_folha')
du.gerou_folha(geracao_folha)

print_carol('Upload count_faixa')
du.count_faixa(count_faixa)

print_carol('Upload media_salarial')
du.media_salarial(media_salarial)

print_carol('Upload turnover')
du.turnover(turnover)
