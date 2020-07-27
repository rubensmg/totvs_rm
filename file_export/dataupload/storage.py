from pycarol import Carol, Storage
from pandas import DataFrame
from datetime import datetime

class DataUpload:
    def __init__(self, login):
        storage = Storage(login)
    
    def conciliacao_emprestimo(self, df: DataFrame):
        filename = 'exportacao_consignado_'+str(datetime.today())[:7]+'.txt'
        
        with open(filename, mode='wt') as f:
            for index, row in df.iterrows():
                file_text = ''
                file_text += row['chapa'].ljust(16, ' ')
                file_text += datetime.strptime(row['datapagamento'], '%Y-%m-%d').strftime('%d%m%Y')
                file_text += '0438'
                file_text += ('0' + row['horapagamento']).ljust(15, ' ')
                file_text += str('{0:.2f}'.format(row['referencia'])).ljust(15, ' ')
                file_text += str('{0:.2f}'.format(row['valor'])).ljust(15, ' ')
                file_text += str('{0:.2f}'.format(row['valor']))
                file_text += 'N'
                file_text += 'N'
                file_text += '\r\n'
                f.write(file_text)
        
        self.storage.save(filename, './' + filename, format='file')