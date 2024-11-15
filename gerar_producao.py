
import csv
import random
from datetime import datetime, timedelta

# Configurações
total_linhas = 5000000
produto_id_min = 13001
produto_id_max = 13120
data_inicio = datetime(2022, 1, 1)
data_fim = datetime.now()
nome_arquivo = 'fat_producao.csv'

# Função para gerar uma data aleatória entre data_inicio e data_fim
def gerar_data_aleatoria(data_inicio, data_fim):
    delta = data_fim - data_inicio
    int_delta = delta.days * 24 * 60 * 60 + delta.seconds
    random_second = random.randrange(int_delta)
    return data_inicio + timedelta(seconds=random_second)

# Gerar registros
registros = []
for i in range(1, total_linhas + 1):
    data_hora = gerar_data_aleatoria(data_inicio, data_fim)
    produto_id = random.randint(produto_id_min, produto_id_max)
    registros.append((data_hora, produto_id))

# Ordenar registros por data_hora
registros.sort(key=lambda x: x[0])

# Adicionar caixa_id sequencial após ordenar
registros = [(i + 1, registro[0], registro[1]) for i, registro in enumerate(registros)]

# Salvar registros em CSV
with open(nome_arquivo, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['caixa_id', 'data_hora', 'produto_id'])
    for registro in registros:
        writer.writerow([registro[0], registro[1].strftime('%Y-%m-%d %H:%M:%S'), registro[2]])

print(f"Arquivo {nome_arquivo} gerado com sucesso!")
