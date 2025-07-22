import pandas as pd
from pyspark.sql import SparkSession
from gender_guesser_br import Genero

diretorio = "Base_de_vendas_COOKIE.xlsx"
arquivo = pd.ExcelFile(diretorio)
sheets = arquivo.sheet_names

spark = SparkSession.builder \
    .appName("Conectar ao MySQL") \
    .config("spark.jars", "mysql-connector-j-9.3.0.jar") \
    .getOrCreate()

# 3. Configurar conexão com MySQL
url = "jdbc:mysql://localhost:3306/testes"
tabela = "tb_fornecedor"
usuario = "root"
senha = "123456"


# O CÓDIGO ABAIXO TRATA TODAS AS SHEETS DA PLANILHA PARA QUE OS HEADERS FIQUEM CORRETOS E OS DADOS POSICIONADOS

# print(sheets)

# for sheet in sheets:
#     print(f"Nome da tabela: {sheet} \n")
#     excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheet)
#     for i in range(0, 4): 
#         if "id" in str(excelSheet.iloc[i, 0]):
#             excelSheet = pd.read_excel(diretorio, sheet_name=sheet, skiprows=i)
#             df = spark.createDataFrame(excelSheet)
#             resultado = df.select("*")
#             resultado.limit(5).show()

# def sexoPorNome(nome):
#     return Genero(nome)()

# O CÓDIGO ABAIXO CRIA UM DF COM OS DADOS DO FORNECEDOR E INSERE NO BANCO DE DADOS

excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheets[4])
for i in range(0, 4): 
    if "id" in str(excelSheet.iloc[i, 0]):
        excelSheet = pd.read_excel(diretorio, sheet_name=sheets[3], skiprows=i)
        df = spark.createDataFrame(excelSheet)
        df = df.select("razao_social", "nome_fantasia", "cnpj")
        df.show()
        
df.write \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", tabela) \
    .option("user", usuario) \
    .option("password", senha) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .mode("append") \
    .save()
        
        
# O CÓDIGO ABAIXO RESOLVE O PROBLEMA DO SEXO(DA MAIORIA DOS CLIENTES) E INSERE EM UMA NOVA COLUNA
        
# print(f"Nome da tabela: {sheets[4]} \n")
# excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheets[4])
# for i in range(0, 4): 
#     if "id" in str(excelSheet.iloc[i, 0]):
#         excelSheet = pd.read_excel(diretorio, sheet_name=sheets[4], skiprows=i)
#         excelSheet = excelSheet.assign(sexo=excelSheet['nome'].apply(sexoPorNome))
#         excelSheet['sexo'] = None
#         nomes = excelSheet['nome'].values
#         for indice, nome in enumerate (nomes):
#             nome = nome.split(" ")[0]
#             excelSheet.loc[indice, 'sexo'] = Genero(nome)()
#         print(excelSheet)
        
        # df = spark.createDataFrame(excelSheet)
        # resultado = df.select("id_cliente","nome")
        # resultado.show(resultado.count())
        # primeiroNome = resultado[0]['nome'].split(" ")[0]
        # genero = Genero(primeiroNome)()
        # print(f"{resultado[0]['nome']} - {genero}")
        # resultado.limit(5).show()
        # df.coalesce(1).write.parquet("parquets")