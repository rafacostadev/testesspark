import pandas as pd
from pyspark.sql import SparkSession
from gender_guesser_br import Genero

diretorio = "Base_de_vendas_COOKIE.xlsx"
arquivo = pd.ExcelFile(diretorio)
sheets = arquivo.sheet_names

# spark = SparkSession.builder.appName("test").getOrCreate()

# for sheet in sheets:
#     print(f"Nome da tabela: {sheet} \n")
#     excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheet)
#     for i in range(0, 4): 
#         if "id" in str(excelSheet.iloc[i, 0]):
#             excelSheet = pd.read_excel(diretorio, sheet_name=sheet, skiprows=i)
#             df = spark.createDataFrame(excelSheet)
#             resultado = df.select("*")
#             resultado.limit(5).show()

def sexoPorNome(nome):
    return Genero(nome)()

print(f"Nome da tabela: {sheets[4]} \n")
excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheets[4])
for i in range(0, 4): 
    if "id" in str(excelSheet.iloc[i, 0]):
        excelSheet = pd.read_excel(diretorio, sheet_name=sheets[4], skiprows=i)
        excelSheet = excelSheet.assign(sexo=excelSheet['nome'].apply(sexoPorNome))
        excelSheet['sexo'] = None
        nomes = excelSheet['nome'].values
        for indice, nome in enumerate (nomes):
            nome = nome.split(" ")[0]
            excelSheet.loc[indice, 'sexo'] = Genero(nome)()
        print(excelSheet)
        
        # df = spark.createDataFrame(excelSheet)
        # resultado = df.select("id_cliente","nome")
        # resultado.show(resultado.count())
        # primeiroNome = resultado[0]['nome'].split(" ")[0]
        # genero = Genero(primeiroNome)()
        # print(f"{resultado[0]['nome']} - {genero}")
        # resultado.limit(5).show()
        # df.coalesce(1).write.parquet("parquets")