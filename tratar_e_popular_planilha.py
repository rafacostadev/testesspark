import pandas as pd
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col , split, explode, trim, regexp_extract, dayofmonth, month, year, date_format, quarter
from gender_guesser_br import Genero

# Configurações de arquivos, de sessão do spark e de conexão com o bd

diretorio = "Base_de_vendas_COOKIE.xlsx"
arquivo = pd.ExcelFile(diretorio)
sheets = arquivo.sheet_names

url = "jdbc:mysql://localhost:3306/loja_cookies"
dw_url = "jdbc:mysql://localhost:3306/modelo_dimensional"
usuario = "root"
senha = "123456"
jarPath = "mysql-connector-j-9.3.0.jar"
driver = "com.mysql.cj.jdbc.Driver"

spark = SparkSession.builder \
    .appName("Conectar ao MySQL") \
    .config("spark.jars", jarPath) \
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false") \
    .getOrCreate()

options = {
    "header": "true",
    "delimiter": ","
}


# Funções auxiliares

def sexoPorNome(nome):
    try:
        resultado = Genero(nome)()
        if resultado in ["masculino", "feminino"]:
            return resultado
        else:
            return "desconhecido"
    except:
        return "desconhecido"

def tratar_clientes():
    # A FUNÇÃO RESOLVE O PROBLEMA DO SEXO(DA MAIORIA DOS CLIENTES) E INSERE OS DADOS TRATADOS NO BANCO DE DADOS
    excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheets[4])
    for i in range(0, 4): 
        if "id" in str(excelSheet.iloc[i, 0]):
            excelSheet = pd.read_excel(diretorio, sheet_name=sheets[4], skiprows=i)
            excelSheet["sexo"] = None
            nomes = excelSheet['nome'].values
            for indice, nome in enumerate (nomes):
                nome = nome.split(" ")[0]
                excelSheet.loc[indice, "sexo"] = sexoPorNome(nome)
            df = spark.createDataFrame(excelSheet)
            df = df.select("id_cliente","nome", "cpf", "sexo")
            
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "tb_cliente") \
        .option("user", usuario) \
        .option("password", senha) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
            
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

def tratar_enderecos_clientes():
    # A FUNÇÃO TRATA OS ENDEREÇOS DOS CLIENTES E INSERE NO BANCO DE DADOS
    excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheets[4])
    for i in range(0, 4): 
        if "id" in str(excelSheet.iloc[i, 0]):
            excelSheet = pd.read_excel(diretorio, sheet_name=sheets[4], skiprows=i)
            cidades = excelSheet["cidade"]
            ufs = excelSheet["uf"] 
            ceps = excelSheet["cep"]
            for indice, cidade in enumerate (cidades):
                if pd.isnull(cidade) or pd.isnull(ufs[indice]) or pd.isnull(ceps[indice]):
                    excelSheet.loc[indice, "cidade"] = "Recife"
                    excelSheet.loc[indice, "uf"] = "PE"
                    excelSheet.loc[indice, "cep"] = "00000-000"
            df = spark.createDataFrame(excelSheet)
            df = df.select("id_cliente","rua","numero", "complemento", "bairro", "cidade", "uf", "cep")
            
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "tb_endereco_cliente") \
        .option("user", usuario) \
        .option("password", senha) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
            
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
    
def tratar_fornecedores():
    # A FUNÇÃO TRATA OS FORNECEDORES E INSERE NO BANCO DE DADOS
    excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheets[3])
    for i in range(0, 4): 
        if "id" in str(excelSheet.iloc[i, 0]):
            excelSheet = pd.read_excel(diretorio, sheet_name=sheets[3], skiprows=i)
            df = spark.createDataFrame(excelSheet)
            df = df.select("id_fornecedor","razao_social", "nome_fantasia", "cnpj", "cidade", "uf")
            
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "tb_fornecedor") \
        .option("user", usuario) \
        .option("password", senha) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
            
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

def tratar_enderecos_fornecedores():
    # A FUNÇÃO TRATA OS ENDEREÇOS DOS FORNECEDORES E INSERE NO BANCO DE DADOS
    excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheets[3])
    for i in range(0, 4): 
        if "id" in str(excelSheet.iloc[i, 0]):
            excelSheet = pd.read_excel(diretorio, sheet_name=sheets[3], skiprows=i)
            cidades = excelSheet["cidade"]
            ufs = excelSheet["uf"] 
            for indice, cidade in enumerate (cidades):
                if pd.isnull(cidade) or pd.isnull(ufs[indice]):
                    excelSheet.loc[indice, "cidade"] = "Recife"
                    excelSheet.loc[indice, "uf"] = "PE"
            df = spark.createDataFrame(excelSheet)
            df = df.select("id_fornecedor","cidade","uf")
            
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "tb_endereco_fornecedor") \
        .option("user", usuario) \
        .option("password", senha) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
                
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
    
def tratar_ingredientes():
    # A FUNÇÃO TRATA OS INGREDIENTES E INSERE NO BANCO DE DADOS
    excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheets[2])
    for i in range(0, 4): 
        if "id" in str(excelSheet.iloc[i, 0]):
            excelSheet = pd.read_excel(diretorio, sheet_name=sheets[2], skiprows=i)
            df = spark.createDataFrame(excelSheet)
            df = df.select("id_ingrediente",col("ingrediente").alias("nome_ingrediente"))
            
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "tb_ingrediente") \
        .option("user", usuario) \
        .option("password", senha) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
                
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
        
def tratar_ingredientes_fornecedores():
    # A FUNÇÃO TRATA OS FORNECEDORES DE CADA INGREDIENTE E INSERE NO BANCO DE DADOS
    excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheets[2])
    for i in range(0, 4): 
        if "id" in str(excelSheet.iloc[i, 0]):
            excelSheet = pd.read_excel(diretorio, sheet_name=sheets[2], skiprows=i)
            df = spark.createDataFrame(excelSheet)
            df = df.select("id_fornecedor", "id_ingrediente")
            
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "tb_fornecedor_ingrediente") \
        .option("user", usuario) \
        .option("password", senha) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
    
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
        
def tratar_tipos_produtos():
    # A FUNÇÃO TRATA OS TIPOS DE PRODUTO INSERE NO BANCO DE DADOS
    excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheets[0])
    for i in range(0, 4): 
        if "id" in str(excelSheet.iloc[i, 0]):
            excelSheet = pd.read_excel(diretorio, sheet_name=sheets[0], skiprows=i)
            df = spark.createDataFrame(excelSheet)
            df = df.select("id_tipo_produto", "descricao")
            
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "tb_tipo_produto") \
        .option("user", usuario) \
        .option("password", senha) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
    
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

def tratar_produtos():
    # A FUNÇÃO TRATA OS PRODUTOS E INSERE NO BANCO DE DADOS
    excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheets[1])
    for i in range(0, 4): 
        if "id" in str(excelSheet.iloc[i, 0]):
            excelSheet = pd.read_excel(diretorio, sheet_name=sheets[1], skiprows=i)
            df = spark.createDataFrame(excelSheet)
            df = df.select("id_produto", "id_tipo_produto", col("descricao").alias("nome"), col("preco_uniatrio").alias("preco_unitario"))
            
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "tb_produto") \
        .option("user", usuario) \
        .option("password", senha) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
    
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

def tratar_venda():
    # A FUNÇÃO AS VENDAS E INSERE NO BANCO DE DADOS
    excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheets[5])
    for i in range(0, 4): 
        if "id" in str(excelSheet.iloc[i, 0]):
            excelSheet = pd.read_excel(diretorio, sheet_name=sheets[5], skiprows=i)
            df = spark.createDataFrame(excelSheet)
            df = df.select("id_venda", "id_cliente", "status", "data_venda")
            
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "tb_venda") \
        .option("user", usuario) \
        .option("password", senha) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
    
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

def tratar_feedback():
    # A FUNÇÃO TRATA OS PRODUTOS E INSERE NO BANCO DE DADOS
    excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheets[6])
    for i in range(0, 4): 
        if "id" in str(excelSheet.iloc[i, 0]):
            excelSheet = pd.read_excel(diretorio, sheet_name=sheets[6], skiprows=i)
            df = spark.createDataFrame(excelSheet)
            df = df.select(col("idvenda").alias("id_venda"), "nota", "comentario")
            
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "tb_feedback") \
        .option("user", usuario) \
        .option("password", senha) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
    
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

def tratar_venda_produto():
    excelSheet = pd.read_excel(diretorio, header = None, sheet_name=sheets[5])
    tabelaProdutos = pd.read_excel(diretorio, header = None, sheet_name=sheets[1])
    for i in range(0, 4): 
        if "id" in str(excelSheet.iloc[i, 0]):
            excelSheet = pd.read_excel(diretorio, sheet_name=sheets[5], skiprows=i)
            tabelaProdutos = pd.read_excel(diretorio, sheet_name=sheets[1], skiprows=3)
            df = spark.createDataFrame(excelSheet)
            df_produtos = spark.createDataFrame(tabelaProdutos)
            df = df.select("id_venda", col("itens vendidos").alias("itens_vendidos"))
            df_tratado = df.withColumn("produtos_tratados", split("itens_vendidos", ","))
            df_tratado = df_tratado.withColumn("produto", explode("produtos_tratados"))
            df_tratado = df_tratado.withColumn("produto", trim("produto"))
            df_tratado = df_tratado.withColumn("quantidade", regexp_extract("produto", r"(\d+)X", 1)) \
            .withColumn("nome_produto", regexp_extract("produto", r"\d+X\s*(.*)", 1))
            df_final = df_tratado.join(df_produtos, df_tratado["nome_produto"] == df_produtos["descricao"], how="left")
            df_final = df_final.select("id_venda", "id_produto", "quantidade")
            
    df_final.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "tb_venda_produto") \
        .option("user", usuario) \
        .option("password", senha) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
    
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
            
def verificarBanco():
    df_venda = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", "tb_venda") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()

    dim_tempo = df_venda.select("data_venda").distinct() \
        .withColumn("dia", dayofmonth("data_venda")) \
        .withColumn("mes", month("data_venda")) \
        .withColumn("ano", year("data_venda")) \
        .withColumn("nome_mes", date_format("data_venda", "MMMM")) \
        .withColumn("dia_da_semana", date_format("data_venda", "EEEE")) \
        .withColumn("trimestre", quarter("data_venda")) \
        .withColumnRenamed("data_venda", "data_completa")

    dim_tempo.write \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "dim_tempo") \
        .option("user", usuario) \
        .option("password", senha) \
        .mode("append") \
        .save()

    df_cliente = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", "tb_cliente") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()

    df_endereco = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", "tb_endereco_cliente") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()

    dim_cliente = df_cliente.join(df_endereco, "id_cliente") \
        .select(
            "id_cliente", "nome", "cpf", "sexo", "data_nascimento",
            "cidade", "uf"
        ).withColumnRenamed("data_nascimento", "data_nascimento")

    dim_cliente.write \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "dim_cliente") \
        .option("user", usuario) \
        .option("password", senha) \
        .mode("append") \
        .save()

    df_produto = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", "tb_produto") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()

    df_tipo = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", "tb_tipo_produto") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()

    dim_produto = df_produto.join(df_tipo, "id_tipo_produto") \
        .select(
            "id_produto",
            col("nome").alias("nome_produto"),
            col("descricao").alias("descricao_tipo_produto"),
            col("preco_unitario").alias("preco_atual")
        )

    dim_produto.write \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "dim_produto") \
        .option("user", usuario) \
        .option("password", senha) \
        .mode("append") \
        .save()

    df_ingrediente = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", "tb_ingrediente") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()

    dim_ingrediente = df_ingrediente.select("id_ingrediente", "nome_ingrediente")

    dim_ingrediente.write \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "dim_ingrediente") \
        .option("user", usuario) \
        .option("password", senha) \
        .mode("append") \
        .save()

    df_fornecedor = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", "tb_fornecedor") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()

    dim_fornecedor = df_fornecedor.select(
        "id_fornecedor", "razao_social", "nome_fantasia", "cnpj", "cidade", "uf"
    )

    dim_fornecedor.write \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "dim_fornecedor") \
        .option("user", usuario) \
        .option("password", senha) \
        .mode("append") \
        .save()

    df_feedback = spark.read \
        .format("jdbc") \
        .option("url", url  ) \
        .option("driver", driver) \
        .option("dbtable", "tb_feedback") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()

    dim_feedback = df_feedback.select("id_feedback", "nota", "comentario")

    dim_feedback.write \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "dim_feedback") \
        .option("user", usuario) \
        .option("password", senha) \
        .mode("append") \
        .save()
        
        # Já tem os dados do modelo relacional
    df_produto_ingrediente = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", "tb_produto_ingrediente") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()

    dim_produto = spark.read \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "dim_produto") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()

    dim_ingrediente = spark.read \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "dim_ingrediente") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()

    bridge_produto_ingrediente = df_produto_ingrediente.alias("pi") \
        .join(dim_produto.alias("p"), col("pi.id_produto") == col("p.id_produto"), "inner") \
        .join(dim_ingrediente.alias("i"), col("pi.id_ingrediente") == col("i.id_ingrediente"), "inner") \
        .select(
            col("p.sk_produto"),
            col("i.sk_ingrediente")
        ).distinct()

    bridge_produto_ingrediente.write \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "bridge_produto_ingrediente") \
        .option("user", usuario) \
        .option("password", senha) \
        .mode("append") \
        .save()

    df_ingrediente_fornecedor = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("driver", driver) \
    .option("dbtable", "tb_fornecedor_ingrediente") \
    .option("user", usuario) \
    .option("password", senha) \
    .load()

    dim_fornecedor = spark.read \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "dim_fornecedor") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()

    bridge_ingrediente_fornecedor = df_ingrediente_fornecedor.alias("ifor") \
        .join(dim_ingrediente.alias("i"), col("ifor.id_ingrediente") == col("i.id_ingrediente"), "inner") \
        .join(dim_fornecedor.alias("f"), col("ifor.id_fornecedor") == col("f.id_fornecedor"), "inner") \
        .select(
            col("i.sk_ingrediente"),
            col("f.sk_fornecedor")
        ).distinct()

    bridge_ingrediente_fornecedor.write \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "bridge_ingrediente_fornecedor") \
        .option("user", usuario) \
        .option("password", senha) \
        .mode("append") \
        .save()
        
    df_venda = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("driver", driver) \
    .option("dbtable", "tb_venda") \
    .option("user", usuario) \
    .option("password", senha) \
    .load()

    dim_tempo = spark.read \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "dim_tempo") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()

    dim_cliente = spark.read \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "dim_cliente") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()

    dim_feedback = spark.read \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "dim_feedback") \
        .option("user", usuario) \
        .option("password", senha) \
        .load()
    
    df_venda_produto = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/loja_cookies") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "tb_venda_produto") \
    .option("user", usuario) \
    .option("password", senha) \
    .load()

    fato_vendas = df_venda.alias("v") \
    .join(dim_tempo.alias("t"), col("v.data_venda") == col("t.data_completa"), "inner") \
    .join(dim_cliente.alias("c"), col("v.id_cliente") == col("c.id_cliente"), "inner") \
    .join(df_venda_produto.alias("vp"), col("v.id_venda") == col("vp.id_venda"), "inner") \
    .join(dim_produto.alias("p"), col("vp.id_produto") == col("p.id_produto"), "inner") \
    .join(dim_feedback.alias("f"), col("v.id_feedback") == col("f.id_feedback"), "inner") \
    .select(
        col("t.sk_tempo"),
        col("c.sk_cliente"),
        col("p.sk_produto"),
        col("f.sk_feedback"),
        col("v.id_venda"),
        col("vp.quantidade_vendida"),
        col("vp.preco_unitario_venda"),
        (col("vp.quantidade_vendida") * col("vp.preco_unitario_venda")).alias("valor_total_item")
    )

    fato_vendas.write \
        .format("jdbc") \
        .option("url", dw_url) \
        .option("driver", driver) \
        .option("dbtable", "fato_vendas") \
        .option("user", usuario) \
        .option("password", senha) \
        .mode("append") \
        .save()
    
# tratar_clientes()
# tratar_enderecos_clientes()
# tratar_fornecedores()
# tratar_enderecos_fornecedores()
# tratar_ingredientes()
# tratar_ingredientes_fornecedores()
# tratar_tipos_produtos()
# tratar_produtos()
# tratar_venda()
# tratar_feedback()
# tratar_venda_produto()

verificarBanco()