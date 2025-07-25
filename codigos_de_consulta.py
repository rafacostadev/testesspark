# Bloco 1: Configuração e Carga de Dados


# --- Instalações e Importações ---
!pip install pyspark -q
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
import re
import pandas as pd
from IPython.display import display


# --- Inicia a Sessão Spark ---
spark = SparkSession.builder.appName("AnaliseCompletaLojaCookies").getOrCreate()


# --- Função de Extração (MELHORADA PARA LER MÚLTIPLOS INSERTS) ---
def extrair_para_spark(nome_tabela, schema, texto_sql):
    padrao_insert = re.compile(f"INSERT INTO `{nome_tabela}` VALUES (.*?);", re.DOTALL)
    matches = padrao_insert.finditer(texto_sql)
    dados_totais = []

    for match in matches:
        if not match: continue

        conteudo_values = match.group(1)
        linhas_texto = re.findall(r'\((.*?)\)', conteudo_values)

        for linha in linhas_texto:
            campos = re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", linha)
            linha_processada = []
            if len(campos) != len(schema.fields): continue

            for i, campo in enumerate(campos):
                campo_limpo = campo.strip()
                valor = None
                if campo_limpo.upper() != 'NULL':
                    valor_str = campo_limpo[1:-1] if campo_limpo.startswith("'") and campo_limpo.endswith("'") else campo_limpo
                    tipo = schema.fields[i].dataType
                    try:
                        if isinstance(tipo, DateType): valor = pd.to_datetime(valor_str).date()
                        elif isinstance(tipo, (IntegerType, LongType)): valor = int(valor_str)
                        elif isinstance(tipo, (FloatType, DoubleType)): valor = float(valor_str)
                        else: valor = valor_str
                    except (ValueError, TypeError): valor = None
                linha_processada.append(valor)
            dados_totais.append(tuple(linha_processada))

    if not dados_totais:
        print(f"AVISO: Nenhum dado encontrado para a tabela '{nome_tabela}'.")
        return None

    return spark.createDataFrame(dados_totais, schema)


# --- Leitura do Arquivo e Definição dos Schemas ---
nome_do_arquivo = 'Dump.sql'
try:
    with open(nome_do_arquivo, 'r', encoding='utf-8') as file:
        sql_content = file.read()

    schema_vendas = StructType([
        StructField("id_venda", IntegerType(), True), StructField("id_cliente", IntegerType(), True),
        StructField("data_venda", DateType(), True), StructField("status", StringType(), True),
        StructField("valor_total", DoubleType(), True)
    ])
    schema_itens = StructType([
        StructField("id_venda_item", IntegerType(), True), StructField("id_venda", IntegerType(), True),
        StructField("id_produto", IntegerType(), True), StructField("quantidade", IntegerType(), True),
        StructField("preco_unitario_venda", DoubleType(), True)
    ])
    schema_produtos = StructType([
        StructField("id_produto", IntegerType(), True), StructField("id_tipo_produto", IntegerType(), True),
        StructField("descricao", StringType(), True), StructField("preco_unitario", DoubleType(), True)
    ])


    # --- Criação e Preparação dos DataFrames ---
    print("Iniciando extração de dados completa...")
    vendas_df = extrair_para_spark('tb_vendas', schema_vendas, sql_content)
    itens_df = extrair_para_spark('tb_venda_itens', schema_itens, sql_content)
    produtos_df = extrair_para_spark('tb_produto', schema_produtos, sql_content)


    if vendas_df and itens_df and produtos_df:
        vendas_validas_df = vendas_df.filter(F.col("status") == "FATURADO").dropna(subset=["valor_total"])
        itens_validos_df = itens_df.join(vendas_validas_df.select("id_venda"), "id_venda", "inner")
        print("\nConfiguração concluída! DataFrames prontos para análise.")
        print(f"Total de registros de vendas carregados: {vendas_df.count()}")
        print(f"Total de registros de itens de venda carregados: {itens_df.count()}")
        print(f"Total de produtos no catálogo: {produtos_df.count()}")
    else:
        print("ERRO: Um ou mais DataFrames não puderam ser criados. Verifique os nomes das tabelas no arquivo SQL.")


except FileNotFoundError:
    print(f"ERRO: Arquivo '{nome_do_arquivo}' não encontrado.")
except Exception as e:
    print(f"Ocorreu um erro inesperado: {e}")

# Bloco 2: Relatório de Distribuição de Vendas


print("\n" + "="*50)
print("RELATÓRIO 1: Distribuição de Vendas por Período")
print("="*50)


# Análise por Mês
print("\n** Vendas por Mês **")
vendas_por_mes_pd = vendas_validas_df.groupBy(F.month("data_venda").alias("mes")).agg(F.sum("valor_total").alias("Faturamento_RS_raw"), F.count("id_venda").alias("N_Vendas")).orderBy("mes").toPandas()
vendas_por_mes_pd['Faturamento_RS'] = vendas_por_mes_pd['Faturamento_RS_raw'].map('R$ {:,.2f}'.format)
display(vendas_por_mes_pd[['mes', 'Faturamento_RS', 'N_Vendas']])


# Análise por Trimestre
print("\n** Vendas por Trimestre **")
vendas_por_trimestre_pd = vendas_validas_df.groupBy(F.quarter("data_venda").alias("trimestre")).agg(F.sum("valor_total").alias("Faturamento_RS_raw"), F.count("id_venda").alias("N_Vendas")).orderBy("trimestre").toPandas()
vendas_por_trimestre_pd['Faturamento_RS'] = vendas_por_trimestre_pd['Faturamento_RS_raw'].map('R$ {:,.2f}'.format)
display(vendas_por_trimestre_pd[['trimestre', 'Faturamento_RS', 'N_Vendas']])


# Análise por Dia da Semana
print("\n** Vendas por Dia da Semana **")
vendas_por_dia_semana_pd = vendas_validas_df.withColumn("dia_da_semana_en", F.date_format(F.col("data_venda"), "EEEE")).groupBy("dia_da_semana_en").agg(F.sum("valor_total").alias("Faturamento_RS_raw"), F.count("id_venda").alias("N_Vendas")).orderBy(F.col("Faturamento_RS_raw").desc()).toPandas()
mapa_dias = {"Monday": "Segunda-feira", "Tuesday": "Terca-feira", "Wednesday": "Quarta-feira", "Thursday": "Quinta-feira", "Friday": "Sexta-feira", "Saturday": "Sabado", "Sunday": "Domingo"}
vendas_por_dia_semana_pd['Dia da Semana'] = vendas_por_dia_semana_pd['dia_da_semana_en'].map(mapa_dias)
vendas_por_dia_semana_pd['Faturamento_RS'] = vendas_por_dia_semana_pd['Faturamento_RS_raw'].map('R$ {:,.2f}'.format)
display(vendas_por_dia_semana_pd[['Dia da Semana', 'Faturamento_RS', 'N_Vendas']])

# Bloco 3: Análise de Dias e Fim de Semana


print("\n" + "="*50)
print("RELATÓRIO 2: Análise de Dias e Fim de Semana")
print("="*50)


vendas_por_dia_semana_pd = vendas_validas_df.withColumn("dia_da_semana_en", F.date_format(F.col("data_venda"), "EEEE")).groupBy("dia_da_semana_en").agg(F.sum("valor_total").alias("Faturamento_RS_raw"), F.count("id_venda").alias("N_Vendas")).orderBy(F.col("Faturamento_RS_raw").desc()).toPandas()


print("\n** Tabela de Desempenho por Dia da Semana **")
mapa_dias = {"Monday": "Segunda-feira", "Tuesday": "Terca-feira", "Wednesday": "Quarta-feira", "Thursday": "Quinta-feira", "Friday": "Sexta-feira", "Saturday": "Sabado", "Sunday": "Domingo"}
vendas_por_dia_semana_pd_formatado = vendas_por_dia_semana_pd.copy()
vendas_por_dia_semana_pd_formatado['Dia da Semana'] = vendas_por_dia_semana_pd_formatado['dia_da_semana_en'].map(mapa_dias)
vendas_por_dia_semana_pd_formatado['Faturamento_RS'] = vendas_por_dia_semana_pd_formatado['Faturamento_RS_raw'].map('R$ {:,.2f}'.format)
display(vendas_por_dia_semana_pd_formatado[['Dia da Semana', 'Faturamento_RS', 'N_Vendas']])


print("\n** Resumo da Análise **")
melhor_dia_faturamento_info = vendas_por_dia_semana_pd.iloc[0]
pior_dia_faturamento_info = vendas_por_dia_semana_pd.iloc[-1]
faturamento_diario_df = vendas_validas_df.groupBy("data_venda").agg(F.sum("valor_total").alias("faturamento_total")).toPandas()
faturamento_diario_df['dia_da_semana_en'] = pd.to_datetime(faturamento_diario_df['data_venda']).dt.day_name()
media_dias_uteis = faturamento_diario_df[~faturamento_diario_df['dia_da_semana_en'].isin(['Saturday', 'Sunday'])]['faturamento_total'].mean()
media_fds = faturamento_diario_df[faturamento_diario_df['dia_da_semana_en'].isin(['Saturday', 'Sunday'])]['faturamento_total'].mean()


print(f"\n- O dia com MAIOR volume de vendas (faturamento) foi: {mapa_dias[melhor_dia_faturamento_info['dia_da_semana_en']]}, com um faturamento total de R$ {melhor_dia_faturamento_info['Faturamento_RS_raw']:,.2f}.")
print(f"- O PIOR dia de vendas (faturamento) foi: {mapa_dias[pior_dia_faturamento_info['dia_da_semana_en']]}, com um faturamento total de R$ {pior_dia_faturamento_info['Faturamento_RS_raw']:,.2f}.")


if media_fds >= media_dias_uteis * 0.8:
    avaliacao = "Viável. O faturamento médio por dia no fim de semana (R$ {:,.2f}) é competitivo em relação à média por dia útil (R$ {:,.2f}).".format(media_fds, media_dias_uteis)
else:
    avaliacao = "Requer atenção. O faturamento médio por dia no fim de semana (R$ {:,.2f}) é consideravelmente inferior à média por dia útil (R$ {:,.2f}).".format(media_fds, media_dias_uteis)
print(f"- Avaliação de viabilidade do fim de semana: {avaliacao}")

# Bloco 4: Análise do Ticket Médio


print("\n" + "="*50)
print("RELATÓRIO 3: Análise do Ticket Médio")
print("="*50)


ticket_medio_df = vendas_validas_df.agg(
    F.sum("valor_total").alias("FaturamentoTotal"),
    F.count("id_venda").alias("NumeroDeVendas")
)


resultado_ticket_df = ticket_medio_df.withColumn(
    "TicketMedio_RS_raw",
    F.col("FaturamentoTotal") / F.col("NumeroDeVendas")
)


resumo_pd = resultado_ticket_df.toPandas()
resumo_pd['Faturamento Total (R$)'] = resumo_pd['FaturamentoTotal'].map('R$ {:,.2f}'.format)
resumo_pd['Ticket Médio (R$)'] = resumo_pd['TicketMedio_RS_raw'].map('R$ {:,.2f}'.format)
resumo_pd.rename(columns={"NumeroDeVendas": "Nº de Vendas"}, inplace=True)


print("\n** Resumo do Desempenho Geral de Vendas Faturadas **")
display(resumo_pd[['Faturamento Total (R$)', 'Nº de Vendas', 'Ticket Médio (R$)']])


ticket_medio_valor = resumo_pd['TicketMedio_RS_raw'].iloc[0]
print(f"\n- O ticket médio geral das vendas faturadas é de R$ {ticket_medio_valor:,.2f}.")

# Bloco 5: Análise de Sazonalidade de Produtos (VERSÃO CORRIGIDA)


print("\n" + "="*50)
print("RELATÓRIO 4: Análise de Sazonalidade de Produtos")
print("="*50)


# DataFrame base para as análises de sazonalidade (vendas faturadas)
df_completo = vendas_validas_df.join(itens_validos_df, "id_venda", "inner") \
                               .join(produtos_df, "id_produto", "inner")


# Primeira visualização (simples, em formato de lista)
print("\n** Tabela de Vendas por Produto e Mês (Formato Lista) **")
sazonalidade_df = df_completo.groupBy(F.col("descricao").alias("Produto"), F.month("data_venda").alias("Mes")) \
                             .agg(F.sum("quantidade").alias("Unidades_Vendidas")) \
                             .orderBy("Produto", "Mes")
sazonalidade_df.show(50, truncate=False)




# --- CORREÇÃO DA MATRIZ DE SAZONALIDADE ---
print("\n" + "="*50)
print("VISUALIZAÇÃO MELHORADA: Matriz de Sazonalidade")
print("="*50)


# Mapeamento de número do mês para nome com prefixo numérico para ordenação
# Ex: 1 -> "01-Jan", 2 -> "02-Fev". Isso garante a ordem cronológica com uma simples ordenação alfabética.
mapa_meses_num_str = {
    1: "01-Jan", 2: "02-Fev", 3: "03-Mar", 4: "04-Abr", 5: "05-Mai", 6: "06-Jun",
    7: "07-Jul", 8: "08-Ago", 9: "09-Set", 10: "10-Out", 11: "11-Nov", 12: "12-Dez"
}
map_expr = F.create_map([F.lit(x) for i in mapa_meses_num_str.items() for x in i])


# Cria a tabela pivot. O .pivot() só criará colunas para os meses que realmente existem nos dados.
matriz_sazonalidade_df = df_completo.withColumn("Mes_Nome", map_expr[F.month("data_venda")]) \
                                    .groupBy("descricao") \
                                    .pivot("Mes_Nome") \
                                    .agg(F.sum("quantidade")) \
                                    .na.fill(0)


# Lógica de reordenação robusta:
# Pega as colunas que foram efetivamente criadas pelo pivot e as ordena.
colunas_pivot = [col for col in matriz_sazonalidade_df.columns if col != 'descricao']
colunas_pivot_ordenadas = sorted(colunas_pivot) # A ordenação alfabética funciona por causa do prefixo "01-", "02-", etc.
colunas_finais = ["descricao"] + colunas_pivot_ordenadas


# Seleciona as colunas na ordem correta para a exibição final
matriz_sazonalidade_df_ordenada = matriz_sazonalidade_df.select(colunas_finais)


print("\n** Unidades Vendidas por Produto ao Longo dos Meses **")
display(matriz_sazonalidade_df_ordenada)

# Bloco 6: Ranking de Produtos (incluindo produtos não vendidos)


print("\n" + "="*50)
print("RELATÓRIO 5: Ranking de Produtos (Distribuição de Vendas)")
print("="*50)


df_produtos_com_vendas = produtos_df.join(
    itens_validos_df,
    "id_produto",
    "left"
)


df_produtos_com_vendas_preenchido = df_produtos_com_vendas.na.fill(0, subset=['quantidade', 'preco_unitario_venda'])


ranking_produtos_df = df_produtos_com_vendas_preenchido.groupBy("id_produto", "descricao") \
    .agg(
        F.sum("quantidade").alias("Total_Unidades_Vendidas"),
        F.sum(F.col("quantidade") * F.col("preco_unitario_venda")).alias("Faturamento_Bruto_raw")
    )


window_spec = Window.orderBy(F.lit('A'))
total_faturamento_geral = F.sum("Faturamento_Bruto_raw").over(window_spec)


ranking_produtos_enriquecido_df = ranking_produtos_df.withColumn(
    "Percentual_Faturamento",
    F.when(total_faturamento_geral == 0, 0)
     .otherwise((F.col("Faturamento_Bruto_raw") / total_faturamento_geral) * 100)
)


ranking_final_pd = ranking_produtos_enriquecido_df.orderBy(F.col("Total_Unidades_Vendidas").desc()).toPandas()


ranking_final_pd['Faturamento Bruto (R$)'] = ranking_final_pd['Faturamento_Bruto_raw'].map('R$ {:,.2f}'.format)
ranking_final_pd['Contribuição (%)'] = ranking_final_pd['Percentual_Faturamento'].map('{:,.2f}%'.format)


ranking_final_pd.rename(columns={"descricao": "Produto", "Total_Unidades_Vendidas": "Unidades Vendidas"}, inplace=True)
display(ranking_final_pd[['Produto', 'Unidades Vendidas', 'Faturamento Bruto (R$)', 'Contribuição (%)']])


if not ranking_final_pd.empty:
    produto_mais_vendido_unidades = ranking_final_pd.iloc[0]
    produto_menos_vendido_unidades = ranking_final_pd.iloc[-1]
    produto_maior_faturamento = ranking_final_pd.sort_values(by='Faturamento_Bruto_raw', ascending=False).iloc[0]


    print("\n** Insights Rápidos (Baseado em Vendas Faturadas) **")
    print(f"- PRODUTO MAIS VENDIDO (unidades): '{produto_mais_vendido_unidades['Produto']}', com {produto_mais_vendido_unidades['Unidades Vendidas']} unidades.")
    print(f"- PRODUTO MENOS VENDIDO (unidades): '{produto_menos_vendido_unidades['Produto']}', com {produto_menos_vendido_unidades['Unidades Vendidas']} unidades.")
    print(f"- PRODUTO DE MAIOR FATURAMENTO: '{produto_maior_faturamento['Produto']}', com um total de {produto_maior_faturamento['Faturamento Bruto (R$)']}.")
else:
    print("Nenhum dado de produto para gerar insights.")

# Bloco 7: Análise Completa de Vendas por Status


print("\n" + "="*50)
print("RELATÓRIO 6: Análise Completa de Vendas por Status do Pedido")
print("="*50)
print("Este relatório mostra TODAS as unidades vendidas, divididas pelo status final da venda.")


df_completo_sem_filtro = produtos_df.join(itens_df, "id_produto", "left") \
                                    .join(vendas_df, "id_venda", "left")


vendas_por_status_df = df_completo_sem_filtro.groupBy("descricao") \
    .pivot("status") \
    .agg(F.sum("quantidade")) \
    .na.fill(0)


colunas_status = [col for col in vendas_por_status_df.columns if col != 'descricao']
vendas_por_status_df = vendas_por_status_df.withColumn("Total_Unidades", sum(vendas_por_status_df[col] for col in colunas_status))


ranking_completo_pd = vendas_por_status_df.orderBy(F.col("Total_Unidades").desc()).toPandas()


ranking_completo_pd.rename(columns={"descricao": "Produto"}, inplace=True)
display(ranking_completo_pd)


print("\n" + "="*30)
print("Insights da Análise Completa")
print("="*30)


if not ranking_completo_pd.empty:
    produto_mais_vendido = ranking_completo_pd.iloc[0]
    produto_menos_vendido = ranking_completo_pd.iloc[-1]


    print(f"\n- PRODUTO MAIS VENDIDO (total de unidades): '{produto_mais_vendido['Produto']}', com {produto_mais_vendido['Total_Unidades']} unidades no total.")
    print(f"- PRODUTO MENOS VENDIDO (total de unidades): '{produto_menos_vendido['Produto']}', com {produto_menos_vendido['Total_Unidades']} unidades no total.")
    print("\n- A tabela acima revela o ciclo de vida completo dos pedidos de cada produto.")
    print("- Produtos com alto volume em 'CANCELADO' ou 'EM ABERTO' podem indicar problemas na operação ou no processamento de pagamentos.")
else:
    print("Não há dados para gerar insights.")

# Bloco 8: Vendas por Status (com Insights Focados em Faturamento)


print("\n" + "="*50)
print("RELATÓRIO 7: Vendas por Status (Produtos com Mais de 1 Unidade Vendida)")
print("="*50)
print("Este relatório mostra o status de vendas apenas para produtos com mais de 1 unidade vendida no total.")


df_completo_sem_filtro = produtos_df.join(itens_df, "id_produto", "left") \
                                    .join(vendas_df, "id_venda", "left")


vendas_por_status_df = df_completo_sem_filtro.groupBy("descricao") \
    .pivot("status") \
    .agg(F.sum("quantidade")) \
    .na.fill(0)


colunas_status = [col for col in vendas_por_status_df.columns if col != 'descricao']
vendas_por_status_df = vendas_por_status_df.withColumn("Total_Unidades", sum(vendas_por_status_df[col] for col in colunas_status))


df_filtrado = vendas_por_status_df.filter(F.col("Total_Unidades") > 1)


ranking_filtrado_pd = df_filtrado.orderBy(F.col("Total_Unidades").desc()).toPandas()


ranking_filtrado_pd.rename(columns={"descricao": "Produto"}, inplace=True)
display(ranking_filtrado_pd)


print("\n" + "="*30)
print("Insights dos Produtos (Baseado em Vendas FATURADAS)")
print("="*30)


if not ranking_filtrado_pd.empty and 'FATURADO' in ranking_filtrado_pd.columns:
    ranking_faturado_pd = ranking_filtrado_pd[ranking_filtrado_pd['FATURADO'] > 0].sort_values(by='FATURADO', ascending=False).copy()


    if not ranking_faturado_pd.empty:
        ranking_faturado_pd['FATURADO'] = pd.to_numeric(ranking_faturado_pd['FATURADO'])

        produto_mais_vendido_faturado = ranking_faturado_pd.iloc[0]
        produto_menos_vendido_faturado = ranking_faturado_pd.iloc[-1]


        print(f"\n- PRODUTO MAIS VENDIDO (faturado): '{produto_mais_vendido_faturado['Produto']}', com {int(produto_mais_vendido_faturado['FATURADO'])} unidades faturadas.")
        print(f"- PRODUTO MENOS VENDIDO (dentre os faturados): '{produto_menos_vendido_faturado['Produto']}', com {int(produto_menos_vendido_faturado['FATURADO'])} unidades faturadas.")
    else:
        print("\nNenhum produto com vendas faturadas foi encontrado no conjunto de dados filtrado.")
else:
    print("Não há dados para gerar insights ou a coluna 'FATURADO' não existe.")

# Bloco 7: Análise Completa de Vendas por Status (com Mais e Menos Vendido)


print("\n" + "="*50)
print("RELATÓRIO 6: Análise Completa de Vendas por Status do Pedido")
print("="*50)
print("Este relatório mostra TODAS as unidades vendidas, divididas pelo status final da venda.")


# 1. Usamos os DataFrames originais (não filtrados por 'FATURADO')
df_completo_sem_filtro = produtos_df.join(itens_df, "id_produto", "left") \
                                    .join(vendas_df, "id_venda", "left")


# 2. Agrupamos por produto e usamos PIVOT na coluna 'status'.
vendas_por_status_df = df_completo_sem_filtro.groupBy("descricao") \
    .pivot("status") \
    .agg(F.sum("quantidade")) \
    .na.fill(0)


# 3. Adicionamos uma coluna de Total de Unidades para facilitar a ordenação
colunas_status = [col for col in vendas_por_status_df.columns if col != 'descricao']
vendas_por_status_df = vendas_por_status_df.withColumn("Total_Unidades", sum(vendas_por_status_df[col] for col in colunas_status))


# 4. Ordenamos pelo total de unidades e preparamos para exibição
ranking_completo_pd = vendas_por_status_df.orderBy(F.col("Total_Unidades").desc()).toPandas()


# 5. Renomeia e exibe o resultado final
ranking_completo_pd.rename(columns={"descricao": "Produto"}, inplace=True)
display(ranking_completo_pd)


# 6. Insights da Análise Completa (incluindo não faturados)
print("\n" + "="*30)
print("Insights da Análise Completa")
print("="*30)


if not ranking_completo_pd.empty:
    produto_mais_vendido = ranking_completo_pd.iloc[0]
    produto_menos_vendido = ranking_completo_pd.iloc[-1]


    print(f"\n- PRODUTO MAIS VENDIDO (total de unidades): '{produto_mais_vendido['Produto']}', com {produto_mais_vendido['Total_Unidades']} unidades no total.")
    print(f"- PRODUTO MENOS VENDIDO (total de unidades): '{produto_menos_vendido['Produto']}', com {produto_menos_vendido['Total_Unidades']} unidades no total.")
    print("\n- A tabela acima revela o ciclo de vida completo dos pedidos de cada produto.")
    print("- Produtos com alto volume em 'CANCELADO' ou 'EM ABERTO' podem indicar problemas na operação ou no processamento de pagamentos.")
else:
    print("Não há dados para gerar insights.")
