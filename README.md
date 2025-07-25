# 📊 Projeto ETL - Base de Vendas COOKIE

Este projeto realiza um processo de **ETL (Extração, Transformação e Carga)** utilizando **Pandas** e **PySpark**, a partir de uma planilha Excel com dados de vendas de uma loja de cookies. O objetivo é inserir os dados em um **modelo de dados relacional** devidamente normalizado.

---

## ⚙️ Tecnologias Utilizadas

- Python 3.11  
- Pandas  
- PySpark
- Gender Guesser Br
- Banco de Dados Relacional (MYSQL)

---

## 🔄 Como Funciona o ETL

### 1. Extração (Extract)
- A planilha `Base_de_vendas_COOKIE.xlsx` é carregada usando **Pandas** ou **PySpark**.
- Cada aba da planilha representa uma entidade diferente (ex: vendas, produtos, clientes).

### 2. Transformação (Transform)
- Conversão de colunas para os nomes e tipos corretos (ex: datas, números).
- Correção de dados duplicados ou inconsistentes.
- Separação de dados em tabelas normalizadas, como:
  - `clientes`
  - `produtos`
  - `vendas`
  - `itens_venda`

### 3. Carga (Load)
- Inserção dos dados tratados em um banco de dados relacional.
- O script `modelo_relacional.sql` pode ser usado para criar as tabelas antes da carga.
