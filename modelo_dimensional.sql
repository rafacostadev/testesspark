DROP DATABASE IF EXISTS modelo_dimensional;
CREATE DATABASE modelo_dimensional;
USE modelo_dimensional;

CREATE TABLE dim_tempo (
    sk_tempo INT PRIMARY KEY AUTO_INCREMENT,
    data_completa DATE,
    dia INT,
    mes INT,
    ano INT,
    nome_mes VARCHAR(20),
    dia_da_semana VARCHAR(20),
    trimestre INT
);

CREATE TABLE dim_cliente (
    sk_cliente INT PRIMARY KEY AUTO_INCREMENT,
    id_cliente INT NOT NULL,
    nome VARCHAR(255) NOT NULL,
    cpf VARCHAR(14) NOT NULL,
    sexo VARCHAR(20),
    data_nascimento DATE,
    cidade VARCHAR(100),
    uf CHAR(2)
);

CREATE TABLE dim_produto (
    sk_produto INT PRIMARY KEY AUTO_INCREMENT,
    id_produto INT NOT NULL,
    nome_produto VARCHAR(255) NOT NULL,
    descricao_tipo_produto VARCHAR(255),
    preco_atual DECIMAL(10, 2)
);

CREATE TABLE dim_feedback (
    sk_feedback INT PRIMARY KEY AUTO_INCREMENT,
    id_feedback INT NOT NULL,
    nota INT,
    comentario TEXT
);

CREATE TABLE dim_ingrediente (
    sk_ingrediente INT PRIMARY KEY AUTO_INCREMENT,
    id_ingrediente INT NOT NULL,
    nome_ingrediente VARCHAR(255) NOT NULL
);

CREATE TABLE dim_fornecedor (
    sk_fornecedor INT PRIMARY KEY AUTO_INCREMENT,
    id_fornecedor INT NOT NULL,
    razao_social VARCHAR(255),
    nome_fantasia VARCHAR(255),
    cnpj VARCHAR(18),
    cidade VARCHAR(100),
    uf CHAR(2)
);

CREATE TABLE bridge_produto_ingrediente (
    sk_produto INT NOT NULL,
    sk_ingrediente INT NOT NULL,
    PRIMARY KEY (sk_produto, sk_ingrediente),
    FOREIGN KEY (sk_produto) REFERENCES dim_produto(sk_produto),
    FOREIGN KEY (sk_ingrediente) REFERENCES dim_ingrediente(sk_ingrediente)
);

CREATE TABLE bridge_ingrediente_fornecedor (
    sk_ingrediente INT NOT NULL,
    sk_fornecedor INT NOT NULL,
    PRIMARY KEY (sk_ingrediente, sk_fornecedor),
    FOREIGN KEY (sk_ingrediente) REFERENCES dim_ingrediente(sk_ingrediente),
    FOREIGN KEY (sk_fornecedor) REFERENCES dim_fornecedor(sk_fornecedor)
);

CREATE TABLE fato_vendas (
    sk_tempo INT NOT NULL,
    sk_cliente INT NOT NULL,
    sk_produto INT NOT NULL,
    sk_feedback INT NOT NULL,
    id_venda INT NOT NULL,
    quantidade_vendida INT NOT NULL,
    preco_unitario_venda DECIMAL(10, 2) NOT NULL,
    valor_total_item DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (sk_tempo, sk_cliente, sk_produto, sk_feedback, id_venda),
    FOREIGN KEY (sk_tempo) REFERENCES dim_tempo(sk_tempo),
    FOREIGN KEY (sk_cliente) REFERENCES dim_cliente(sk_cliente),
    FOREIGN KEY (sk_produto) REFERENCES dim_produto(sk_produto),
    FOREIGN KEY (sk_feedback) REFERENCES dim_feedback(sk_feedback)
);