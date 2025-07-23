DROP DATABASE IF EXISTS `loja_cookies_dimensional`;
CREATE DATABASE  IF NOT EXISTS `loja_cookies_dimensional`;
USE `loja_cookies_dimensional`;

DROP TABLE IF EXISTS `dim_fornecedor_endereco`;
CREATE TABLE `dim_fornecedor_endereco` (
  `sk_id_fornecedor_endereco` int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `id_fornecedor_endereco` int NOT NULL,
  `id_fornecedor` INT NOT NULL,
  `cep_fornecedor` char(10) DEFAULT '000000-000' NOT NULL,
  `complemento_fornecedor` varchar(60),
  `rua_fornecedor` varchar(60) NOT NULL,
  `numero_fornecedor` varchar(10) NOT NULL,
  `cidade_fornecedor` varchar(40) DEFAULT 'Recife' NOT NULL,
  `bairro_fornecedor` varchar(60) NOT NULL,
  `uf_fornecedor` char(2) DEFAULT 'PE' NOT NULL
);

DROP TABLE IF EXISTS `dim_fornecedor`;
CREATE TABLE `dim_fornecedor` (
  `sk_id_fornecedor` int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `id_fornecedor` int NOT NULL,
  `id_fornecedor_endereco` INT NOT NULL,
  `razao_social` varchar(60) NOT NULL,
  `nome_fantasia` varchar(60) NOT NULL,
  `cnpj_fornecedor` char(18) NOT NULL,
);

-- CONTINUAR CONSTRUÇÃO DE TABELAS DE FORNECEDOR

DROP TABLE IF EXISTS `dim_cliente_endereco`;
CREATE TABLE `dim_cliente_endereco` (
  `sk_id_cliente_endereco` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `id_cliente_endereco` INT NOT NULL,
  `id_cliente` INT NOT NULL,
  `bairro_cliente` varchar(60) NOT NULL,
  `numero_cliente` varchar(10) NOT NULL,
  `complemento_cliente` varchar(60) NOT NULL,
  `cep_cliente` char(10) DEFAULT '000000-000' NOT NULL,
  `uf_cliente` char(2) DEFAULT 'PE',
  `cidade_cliente` varchar(40) DEFAULT 'Recife' NOT NULL,
  `rua_cliente` varchar(60) NOT NULL,
);

DROP TABLE IF EXISTS `dim_cliente`;
CREATE TABLE `dim_cliente` (
  `sk_id_cliente` int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `id_cliente` int NOT NULL,
  `id_cliente_endereco` INT NOT NULL,
  `id_venda` INT NOT NULL,
  `nome_cliente` varchar(60),
  `cpf_cliente` varchar(11) NOT NULL,
  `sexo_cliente` ENUM("masculino", "feminino", "desconhecido") NOT NULL,
  `data_nascimento_cliente` DATE NOT NULL,
  `dia_nascimento_cliente` INT NOT NULL,
  `mes_nascimento_cliente` INT NOT NULL,
  `ano_nascimento_cliente` INT NOT NULL,
);

DROP TABLE IF EXISTS `dim_fornecedor`;
CREATE TABLE `dim_fornecedor` (
  `sk_id_fornecedor` int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `id_fornecedor` int NOT NULL,
  `id_fornecedor_endereco` INT NOT NULL,
  `razao_social` char(10) DEFAULT '000000-000'NOT NULL,
  `nome_fantasia` varchar(60),
  `cnpj_fornecedor` varchar(10) DEFAULT 's/n',
  `numero_fornecedor` varchar(60) NOT NULL,
  `cidade_fornecedor` varchar(40) DEFAULT 'Recife' NOT NULL,
  `bairro_fornecedor` varchar(60) DEFAULT 'PE' NOT NULL,
  `uf_fornecedor` char(2) DEFAULT '000000-000'NOT NULL,
  FOREIGN KEY (id_cliente) REFERENCES tb_cliente(id_cliente)
);