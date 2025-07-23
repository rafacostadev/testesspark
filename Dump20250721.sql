CREATE DATABASE  IF NOT EXISTS `loja_cookies`
USE `loja_cookies`;

DROP TABLE IF EXISTS `tb_cliente`;
CREATE TABLE `tb_cliente` (
  `id_cliente` int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `nome` varchar(60) NOT NULL,
  `cpf` char(11) NOT NULL,
  `sexo` ENUM('masculino', 'feminino', 'desconhecido')
) 

DROP TABLE IF EXISTS `tb_endereco_cliente`;
CREATE TABLE `tb_endereco_cliente` (
  `id_endereco` int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `id_cliente` int NOT NULL,
  `rua` varchar(60) NOT NULL,
  `numero` varchar(10) DEFAULT 's/n',
  `complemento` varchar(60),
  `bairro` varchar(60) NOT NULL,
  `cidade` varchar(40) DEFAULT 'Recife' NOT NULL,
  `uf` char(2) DEFAULT 'PE' NOT NULL,
  `cep` char(10) DEFAULT '000000-000'NOT NULL,
  FOREIGN KEY (id_cliente) REFERENCES tb_cliente(id_cliente)
)

DROP TABLE IF EXISTS `tb_fornecedor`;
CREATE TABLE `tb_fornecedor` (
  `id_fornecedor` int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `razao_social` varchar(50) NOT NULL,
  `nome_fantasia` varchar(30) NOT NULL,
  `cnpj` char(18) NOT NULL,
  `cidade` varchar(40) NOT NULL,
  `uf` char(2) NOT NULL
)

DROP TABLE IF EXISTS `tb_endereco_fornecedor`;
CREATE TABLE `tb_endereco_fornecedor` (
  `id_endereco` int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `id_fornecedor` int NOT NULL,
  `rua` varchar(60),
  `numero` varchar(10) DEFAULT 's/n',
  `complemento` varchar(60),
  `bairro` varchar(60),
  `cidade` varchar(40) DEFAULT 'Recife' NOT NULL,
  `uf` char(2) DEFAULT 'PE' NOT NULL,
  `cep` char(10) DEFAULT '000000-000',
  FOREIGN KEY (id_fornecedor) REFERENCES tb_fornecedor(id_fornecedor)
)

DROP TABLE IF EXISTS `tb_ingrediente`;
CREATE TABLE `tb_ingrediente` (
  `id_ingrediente` int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `nome_ingrediente` varchar(60) DEFAULT NULL,
  `estoque` INT
)

DROP TABLE IF EXISTS `tb_fornecedor_ingrediente`;
CREATE TABLE `tb_fornecedor_ingrediente` (
  `id_fornecedor` int NOT NULL,
  `id_ingrediente` int NOT NULL,
  FOREIGN KEY (`id_fornecedor`) REFERENCES `tb_fornecedor` (`id_fornecedor`),
  FOREIGN KEY (`id_ingrediente`) REFERENCES `tb_ingrediente` (`id_ingrediente`)
)

DROP TABLE IF EXISTS `tb_produto`;
CREATE TABLE `tb_produto` (
  `id_produto` int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `id_tipo_produto` int NOT NULL,
  `nome` VARCHAR(80) NOT NULL,
  `preco_unitario` decimal(13,2) NOT NULL,
  `estoque` INT,
  FOREIGN KEY (`id_tipo_produto`) REFERENCES `tb_tipo_produto` (`id_tipo_produto`)
)

DROP TABLE IF EXISTS `tb_tipo_produto`;
CREATE TABLE `tb_tipo_produto` (
  `id_tipo_produto` int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `descricao` varchar(60) NOT NULL
)

DROP TABLE IF EXISTS `tb_produto_ingrediente`;
CREATE TABLE `tb_produto_ingrediente` (
  `id_produto` int not null,
  `id_ingrediente` int not null,
  FOREIGN KEY (`id_produto`) REFERENCES `tb_produto` (`id_produto`),
  FOREIGN KEY (`id_ingrediente`) REFERENCES `tb_ingrediente` (`id_ingrediente`)
)

DROP TABLE IF EXISTS `tb_venda`;
CREATE TABLE `tb_venda` (
  `id_venda` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `id_cliente` INT NOT NULL,
  `status` ENUM('Faturado', 'Cancelado'),
  `data_venda` DATE NOT NULL,
  FOREIGN KEY (`id_cliente`) REFERENCES `tb_cliente` (`id_cliente`)
)

DROP TABLE IF EXISTS `tb_venda_produto`;
CREATE TABLE `tb_venda_produto` (
  `id_venda` INT NOT NULL,
  `id_produto` INT NOT NULL,
  `quantidade` INT NOT NULL,
  FOREIGN KEY (`id_venda`) REFERENCES `tb_venda` (`id_venda`),
  FOREIGN KEY (`id_produto`) REFERENCES `tb_produto` (`id_produto`)
) 

DROP TABLE IF EXISTS `tb_feedback`;
CREATE TABLE `tb_feedback` (
  `id_feedback` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `id_venda` INT NOT NULL,
  `nota` INT NOT NULL,
  `comentario` VARCHAR(80) NOT NULL,
  FOREIGN KEY (`id_venda`) REFERENCES `tb_venda` (`id_venda`)
) 