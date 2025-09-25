DROP TABLE IF EXISTS "Similar" CASCADE;
DROP TABLE IF EXISTS Avaliacao CASCADE;
DROP TABLE IF EXISTS Produto_Categoria CASCADE;
DROP TABLE IF EXISTS Categoria CASCADE;
DROP TABLE IF EXISTS Cliente CASCADE;
DROP TABLE IF EXISTS Produto CASCADE;

CREATE TABLE Produto (
    id_produto INT PRIMARY KEY,
    asin VARCHAR(15) UNIQUE NOT NULL,
    nome_produto TEXT NOT NULL,
    grupo VARCHAR(50) NOT NULL,
    posicao_ranking INTEGER
);

CREATE TABLE Categoria (
    id_categoria SERIAL PRIMARY KEY,
    nome_categoria TEXT UNIQUE NOT NULL
);

CREATE TABLE Produto_Categoria (
    id_produto INT NOT NULL,
    id_categoria INT NOT NULL,
    PRIMARY KEY (id_produto, id_categoria),
    FOREIGN KEY (id_produto) REFERENCES Produto(id_produto) ON DELETE CASCADE,
    FOREIGN KEY (id_categoria) REFERENCES Categoria(id_categoria) ON DELETE CASCADE
);

CREATE TABLE Cliente (
    id_cliente VARCHAR(70) PRIMARY KEY
);

CREATE TABLE Avaliacao (
    id_review SERIAL PRIMARY KEY,
    id_produto INT NOT NULL,
    id_cliente VARCHAR(70) NOT NULL,
    data DATE NOT NULL,
    hora TIME NULL,
    rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    votos INTEGER NOT NULL,
    helpful INTEGER NOT NULL,
    FOREIGN KEY (id_produto) REFERENCES Produto(id_produto) ON DELETE CASCADE,
    FOREIGN KEY (id_cliente) REFERENCES Cliente(id_cliente) ON DELETE CASCADE
);

CREATE TABLE "Similar" (
    id_produto INT NOT NULL,
    id_produto_similar INT NOT NULL,
    PRIMARY KEY (id_produto, id_produto_similar),
    FOREIGN KEY (id_produto) REFERENCES Produto(id_produto) ON DELETE CASCADE,
    FOREIGN KEY (id_produto_similar) REFERENCES Produto(id_produto) ON DELETE CASCADE
);
