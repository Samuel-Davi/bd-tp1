# src/tp1_3.3.py
import argparse
import psycopg
import pandas as pd
import os
pd.set_option("display.max_colwidth", None) 
pd.set_option("display.width", 120)         


from tabulate import tabulate

def run_query(conn, query, params=None, output=None, filename=None):
    df = pd.read_sql(query, conn, params=params)
    print("\n=== Resultado ===")
    print(tabulate(df.head(20), headers="keys", tablefmt="psql", showindex=False))
    if output and filename:
        os.makedirs(output, exist_ok=True)
        df.to_csv(os.path.join(output, filename), index=False)
    return df


def q1(conn, asin, output):
    print("\n[Q1] Top 5 comentários úteis positivos e negativos")
    query = """
    (SELECT a.data, a.id_cliente, a.rating, a.votos, a.helpful
     FROM avaliacao a
     JOIN produto p ON a.id_produto = p.id_produto
     WHERE p.asin = %(asin)s
     ORDER BY a.helpful DESC, a.rating DESC
     LIMIT 5)
    UNION
    (SELECT a.data, a.id_cliente, a.rating, a.votos, a.helpful
     FROM avaliacao a
     JOIN produto p ON a.id_produto = p.id_produto
     WHERE p.asin = %(asin)s
     ORDER BY a.helpful DESC, a.rating ASC
     LIMIT 5);
    """
    return run_query(conn, query, {"asin": asin}, output, "q1_reviews.csv")

def q2(conn, asin, output):
    print("\n[Q2] Produtos similares com melhor salesrank")
    query = """
    SELECT ps.asin, ps.nome_produto, ps.posicao_ranking
    FROM "Similar" s
    JOIN produto p ON s.id_produto = p.id_produto
    JOIN produto ps ON s.id_produto_similar = ps.id_produto
    WHERE p.asin = %(asin)s
      AND ps.posicao_ranking < p.posicao_ranking
    ORDER BY ps.posicao_ranking ASC
    LIMIT 20;
    """
    return run_query(conn, query, {"asin": asin}, output, "q2_similares.csv")

def q3(conn, asin, output):
    print("\n[Q3] Evolução diária da média de avaliações")
    query = """
    SELECT a.data AS dia, AVG(a.rating) AS media_rating
    FROM avaliacao a
    JOIN produto p ON a.id_produto = p.id_produto
    WHERE p.asin = %(asin)s
    GROUP BY a.data
    ORDER BY dia;
    """
    return run_query(conn, query, {"asin": asin}, output, "q3_evolucao.csv")

def q4(conn, output):
    print("\n[Q4] Top 10 produtos líderes de venda em cada grupo")
    query = """
    SELECT grupo, asin, nome_produto, posicao_ranking
    FROM (
        SELECT p.grupo, p.asin, p.nome_produto, p.posicao_ranking,
               ROW_NUMBER() OVER (PARTITION BY p.grupo ORDER BY p.posicao_ranking ASC) AS pos
        FROM produto p
        WHERE p.posicao_ranking IS NOT NULL
    ) t
    WHERE pos <= 10
    ORDER BY grupo, pos;
    """
    return run_query(conn, query, None, output, "q4_top10_vendas.csv")

def q5(conn, output):
    print("\n[Q5] Top 10 produtos com maior média de avaliações úteis positivas")
    query = """
    SELECT p.asin, p.nome_produto, AVG(a.helpful) AS media_util
    FROM avaliacao a
    JOIN produto p ON a.id_produto = p.id_produto
    GROUP BY p.asin, p.nome_produto
    ORDER BY media_util DESC
    LIMIT 10;
    """
    return run_query(conn, query, None, output, "q5_top10_util.csv")

def q6(conn, output):
    print("\n[Q6] Top 5 categorias com maior média de avaliações úteis positivas por produto")
    query = """
    SELECT c.nome_categoria, AVG(a.helpful) AS media_util
    FROM avaliacao a
    JOIN produto_categoria pc ON a.id_produto = pc.id_produto
    JOIN categoria c ON pc.id_categoria = c.id_categoria
    GROUP BY c.nome_categoria
    ORDER BY media_util DESC
    LIMIT 5;
    """
    return run_query(conn, query, None, output, "q6_top5_categorias.csv")

def q7(conn, output):
    print("\n[Q7] Top 10 clientes que mais comentaram por grupo")
    query = """
    SELECT p.grupo, a.id_cliente, COUNT(*) AS qtd_comentarios
    FROM avaliacao a
    JOIN produto p ON a.id_produto = p.id_produto
    GROUP BY p.grupo, a.id_cliente
    ORDER BY p.grupo, qtd_comentarios DESC
    LIMIT 10;
    """
    return run_query(conn, query, None, output, "q7_top10_clientes.csv")

def main():
    parser = argparse.ArgumentParser(description="Dashboard TP1")
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", required=True)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--product-asin", help="ASIN do produto para consultas específicas")
    parser.add_argument("--output", default="/app/out")
    args = parser.parse_args()

    conn_str = f"dbname={args.db_name} user={args.db_user} password={args.db_pass} host={args.db_host} port={args.db_port}"
    with psycopg.connect(conn_str) as conn:
        if args.product_asin:
            q1(conn, args.product_asin, args.output)
            q2(conn, args.product_asin, args.output)
            q3(conn, args.product_asin, args.output)
        q4(conn, args.output)
        q5(conn, args.output)
        q6(conn, args.output)
        q7(conn, args.output)

if __name__ == "__main__":
    main()
