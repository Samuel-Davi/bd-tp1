import argparse
import psycopg
import sys
import re
from datetime import datetime
import json

# Regex aceitando "customer" ou "cutomer"
REVIEW_REGEX = re.compile(
    r"(\d{4}-\d{1,2}-\d{1,2})\s+(?:cutomer|customer):\s*(\S+)\s+rating:\s*(\d+)\s+votes:\s*(\d+)\s+helpful:\s*(\d+)",
    re.IGNORECASE
)

def parse_file(file_path):
    products = []
    similars = []
    categories = []
    reviews = []
    customers = set()
    current_product = {}

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("Id:"):
                if current_product:
                    products.append(current_product)
                current_product = {"id_produto": line.split("Id:")[1].strip()}
            elif line.startswith("ASIN:"):
                current_product["asin"] = line.split("ASIN:")[1].strip()
            elif line.startswith("title:"):
                current_product["title"] = line.split("title:")[1].strip()
            elif line.startswith("group:"):
                current_product["group"] = line.split("group:")[1].strip()
            elif line.startswith("salesrank:"):
                try:
                    current_product["salesrank"] = int(line.split("salesrank:")[1].strip())
                except:
                    current_product["salesrank"] = None
            elif line.startswith("similar:"):
                parts = line.split()
                main_asin = current_product.get("asin")
                current_product.setdefault("similars", [])
                for sim_asin in parts[2:]:
                    current_product["similars"].append(sim_asin)
            elif line.startswith("|"):
                cats = [c.strip() for c in line.split("|") if c.strip()]
                current_product.setdefault("categories", []).extend(cats)
            else:
                match = REVIEW_REGEX.search(line)
                if match:
                    try:
                        review_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
                        customer_id = match.group(2)
                        rating = int(match.group(3))
                        votes = int(match.group(4))
                        helpful = int(match.group(5))
                        asin = current_product.get("asin")
                        reviews.append((current_product.get("id_produto"), asin, customer_id, review_date, rating, votes, helpful))
                        customers.add(customer_id)
                    except Exception as e:
                        print(f"[WARN] Review ignorado: {line} ({e})")

        if current_product:
            products.append(current_product)

    return products, reviews, customers


def insert_into_db(products, reviews, customers, conn):
    count_produtos = 0
    count_categorias = 0
    count_clientes = 0
    count_avaliacoes = 0
    count_similares = 0

    with conn.cursor() as cur:
        for product in products:
            asin = product.get('asin')
            id_produto = product.get('id_produto')
            nome_produto = product.get('title') or 'Sem Nome'
            grupo = product.get('group') or 'Sem Grupo'
            posicao_ranking = product.get('salesrank')

            try:
                cur.execute("""
                    INSERT INTO produto (id_produto, asin, nome_produto, grupo, posicao_ranking)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id_produto) DO NOTHING
                """, (id_produto, asin, nome_produto, grupo, posicao_ranking))
                count_produtos += cur.rowcount

                # Categorias
                for cat in set(product.get('categories', [])):
                    cur.execute("""
                        INSERT INTO categoria (nome_categoria)
                        VALUES (%s)
                        ON CONFLICT (nome_categoria) DO NOTHING
                    """, (cat,))
                    cur.execute("SELECT id_categoria FROM categoria WHERE nome_categoria=%s", (cat,))
                    id_categoria = cur.fetchone()[0]
                    cur.execute("""
                        INSERT INTO produto_categoria (id_produto, id_categoria)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                    """, (id_produto, id_categoria))
                    count_categorias += cur.rowcount

                # Similares
                for sim_asin in product.get('similars', []):
                    cur.execute("SELECT id_produto FROM produto WHERE asin = %s", (sim_asin,))
                    row = cur.fetchone()
                    if row:
                        sim_id = row[0]
                        cur.execute("""
                            INSERT INTO "Similar" (id_produto, id_produto_similar)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                        """, (id_produto, sim_id))
                        count_similares += cur.rowcount

                conn.commit()
            except Exception as e:
                print(f"[ERRO] Produto {asin} falhou: {e}")
                conn.rollback()

        # Clientes
        for c in customers:
            cur.execute("""
                INSERT INTO cliente (id_cliente)
                VALUES (%s)
                ON CONFLICT (id_cliente) DO NOTHING
            """, (c,))
            count_clientes += cur.rowcount

        # Avaliações
        for r in reviews:
            try:
                cur.execute("""
                    INSERT INTO avaliacao (id_produto, id_cliente, data, hora, rating, votos, helpful)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (r[0], r[2], r[3], None, r[4], r[5], r[6]))
                count_avaliacoes += cur.rowcount
            except Exception as e:
                print(f"[WARN] Avaliação ignorada para produto {r[1]}: {e}")

        conn.commit()

    print(f"[INFO] Produtos inseridos: {count_produtos}")
    print(f"[INFO] Categorias inseridas: {count_categorias}")
    print(f"[INFO] Clientes inseridos: {count_clientes}")
    print(f"[INFO] Avaliações inseridas: {count_avaliacoes}")
    print(f"[INFO] Relações de produtos similares inseridas: {count_similares}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", required=True)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    try:
        products, reviews, customers = parse_file(args.input)
        print(f"[INFO] Produtos: {len(products)}, Reviews: {len(reviews)}, Clientes: {len(customers)}")
    except Exception as e:
        print(f"[ERRO] Falha ao processar arquivo: {e}")
        sys.exit(1)

    conn_str = f"dbname={args.db_name} user={args.db_user} password={args.db_pass} host={args.db_host} port={args.db_port}"
    try:
        with psycopg.connect(conn_str) as conn:
            insert_into_db(products, reviews, customers, conn)
    except Exception as e:
        print(f"[ERRO] Falha ao inserir no banco: {e}")
        sys.exit(2)

    print("[INFO] Concluído com sucesso!")
    sys.exit(0)


if __name__ == '__main__':
    main()
