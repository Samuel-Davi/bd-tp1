#!/usr/bin/env python3
import argparse
import psycopg
import re
from datetime import datetime
import csv
import os
import tempfile
import urllib.request
import gzip

# Regex pra aceitar cutomer e customer no meta
REVIEW_REGEX = re.compile(
    r"(\d{4}-\d{1,2}-\d{1,2})\s+(?:cutomer|customer):\s*(\S+)\s+rating:\s*(\d+)\s+votes:\s*(\d+)\s+helpful:\s*(\d+)",
    re.IGNORECASE
)

DATA_DIR = '/data'
GZ_URL = 'https://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz'
GZ_PATH = os.path.join(DATA_DIR, 'amazon-meta.txt.gz')
INPUT_PATH = os.path.join(DATA_DIR, 'snap_amazon.txt')

def download_and_extract():
    if not os.path.exists(INPUT_PATH):
        print('Baixando arquivo amazon-meta.txt.gz...')
        urllib.request.urlretrieve(GZ_URL, GZ_PATH)
        print('Descompactando...')
        with gzip.open(GZ_PATH, 'rb') as gz_in, open(INPUT_PATH, 'wb') as txt_out:
            txt_out.write(gz_in.read())
        print('Arquivo baixado e descompactado.')
    else:
        print('Arquivo snap_amazon.txt já existe.')

def parse_file(file_path):
    products = []
    similars = []
    reviews = []
    customers = set()
    current_product = {}

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("Id:"):
                if current_product:
                    products.append(current_product)
                current_product = {"id_produto": int(line.split("Id:")[1].strip())}
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
                for sim_asin in parts[2:]:
                    similars.append((main_asin, sim_asin))
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
                        reviews.append((asin, customer_id, review_date, "00:00:00", rating, votes, helpful))
                        customers.add(customer_id)
                    except Exception as e:
                        print(f"[WARN] Review ignorado: {line} ({e})")

        if current_product:
            products.append(current_product)

    return products, reviews, customers, similars


def batch_insert(cur, query, data, batch_size=10000, entity=""):
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        cur.executemany(query, batch)
        total += len(batch)
        print(f"[INFO] Inseridos {total} {entity}...")
    return total


def insert_into_db(products, reviews, customers, similars, conn, batch_size=10000):
    with conn.cursor() as cur:
        # Produtos
        product_values = [
            (p["id_produto"], p.get("asin"), p.get("title") or "Sem Nome",
             p.get("group") or "Sem Grupo", p.get("salesrank"))
            for p in products
        ]
        count_prod = batch_insert(cur,
            "INSERT INTO Produto (id_produto, asin, nome_produto, grupo, posicao_ranking) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;",
            product_values, batch_size, "produtos")

        # Categorias
        for p in products:
            for cat in set(p.get("categories", [])):
                cur.execute(
                    "INSERT INTO Categoria (nome_categoria) VALUES (%s) ON CONFLICT (nome_categoria) DO NOTHING;",
                    (cat,))
        conn.commit()
        print("[INFO] Categorias inseridas")

        # Produto_Categoria
        cur.execute("SELECT id_categoria, nome_categoria FROM Categoria;")
        cat_map = {nome: cid for cid, nome in cur.fetchall()}
        prod_cat_values = []
        for p in products:
            for cat in set(p.get("categories", [])):
                if cat in cat_map:
                    prod_cat_values.append((p["asin"], cat_map[cat]))
        count_pc = batch_insert(cur,
            "INSERT INTO Produto_Categoria (id_produto, id_categoria) VALUES (%s,%s) ON CONFLICT DO NOTHING;",
            prod_cat_values, batch_size, "produto-categoria")

        # Clientes
        count_cli = batch_insert(cur,
            "INSERT INTO Cliente (id_cliente) VALUES (%s) ON CONFLICT DO NOTHING;",
            [(c,) for c in customers], batch_size, "clientes")

        # Similares 
        cur.execute("SELECT asin FROM Produto;")
        valid_asins = {row[0].strip() for row in cur.fetchall()}

        sim_values = [(asin, sim_asin) for asin, sim_asin in similars
                      if asin in valid_asins and sim_asin in valid_asins]

        count_sim = batch_insert(cur,
            "INSERT INTO \"Similar\" (id_asin, id_asin_similar) VALUES (%s,%s) ON CONFLICT DO NOTHING;",
            sim_values, batch_size, "similares")

        # Avaliações com COPY
        tmpfile = tempfile.NamedTemporaryFile(delete=False, mode="w", newline="", suffix=".csv")
        writer = csv.writer(tmpfile)
        for r in reviews:
            writer.writerow(r)  # (asin, id_cliente, data, hora, rating, votos, helpful)
        tmpfile.close()

        with open(tmpfile.name, "r", encoding="utf-8") as f:
            with cur.copy("COPY Avaliacao (id_produto, id_cliente, data, hora, rating, votos, helpful) FROM STDIN WITH CSV") as copy:
                copy.write(f.read())

        os.remove(tmpfile.name)
        count_reviews = len(reviews)
        print(f"[INFO] {count_reviews} avaliações inseridas")

        conn.commit()

    print(f"[INFO] Produtos inseridos: {count_prod}")
    print(f"[INFO] Categorias inseridas: {len(cat_map)}")
    print(f"[INFO] Clientes inseridos: {count_cli}")
    print(f"[INFO] Avaliações inseridas: {count_reviews}")
    print(f"[INFO] Relações produto-categoria inseridas: {count_pc}")
    print(f"[INFO] Relações similares inseridas: {count_sim}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", required=True)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--batch-size", type=int, default=50000, help="Tamanho do batch de inserts (default=50000)")
    args = parser.parse_args()

    download_and_extract()

    print(f"[INFO] Lendo arquivo {args.input}...")
    products, reviews, customers, similars = parse_file(args.input)
    print(f"[INFO] Produtos: {len(products)}, Reviews: {len(reviews)}, Clientes: {len(customers)}, Similares: {len(similars)}")

    conn_str = f"host={args.db_host} port={args.db_port} dbname={args.db_name} user={args.db_user} password={args.db_pass}"
    with psycopg.connect(conn_str) as conn:
        insert_into_db(products, reviews, customers, similars, conn, args.batch_size)

    print("[INFO] Concluído com sucesso!")


if __name__ == "__main__":
    main()
