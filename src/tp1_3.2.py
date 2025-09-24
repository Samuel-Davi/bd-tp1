import json
import os
import psycopg
import urllib.request
import gzip
from datetime import date, datetime

DATA_DIR = '/data'
GZ_URL = 'https://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz'
GZ_PATH = os.path.join(DATA_DIR, 'amazon-meta.txt.gz')
INPUT_PATH = os.path.join(DATA_DIR, 'amazon-meta.txt')
OUTPUT_PATH = os.path.join(DATA_DIR, 'amazon-meta.json')


def download_and_extract():
    if not os.path.exists(INPUT_PATH):
        print('Baixando arquivo amazon-meta.txt.gz...')
        urllib.request.urlretrieve(GZ_URL, GZ_PATH)
        print('Descompactando...')
        with gzip.open(GZ_PATH, 'rb') as gz_in, open(INPUT_PATH, 'wb') as txt_out:
            txt_out.write(gz_in.read())
        print('Arquivo baixado e descompactado.')
    else:
        print('Arquivo amazon-meta.txt já existe.')


def parse_amazon_meta(file_path):
    products = []
    with open(file_path, 'r', encoding='latin-1') as f:
        product = {}
        for line in f:
            line = line.strip()
            if line.startswith('Id:'):
                if product:
                    products.append(product)
                product = {'Id': line.split('Id:')[1].strip()}
            elif line.startswith('ASIN:'):
                product['ASIN'] = line.split('ASIN:')[1].strip()
            elif line.startswith('title:'):
                product['title'] = line.split('title:')[1].strip()
            elif line.startswith('group:'):
                product['group'] = line.split('group:')[1].strip()
            elif line.startswith('salesrank:'):
                sr = line.split('salesrank:')[1].strip()
                product['salesrank'] = int(sr) if sr.isdigit() else None
            elif line.startswith('similar:'):
                product['similar'] = line.split('similar:')[1].strip().split()
            elif line.startswith('|'):
                # Linha de categoria hierárquica
                cats = [c.strip() for c in line.split('|') if c.strip()]
                if "categories" not in product:
                    product["categories"] = []
                product["categories"].extend(cats)
            elif line and line[0].isdigit() and "customer:" in line:
                # review detalhado
                parts = line.split()
                try:
                    data_str = parts[0]
                    # trata datas no formato YYYY-MM-DD, YYYY-M-D, etc.
                    try:
                        data = date.fromisoformat(data_str)
                    except Exception:
                        data = datetime.strptime(data_str, "%Y-%m-%d").date()
                    cliente = parts[2]
                    rating = int(parts[4])
                    votos = int(parts[6])
                    helpful = int(parts[8])
                    if "reviews_detalhados" not in product:
                        product["reviews_detalhados"] = []
                    product["reviews_detalhados"].append({
                        "data": data,
                        "cliente": cliente,
                        "rating": rating,
                        "votos": votos,
                        "helpful": helpful
                    })
                except Exception:
                    pass
        if product:
            products.append(product)
    return products


def insert_products_to_db(products, conn):
    """Insere produtos, categorias, clientes e avaliações detalhadas."""
    with conn.cursor() as cur:
        for product in products:
            asin = product.get('ASIN')
            id_produto = product.get('Id')
            nome_produto = product.get('title') or 'Sem Nome'
            grupo = product.get('group') or 'Sem Grupo'
            posicao_ranking = product.get('salesrank')

            try:
                # Produto
                cur.execute("""
                    INSERT INTO produto (id_produto, asin, nome_produto, grupo, posicao_ranking)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id_produto) DO NOTHING
                """, (id_produto, asin, nome_produto, grupo, posicao_ranking))

                # Categorias
                for cat in set(product.get('categories', [])):  # evita duplicatas
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

                # Clientes + Avaliações
                for r in product.get("reviews_detalhados", []):
                    cur.execute("""
                        INSERT INTO cliente (id_cliente)
                        VALUES (%s)
                        ON CONFLICT (id_cliente) DO NOTHING
                    """, (r["cliente"],))

                    # insere avaliação
                    cur.execute("""
                        INSERT INTO avaliacao (id_produto, id_cliente, data, hora, rating, votos, helpful)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        id_produto,
                        r["cliente"],
                        r["data"],   # já é objeto date
                        None,        # hora agora é NULL
                        r["rating"],
                        r["votos"],
                        r["helpful"]
                    ))

                conn.commit()
            except Exception as e:
                print(f"Erro inserindo dados do produto {asin}: {e}")
                conn.rollback()


def insert_similares_to_db(products, conn):
    """Insere relações de produtos similares (ASIN -> id_produto)."""
    with conn.cursor() as cur:
        for product in products:
            try:
                id_prod = int(product.get('Id', 0))
                for sim_asin in product.get('similar', []):
                    cur.execute("SELECT id_produto FROM produto WHERE asin = %s", (sim_asin,))
                    row = cur.fetchone()
                    if row:
                        sim_id = row[0]
                        cur.execute("""
                            INSERT INTO "Similar" (id_produto, id_produto_similar)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                        """, (id_prod, sim_id))
            except Exception as e:
                print(f"Erro inserindo similares do produto {product.get('ASIN')}: {e}")
        conn.commit()
    print("Relações de produtos similares inseridas.")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", required=True)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    with open(args.input, 'r', encoding='utf-8') as f:
        data = json.load(f)

    conn_str = f"dbname={args.db_name} user={args.db_user} password={args.db_pass} host={args.db_host} port={args.db_port}"
    with psycopg.connect(conn_str) as conn:
        insert_products_to_db(data, conn)
        insert_similares_to_db(data, conn)


if __name__ == '__main__':
    main()
   
