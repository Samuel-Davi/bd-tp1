import json
import os
import urllib.request
import gzip

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
                product['salesrank'] = line.split('salesrank:')[1].strip()
            elif line.startswith('similar:'):
                product['similar'] = line.split('similar:')[1].strip().split()
            elif line.startswith('categories:'):
                product['categories'] = line.split('categories:')[1].strip()
            elif line.startswith('reviews:'):
                product['reviews'] = line.split('reviews:')[1].strip()
        if product:
            products.append(product)
    return products


def main():
    download_and_extract()
    data = parse_amazon_meta(INPUT_PATH)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"JSON salvo em {OUTPUT_PATH}")

if __name__ == '__main__':
    main()