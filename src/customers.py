
import psycopg
from dotenv import load_dotenv
import os
import src.common as common
from src.common import check_null

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")
# 3 metodi ETL per CUSTOMERS

def extract():
    print("questo è il metodo EXTRACT dei clienti")
    df = common.readFile()
    return df

def transform(df):
    print("questo è il metodo TRANSFORM dei clienti")
    print (df)
    df = common.drop_duplicates(df)
    df = check_null(df)
    df = common.format_cap(df)
    common.save_processed(df)
    print(df)
    return df

def load(df):
    print("questo è il metodo LOAD dei clienti")
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """
            CREATE TABLE customers (
            pk_customer VARCHAR PRIMARY KEY,
            region VARCHAR,
            city VARCHAR,
            cap VARCHAR 
            );
            """
            try:
                cur.execute(sql)
            except psycopg.errors.DuplicateTable as ex:
                conn.commit()
                print(ex)
                domanda = input("Vuoi cancellare la tabella? SI NO -> ")
                if domanda == "SI":
                    sql_delete = """DROP TABLE customers"""
                    cur.execute(sql_delete)
                    conn.commit()
                    print("Ricreo la tabella customers...")
                    cur.execute(sql)
                #se risponde si cancellare tabella
            sql = """
            INSERT INTO customers
            (pk_customer, region, city, cap) 
            VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;
            """
            common.caricamento_percentuale(df,cur,sql)
            conn.commit()

def main():
    print("questo è il metodo MAIN dei clienti")
    df = extract()
    df = transform(df)
    load(df)

# Voglio usare questo file come fosse un modulo:
# i metodi definiti sopra andranno importati per poter essere utilizzati.

if __name__ == "__main__": # Indica ciò che viene eseguito quando eseguo direttamente
    main()

