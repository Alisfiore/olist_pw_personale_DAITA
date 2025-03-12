# 3 metodi ETL per PRODUCTS + main

def extract():
    print("questo è il metodo EXTRACT dei prodotti")

def transform():
    print("questo è il metodo TRANSFORM dei prodotti")

def load():
    print("questo è il metodo LOAD dei prodotti")

def main():
    print("questo è il metodo MAIN dei prodotti")

# Voglio usare questo file come fosse un modulo:
# i metodi definiti sopra andranno importati per poter essere utilizzati.

if __name__ == "__main__": # Indica ciò che viene eseguito quando eseguo direttamente
    main()