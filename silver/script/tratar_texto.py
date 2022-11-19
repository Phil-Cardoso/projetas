import unicodedata
# func para Normalizar o cabeçalho


def norm_cab(lista):

    colunas = {}

    for coluna in lista:
        coluna_antiga = coluna
        coluna_nova = ''.join(caracter for caracter in unicodedata.normalize('NFKD', coluna_antiga)
                              if not unicodedata.combining(caracter))

        coluna_nova = coluna_nova.strip().lower().replace(
            ' ', '_').replace('.', '_').replace('-', '_')

        colunas[f'{coluna_antiga}'] = coluna_nova

    return colunas
