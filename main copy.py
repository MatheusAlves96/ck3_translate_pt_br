import os
import yaml
import pandas as pd
import argparse
import logging
from googletrans import Translator
from deep_translator import GoogleTranslator
from translate import Translator as TranslatorSec
import traceback
import re
import concurrent.futures
import time
from time import sleep

TRADUTOR_OPTION = 3
TRADUTOR_SR = 'en'
TRADUTOR_DEST = 'pt'

# Configuração básica de logging com detalhes de linha e função
logging.basicConfig(
    filename='translation.log',
    level=logging.INFO,
    format='%(asctime)s\n  Type: %(levelname)s\n  %(message)s'
)

def find_localization_folders(start_path):
    localization_folders = []
    
    for root, dirs, files in os.walk(start_path):
        if 'localization' in dirs:
            localization_folders.append(os.path.join(root, 'localization'))
    
    logging.info(f"Encontrados {len(localization_folders)} diretórios de localização em {start_path}")
    return localization_folders

def get_english_yml_files(folder_path):
    english_yml_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('english.yml'):
                english_yml_files.append(os.path.join(root, file))
    logging.info(f"Encontrados {len(english_yml_files)} arquivos YML de inglês em {folder_path}")
    return english_yml_files

def extract_yml_content(yml_files):
    data = []
    for file in yml_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.splitlines()
                for line in lines:
                    if line.startswith('#'):
                        continue
                    if ":0 " in line:
                        key, value = line.split(":0 ", 1)
                        key = key.strip()
                        value = value.strip().strip('"')
                        data.append([file, key, value, ''])  # Adiciona uma coluna vazia para 'Translate Text'
            logging.info(f"Conteúdo extraído do arquivo {file} com sucesso")
        except Exception as e:
            logging.error(f"Erro ao ler o arquivo {file}:\n{traceback.format_exc()}")
    logging.info(f"Conteúdo extraído de {len(yml_files)} arquivos YML")
    return data

def save_to_excel(data, excel_file):
    if os.path.exists(excel_file):
        existing_df = pd.read_excel(excel_file)
        existing_ids = set(existing_df['ID'])
        new_data = [row for row in data if row[1] not in existing_ids]
        if new_data:
            new_df = pd.DataFrame(new_data, columns=['Filename', 'ID', 'English Text', 'Translate Text'])
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df.to_excel(excel_file, index=False)
            for row in new_data:
                logging.info(f"Novo ID encontrado:\n    ID: {row[1]}\n    Texto em inglês: '{row[2]}'")
        else:
            logging.info("Nenhum novo ID foi adicionado ao arquivo Excel existente.")
    else:
        df = pd.DataFrame(data, columns=['Filename', 'ID', 'English Text', 'Translate Text'])
        df.to_excel(excel_file, index=False)
        logging.info(f"Criado um novo arquivo Excel {excel_file} com {len(data)} IDs.")

def translate_yml_files(excel_file, start_path):
    df = pd.read_excel(excel_file)
    translations = df.dropna(subset=['Translate Text']).set_index('ID')['Translate Text'].to_dict()

    translated_count = 0
    for root, dirs, files in os.walk(start_path):
        for file in files:
            if file.endswith('spanish.yml'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        lines = f.readlines()

                    with open(file_path, 'w', encoding='utf-8') as f:
                        for line in lines:
                            if ":0 " in line:
                                key = line.split(":0 ", 1)[0].strip()
                                if key in translations:
                                    old_value = df.loc[df['ID'] == key, 'English Text'].iloc[0]
                                    new_value = translations[key]
                                    line = f'{key}:0 "{new_value}"\n'
                                    translated_count += 1
                                    logging.info(f"Tradução concluída:\n    ID: '{key}'\n    Texto original: '{old_value}'\n    Novo texto: '{new_value}'")

                            f.write(line)
                except Exception as e:
                    logging.error(f"Erro ao traduzir o arquivo {file_path}:\n{traceback.format_exc()}")

    logging.info(f"Foram aplicadas traduções para {translated_count} IDs usando o arquivo Excel {excel_file}.")

def criar_arquivo_yaml(nome_arquivo):
    if not os.path.exists(nome_arquivo):
        try:
            with open(nome_arquivo, 'w', encoding='utf-8') as file:
                yaml.dump({}, file, default_flow_style=False, allow_unicode=True)
            logging.info(f"Arquivo yml criado com sucesso: {nome_arquivo}")
            return True
        except Exception as e:
            logging.error(f"Erro ao criar o arquivo {nome_arquivo}:\n{traceback.format_exc()}")
            return False
    logging.info(f"Arquivo yml já existe: {nome_arquivo}")
    return True

def ler_chaves_yaml(nome_arquivo):
    chaves = []
    try:
        with open(nome_arquivo, 'r', encoding='utf-8') as file:
            data = yaml.safe_load(file)
            if data is not None:
                chaves = list(data.keys())
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo {nome_arquivo}:\n{traceback.format_exc()}")
    logging.info(f"Arquivo yml {nome_arquivo} lido com sucesso, carregadas {len(chaves)} chaves")
    return chaves

def salvar_chave_valor_yaml(nome_arquivo, chave, valor):
    try:
        with open(nome_arquivo, 'r', encoding='utf-8') as file:
            data = yaml.safe_load(file) or {}

        data[chave] = str(valor)

        with open(nome_arquivo, 'w', encoding='utf-8') as file:
            yaml.safe_dump(data, file, default_flow_style=False, allow_unicode=True)
        
        logging.debug(f"Chave-valor salva com sucesso:\n    Chave: '{chave}'\n    Valor: '{valor}'\n    Arquivo: '{nome_arquivo}'")
        return True
    except Exception as e:
        logging.error(f"Erro ao salvar chave-valor:\n    Chave: '{chave}'\n    Valor: '{valor}'\n    Arquivo: '{nome_arquivo}'\n{traceback.format_exc()}")
        return False

def estado_do_texto(texto):
    try:
        if not texto.strip():
            return "indefinido"

        if texto.isupper():
            return "uppercase"
        elif texto.islower():
            return "lowercase"
        elif texto.istitle():
            return "titlecase"
        else:
            return "misto"
    except Exception as e:
        logging.error(f"Erro ao verificar estado do texto:\n    Texto: '{texto}'\n{traceback.format_exc()}")
        return "indefinido"

def identificar_texto_entre_marcadores(texto):
    try:
        partes = re.split(r'(\$[^\$]*\$|\[[^\]]*\]|\#[^\#]*\#|\\n|\-)', texto)
        partes_formatadas = []
        for parte in partes:
            if parte.startswith("$") or parte.startswith("[") or parte.startswith("#") or parte == "\n":
                partes_formatadas.append(parte)
            else:
                partes_formatadas.extend(re.split(r'(\$[^\$]*\$|\[[^\]]*\]|\#[^\#]*\#|\n)', parte))
        logging.debug(f"Texto dividido em partes para tradução:\n    Texto original: '{texto}'\n    Partes: {len(partes_formatadas)}")
        return partes_formatadas
    except Exception as e:
        logging.error(f"Erro ao identificar texto entre marcadores:\n    Texto: '{texto}'\n{traceback.format_exc()}")
        return []

def append_traducao_yaml(text_english_to_translate, yaml_file, key, not_translate, translator):
    try:
        text_parts = identificar_texto_entre_marcadores(text_english_to_translate)
        text_traduzido = ''
        for part in text_parts:
            if part != '':
                if any(part.startswith(itens) for itens in not_translate):
                    text_traduzido += part
                else:
                    if TRADUTOR_OPTION == 1:
                        text_traduzido_ = translator.translate(part, src='en', dest='pt').text
                    else:
                        text_traduzido_ = translator.translate(part)
                    if text_traduzido_:
                        if part.startswith(' '):
                            text_traduzido_ = ' ' + text_traduzido_
                        if part.endswith(' '):
                            text_traduzido_ = text_traduzido_ + ' '
                        estado_original = estado_do_texto(part)
                        if estado_original == "uppercase":
                            text_traduzido += text_traduzido_.upper()
                        elif estado_original == "lowercase":
                            text_traduzido += text_traduzido_.lower()
                        elif estado_original == "titlecase":
                            text_traduzido += text_traduzido_.capitalize()
                        else:
                            text_traduzido += text_traduzido_
        if text_traduzido:
            salvar_chave_valor_yaml(yaml_file, key, text_traduzido)
            logging.info(f"Texto traduzido com sucesso:\n    ID: '{key}'\n    Texto original: '{text_english_to_translate}'\n    Novo texto: '{text_traduzido}'")
            return True
        else:
            salvar_chave_valor_yaml(yaml_file, key, text_english_to_translate)
            logging.warning(f"Não foi possível traduzir, mantendo o original:\n    ID: '{key}'\n    Texto original: '{text_english_to_translate}'")
            return True
    except Exception as e:
        logging.error(f"Erro ao traduzir texto:\n    ID: '{key}'\n    Texto original: '{text_english_to_translate}'\n{traceback.format_exc()}")
        return False

def create_yaml_translate_file(excel_file, translator, translate_limit):
    yaml_file = str(excel_file).replace('xlsx', 'yml')
    if not criar_arquivo_yaml(yaml_file):
        return

    df = pd.read_excel(excel_file)
    id_df = df['ID']
    text_english_df = df['English Text']
    chaves_traduzidas = ler_chaves_yaml(yaml_file)
    count = 0

    not_translate = ['\\', '#', '$', '[', '-']
    total = len(id_df)

    start_time = time.time()
    next_update_time = start_time + 5

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for key, text_english_to_translate in zip(id_df, text_english_df):
            if not str(key).startswith("#"):
                if key not in chaves_traduzidas:
                    future = executor.submit(append_traducao_yaml, text_english_to_translate, yaml_file, key, not_translate, translator)
                    futures.append(future)
                    count += 1

                    try:
                        if int(translate_limit) > 0 and count >= translate_limit:
                            logging.info(f"Limite de traduções atingido: {translate_limit}")
                            break
                    except Exception as e:
                        logging.error(f"Valor inválido para translate_limit: {translate_limit}\n{traceback.format_exc()}")

        for future in concurrent.futures.as_completed(futures):
            future.result()

    # Relatório final de progresso
    chaves_traduzidas_atualizadas = ler_chaves_yaml(yaml_file)
    logging.info(f"Progresso final: {len(chaves_traduzidas_atualizadas)}/{total} linhas traduzidas.")

def main(action, start_path, excel_file, translate_limit):
    max_instances = 10
    if TRADUTOR_OPTION == 1:
        translator = Translator(service_urls=['translate.google.com.br'])
    elif TRADUTOR_OPTION == 2:
        translator = TranslatorSec(from_lang="english", to_lang="pt-br")
    else:
        translator = GoogleTranslator(source=TRADUTOR_SR, target=TRADUTOR_DEST)
    
    if action == 'create_translate_file':
        localization_folders = find_localization_folders(start_path)

        all_yml_files = []
        for folder in localization_folders:
            all_yml_files.extend(get_english_yml_files(folder))

        extracted_data = extract_yml_content(all_yml_files)

        save_to_excel(extracted_data, excel_file)
        logging.info(f"Arquivo Excel {excel_file} criado com sucesso com base nos arquivos de localizacão em {start_path}.")
        
    elif action == 'create_yaml_translate_file':
        create_yaml_translate_file(excel_file, translator, translate_limit)

    elif action == 'translate':
        translate_yml_files(excel_file, start_path)
        logging.info(f"As traduções foram aplicadas usando o arquivo Excel {excel_file} nos arquivos de localizacão em {start_path}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Processa arquivos de localizacão e gera/traduz arquivos de localizacão.')
    parser.add_argument('action', choices=['create_translate_file', 'translate', 'create_yaml_translate_file'], help='Ação a ser executada')
    parser.add_argument('--start-path', required=True, help='Caminho inicial para procurar arquivos de localizacão')
    parser.add_argument('--excel-file', required=True, help='Nome do arquivo Excel de entrada/saída')
    parser.add_argument('--translate-limit', default=0, help='Limite de execuções')

    args = parser.parse_args()

    main(args.action, args.start_path, args.excel_file, args.translate_limit)
