import os
import yaml
import pandas as pd
import argparse
import logging
from googletrans import Translator
from deep_translator import GoogleTranslator
from translate import Translator as TranslatorSec
from model import CrusaderKingTranslate, AllDatabaseManager as DatabaseManager
from deep_translator.exceptions import TooManyRequests
import traceback
import re
import threading
from time import time, sleep
import signal
from colorama import Fore, Style, init

logging.basicConfig(
    filename='translation.log',
    level=logging.INFO,
    format='%(asctime)s\n  Type: %(levelname)s\n  %(message)s'
)


class ThreadManager:
    def __init__(self):
        self.threads = []
        self.stop_event = threading.Event()

    def start_thread(self, target, args):
        t = threading.Thread(target=target, args=args)
        t.start()
        self.threads.append(t)

    def stop_threads(self):
        self.stop_event.set()
        for t in self.threads:
            t.join()

    def signal_handler(self, sig, frame):
        self.stop_threads()

    def setup_signal_handling(self):
        signal.signal(signal.SIGINT, self.signal_handler)

class TranslateCK3():
    def __init__(self, db_manager, start_path) -> None:
        self.db_manager = db_manager
        self.start_path = start_path
    
    def run(self):
        self.translate_yml_files()

    def translate_yml_files(self):
        entries = self.db_manager.session.query(CrusaderKingTranslate).all()

        translated_count = 0
        for root, dirs, files in os.walk(self.start_path):
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
                                    entry = self.db_manager.session.query(CrusaderKingTranslate).filter_by(id_translate=key).first()
                                    if entry and entry.translate_wish:
                                        old_value = entry.translate_original
                                        new_value = entry.translate_wish
                                        line = f'{key}:0 "{new_value}"\n'
                                        translated_count += 1
                                        logging.info(f"Tradução concluída:\n    ID: '{key}'\n    Texto original: '{old_value}'\n    Novo texto: '{new_value}'")
                                f.write(line)
                    except Exception as e:
                        logging.error(f"Erro ao traduzir o arquivo {file_path}:\n{traceback.format_exc()}")
        logging.info(f"Foram aplicadas traduções para {translated_count} IDs usando o banco de dados.")
        
class ProgressMonitor:
    def __init__(self):
        self.start_time = time()
        init(autoreset=True)
        
    def clear_console(self):
        os.system('cls')
    
    def monitoring_progress(self, db_manager, thread_manager):
        try:
            while not thread_manager.stop_event.is_set():
                self.total_db_count = db_manager.get_all_count()
                self.translate_concluido = db_manager.get_count_translate_wish_not_empty()
                
                if self.total_db_count > 0:
                    self.progress_percentage = (self.translate_concluido / self.total_db_count) * 100
                else:
                    self.progress_percentage = 0
                
                elapsed_time = time() - self.start_time

                if self.translate_concluido > 0:
                    average_time_per_translation = elapsed_time / self.translate_concluido
                    remaining_translations = self.total_db_count - self.translate_concluido
                    estimated_time_remaining = remaining_translations * average_time_per_translation
                else:
                    estimated_time_remaining = float('inf')
                
                hours, rem = divmod(estimated_time_remaining, 3600)
                minutes, seconds = divmod(rem, 60)
                
                logging.info(f"Progresso: {self.translate_concluido}/{self.total_db_count} "
                            f"({self.progress_percentage:.2f}%) concluído.")
                logging.info(f"Tempo estimado restante: {int(hours):02}:{int(minutes):02}:{int(seconds):02} (hh:mm:ss)")
                self.clear_console()
                print(f"{Fore.GREEN}Progresso: {self.translate_concluido}/{self.total_db_count} "
                    f"({self.progress_percentage:.2f}%) concluído.")
                print(f"{Fore.YELLOW}Tempo estimado restante: {int(hours):02}:{int(minutes):02}:{int(seconds):02} (hh:mm:ss)")
                
                for i in range(10):
                    sleep(1)
        except KeyboardInterrupt:
            logging.info("Interrupção pelo usuário recebida. Parando threads...")
            thread_manager.stop_threads()
        except:
            thread_manager.stop_threads()

class Main:
    def __init__(self, action, start_path, translate_limit, db_uri, thread_max) -> None:
        self.action = action
        self.start_path = start_path
        self.translate_limit = translate_limit
        self.db_uri = db_uri
        self.db_manager = DatabaseManager(self.db_uri)
        self.tradutor_selected = 3
        self.tradutor_src = 'en'
        self.tradutor_dest = 'pt'
        #self.thread_manager = ThreadManager()
        self.thread_max = thread_max

        #self.thread_manager.setup_signal_handling()

        if self.tradutor_selected == 1:
            self.translator = Translator(service_urls=['translate.google.com.br'])
        elif self.tradutor_selected == 2:
            self.translator = TranslatorSec(from_lang=self.tradutor_src, to_lang=self.tradutor_dest)
        else:
            self.translator = GoogleTranslator(source=self.tradutor_src, target=self.tradutor_dest)

    def get_english_yml_files(self, folder_path):
        english_yml_files = []
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('english.yml'):
                    english_yml_files.append(os.path.join(root, file))
        logging.info(f"Encontrados {len(english_yml_files)} arquivos YML de inglês em {folder_path}")
        return english_yml_files

    def extract_yml_content(self, yml_files):
        data = []
        for file in yml_files:
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    lines = content.splitlines()
                    for line in lines:
                        if line.startswith('#'):
                            continue
                        if ":" in line:
                            key, value = line.split(":", 1)
                            print(key, value)
                            key = key.strip()
                            value = value.strip().strip('"')
                            data.append([file, key, value, ''])  # Adiciona uma coluna vazia para 'Translate Text'
                logging.info(f"Conteúdo extraído do arquivo {file} com sucesso")
            except Exception as e:
                logging.error(f"Erro ao ler o arquivo {file}:\n{traceback.format_exc()}")
        logging.info(f"Conteúdo extraído de {len(yml_files)} arquivos YML")
        return []

    def estado_do_texto(self, texto):
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

    def identificar_texto_entre_marcadores(self, texto):
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

    def translate_text(self, entry, src_lang='en', dest_lang='pt'):
        not_translate = ['\\', '#', '$', '[', '-']

        try:
            text_parts = self.identificar_texto_entre_marcadores(entry.translate_original)
            text_traduzido = ''
            for part in text_parts:
                if part != '':
                    if any(part.startswith(itens) for itens in not_translate):
                        text_traduzido += part
                    else:
                        try:
                            if self.tradutor_selected == 1:
                                text_traduzido_ = self.translator.translate(part, src_lang, dest_lang).text
                            else:
                                text_traduzido_ = self.translator.translate(part)
                        except TooManyRequests:
                            logging.warning(f"Erro ao traduzir: Muitas requisições. Pausando thread por 5 segundos.")
                            sleep(60)
                            return 1
                        except Exception as e:
                            exception_name = type(e).__name__
                            logging.error(f"Falha ao traduzir:\n    ID: '{entry.id_translate}'\n    Texto original: '{entry.translate_original}'\n    Error:\n     {exception_name}\n     {traceback.format_exc()}")
                            text_traduzido_ = ''
                        if text_traduzido_:
                            if part.startswith(' '):
                                text_traduzido_ = ' ' + text_traduzido_
                            if part.endswith(' '):
                                text_traduzido_ = text_traduzido_ + ' '
                            estado_original = self.estado_do_texto(part)
                            if estado_original == "uppercase":
                                text_traduzido += text_traduzido_.upper()
                            elif estado_original == "lowercase":
                                text_traduzido += text_traduzido_.lower()
                            elif estado_original == "titlecase":
                                text_traduzido += text_traduzido_.capitalize()
                            else:
                                text_traduzido += text_traduzido_
            if text_traduzido:
                logging.info(f"Texto traduzido com sucesso:\n    ID: '{entry.id_translate}'\n    Texto original: '{entry.translate_original}'\n    Novo texto: '{text_traduzido}'")
                return text_traduzido
            else:
                logging.warning(f"Não foi possível traduzir, mantendo o original:    ID: '{entry.id_translate}'\n    Texto original: '{entry.translate_original}'\n")
                return entry.translate_original
        except Exception as e:
            logging.error(f"Erro ao traduzir texto:\n    Texto original: '{entry.translate_original}'\n{traceback.format_exc()}")
            return None

    def find_localization_folders(self):
        localization_folders = []

        for root, dirs, files in os.walk(self.start_path):
            if 'localization' in dirs:
                localization_folders.append(os.path.join(root, 'localization'))

        logging.info(f"Encontrados {len(localization_folders)} diretórios de localização em {self.start_path}")
        return localization_folders

    def mass_update_translate_wish(self, db_manager, translate_limit):
        count = 0
        entry = True
        while entry and not self.thread_manager.stop_event.is_set():
            update = True
            entry = db_manager.get_next_update()
            if entry:
                translate_original = entry.translate_original
                if entry.translate_wish == '' or entry.translate_wish == None:
                    ja_foi_traduzido = db_manager.check_exist_original_translate(translate_original)
                    if not ja_foi_traduzido:
                        translate_wish = self.translate_text(entry)
                        if translate_wish == 1:
                            translate_wish = ''
                            update = False
                            db_manager.update_review(entry, False)
                    else:
                        translate_wish = ja_foi_traduzido
                    if update:
                        db_manager.update_translate_wish(entry, translate_wish)
                else:
                    continue
                if count > 0:
                    if count < int(translate_limit):
                        break
                count += 1
            else:
                break

    def run(self):
        if self.action == 'get_all_translate_en':
            localization_folders = self.find_localization_folders()

            all_yml_files = []
            for folder in localization_folders:
                all_yml_files.extend(self.get_english_yml_files(folder))

            extracted_data = self.extract_yml_content(all_yml_files)

            self.db_manager.create_db_and_insert_data(extracted_data)
            logging.info(f"Dados inseridos no banco de dados.")

        elif self.action == 'add_wish_translate':
            print(int(self.thread_max))
            for i in range(int(self.thread_max)):
                self.thread_manager.start_thread(self.mass_update_translate_wish, (DatabaseManager(self.db_uri), self.translate_limit))

            ProgressMonitor().monitoring_progress(DatabaseManager(self.db_uri), self.thread_manager)
            for t in self.thread_manager.threads:
                t.join()

            logging.info(f"Tradução concluída para um total de {self.translate_limit} entradas.")

        elif self.action == 'translate':
            TranslateCK3(self.db_manager, self.start_path).run()
            logging.info(f"As traduções foram aplicadas no DB.")

if __name__ == "__main__":
    from config import DB_URI
    
    parser = argparse.ArgumentParser(description='Processa arquivos de localização e gera/traduz arquivos de localização.')
    parser.add_argument('action', choices=['get_all_translate_en', 'translate', 'add_wish_translate'], help='Ação a ser executada')
    parser.add_argument('--start-path', required=True, help='Caminho inicial para procurar arquivos de localização')
    parser.add_argument('--translate-limit', default=0, help='Limite de execuções')
    parser.add_argument('--threads-max', default=4, help='Limite de execuções')

    args = parser.parse_args()
    

    Main(args.action, args.start_path, args.translate_limit, DB_URI, args.threads_max).run()
    
