from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
import logging

Base = declarative_base()

class CrusaderKingTranslateLog(Base):
    __tablename__ = 'crusader_king_translate_log'
    
    id = Column(Integer, primary_key=True)
    date_log = Column(DateTime, default=datetime.now())
    acao = Column(String, default=None)
    id_ck_db = Column(Integer)
    filename = Column(String, default=None)
    new_filename = Column(String, default=None)
    id_translate = Column(String, default=None)
    new_id_translate = Column(String, default=None)
    translate_original = Column(String, default=None)
    new_translate_original = Column(String, default=None)
    translate_wish = Column(String, default=None)
    new_translate_wish = Column(String, default=None)

class CrusaderKingTranslate(Base):
    __tablename__ = 'crusader_king_translate'
    
    id = Column(Integer, primary_key=True)
    filename = Column(String)
    id_translate = Column(String)
    translate_original = Column(String)
    translate_wish = Column(String)
    revisada = Column(Boolean, default=False, nullable=False)

class AllDatabaseManager:
    def __init__(self, db_uri):
        self.engine = create_engine(db_uri, echo=False)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
    def create_new_log_entry(self, acao, id_ck_db = None,filename = None,new_filename = None,id_translate = None,new_id_translate = None,translate_original = None,new_translate_original = None,translate_wish = None,new_translate_wish = None):
        try:    
            new_entry = CrusaderKingTranslateLog(
                acao = acao,
                date_log = datetime.now(),
                id_ck_db = id_ck_db,
                filename = filename,
                new_filename = new_filename,
                id_translate = id_translate,
                new_id_translate = new_id_translate,
                translate_original = translate_original,
                new_translate_original = new_translate_original,
                translate_wish = translate_wish,
                new_translate_wish = new_translate_wish
            )
            self.session.add(new_entry)
            self.session.commit()
        except Exception as e:
            logging.error(f"Erro ao inserir dados no banco de dados PostgreSQL:\n{e}")
            self.session.rollback()
        
    def update_review(self, entry, review):
        entry.revisada = review
        self.session.commit()
        
    def get_all_count(self):
        try:
            count = self.session.query(CrusaderKingTranslate).count()
            logging.info(f"Contagem total de registros: {count}")
            return count
        except Exception as e:
            logging.error(f"Erro ao obter contagem total de registros:\n{e}")
            return None
        
    def get_count_translate_wish_not_empty(self):
        try:
            count = self.session.query(CrusaderKingTranslate).filter(CrusaderKingTranslate.translate_wish != '').count()
            return count
        except Exception as e:
            logging.error(f"Erro ao obter a contagem de registros onde translate_wish não está vazio:\n{e}")
            return None
        
    def check_first_register(self, filename, id_translate, translate_original):
        try:
            result = self.session.query(CrusaderKingTranslate).\
                filter(and_(CrusaderKingTranslate.filename == filename,
                            CrusaderKingTranslate.id_translate == id_translate,
                            CrusaderKingTranslate.translate_original == translate_original
                            )).first()
            if result:
                return result
            else:
                return True
        except Exception as e:
            logging.error(f"Erro db:\n{e}")
            return False

    def insert_data(self, data):
        list_to_insert = []
        for item in data:
            filename, id_translate, translate_original, translate_wish = item
            check_new = self.check_first_register(filename, id_translate, translate_original)
            if check_new:
                new_entry = CrusaderKingTranslate(
                    filename=filename,
                    id_translate=id_translate,
                    translate_original=translate_original,
                    translate_wish=translate_wish
                )
                self.create_new_log_entry("Nova entrada no db", id_ck_db=new_entry.id,new_filename = filename,new_id_translate = id_translate,new_translate_original = translate_original,new_translate_wish = translate_wish)
                list_to_insert.append(new_entry)
                logging.info(f"Adicionando entrada: {filename} - {id_translate} à fila de inserção.")
            else:
                logging.info(f"Entrada ja existente: {filename} - {id_translate}.")
        
        try:
            self.session.add_all(list_to_insert)
            self.session.commit()
            logging.info("Dados inseridos no banco de dados PostgreSQL.")
        except Exception as e:
            logging.error(f"Erro ao inserir dados no banco de dados PostgreSQL:\n{e}")
            self.session.rollback()
        
    def create_db_and_insert_data(self, data):
        self.insert_data(data)
        logging.info("Dados inseridos no banco de dados PostgreSQL.")
        
    def close(self):
        self.session.close()
        
    def get_next_update(self):
        try:
            entry = self.session.query(CrusaderKingTranslate).\
                filter(CrusaderKingTranslate.revisada == False).\
                order_by(CrusaderKingTranslate.id.asc()).first()
            if entry:
                self.update_review(entry, True)
                self.create_new_log_entry("Retornado para iniciar tratamento",id_ck_db=entry.id, filename = entry.filename, id_translate = entry.id_translate, translate_original = entry.translate_original, translate_wish = entry.translate_wish)
                return entry
            else:
                logging.info("Não há registros com translate_wish vazio e translate_original não vazio.")
                return None
        except Exception as e:
            logging.error(f"Erro ao buscar o primeiro registro com translate_wish vazio e translate_original não vazio:\n{e}")
            return None
        
    def check_exist_original_translate(self, translate_original):
        try:
            entry = self.session.query(CrusaderKingTranslate).filter_by(translate_original=translate_original).first()
            if entry:
                if entry.translate_wish != '' and entry.translate_wish != 'working' and entry.translate_wish != None:
                    self.create_new_log_entry("Encontrado texto ja traduzido", id_ck_db=entry.id, translate_original = entry.translate_original,translate_wish = entry.translate_wish)
                    return entry.translate_wish
                return False
            else:
                return False
        except Exception as e:
            logging.error(f"Erro ao buscar texto original no banco de dados PostgreSQL:\n{e}")
            return False
    
    def update_translate_wish(self, entry, translate_wish):
        try:
            last_translate_wish = entry.translate_wish
            entry.translate_wish = translate_wish
            self.session.commit()
            self.create_new_log_entry("ATT coluna translate_wish", id_ck_db=entry.id, filename = entry.filename,id_translate = entry.id_translate,translate_original = entry.translate_original,new_translate_wish = translate_wish)
            logging.info(f"DB atualizado:\n   Coluna: translate_wish\n   ID db: '{entry.id}'\n   ID translate: '{entry.id_translate}'\n   Last value: '{last_translate_wish}'\n   New value: '{translate_wish}'")
        except Exception as e:
            logging.error(f"Erro ao atualizar translate_wish no banco de dados PostgreSQL:\n{e}")
            self.session.rollback()
