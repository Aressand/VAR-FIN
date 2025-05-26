#!/usr/bin/env python3
"""
Processore per file data.xlsx
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Optional
from tqdm import tqdm

from config import Config
from utils.validators import IMEIValidator, DataFrameValidator, BusinessValidator

logger = logging.getLogger(__name__)

class DataFileProcessor:
    """Processore dedicato per il file data.xlsx."""
    
    def __init__(self, file_path: Path):
        """
        Inizializza il processore.
        
        Args:
            file_path: Path del file data.xlsx
        """
        self.file_path = file_path
        self.data_map = {}
        self.stats = {}
        
    def load_and_process(self) -> Dict[str, Dict]:
        """
        Carica e processa il file data.xlsx.
        
        Returns:
            Dict mappato per IMEI con dati finanziari
            
        Raises:
            Exception: Se il file non può essere processato
        """
        logger.info(f"Caricamento file dati finanziari: {self.file_path.name}")
        
        try:
            # Carica DataFrame
            df = self._load_dataframe()
            if df.empty:
                logger.warning("File data.xlsx vuoto - continuando senza dati finanziari")
                return {}
            
            # Valida struttura
            self._validate_structure(df)
            
            # Processa dati
            self._process_records(df)
            
            # Valida risultati
            self._validate_results()
            
            logger.info(f"Dati finanziari processati: {len(self.data_map)} IMEI")
            return self.data_map
            
        except Exception as e:
            logger.error(f"Errore elaborazione data.xlsx: {e}")
            logger.info("Continuando senza dati finanziari...")
            return {}
    
    def _load_dataframe(self) -> pd.DataFrame:
        """Carica il DataFrame dal file."""
        try:
            df = pd.read_excel(self.file_path)
            logger.info(f"Caricati {len(df)} record da {self.file_path.name}")
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Colonne trovate: {list(df.columns)}")
                if len(df) > 0:
                    logger.debug(f"Prima riga esempio: {df.iloc[0].to_dict()}")
            
            return df
            
        except Exception as e:
            raise Exception(f"Impossibile leggere {self.file_path}: {e}")
    
    def _validate_structure(self, df: pd.DataFrame) -> None:
        """Valida la struttura del DataFrame."""
        # Valida presenza colonne
        is_valid, missing = DataFrameValidator.validate_columns(
            df, Config.DATA_REQUIRED_COLUMNS, f"data.xlsx"
        )
        
        if missing:
            logger.warning(f"Colonne mancanti in data.xlsx: {missing}")
            # Continua comunque se non tutte le colonne sono presenti
        
        # Valida che non sia vuoto
        DataFrameValidator.validate_not_empty(df, "data.xlsx")
    
    def _process_records(self, df: pd.DataFrame) -> None:
        """Processa i record del DataFrame."""
        logger.info("Elaborazione record dati finanziari...")
        
        # Progress bar per file grandi
        iterator = tqdm(df.iterrows(), total=len(df), desc="Elaborazione data.xlsx") if len(df) > 100 else df.iterrows()
        
        processed_count = 0
        validation_warnings = []
        
        for _, row in iterator:
            # Valida IMEI
            imei_raw = row.get(Config.DATA_COLUMN_MAPPING['imei'])
            imei_clean = IMEIValidator.validate(imei_raw)
            
            if not imei_clean:
                continue
            
            # Estrae dati usando mapping configurazione
            record_data = self._extract_record_data(row)
            
            # Valida consistenza business
            warnings = BusinessValidator.validate_financial_data_consistency(record_data)
            if warnings:
                validation_warnings.extend(warnings)
            
            # Aggiunge al mapping
            self.data_map[imei_clean] = record_data
            processed_count += 1
        
        # Log warning se presenti
        if validation_warnings:
            logger.warning(f"Rilevate {len(validation_warnings)} inconsistenze dati finanziari")
            for warning in validation_warnings[:5]:  # Mostra solo le prime 5
                logger.warning(f"  {warning}")
        
        self.stats['processed_records'] = processed_count
        self.stats['total_records'] = len(df)
        self.stats['validation_warnings'] = len(validation_warnings)
    
    def _extract_record_data(self, row: pd.Series) -> Dict:
        """
        Estrae dati dal record usando il mapping configurazione.
        
        Args:
            row: Riga pandas Series
            
        Returns:
            Dict con dati estratti e normalizzati
        """
        return {
            'finanziaria': str(row.get(Config.DATA_COLUMN_MAPPING['finanziaria'], '')).strip(),
            'importo_finanziato_wind': self._safe_float(row.get(Config.DATA_COLUMN_MAPPING['importo_finanziato'], 0)),
            'id_pratica': str(row.get(Config.DATA_COLUMN_MAPPING['id_pratica'], '')).strip(),
            'tipo_finanz': str(row.get(Config.DATA_COLUMN_MAPPING['tipo_finanz'], '')).strip(),
            'n_ldc_findomestic': self._safe_float_or_string(row.get(Config.DATA_COLUMN_MAPPING['n_ldc_findomestic'], '')),
            'stato_prat': str(row.get(Config.DATA_COLUMN_MAPPING['stato_prat'], '')).strip(),
            'codice': self._safe_int_or_string(row.get(Config.DATA_COLUMN_MAPPING['codice'], ''))
        }
    
    def _safe_float(self, value) -> float:
        """Conversione sicura a float."""
        if pd.isna(value) or value == '':
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def _safe_float_or_string(self, value) -> str:
        """Conversione sicura a float o stringa se numerico."""
        if pd.isna(value) or value == '':
            return ''
        try:
            # Se è un numero, lo formatta come stringa senza decimali se è intero
            float_val = float(value)
            if float_val.is_integer():
                return str(int(float_val))
            else:
                return str(float_val)
        except (ValueError, TypeError):
            return str(value).strip()
    
    def _safe_int_or_string(self, value) -> str:
        """Conversione sicura a int o stringa."""
        if pd.isna(value) or value == '':
            return ''
        try:
            return str(int(float(value)))
        except (ValueError, TypeError):
            return str(value).strip()
    
    def _validate_results(self) -> None:
        """Valida i risultati finali."""
        BusinessValidator.validate_imei_uniqueness(self.data_map, "data.xlsx")
        
        if self.data_map:
            # Statistiche per tipo finanziaria
            finanziarie = {}
            for data in self.data_map.values():
                fin = data.get('finanziaria', 'VUOTO')
                finanziarie[fin] = finanziarie.get(fin, 0) + 1
            
            logger.info(f"Breakdown finanziarie: {finanziarie}")
        
    def get_statistics(self) -> Dict:
        """Restituisce statistiche elaborate."""
        return self.stats.copy()
    
    def get_imei_count(self) -> int:
        """Restituisce numero IMEI processati."""
        return len(self.data_map)
    
    def has_data(self) -> bool:
        """Verifica se ci sono dati caricati."""
        return len(self.data_map) > 0