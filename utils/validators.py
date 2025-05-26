#!/usr/bin/env python3
"""
Utilities per validazioni VAR Processor
"""

import re
import pandas as pd
from typing import Optional, List, Dict, Tuple
import logging

logger = logging.getLogger(__name__)

class IMEIValidator:
    """Validatore per codici IMEI."""
    
    @staticmethod
    def validate(imei) -> Optional[str]:
        """
        Valida e normalizza un codice IMEI.
        
        Args:
            imei: Codice IMEI da validare
            
        Returns:
            IMEI normalizzato se valido, None altrimenti
        """
        if pd.isna(imei) or imei == '':
            return None
        
        # Converte a stringa e rimuove caratteri non numerici
        cleaned = re.sub(r'\D', '', str(imei))
        
        # Verifica lunghezza esatta di 15 cifre
        if len(cleaned) == 15 and cleaned.isdigit():
            return cleaned
        
        return None
    
    @staticmethod
    def validate_batch(imei_series: pd.Series) -> Tuple[pd.Series, Dict[str, int]]:
        """
        Valida una serie di IMEI in batch.
        
        Args:
            imei_series: Serie pandas con IMEI
            
        Returns:
            Tuple con (serie IMEI puliti, statistiche)
        """
        cleaned_series = imei_series.apply(IMEIValidator.validate)
        
        stats = {
            'total': len(imei_series),
            'valid': cleaned_series.notna().sum(),
            'invalid': cleaned_series.isna().sum()
        }
        
        if stats['invalid'] > 0:
            logger.warning(f"IMEI validation: {stats['valid']}/{stats['total']} validi, {stats['invalid']} invalidi")
        
        return cleaned_series, stats


class DataFrameValidator:
    """Validatore per strutture DataFrame."""
    
    @staticmethod
    def validate_columns(df: pd.DataFrame, required_columns: List[str], 
                        source_name: str = "DataFrame") -> Tuple[bool, List[str]]:
        """
        Valida presenza colonne richieste.
        
        Args:
            df: DataFrame da validare
            required_columns: Lista colonne richieste
            source_name: Nome sorgente per logging
            
        Returns:
            Tuple con (è_valido, colonne_mancanti)
        """
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"{source_name}: colonne mancanti {missing_columns}")
            logger.info(f"{source_name}: colonne disponibili {list(df.columns)}")
            return False, missing_columns
        
        logger.info(f"{source_name}: tutte le colonne richieste presenti")
        return True, []
    
    @staticmethod
    def validate_data_types(df: pd.DataFrame, expected_types: Dict[str, type],
                           source_name: str = "DataFrame") -> bool:
        """
        Valida tipi di dato delle colonne.
        
        Args:
            df: DataFrame da validare
            expected_types: Dict con {colonna: tipo_atteso}
            source_name: Nome sorgente per logging
            
        Returns:
            True se tutti i tipi sono corretti
        """
        issues = []
        
        for column, expected_type in expected_types.items():
            if column in df.columns:
                actual_type = df[column].dtype
                if not pd.api.types.is_dtype_equal(actual_type, expected_type):
                    issues.append(f"{column}: atteso {expected_type}, trovato {actual_type}")
        
        if issues:
            logger.warning(f"{source_name}: problemi tipi dati: {issues}")
            return False
        
        return True
    
    @staticmethod
    def validate_not_empty(df: pd.DataFrame, source_name: str = "DataFrame") -> bool:
        """
        Valida che il DataFrame non sia vuoto.
        
        Args:
            df: DataFrame da validare
            source_name: Nome sorgente per logging
            
        Returns:
            True se non vuoto
        """
        if df.empty:
            logger.error(f"{source_name}: DataFrame vuoto")
            return False
        
        logger.info(f"{source_name}: {len(df)} righe caricate")
        return True


class FileValidator:
    """Validatore per file di input."""
    
    @staticmethod
    def validate_file_exists(file_path, required: bool = True) -> bool:
        """
        Valida esistenza file.
        
        Args:
            file_path: Path del file
            required: Se il file è obbligatorio
            
        Returns:
            True se il file esiste o non è richiesto
        """
        from pathlib import Path
        
        path = Path(file_path)
        exists = path.exists()
        
        if not exists and required:
            logger.error(f"File obbligatorio non trovato: {path}")
            return False
        elif not exists and not required:
            logger.warning(f"File opzionale non trovato: {path}")
            return True
        else:
            logger.info(f"File trovato: {path.name}")
            return True
    
    @staticmethod
    def validate_file_readable(file_path) -> bool:
        """
        Valida che il file sia leggibile.
        
        Args:
            file_path: Path del file
            
        Returns:
            True se leggibile
        """
        from pathlib import Path
        
        try:
            path = Path(file_path)
            with open(path, 'rb') as f:
                f.read(1)  # Prova a leggere 1 byte
            return True
        except Exception as e:
            logger.error(f"File non leggibile {path}: {e}")
            return False


class BusinessValidator:
    """Validatore per regole business specifiche."""
    
    @staticmethod
    def validate_imei_uniqueness(imei_map: Dict, source_name: str) -> bool:
        """
        Valida unicità IMEI in una mappa.
        
        Args:
            imei_map: Dict con IMEI come chiavi
            source_name: Nome sorgente per logging
            
        Returns:
            True se tutti gli IMEI sono unici
        """
        total_imei = len(imei_map)
        logger.info(f"{source_name}: {total_imei} IMEI unici processati")
        return True
    
    @staticmethod
    def validate_financial_data_consistency(data_record: Dict) -> List[str]:
        """
        Valida consistenza dati finanziari.
        
        Args:
            data_record: Record dati finanziari
            
        Returns:
            Lista messaggi di warning
        """
        warnings = []
        
        # Verifica coerenza finanziaria
        finanziaria = data_record.get('finanziaria', '')
        importo = data_record.get('importo_finanziato_wind', 0)
        
        if finanziaria in ['FINDOMESTIC', 'COMPASS'] and importo == 0:
            warnings.append(f"Finanziaria {finanziaria} ma importo 0")
        
        if not finanziaria and importo > 0:
            warnings.append(f"Importo {importo} ma finanziaria vuota")
        
        return warnings
    
    @staticmethod
    def validate_difference_calculation(original: float, credit: float, 
                                      ndc: float, difference: float,
                                      causale: str) -> bool:
        """
        Valida correttezza calcolo differenza.
        
        Args:
            original: Importo originale
            credit: Importo credito
            ndc: Importo NDC
            difference: Differenza calcolata
            causale: Causale per logica
            
        Returns:
            True se il calcolo è corretto
        """
        expected = None
        
        if causale == 'TEL_INCLUSO':
            expected = original - credit
        elif causale == 'PROMOCASH':
            expected = original - ndc
        else:
            expected = original - credit
        
        # Tolleranza per arrotondamenti float
        tolerance = 0.01
        is_correct = abs(difference - expected) <= tolerance
        
        if not is_correct:
            logger.warning(f"Differenza incorretta: attesa {expected:.2f}, calcolata {difference:.2f}")
        
        return is_correct
