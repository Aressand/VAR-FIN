#!/usr/bin/env python3
"""
Configurazioni per VAR Processor Production
"""

import logging
from pathlib import Path
from typing import Dict, List

class Config:
    """Configurazioni centrali per l'applicazione."""
    
    # Versione applicazione
    VERSION = "2.0.0"
    APP_NAME = "VAR Workflow Processor"
    
    # Logging
    LOG_LEVEL = logging.INFO
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOG_FILE = 'var_processor.log'
    
    # File patterns
    POST_VENDITA_PATTERNS = [
        "post_vendita_fisici.xls",
        "post_vendita_fisici.xlsx"
    ]
    
    TI_PATTERN = "telefono_incluso_*.xlsx"
    
    DATA_PATTERNS = [
        "data.xlsx", 
        "data.xls"
    ]
    
    # Colonne richieste per ogni file
    POST_VENDITA_REQUIRED_COLUMNS = [
        'IMEI', 'Punto Vendita', 'Cliente', 'Data Scarico', 
        'IMPORTO CREDITO', 'ID Vendita', 'IMPORTO NDC', 
        'Modalita vendita', 'IMPORTO FINANZIATO'
    ]
    
    TI_REQUIRED_COLUMNS = [
        'IMEI/SERIALE', 'RAGIONE SOCIALE DEALER', 'CODICE POS',
        'NUMERO NOTA CREDITO', 'CAUSALE', 'IMPORTO ORIGINALE'
    ]
    
    DATA_REQUIRED_COLUMNS = [
        'IMEI Telefono Incluso', 'Finanziaria', 'Importo Finanziato', 
        'Id Pratica', 'Tipo Finanz', 'N° L.d.C. o N° Prat. Findomestic', 
        'Stato Prat.', 'Codice'
    ]
    
    # Mapping colonne data.xlsx
    DATA_COLUMN_MAPPING = {
        'imei': 'IMEI Telefono Incluso',
        'finanziaria': 'Finanziaria', 
        'importo_finanziato': 'Importo Finanziato',
        'id_pratica': 'Id Pratica',
        'tipo_finanz': 'Tipo Finanz',
        'n_ldc_findomestic': 'N° L.d.C. o N° Prat. Findomestic',
        'stato_prat': 'Stato Prat.',
        'codice': 'Codice'
    }
    
    # Colonne output Excel (ordine finale)
    OUTPUT_COLUMNS = [
        'IMEI',
        'Ragione sociale Dealer',
        'Codice POS', 
        'Numero Nota di Credito',
        'Causale',
        'Punto vendita',
        'ID Vendita',
        'Cliente',
        'Data Scarico',
        'Modalita vendita',
        'POS-Finanziato',
        'FINANZIARIA',
        'Id Pratica',
        'Tipo Finanz',
        'N° L.d.C. o N° Prat. Findomestic',
        'Stato Prat',
        'W-Importo Originale',
        'W-Finanziato Wind',
        'B-IMPORTO CREDITO',
        'B-IMPORTO NDC',
        'B-IMPORTO FINANZIATO',
        'Differenza'
    ]
    
    # Colonne valute (per formattazione)
    CURRENCY_COLUMNS = [
        'W-Importo Originale', 'W-Finanziato Wind', 
        'B-IMPORTO CREDITO', 'B-IMPORTO NDC', 
        'B-IMPORTO FINANZIATO', 'Differenza'
    ]
    
    # Logiche differenza
    FINANZIARIE_PRIORITY = ['FINDOMESTIC', 'COMPASS']
    CAUSALE_TEL_INCLUSO = 'TEL_INCLUSO'
    CAUSALE_PROMOCASH = 'PROMOCASH'
    
    # Performance
    CHUNK_SIZE = 1000  # Per file molto grandi
    MAX_MEMORY_ROWS = 10000  # Limite righe in memoria
    
    # Output
    DEFAULT_OUTPUT_PREFIX = "VAR_Report"
    EXCEL_SHEET_NAME = "VAR Report"
    
    # Backup
    ENABLE_BACKUP = True
    BACKUP_DIR = "backup"
    MAX_BACKUPS = 5
    
    @classmethod
    def get_output_filename(cls, timestamp: str = None) -> str:
        """Genera nome file output con timestamp."""
        if not timestamp:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{cls.DEFAULT_OUTPUT_PREFIX}_{timestamp}.xlsx"
    
    @classmethod
    def setup_logging(cls, log_level: int = None, log_file: str = None) -> logging.Logger:
        """Configura logging centralizzato."""
        level = log_level or cls.LOG_LEVEL
        file = log_file or cls.LOG_FILE
        
        # Rimuove handler esistenti
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Configura nuovo logging
        logging.basicConfig(
            level=level,
            format=cls.LOG_FORMAT,
            handlers=[
                logging.FileHandler(file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        # Riduce verbosità librerie esterne
        logging.getLogger('openpyxl').setLevel(logging.WARNING)
        logging.getLogger('pandas').setLevel(logging.WARNING)
        
        return logging.getLogger(cls.APP_NAME)
