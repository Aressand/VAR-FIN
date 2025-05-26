#!/usr/bin/env python3
"""
VAR Processor principale - Production Version
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from tqdm import tqdm

from config import Config
from processors.data_processor import DataFileProcessor
from utils.validators import IMEIValidator, DataFrameValidator, FileValidator
from utils.calculators import DifferenceCalculator, CurrencyFormatter

logger = logging.getLogger(__name__)

class VARProcessor:
    """Processore principale per il workflow VAR - Production Version."""
    
    def __init__(self, input_directory: str = "."):
        """
        Inizializza il processore VAR.
        
        Args:
            input_directory: Directory contenente i file da elaborare
        """
        self.input_dir = Path(input_directory)
        self.post_vendita_file = None
        self.ti_files = []
        self.data_file = None
        self.output_records = []
        self.stats = {}
        
        logger.info(f"Inizializzato VAR Processor: {self.input_dir.absolute()}")
    
    def run(self, output_filename: str = None) -> str:
        """
        Esegue l'intero workflow VAR.
        
        Args:
            output_filename: Nome del file di output (opzionale)
            
        Returns:
            Path del file Excel generato
            
        Raises:
            Exception: Se l'elaborazione fallisce
        """
        logger.info("Avvio workflow VAR...")
        
        try:
            # 1. Ricerca e validazione file
            self._find_and_validate_files()
            
            # 2. Caricamento dati
            post_vendita_df = self._load_post_vendita_data()
            ti_df = self._load_ti_data()
            data_map = self._load_financial_data()
            
            # 3. Elaborazione matching
            self.output_records = self._process_matching(post_vendita_df, ti_df, data_map)
            
            # 4. Generazione output
            output_path = self._generate_excel_output(output_filename)
            
            # 5. Calcolo statistiche finali
            self._calculate_final_statistics()
            
            logger.info("Workflow VAR completato con successo")
            return output_path
            
        except Exception as e:
            logger.error(f"Errore durante l'elaborazione: {e}")
            raise
    
    def _find_and_validate_files(self) -> None:
        """Trova e valida tutti i file necessari."""
        logger.info("Ricerca e validazione file...")
        
        # File post_vendita_fisici (obbligatorio)
        for pattern in Config.POST_VENDITA_PATTERNS:
            files = list(self.input_dir.glob(pattern))
            if files:
                self.post_vendita_file = files[0]
                logger.info(f"File post vendita: {self.post_vendita_file.name}")
                break
        
        if not self.post_vendita_file:
            raise FileNotFoundError("File post_vendita_fisici non trovato")
        
        # File telefono_incluso (obbligatori)
        self.ti_files = list(self.input_dir.glob(Config.TI_PATTERN))
        if not self.ti_files:
            raise FileNotFoundError("Nessun file telefono_incluso trovato")
        
        logger.info(f"File TI trovati: {len(self.ti_files)}")
        for ti_file in self.ti_files:
            logger.info(f"  • {ti_file.name}")
        
        # File data.xlsx (opzionale)
        for pattern in Config.DATA_PATTERNS:
            files = list(self.input_dir.glob(pattern))
            if files:
                self.data_file = files[0]
                logger.info(f"File dati finanziari: {self.data_file.name}")
                break
        
        if not self.data_file:
            logger.warning("File data.xlsx non trovato - continuando senza dati finanziari")
        
        # Valida leggibilità file
        all_files = [self.post_vendita_file] + self.ti_files
        if self.data_file:
            all_files.append(self.data_file)
        
        for file_path in all_files:
            if not FileValidator.validate_file_readable(file_path):
                raise Exception(f"File non leggibile: {file_path}")
    
    def _load_post_vendita_data(self) -> pd.DataFrame:
        """Carica e processa i dati dal file post_vendita_fisici."""
        logger.info(f"Caricamento {self.post_vendita_file.name}...")
        
        try:
            # Gestisce file HTML mascherati da XLS
            if self.post_vendita_file.suffix.lower() == '.xls':
                try:
                    df = pd.read_excel(self.post_vendita_file, engine='xlrd')
                except:
                    logger.info("File XLS sembra essere HTML, lettura come HTML...")
                    df = pd.read_html(self.post_vendita_file)[0]
            else:
                df = pd.read_excel(self.post_vendita_file)
            
            logger.info(f"Caricati {len(df)} record da post_vendita_fisici")
            
            # Valida struttura
            DataFrameValidator.validate_columns(
                df, Config.POST_VENDITA_REQUIRED_COLUMNS, "post_vendita_fisici"
            )
            
            # Valida e pulisce IMEI
            df['IMEI_CLEAN'], imei_stats = IMEIValidator.validate_batch(df['IMEI'])
            logger.info(f"IMEI post vendita: {imei_stats['valid']}/{imei_stats['total']} validi")
            
            # Filtra solo righe con IMEI validi
            df_clean = df[df['IMEI_CLEAN'].notna()].copy()
            
            return df_clean
            
        except Exception as e:
            raise Exception(f"Errore caricamento post_vendita_fisici: {e}")
    
    def _load_ti_data(self) -> pd.DataFrame:
        """Carica e combina i dati da tutti i file telefono_incluso."""
        logger.info("Caricamento file telefono_incluso...")
        
        all_ti_data = []
        total_records = 0
        
        for ti_file in self.ti_files:
            try:
                logger.info(f"Elaborazione {ti_file.name}...")
                
                df = pd.read_excel(ti_file)
                total_records += len(df)
                
                # Valida struttura
                DataFrameValidator.validate_columns(
                    df, Config.TI_REQUIRED_COLUMNS, ti_file.name
                )
                
                # Normalizza colonne
                if 'IMEI/SERIALE' in df.columns:
                    df = df.rename(columns={'IMEI/SERIALE': 'IMEI'})
                
                # Valida IMEI
                df['IMEI_CLEAN'], imei_stats = IMEIValidator.validate_batch(df['IMEI'])
                
                # Filtra righe valide
                valid_rows = df[df['IMEI_CLEAN'].notna()].copy()
                logger.info(f"{ti_file.name}: {imei_stats['valid']}/{imei_stats['total']} IMEI validi")
                
                # Aggiunge tracciabilità
                valid_rows['SOURCE_FILE'] = ti_file.name
                
                all_ti_data.append(valid_rows)
                
            except Exception as e:
                logger.error(f"Errore caricamento {ti_file.name}: {e}")
                continue
        
        if not all_ti_data:
            raise Exception("Nessun file TI caricato con successo")
        
        # Combina tutti i DataFrame
        combined_df = pd.concat(all_ti_data, ignore_index=True)
        logger.info(f"TI combinati: {len(combined_df)} record validi da {total_records} totali")
        
        return combined_df
    
    def _load_financial_data(self) -> Dict[str, Dict]:
        """Carica dati finanziari (opzionale)."""
        if not self.data_file:
            return {}
        
        try:
            processor = DataFileProcessor(self.data_file)
            data_map = processor.load_and_process()
            
            if processor.has_data():
                logger.info(f"Dati finanziari caricati: {processor.get_imei_count()} IMEI")
            
            return data_map
            
        except Exception as e:
            logger.warning(f"Errore caricamento dati finanziari: {e}")
            return {}
    
    def _process_matching(self, post_vendita_df: pd.DataFrame, 
                         ti_df: pd.DataFrame, data_map: Dict) -> List[Dict]:
        """Esegue il matching tra i dati."""
        logger.info("Elaborazione matching IMEI...")
        
        # Crea mapping per accesso rapido
        post_vendita_map = self._create_post_vendita_mapping(post_vendita_df)
        ti_map = self._create_ti_mapping(ti_df)
        
        logger.info(f"Mapping creati: {len(post_vendita_map)} post vendita, {len(ti_map)} TI")
        
        # Processa matching con progress bar
        output_records = []
        matched_count = 0
        pv_only_count = 0
        
        # Progress bar per post vendita
        pv_iterator = tqdm(post_vendita_map.items(), desc="Matching post vendita", 
                          disable=len(post_vendita_map) < 100)
        
        for imei, pv_data in pv_iterator:
            ti_data = ti_map.get(imei)
            data_financial = data_map.get(imei, {})
            
            if ti_data:
                # Match trovato
                matched_count += 1
                record = self._create_matched_record(imei, pv_data, ti_data, data_financial)
            else:
                # Solo post vendita
                pv_only_count += 1
                record = self._create_post_vendita_only_record(imei, pv_data, data_financial)
            
            output_records.append(record)
        
        # Aggiungi IMEI presenti solo nei TI
        ti_only_count = 0
        for imei, ti_data in ti_map.items():
            if imei not in post_vendita_map:
                ti_only_count += 1
                data_financial = data_map.get(imei, {})
                record = self._create_ti_only_record(imei, ti_data, data_financial)
                output_records.append(record)
        
        # Log statistiche matching
        logger.info(f"Matching completato:")
        logger.info(f"  • IMEI totali: {len(output_records)}")
        logger.info(f"  • IMEI matched: {matched_count}")
        logger.info(f"  • Solo post vendita: {pv_only_count}")
        logger.info(f"  • Solo TI: {ti_only_count}")
        
        # Ordina per Data Scarico
        output_records.sort(key=lambda x: x.get('Data Scarico') or '', reverse=True)
        
        return output_records
    
    def _create_post_vendita_mapping(self, df: pd.DataFrame) -> Dict:
        """Crea mapping IMEI -> dati post vendita."""
        mapping = {}
        for _, row in df.iterrows():
            imei = row['IMEI_CLEAN']
            mapping[imei] = {
                'punto_vendita': str(row.get('Punto Vendita', '')),
                'id_vendita': str(row.get('ID Vendita', '')),
                'cliente': str(row.get('Cliente', '')),
                'data_scarico': row.get('Data Scarico'),
                'modalita_vendita': str(row.get('Modalita vendita', '')),
                'importo_credito': CurrencyFormatter.format_value(row.get('IMPORTO CREDITO', 0)),
                'importo_ndc': CurrencyFormatter.format_value(row.get('IMPORTO NDC', 0)),
                'importo_finanziato': CurrencyFormatter.format_value(row.get('IMPORTO FINANZIATO', 0))
            }
        return mapping
    
    def _create_ti_mapping(self, df: pd.DataFrame) -> Dict:
        """Crea mapping IMEI -> dati TI."""
        mapping = {}
        for _, row in df.iterrows():
            imei = row['IMEI_CLEAN']
            mapping[imei] = {
                'ragione_sociale_dealer': str(row.get('RAGIONE SOCIALE DEALER', '')),
                'codice_pos': str(row.get('CODICE POS', '')),
                'numero_nota_credito': str(row.get('NUMERO NOTA CREDITO', '')),
                'causale': str(row.get('CAUSALE', '')),
                'importo_originale': CurrencyFormatter.format_value(abs(float(row.get('IMPORTO ORIGINALE', 0)))),
                'source_file': str(row.get('SOURCE_FILE', ''))
            }
        return mapping
    
    def _create_matched_record(self, imei: str, pv_data: Dict, 
                              ti_data: Dict, data_financial: Dict) -> Dict:
        """Crea record per IMEI matched."""
        differenza = DifferenceCalculator.calculate(
            causale=ti_data['causale'],
            finanziaria=data_financial.get('finanziaria', ''),
            importo_originale=ti_data['importo_originale'],
            importo_credito=pv_data['importo_credito'],
            importo_ndc=pv_data['importo_ndc'],
            importo_finanziato_wind=data_financial.get('importo_finanziato_wind', 0.0),
            importo_finanziato_post=pv_data['importo_finanziato']
        )
        
        return {
            'IMEI': imei,
            'Ragione sociale Dealer': ti_data['ragione_sociale_dealer'],
            'Codice POS': ti_data['codice_pos'],
            'Numero Nota di Credito': ti_data['numero_nota_credito'],
            'Causale': ti_data['causale'],
            'Punto vendita': pv_data['punto_vendita'],
            'ID Vendita': pv_data['id_vendita'],
            'Cliente': pv_data['cliente'],
            'Data Scarico': pv_data['data_scarico'],
            'Modalita vendita': pv_data['modalita_vendita'],
            'POS-Finanziato': data_financial.get('codice', ''),
            'FINANZIARIA': data_financial.get('finanziaria', ''),
            'Id Pratica': data_financial.get('id_pratica', ''),
            'Tipo Finanz': data_financial.get('tipo_finanz', ''),
            'N° L.d.C. o N° Prat. Findomestic': data_financial.get('n_ldc_findomestic', ''),
            'Stato Prat': data_financial.get('stato_prat', ''),
            'W-Importo Originale': ti_data['importo_originale'],
            'W-Finanziato Wind': data_financial.get('importo_finanziato_wind', 0.0),
            'B-IMPORTO CREDITO': pv_data['importo_credito'],
            'B-IMPORTO NDC': pv_data['importo_ndc'],
            'B-IMPORTO FINANZIATO': pv_data['importo_finanziato'],
            'Differenza': differenza,
            '_TIPO': 'MATCHED',
            '_SOURCE_TI': ti_data['source_file']
        }
    
    def _create_post_vendita_only_record(self, imei: str, pv_data: Dict, 
                                        data_financial: Dict) -> Dict:
        """Crea record per IMEI solo in post vendita."""
        differenza = DifferenceCalculator.calculate(
            causale='',
            finanziaria=data_financial.get('finanziaria', ''),
            importo_originale=0.0,
            importo_credito=pv_data['importo_credito'],
            importo_ndc=pv_data['importo_ndc'],
            importo_finanziato_wind=data_financial.get('importo_finanziato_wind', 0.0),
            importo_finanziato_post=pv_data['importo_finanziato']
        )
        
        return {
            'IMEI': imei,
            'Ragione sociale Dealer': '',
            'Codice POS': '',
            'Numero Nota di Credito': '',
            'Causale': '',
            'Punto vendita': pv_data['punto_vendita'],
            'ID Vendita': pv_data['id_vendita'],
            'Cliente': pv_data['cliente'],
            'Data Scarico': pv_data['data_scarico'],
            'Modalita vendita': pv_data['modalita_vendita'],
            'POS-Finanziato': data_financial.get('codice', ''),
            'FINANZIARIA': data_financial.get('finanziaria', ''),
            'Id Pratica': data_financial.get('id_pratica', ''),
            'Tipo Finanz': data_financial.get('tipo_finanz', ''),
            'N° L.d.C. o N° Prat. Findomestic': data_financial.get('n_ldc_findomestic', ''),
            'Stato Prat': data_financial.get('stato_prat', ''),
            'W-Importo Originale': 0.0,
            'W-Finanziato Wind': data_financial.get('importo_finanziato_wind', 0.0),
            'B-IMPORTO CREDITO': pv_data['importo_credito'],
            'B-IMPORTO NDC': pv_data['importo_ndc'],
            'B-IMPORTO FINANZIATO': pv_data['importo_finanziato'],
            'Differenza': differenza,
            '_TIPO': 'POST_VENDITA_ONLY'
        }
    
    def _create_ti_only_record(self, imei: str, ti_data: Dict, 
                              data_financial: Dict) -> Dict:
        """Crea record per IMEI solo in TI."""
        differenza = DifferenceCalculator.calculate(
            causale=ti_data['causale'],
            finanziaria=data_financial.get('finanziaria', ''),
            importo_originale=ti_data['importo_originale'],
            importo_credito=0.0,
            importo_ndc=0.0,
            importo_finanziato_wind=data_financial.get('importo_finanziato_wind', 0.0),
            importo_finanziato_post=0.0
        )
        
        return {
            'IMEI': imei,
            'Ragione sociale Dealer': ti_data['ragione_sociale_dealer'],
            'Codice POS': ti_data['codice_pos'],
            'Numero Nota di Credito': ti_data['numero_nota_credito'],
            'Causale': ti_data['causale'],
            'Punto vendita': '',
            'ID Vendita': '',
            'Cliente': '',
            'Data Scarico': '',
            'Modalita vendita': '',
            'POS-Finanziato': data_financial.get('codice', ''),
            'FINANZIARIA': data_financial.get('finanziaria', ''),
            'Id Pratica': data_financial.get('id_pratica', ''),
            'Tipo Finanz': data_financial.get('tipo_finanz', ''),
            'N° L.d.C. o N° Prat. Findomestic': data_financial.get('n_ldc_findomestic', ''),
            'Stato Prat': data_financial.get('stato_prat', ''),
            'W-Importo Originale': ti_data['importo_originale'],
            'W-Finanziato Wind': data_financial.get('importo_finanziato_wind', 0.0),
            'B-IMPORTO CREDITO': 0.0,
            'B-IMPORTO NDC': 0.0,
            'B-IMPORTO FINANZIATO': 0.0,
            'Differenza': differenza,
            '_TIPO': 'TI_ONLY',
            '_SOURCE_TI': ti_data['source_file']
        }
    
    def _generate_excel_output(self, output_filename: str = None) -> str:
        """Genera il file Excel di output."""
        if not output_filename:
            output_filename = Config.get_output_filename()
        
        output_path = self.input_dir / output_filename
        logger.info(f"Generazione file Excel: {output_filename}")
        
        # Crea DataFrame per l'output
        df_output = pd.DataFrame(self.output_records)
        
        # Rimuove colonne di servizio
        service_columns = [col for col in df_output.columns if col.startswith('_')]
        df_output = df_output.drop(columns=service_columns, errors='ignore')
        
        # Riordina colonne secondo configurazione
        available_columns = [col for col in Config.OUTPUT_COLUMNS if col in df_output.columns]
        df_output = df_output[available_columns]
        
        # Formatta colonne valute
        CurrencyFormatter.format_currency_columns(df_output, Config.CURRENCY_COLUMNS)
        
        # Ordina per Data Scarico (più recenti prima)
        if 'Data Scarico' in df_output.columns:
            df_output['Data Scarico'] = pd.to_datetime(df_output['Data Scarico'], errors='coerce')
            df_output = df_output.sort_values('Data Scarico', ascending=False, na_position='last')
        
        # Scrive file Excel con formattazione
        self._write_excel_file(df_output, output_path)
        
        logger.info(f"File Excel generato: {output_path}")
        return str(output_path)
    
    def _write_excel_file(self, df: pd.DataFrame, output_path: Path) -> None:
        """Scrive il file Excel con formattazione professionale."""
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Scrive il foglio principale
            df.to_excel(writer, sheet_name=Config.EXCEL_SHEET_NAME, index=False)
            
            # Ottiene worksheet per formattazione
            worksheet = writer.sheets[Config.EXCEL_SHEET_NAME]
            
            # Formatta intestazioni
            for cell in worksheet[1]:
                if cell.value:  # Solo celle con contenuto
                    try:
                        # Usa metodo moderno per evitare deprecation warning
                        from openpyxl.styles import Font, PatternFill
                        cell.font = Font(bold=True, size=11)
                        cell.fill = PatternFill(start_color="E8E8E8", end_color="E8E8E8", fill_type="solid")
                    except ImportError:
                        # Fallback per versioni più vecchie
                        cell.font = cell.font.copy(bold=True)
                        cell.fill = cell.fill.copy(fgColor="E8E8E8")
            
            # Auto-width colonne con limite massimo
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                
                for cell in column:
                    try:
                        if cell.value and len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        continue
                
                # Imposta larghezza con limiti ragionevoli
                adjusted_width = min(max(max_length + 2, 10), 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # Freeze panes su prima riga
            worksheet.freeze_panes = "A2"
    
    def _calculate_final_statistics(self) -> None:
        """Calcola statistiche finali per il processore."""
        if not self.output_records:
            return
        
        # Statistiche base
        total_difference = sum(record.get('Differenza', 0) for record in self.output_records)
        
        # Conta per tipo
        type_counts = {}
        for record in self.output_records:
            record_type = record.get('_TIPO', 'UNKNOWN')
            type_counts[record_type] = type_counts.get(record_type, 0) + 1
        
        self.stats = {
            'total_imei': len(self.output_records),
            'matched_imei': type_counts.get('MATCHED', 0),
            'pv_only_imei': type_counts.get('POST_VENDITA_ONLY', 0),
            'ti_only_imei': type_counts.get('TI_ONLY', 0),
            'total_difference': round(total_difference, 2)
        }
        
        logger.info(f"Statistiche finali: {self.stats}")
    
    def get_output_records(self) -> List[Dict]:
        """Restituisce i record di output per analisi esterne."""
        return self.output_records.copy()
    
    def get_statistics(self) -> Dict:
        """Restituisce le statistiche elaborate."""
        return self.stats.copy()
