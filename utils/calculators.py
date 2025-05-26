#!/usr/bin/env python3
"""
Utilities per calcoli VAR Processor
"""

import logging
from typing import Dict, Any
from config import Config

logger = logging.getLogger(__name__)

class DifferenceCalculator:
    """Calcolatore dedicato per la logica delle differenze."""
    
    @staticmethod
    def calculate(causale: str, finanziaria: str, 
                 importo_originale: float, importo_credito: float, 
                 importo_ndc: float, importo_finanziato_wind: float, 
                 importo_finanziato_post: float) -> float:
        """
        Calcola la differenza basata su causale e finanziaria.
        
        Logica prioritaria:
        1. Se FINANZIARIA = "FINDOMESTIC" o "COMPASS" -> W-Finanziato Wind - B-IMPORTO FINANZIATO
        2. Se Causale = "TEL_INCLUSO" -> W-Importo Originale - B-IMPORTO CREDITO  
        3. Se Causale = "PROMOCASH" -> W-Importo Originale - B-IMPORTO NDC
        4. Default -> W-Importo Originale - B-IMPORTO CREDITO
        
        Args:
            causale: Causale del record TI
            finanziaria: Tipo finanziaria
            importo_originale: Importo originale da TI
            importo_credito: Importo credito da post vendita
            importo_ndc: Importo NDC da post vendita
            importo_finanziato_wind: Importo finanziato da data.xlsx
            importo_finanziato_post: Importo finanziato da post vendita
            
        Returns:
            Differenza calcolata
        """
        
        # Priorità 1: Logica finanziaria
        if finanziaria in Config.FINANZIARIE_PRIORITY:
            result = importo_finanziato_wind - importo_finanziato_post
            logger.debug(f"Differenza finanziaria {finanziaria}: {importo_finanziato_wind} - {importo_finanziato_post} = {result}")
            return result
        
        # Priorità 2: Logica causale
        if causale == Config.CAUSALE_TEL_INCLUSO:
            result = importo_originale - importo_credito
            logger.debug(f"Differenza TEL_INCLUSO: {importo_originale} - {importo_credito} = {result}")
            return result
        elif causale == Config.CAUSALE_PROMOCASH:
            result = importo_originale - importo_ndc
            logger.debug(f"Differenza PROMOCASH: {importo_originale} - {importo_ndc} = {result}")
            return result
        
        # Default
        result = importo_originale - importo_credito
        logger.debug(f"Differenza default: {importo_originale} - {importo_credito} = {result}")
        return result
    
    @staticmethod
    def get_calculation_method(causale: str, finanziaria: str) -> str:
        """
        Restituisce il metodo di calcolo utilizzato.
        
        Args:
            causale: Causale del record
            finanziaria: Tipo finanziaria
            
        Returns:
            Stringa descrittiva del metodo
        """
        if finanziaria in Config.FINANZIARIE_PRIORITY:
            return f"FINANZIARIA_{finanziaria}"
        elif causale == Config.CAUSALE_TEL_INCLUSO:
            return "CAUSALE_TEL_INCLUSO"
        elif causale == Config.CAUSALE_PROMOCASH:
            return "CAUSALE_PROMOCASH"
        else:
            return "DEFAULT"


class StatisticsCalculator:
    """Calcolatore per statistiche elaborate."""
    
    @staticmethod
    def calculate_summary(output_records: list) -> Dict[str, Any]:
        """
        Calcola statistiche riepilogative.
        
        Args:
            output_records: Lista record di output
            
        Returns:
            Dict con statistiche complete
        """
        if not output_records:
            return {
                'total_records': 0,
                'matched': 0,
                'post_vendita_only': 0,
                'ti_only': 0,
                'total_difference': 0.0,
                'avg_difference': 0.0,
                'min_difference': 0.0,
                'max_difference': 0.0
            }
        
        # Conta per tipo
        matched = sum(1 for r in output_records if r.get('_TIPO') == 'MATCHED')
        pv_only = sum(1 for r in output_records if r.get('_TIPO') == 'POST_VENDITA_ONLY')
        ti_only = sum(1 for r in output_records if r.get('_TIPO') == 'TI_ONLY')
        
        # Calcoli differenze
        differences = [r.get('Differenza', 0) for r in output_records]
        total_diff = sum(differences)
        avg_diff = total_diff / len(differences) if differences else 0
        min_diff = min(differences) if differences else 0
        max_diff = max(differences) if differences else 0
        
        return {
            'total_records': len(output_records),
            'matched': matched,
            'post_vendita_only': pv_only,
            'ti_only': ti_only,
            'total_difference': round(total_diff, 2),
            'avg_difference': round(avg_diff, 2),
            'min_difference': round(min_diff, 2),
            'max_difference': round(max_diff, 2)
        }
    
    @staticmethod
    def calculate_financial_breakdown(output_records: list) -> Dict[str, Any]:
        """
        Calcola breakdown per tipo finanziaria.
        
        Args:
            output_records: Lista record di output
            
        Returns:
            Dict con breakdown finanziarie
        """
        breakdown = {
            'FINDOMESTIC': {'count': 0, 'total_diff': 0.0},
            'COMPASS': {'count': 0, 'total_diff': 0.0},
            'VAR': {'count': 0, 'total_diff': 0.0},
            'ALTRI': {'count': 0, 'total_diff': 0.0}
        }
        
        for record in output_records:
            finanziaria = record.get('FINANZIARIA', '').upper()
            differenza = record.get('Differenza', 0)
            
            if finanziaria in breakdown:
                breakdown[finanziaria]['count'] += 1
                breakdown[finanziaria]['total_diff'] += differenza
            else:
                breakdown['ALTRI']['count'] += 1
                breakdown['ALTRI']['total_diff'] += differenza
        
        # Arrotonda valori
        for key in breakdown:
            breakdown[key]['total_diff'] = round(breakdown[key]['total_diff'], 2)
        
        return breakdown
    
    @staticmethod
    def calculate_causale_breakdown(output_records: list) -> Dict[str, Any]:
        """
        Calcola breakdown per causale.
        
        Args:
            output_records: Lista record di output
            
        Returns:
            Dict con breakdown causali
        """
        breakdown = {}
        
        for record in output_records:
            causale = record.get('Causale', 'VUOTO').upper()
            differenza = record.get('Differenza', 0)
            
            if causale not in breakdown:
                breakdown[causale] = {'count': 0, 'total_diff': 0.0}
            
            breakdown[causale]['count'] += 1
            breakdown[causale]['total_diff'] += differenza
        
        # Arrotonda valori
        for key in breakdown:
            breakdown[key]['total_diff'] = round(breakdown[key]['total_diff'], 2)
        
        return breakdown


class CurrencyFormatter:
    """Formattatore per valori monetari."""
    
    @staticmethod
    def format_value(value: float, decimals: int = 2) -> float:
        """
        Formatta valore monetario.
        
        Args:
            value: Valore da formattare
            decimals: Numero decimali
            
        Returns:
            Valore formattato
        """
        if value is None or pd.isna(value):
            return 0.0
        
        return round(float(value), decimals)
    
    @staticmethod
    def format_currency_columns(df, currency_columns: list) -> None:
        """
        Formatta colonne valute in un DataFrame.
        
        Args:
            df: DataFrame da formattare
            currency_columns: Lista colonne valute
        """
        for col in currency_columns:
            if col in df.columns:
                df[col] = df[col].apply(CurrencyFormatter.format_value)
    
    @staticmethod
    def format_for_display(value: float, currency: str = "EUR") -> str:
        """
        Formatta valore per visualizzazione.
        
        Args:
            value: Valore numerico
            currency: Simbolo valuta
            
        Returns:
            Stringa formattata
        """
        return f"{currency} {value:,.2f}"


# Import pandas per i metodi che lo usano
import pandas as pd
