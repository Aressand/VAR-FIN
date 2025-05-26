#!/usr/bin/env python3
"""
VAR Workflow Processor - Production Version
============================================

Entry point principale per l'elaborazione VAR in produzione.

Autore: VAR Workflow Team
Versione: 2.0.0 Production Ready
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

# Import moduli locali
from config import Config
from processors.var_processor import VARProcessor
from utils.calculators import StatisticsCalculator

def setup_environment():
    """Configura l'ambiente di esecuzione."""
    # Setup logging
    logger = Config.setup_logging()
    
    # Log informazioni sistema
    logger.info(f"=" * 60)
    logger.info(f"{Config.APP_NAME} v{Config.VERSION}")
    logger.info(f"Avvio: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    logger.info(f"Python: {sys.version}")
    logger.info(f"=" * 60)
    
    return logger

def parse_arguments():
    """Parsing argomenti linea di comando."""
    parser = argparse.ArgumentParser(
        description=f"{Config.APP_NAME} v{Config.VERSION}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi di utilizzo:
  python main.py                           # Elaborazione standard
  python main.py --input /path/to/files   # Directory personalizzata
  python main.py --output custom_report   # Nome output personalizzato
  python main.py --verbose                # Output dettagliato
  python main.py --quiet                  # Solo errori
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        type=str,
        default='.',
        help='Directory contenente i file di input (default: directory corrente)'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Nome file di output (default: VAR_Report_TIMESTAMP.xlsx)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Output dettagliato (DEBUG level)'
    )
    
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Output minimo (solo errori)'
    )
    
    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='Disabilita backup automatico'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Solo validazione file, nessuna elaborazione'
    )
    
    return parser.parse_args()

def configure_logging_level(args):
    """Configura il livello di logging in base agli argomenti."""
    if args.verbose:
        level = logging.DEBUG
    elif args.quiet:
        level = logging.ERROR
    else:
        level = logging.INFO
    
    # Riconfigura logging con nuovo livello
    return Config.setup_logging(log_level=level)

def validate_input_directory(input_dir: Path, logger) -> bool:
    """
    Valida la directory di input.
    
    Args:
        input_dir: Path della directory
        logger: Logger per output
        
    Returns:
        True se la directory è valida
    """
    if not input_dir.exists():
        logger.error(f"Directory non trovata: {input_dir}")
        return False
    
    if not input_dir.is_dir():
        logger.error(f"Il path non è una directory: {input_dir}")
        return False
    
    # Verifica presenza file essenziali
    post_vendita_found = any(input_dir.glob(pattern) for pattern in Config.POST_VENDITA_PATTERNS)
    ti_found = list(input_dir.glob(Config.TI_PATTERN))
    
    if not post_vendita_found:
        logger.error(f"File post_vendita_fisici non trovato in {input_dir}")
        return False
    
    if not ti_found:
        logger.error(f"Nessun file telefono_incluso trovato in {input_dir}")
        return False
    
    logger.info(f"Directory input validata: {input_dir}")
    logger.info(f"  • File post vendita: trovato")
    logger.info(f"  • File TI: {len(ti_found)} trovati")
    
    # Verifica file data (opzionale)
    data_found = any(input_dir.glob(pattern) for pattern in Config.DATA_PATTERNS)
    if data_found:
        logger.info(f"  • File dati finanziari: trovato")
    else:
        logger.warning(f"  • File dati finanziari: non trovato (opzionale)")
    
    return True

def create_backup(output_path: Path, logger) -> Optional[Path]:
    """
    Crea backup del file di output se già esiste.
    
    Args:
        output_path: Path del file output
        logger: Logger per output
        
    Returns:
        Path del backup creato o None
    """
    if not output_path.exists():
        return None
    
    try:
        backup_dir = output_path.parent / Config.BACKUP_DIR
        backup_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{output_path.stem}_backup_{timestamp}{output_path.suffix}"
        backup_path = backup_dir / backup_name
        
        # Copia file
        import shutil
        shutil.copy2(output_path, backup_path)
        
        logger.info(f"Backup creato: {backup_path}")
        
        # Cleanup vecchi backup
        cleanup_old_backups(backup_dir, output_path.stem, logger)
        
        return backup_path
        
    except Exception as e:
        logger.warning(f"Impossibile creare backup: {e}")
        return None

def cleanup_old_backups(backup_dir: Path, file_prefix: str, logger):
    """Rimuove backup vecchi oltre il limite configurato."""
    try:
        pattern = f"{file_prefix}_backup_*.xlsx"
        backup_files = sorted(backup_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
        
        if len(backup_files) > Config.MAX_BACKUPS:
            for old_backup in backup_files[Config.MAX_BACKUPS:]:
                old_backup.unlink()
                logger.debug(f"Rimosso backup vecchio: {old_backup.name}")
                
    except Exception as e:
        logger.warning(f"Errore cleanup backup: {e}")

def print_summary(stats: dict, output_path: str, logger):
    """Stampa riepilogo finale."""
    print(f"\n{'=' * 60}")
    print(f"✅ ELABORAZIONE COMPLETATA!")
    print(f"{'=' * 60}")
    print(f"📁 File generato: {output_path}")
    print(f"")
    print(f"📊 STATISTICHE:")
    print(f"   • IMEI totali elaborati: {stats['total_records']:,}")
    print(f"   • IMEI con match TI: {stats['matched']:,}")
    print(f"   • Solo post vendita: {stats['post_vendita_only']:,}")
    print(f"   • Solo TI: {stats['ti_only']:,}")
    print(f"")
    print(f"💰 DIFFERENZE:")
    print(f"   • Totale: EUR {stats['total_difference']:,.2f}")
    print(f"   • Media: EUR {stats['avg_difference']:,.2f}")
    print(f"   • Min: EUR {stats['min_difference']:,.2f}")
    print(f"   • Max: EUR {stats['max_difference']:,.2f}")
    print(f"")
    
    # Breakdown se disponibile
    if 'financial_breakdown' in stats:
        print(f"🏦 BREAKDOWN FINANZIARIE:")
        for fin_type, data in stats['financial_breakdown'].items():
            if data['count'] > 0:
                print(f"   • {fin_type}: {data['count']} record, EUR {data['total_diff']:,.2f}")
        print(f"")
    
    print(f"📝 Log dettagli: var_processor.log")
    print(f"{'=' * 60}")

def main():
    """Funzione principale."""
    try:
        # Setup iniziale
        logger = setup_environment()
        args = parse_arguments()
        
        # Configura logging finale
        logger = configure_logging_level(args)
        
        # Valida input
        input_dir = Path(args.input).resolve()
        if not validate_input_directory(input_dir, logger):
            return 1
        
        # Solo validazione se richiesto
        if args.validate_only:
            logger.info("Validazione completata con successo")
            print("✅ Validazione completata - tutti i file necessari sono presenti")
            return 0
        
        # Determina output filename
        output_filename = args.output or Config.get_output_filename()
        output_path = input_dir / output_filename
        
        # Backup se necessario
        if Config.ENABLE_BACKUP and not args.no_backup:
            create_backup(output_path, logger)
        
        # Elaborazione principale
        logger.info("Inizio elaborazione VAR workflow...")
        
        processor = VARProcessor(str(input_dir))
        result_path = processor.run(output_filename)
        
        # Calcola statistiche avanzate
        output_records = processor.get_output_records()
        stats = StatisticsCalculator.calculate_summary(output_records)
        stats['financial_breakdown'] = StatisticsCalculator.calculate_financial_breakdown(output_records)
        stats['causale_breakdown'] = StatisticsCalculator.calculate_causale_breakdown(output_records)
        
        # Riepilogo finale
        print_summary(stats, result_path, logger)
        
        logger.info("Elaborazione completata con successo")
        return 0
        
    except KeyboardInterrupt:
        print("\n❌ Elaborazione interrotta dall'utente")
        return 1
    except Exception as e:
        logger.error(f"Errore critico: {e}")
        print(f"\n❌ ERRORE: {e}")
        print("📝 Controlla var_processor.log per dettagli completi")
        return 1

if __name__ == "__main__":
    sys.exit(main())
