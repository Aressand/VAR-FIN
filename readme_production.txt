# VAR Workflow Processor - Production Version

**Versione:** 2.0.0 Production Ready  
**Autore:** VAR Workflow Team  
**Data:** 2025

## 📋 Descrizione

Sistema di elaborazione automatica per il workflow VAR (Vendite A Rate) che processa file Excel di rimborsi terminali, telefono incluso e dati finanziari per generare report consolidati.

## 🚀 Caratteristiche Production

- ✅ **Architettura modulare** - Codice organizzato e manutenibile
- ✅ **Logging professionale** - Tracciamento completo delle operazioni
- ✅ **Gestione errori robusta** - Recupero automatico da errori non critici
- ✅ **Progress tracking** - Barre di progresso per file grandi
- ✅ **Backup automatico** - Salvataggio versioni precedenti
- ✅ **Configurazione centralizzata** - Tutte le impostazioni in un posto
- ✅ **Validazioni complete** - Controlli di integrità dati
- ✅ **Performance ottimizzate** - Gestione efficiente memoria
- ✅ **CLI avanzata** - Interfaccia linea di comando professionale

## 📦 Installazione

### Prerequisiti
```bash
Python 3.8+ 
pip (package manager)
```

### Setup Veloce
```bash
# 1. Installa dipendenze
pip install -r requirements.txt

# 2. Verifica installazione
python main.py --validate-only

# 3. Prima esecuzione
python main.py
```

### Setup Avanzato
```bash
# Per sviluppatori - installazione completa
pip install -r requirements.txt
pip install pytest black mypy  # Tools sviluppo

# Setup ambiente sviluppo
python -m pytest tests/  # Esegui test
black --check .           # Verifica formattazione
mypy .                    # Type checking
```

## 🎯 Utilizzo

### Utilizzo Base
```bash
# Elaborazione standard (directory corrente)
python main.py

# Directory personalizzata
python main.py --input /path/to/files

# Nome output personalizzato
python main.py --output "Report_Gennaio_2025.xlsx"
```

### Opzioni Avanzate
```bash
# Output dettagliato (debug)
python main.py --verbose

# Output minimo (solo errori)
python main.py --quiet

# Solo validazione file
python main.py --validate-only

# Disabilita backup automatico
python main.py --no-backup

# Aiuto completo
python main.py --help
```

### Esempi Pratici
```bash
# Elaborazione mensile
python main.py --input ./dati_maggio --output "VAR_Maggio_2025.xlsx" --verbose

# Validazione rapida
python main.py --validate-only --input ./nuovi_dati

# Elaborazione silenziosa per automazione
python main.py --quiet --no-backup > elaborazione.log 2>&1
```

## 📁 Struttura File

### File Richiesti
```
directory_input/
├── post_vendita_fisici.xlsx          # Obbligatorio - Vendite principale
├── telefono_incluso_*.xlsx           # Obbligatorio - Uno o più file TI
└── data.xlsx                         # Opzionale - Dati finanziari
```

### File Generati
```
directory_output/
├── VAR_Report_YYYYMMDD_HHMMSS.xlsx  # Report principale
├── var_processor.log                 # Log dettagliato
└── backup/                           # Backup automatici
    ├── VAR_Report_backup_*.xlsx
    └── ...
```

## 📊 Struttura Output Excel

Il file Excel generato contiene le seguenti colonne:

| Colonna | Fonte | Descrizione |
|---------|-------|-------------|
| IMEI | Post Vendita | Identificativo univoco terminale |
| Ragione sociale Dealer | TI | Nome dealer |
| Codice POS | TI | Codice punto vendita |
| Numero Nota di Credito | TI | Numero documento |
| Causale | TI | TEL_INCLUSO o PROMOCASH |
| Punto vendita | Post Vendita | Nome punto vendita |
| ID Vendita | Post Vendita | Identificativo vendita |
| Cliente | Post Vendita | Nome cliente |
| Data Scarico | Post Vendita | Data operazione |
| Modalita vendita | Post Vendita | Tipo vendita |
| POS-Finanziato | Data.xlsx | Codice finanziaria |
| FINANZIARIA | Data.xlsx | FINDOMESTIC/COMPASS |
| Id Pratica | Data.xlsx | Identificativo pratica |
| Tipo Finanz | Data.xlsx | Tipo finanziamento |
| N° L.d.C. o N° Prat. Findomestic | Data.xlsx | Numero pratica |
| Stato Prat | Data.xlsx | Stato pratica |
| W-Importo Originale | TI | Importo originale (Wind) |
| W-Finanziato Wind | Data.xlsx | Importo finanziato Wind |
| B-IMPORTO CREDITO | Post Vendita | Importo credito base |
| B-IMPORTO NDC | Post Vendita | Importo NDC base |
| B-IMPORTO FINANZIATO | Post Vendita | Importo finanziato base |
| **Differenza** | Calcolato | **Risultato finale** |

## 🧮 Logica Calcolo Differenza

La colonna **Differenza** viene calcolata con questa priorità:

1. **FINANZIARIA = "FINDOMESTIC" o "COMPASS"**  
   `Differenza = W-Finanziato Wind - B-IMPORTO FINANZIATO`

2. **Causale = "TEL_INCLUSO"**  
   `Differenza = W-Importo Originale - B-IMPORTO CREDITO`

3. **Causale = "PROMOCASH"**  
   `Differenza = W-Importo Originale - B-IMPORTO NDC`

4. **DEFAULT**  
   `Differenza = W-Importo Originale - B-IMPORTO CREDITO`

## 🔧 Configurazione

Le configurazioni si trovano in `config.py`:

```python
# Modifica pattern file
POST_VENDITA_PATTERNS = ["post_vendita_fisici.xls", "post_vendita_fisici.xlsx"]
TI_PATTERN = "telefono_incluso_*.xlsx"

# Configurazione logging
LOG_LEVEL = logging.INFO  # DEBUG, INFO, WARNING, ERROR

# Configurazione backup
ENABLE_BACKUP = True
MAX_BACKUPS = 5

# Performance per file grandi
CHUNK_SIZE = 1000
MAX_MEMORY_ROWS = 10000
```

## 📝 Logging

Il sistema genera log dettagliati in `var_processor.log`:

```
2025-05-26 14:30:15 - VAR Workflow Processor - INFO - Inizializzato VAR Processor
2025-05-26 14:30:16 - VAR Workflow Processor - INFO - File post vendita: post_vendita_fisici.xlsx
2025-05-26 14:30:17 - VAR Workflow Processor - INFO - IMEI post vendita: 309/311 validi
2025-05-26 14:30:18 - VAR Workflow Processor - INFO - TI combinati: 166 record validi
2025-05-26 14:30:19 - VAR Workflow Processor - INFO - Dati finanziari caricati: 144 IMEI
2025-05-26 14:30:20 - VAR Workflow Processor - INFO - Matching completato: 321 IMEI totali
```

## ⚡ Performance

### File Piccoli (< 1000 righe)
- Tempo elaborazione: < 30 secondi
- Memoria utilizzo: < 100 MB

### File Medi (1000-10000 righe)
- Tempo elaborazione: 1-3 minuti
- Memoria utilizzo: < 500 MB
- Progress bar automatica

### File Grandi (> 10000 righe)
- Elaborazione chunked automatica
- Progress bar dettagliato
- Ottimizzazioni memoria

## 🛠️ Troubleshooting

### Errori Comuni

**File non trovato:**
```bash
python main.py --validate-only  # Verifica presenza file
```

**IMEI non validi:**
```
# Nel log apparirà:
WARN - IMEI validation: 305/311 validi, 6 invalidi
```
*Soluzione: Verifica formato IMEI (15 cifre esatte)*

**Memoria insufficiente:**
```python
# In config.py ridurre:
MAX_MEMORY_ROWS = 5000
CHUNK_SIZE = 500
```

**Errori encoding:**
```bash
# Assicurarsi che i file Excel non siano corrotti
python main.py --verbose  # Per log dettagliato
```

### Debug Avanzato

```bash
# Log completo debug
python main.py --verbose 2>&1 | tee debug.log

# Solo errori
python main.py --quiet 2> errori.log

# Test performance
time python main.py --input ./test_data
```

## 🔄 Automazione

### Script Batch (Windows)
```batch
@echo off
cd /d "C:\VAR_Processor"
python main.py --input "\\server\dati\var" --output "VAR_%date:~-4,4%%date:~-10,2%%date:~-7,2%.xlsx" --quiet
if %errorlevel% equ 0 (
    echo Elaborazione completata con successo
) else (
    echo Errore durante elaborazione
)
```

### Cron Job (Linux/Mac)
```bash
# Ogni giorno alle 08:00
0 8 * * * cd /home/user/var_processor && python main.py --input /data/var --quiet
```

### Task Scheduler (Windows)
1. Apri "Utilità di pianificazione"
2. Crea attività di base
3. Trigger: Giornaliero ore 08:00
4. Azione: `python.exe main.py --input C:\dati\var --quiet`

## 📞 Supporto

### Informazioni di Debug
Prima di segnalare problemi, raccogli queste informazioni:

```bash
# Versione Python e sistema
python --version
python main.py --help | head -5

# Log dell'errore
python main.py --verbose > debug.log 2>&1

# Dimensioni file
ls -la *.xlsx
```

### File di Log
- `var_processor.log` - Log principale
- `debug.log` - Log debug completo (se generato)
- File Excel di output per verifica risultati

---

**VAR Workflow Processor v2.0.0 - Production Ready**  
*Sistema robusto e scalabile per l'elaborazione VAR automatizzata*
