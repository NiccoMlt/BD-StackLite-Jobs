# BD-StackLite-Jobs

Elaborato di progetto per l'esame Big Data, anno 2018-2019.

## Requisiti

Il progetto è pensato per studiare il contenuto del dataset [StackLite](https://www.kaggle.com/stackoverflow/stacklite).

Sono stati individuati due job:

  - effettuare la proporzione tra post realizzati in giorni feriali e in giorni festivi per ciascun tag
    - ordinare i risultati per valore della proporzione e per quantità di post
  
  - suddividere in 4 bin i dati in base a valori arbitrari su score e risposte: i 4 bin saranno
	- score basso, numero di risposte basso
	- score basso, numero di risposte alto
	- score alto, numero di risposte basso
	- score alto, numero di risposte alto

    Per ognuna delle 4 coppie score-risposte i risultati saranno i primi 10 tag in ordine di score e numero di risposte.
  
  - [...]

## Team members

  - Niccolò Maltoni: [niccolo.maltoni@studio.unibo.it](mailto:niccolo.maltoni@studio.unibo.it)
  - Luca Semprini: [luca.semprini10@studio.unibo.it](mailto:luca.semprini10@studio.unibo.it)

## Software versions

Questo progetto utilizza le seguenti versioni delle piattaforme di sviluppo su cui si appoggia:

- **Hadoop** version: **2.6.0-cdh5.13.1**
- **Spark** version: **2.1.0.cloudera2**
- **Scala** version: **2.11.8**
- **JVM** version: **1.7.0_67**

## Scripts

La cartella [./scripts](https://github.com/NiccoMlt/BigData-18-19-scripts) è un Git submodule contenente gli script per facilitare la connessione remota.

Clonare il repo con `git clone --recursive` invece che con `git clone` per scaricare anch gli script bash.

