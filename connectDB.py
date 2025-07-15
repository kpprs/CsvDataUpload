import pyodbc
import sqlite3

##funktio til at forbinde til DB
def connectDB(Prod, logger):
    if Prod.lower() == 'true':
        boolProd = True
    elif Prod.lower() == 'false':
        boolProd = False
    else:
        print('Invalid input')
        raise ValueError

    ##forbind til prod eller test
    if boolProd:
        ##opret forbindelse til Prod-DB
        logger.info('Forbinder til Prod-DB')
        server = r'fiskk2003-prod.sql.kk.dk\instans10'
        database = 'FISKK2003-KFF_RobotSQL'
        table = 'dbo.[KFF.BoernogData.IMS]' 
        conn_str = (
            'DRIVER=SQL Server;'
            f'SERVER={server};'
            f'DATABASE={database};'
            'Trusted_Connection=yes;'
            )
        conn = pyodbc.connect(conn_str)   
        cur = conn.cursor()
    else:
        ##opret forbindelse til test-DB
        logger.info('Forbinder til test-DB')
        server = None
        table = 'IntMat'
        database = 'Python_IMS_SQL.sqlite'
        conn = sqlite3.connect(database)
        cur = conn.cursor()

        ##opret en ren tabel med executescript()
        cur.execute('DROP TABLE IF EXISTS IntMat;')
        cur.executescript('''
        CREATE TABLE IntMAt (
            Tidspunkt 	        TEXT,
            Filial 	            TEXT,
            Handlingstype 	    TEXT,
            Tidligere_filial 	TEXT,
            Tidligere_placering TEXT,
            Ny_filial 	        TEXT,
            Ny_placering 	    TEXT,
            Materiale_ID 	    TEXT,
            FAUST_nr 	        TEXT,
            Titel 	            TEXT,
            Forfatter 	        TEXT,
            Flydkode 	        TEXT,
            FasT_filial 	    TEXT,
            Afdelingstype 	    TEXT,
            Afdeling 	        TEXT,
            Opstilling 	        TEXT,
            Delopstilling 	    TEXT,
            Materialegruppe 	TEXT,
            Materialetype 	    TEXT,
            Klassifikation 	    TEXT,
            Alfabetisering 	    TEXT,
            Fjernlån 	        TEXT,
            Periodika_nummer 	TEXT,
            Periodika_volume 	TEXT,
            Periodika_år 	    TEXT,
            oprettet_af         TEXT,
            oprettet_tidspunkt  DATETIME,
            opdateret_af        TEXT,
            opdateret_sidst     DATETIME,
            aktiv               INTEGER,
            jobid               TEXT
        );
        ''')
    
    ##log afhængigt af miljø
    if conn is None and (boolProd is False):
        logger.critical(f'Kunne ikke forbinde til {database}')
        raise Exception('Ingen forbindelse til test-miljø; afbryder')
    elif conn is None:
        logger.critical(f'Kunne ikke forbinde til {server}')
        raise Exception('Ingen forbindelse til Prod-miljø; afbryder')
    elif conn and (boolProd is False):
        logger.info(f'Forbindelse til {database} etableret')
        print('Forbundet til test-DB')
    elif conn and (boolProd is True):
        logger.info(f'Forbindelse til {table} etableret')
        print(f'Forbundet til DB {database, table}')
    
    return conn, cur, boolProd, server, database, table