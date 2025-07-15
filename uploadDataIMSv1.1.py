##importér biblioteker
from datetime import datetime
import os
import pyodbc
import logging
import pandas as pd
import sqlite3 ##bruges til test
from connectDB import connectDB

##angiv logindstillinger
def log():
    log = logging.getLogger('LogIMS')
    log.setLevel(logging.DEBUG)

    logHandle = logging.FileHandler(
        'Python_IMS_HistoriskData.log',
        encoding="utf-8",
        mode="a"
        )

    logFormat = logging.Formatter(
        "{asctime} - {levelname} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M:%S"
        )

    logHandle.setFormatter(logFormat)
    log.addHandler(logHandle)
    return log
logger = log()

##angiv test- eller prod-miljø
Prod = input('Angiv prod [bool]: ')
conn, cur, boolProd, server, database, table = connectDB(Prod, logger)

##hent og log PID og bruger
strUser =  os.getlogin() 
strJobId = 'P' + str(os.getpid())
logger.info(f'Bruger: {strUser}; jobid: {strJobId}')

##åbn fil fra IMS, læs og log antal rækker
strFileHandle = open(input('Angiv den fil, der skal uploades: '))
intRowCount = sum(1 for _ in strFileHandle)
strFileHandle.seek(0)
logger.info(f'Antal rækker i fil: {intRowCount}')
print('Rapport læst')

##angiv størrelse på uploadbatch
batchStørrelse = 1000
listBatch = []

##forsøg at uploade data
try:
    print('Starter upload')
    logger.info('Starter upload')
    logger.info('Upload starttid: ' + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    next(strFileHandle)
    ##gennemgå csv, split linjer og angiv variabler
    for index, line in enumerate(strFileHandle):
            listData = line.split(';')
            
            strTidspunkt = listData[0]
            strFilial = listData[1] 
            strHandlingstype = listData[2] 
            strTidligerefilial = listData[3]
            strTidligereplacering = listData[4]
            strNyfilial = listData[5]
            strNyplacering = listData[6]
            strMateriale_ID = listData[7]
            strFAUST_nr	= listData[8]
            strTitel = listData[9]
            strForfatter = listData[10]
            strFlydkode	= listData[11]
            strFastfilial = listData[12]
            strAfdelingstype = listData[13]
            strAfdeling	= listData[14]
            strOpstilling = listData[15]
            strDelopstilling = listData[16]
            strMaterialegruppe = listData[17]
            strMaterialetype = listData[18]
            strKlassifikation = listData[19]
            strAlfabetisering = listData[20]
            strFjernlån	= listData[21]
            strPeriodika_nummer	= listData[22]
            strPeriodika_volume	= listData[23]
            strPeriodika_år	= listData[24]

            ##define timestamp variables
            ##sqlite3 kan i python 3.12 ikke håndtere datetime variabler; disse sættes derfor til strenge hvis test
            if boolProd:
                oprettet_tidspunkt = datetime.now()
                opdateret_sidst = datetime.now()
            else:
                oprettet_tidspunkt = str(datetime.now())
                opdateret_sidst = str(datetime.now())

            ##læg variabler i batch til upload                           
            listBatch.append((strTidspunkt, strFilial, strHandlingstype, strTidligerefilial, strTidligereplacering, strNyfilial,
                      strNyplacering, strMateriale_ID, strFAUST_nr, strTitel, strForfatter, strFlydkode, strFastfilial,
                      strAfdelingstype, strAfdeling, strOpstilling, strDelopstilling, strMaterialegruppe, strMaterialetype,
                      strKlassifikation, strAlfabetisering, strFjernlån, strPeriodika_nummer, strPeriodika_volume, strPeriodika_år,
                      strUser, oprettet_tidspunkt, strUser, opdateret_sidst, 1, strJobId))

            if len(listBatch) == batchStørrelse:
                cur.executemany(f'''insert into {table} 
                                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                    listBatch)
                listBatch = []

            ##beregn færdiggørelsesprocent
            flProcentFærdig = (index / intRowCount) * 100
            print(f"{flProcentFærdig:.3f}% færdig", end='\r')

    ##Indsæt resterende rækker
    if listBatch:
        print('Indsætter resterende data')
        cur.executemany(f'''insert into {table} 
                        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                        listBatch)
    
    strFileHandle.close()        
    boolCommit = True
    print('Upload af fil fuldført - gemmer data')

##fang evt. fejl og log
except Exception as E:
    logger.error(f'Der opstod en fejl ifm. upload til DB: {E}')
    print(f'Fejl ifm. upload: {E}')
    boolCommit = False

##upload hvis ingen fejl
if boolCommit:
    conn.commit()
    print('Upload fuldført')
    logger.info('Upload fuldført')
    logger.info('Upload sluttid: ' + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
else:
    logger.error('Fejl ifm. upload')

print('Lukker forbindelse til DB')
conn.close()