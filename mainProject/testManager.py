import dbManager

dbmanager = dbManager.dbManager()

index = "forensic_db"
doc_type = "prova inserimento"
body = {
    "filePath": "/home/prova",
    "size": "123456"
}

dbmanager.push(index,doc_type,body)