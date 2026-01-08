from src.connection import postgre_connection

def getData():
    conn, cur = postgre_connection()

    getAll = "SELECT * FROM legal_document"
    cur.execute(getAll)
    results = cur.fetchall()
    allDocuments = []

    for result in results:
        allDocuments.append(result)

    cur.close()
    conn.close()

    return allDocuments