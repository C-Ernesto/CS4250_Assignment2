#-------------------------------------------------------------------------
# AUTHOR: Christopher Ernesto
# FILENAME: db_connection.py
# SPECIFICATION: Creating an inverted index using python
# FOR: CS 4250- Assignment #2
# TIME SPENT: 3 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import psycopg2
from psycopg2.extras import RealDictCursor

def connectDataBase():

    # Create a database connection object using psycopg2
    DB_NAME = "corpus"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=RealDictCursor)

        cur = conn.cursor()
        createTables(cur, conn)

        return conn

    except:
        print("Database not connected successfully")


def createTables(cur, conn):
    try:
        # terms
        sql = "CREATE TABLE IF NOT EXISTS public.terms(" \
                "term text NOT NULL," \
                "num_chars integer NOT NULL," \
                "CONSTRAINT terms_pkey PRIMARY KEY (term))"
        cur.execute(sql)

        # categories
        sql = "CREATE TABLE IF NOT EXISTS public.categories(" \
                "id_cat integer NOT NULL," \
                "name text NOT NULL," \
                "CONSTRAINT categories_pkey PRIMARY KEY (id_cat))"
        cur.execute(sql)

        # documents
        sql = "CREATE TABLE IF NOT EXISTS public.documents(" \
              "doc integer NOT NULL," \
              "text text NOT NULL," \
              "title text NOT NULL," \
              "num_chars integer NOT NULL," \
              "date date NOT NULL," \
              "id_cat integer NOT NULL," \
              "CONSTRAINT documents_pkey PRIMARY KEY (doc)," \
              "CONSTRAINT fk FOREIGN KEY (id_cat)" \
              "REFERENCES public.categories (id_cat) MATCH SIMPLE " \
              "ON UPDATE NO ACTION " \
              "ON DELETE NO ACTION)"
        cur.execute(sql)

        # index
        sql = "CREATE TABLE IF NOT EXISTS public.index(" \
                "term text NOT NULL," \
                "doc integer NOT NULL," \
                "term_count integer NOT NULL," \
                "CONSTRAINT index_pkey PRIMARY KEY (term, doc)," \
                "CONSTRAINT doc_fk FOREIGN KEY (doc)" \
                    "REFERENCES public.documents (doc) MATCH SIMPLE " \
                    "ON UPDATE NO ACTION " \
                    "ON DELETE NO ACTION," \
                "CONSTRAINT term_fk FOREIGN KEY (term)" \
                    "REFERENCES public.terms (term) MATCH SIMPLE " \
                    "ON UPDATE NO ACTION " \
                    "ON DELETE NO ACTION)"
        cur.execute(sql)

        conn.commit()

    except Exception as e:

        conn.rollback()
        # print(e)
        print ("There was a problem during the database creation or the database already exists.")


def createCategory(cur, catId, catName):

    # Insert a category in the database
    sql = "INSERT INTO categories(id_cat, name) VALUES(%s, %s)"

    val = [catId, catName]
    cur.execute(sql, val)

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    sql = "SELECT id_cat FROM categories WHERE name=%(catName)s"

    cur.execute(sql, {'catName': docCat})

    recset = cur.fetchall()
    category = recset[0]['id_cat']

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    copyText = docText.replace(" ", "")
    punc = ".!:;,?"
    for ele in punc:
        if ele in copyText:
            copyText = copyText.replace(ele, "")
    nChars = len(copyText)
    sql = "INSERT INTO documents(doc, text, title, num_chars, date, id_cat) VALUES(%(id)s, %(text)s, %(title)s, " \
           "%(numChar)s, %(date)s, %(idCat)s)"

    val = {'id': docId,
           'text': docText,
           'title': docTitle,
           'numChar': nChars,
           'date': docDate,
           'idCat': category}

    cur.execute(sql, val)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    copyText = docText.lower()
    for ele in punc:
        if ele in copyText:
            copyText = copyText.replace(ele, "")

    terms = copyText.split(" ")

    # check if terms exist
    for ele in terms:
        cur.execute("SELECT * FROM terms WHERE term=%(term)s", {'term': ele})
        recset = cur.fetchall()
        if not recset:
            cur.execute("INSERT INTO terms(term, num_chars) VALUES(%(term)s, %(numChar)s)", {'term': ele, 'numChar':len(ele)})

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    termCount = {}
    for ele in terms:
        if ele not in termCount:
            termCount[ele] = terms.count(ele)

    for ele in termCount:
        cur.execute("INSERT INTO index(term, doc, term_count) VALUES(%(term)s, %(id)s, %(count)s)",
                    {'term': ele, 'id': docId, 'count': termCount[ele]})

def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    sql = 'SELECT term FROM index WHERE doc=%(docid)s'
    cur.execute(sql, {'docid': docId})
    recset = cur.fetchall()

    for ele in recset:
        term = ele['term']
        sql = 'DELETE FROM index WHERE term=%(term)s AND doc=%(docid)s'
        cur.execute(sql, {'term': term, 'docid': docId})

        # check if no more occurrences
        sql = 'SELECT * FROM index WHERE term = %(term)s'
        cur.execute(sql, {'term': term})

        occurrence = cur.fetchall()

        if not occurrence:
            cur.execute('DELETE FROM terms WHERE term=%(term)s', {'term': term})

    # 2 Delete the document from the database
    cur.execute('DELETE FROM documents WHERE doc=%(docid)s', {'docid': docId})

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    sql = 'SELECT index.term, documents.title, index.term_count FROM index INNER JOIN documents ON index.doc = documents.doc ORDER BY index.term ASC'

    cur.execute(sql)
    recset = cur.fetchall()

    # index = ""
    index = {}

    for row in recset:
        docs = "%s:%s" % (row['title'], row['term_count'])
        if row['term'] in index:
            val = index[row['term']] + ", " + docs
            index[row['term']] = val
        else:
            index[row['term']] = docs

    return index
