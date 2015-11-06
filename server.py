import json, pyhdb, os, sys

from flask import Flask, jsonify, Response, request
from flask.ext.cors import CORS


static_folder = "static"
if len(sys.argv) >= 2:
    static_folder = sys.argv[1]

app = Flask(__name__, static_folder=static_folder)
CORS(app, supports_credentials=True)

SERVER_ROOT = os.path.dirname(os.path.realpath(__file__))
connection = None

def init():
    global connection

    with open(SERVER_ROOT + "/secrets.json") as f:
        secrets = json.load(f)

    connection = pyhdb.connect(
        host=secrets['host'],
        port=secrets['port'],
        user=secrets['username'],
        password=secrets['password']
    )

    app.run(port=8080, debug=True)


@app.route('/documents')
def get_documents():
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM LEARNING_TO_NOTE.DOCUMENTS")
    documents = list()
    for result in cursor.fetchall():
        documents.append(result[0])
    cursor.close()
    return respond_with(documents)


@app.route('/documents/<document_id>', methods=['GET', 'POST'])
def get_document(document_id):
    if request.method == 'GET':
        return load_document(document_id)
    else:
        save_document(document_id, request.get_json())
        return ""

def save_document(document_id, data):
    print "Received so much stuff: " + str(data)
    cursor = connection.cursor()
    save_document_text(document_id, data['text'], cursor)
    save_annotations(document_id, data['denotations'], cursor)
    save_relations(document_id, data['relations'], cursor)
    connection.commit()

def save_document_text(document_id, text, cursor):
    # cursor.execute("UPDATE LEARNING_TO_NOTE.DOCUMENTS SET TEXT = ? WHERE ID = ?", (text, document_id))
    # cursor.execute("INSERT OR IGNORE INTO LEARNING_TO_NOTE.DOCUMENTS VALUES (?, ?)", (document_id, text))
    cursor.execute("UPSERT LEARNING_TO_NOTE.DOCUMENTS VALUES (?, ?) WHERE id = ?", (document_id, text, document_id))

def save_annotations(document_id, annotations, cursor):
    existing_entities = []
    created_entities = []
    for annotation in annotations:
        cursor.execute("SELECT ID FROM LEARNING_TO_NOTE.ENTITIES WHERE TEXT = ? AND TYPE = ?",
                        (annotation['obj'], annotation['type']))
        result = cursor.fetchone()
        entity = {}
        entity['text'] = annotation['obj']
        entity['type'] = annotation['type']
        if not result is None:
            entity['id'] = result
            existing_entities.append(entity)
        else:
            #entity['id'] = generate_new_entity_id()
            created_entities.append(entity)
    entity_tuples = map(lambda entity: (entity['text'], entity['type']), created_entities)
    cursor.executemany("INSERT INTO LEARNING_TO_NOTE.ENTITIES (TEXT, TYPE) VALUES (%s, %s)", entity_tuples)
    cursor.execute("DELETE FROM LEARNING_TO_NOTE.DOC_ENTITIES WHERE DOC_ID = ?", document_id)
    doc_entity_tuples = map(lambda entity: (document_id, entity['id'], entity['span']['begin'], entity['span']['end']) ,
                            existing_entities + created_entities)
    cursor.executemany("INSERT INTO LEARNING_TO_NOTE.DOC_ENTITIES (DOC_ID, ENTITY_ID, START, END) \
                        VALUES (%s, %s, %s, %s)", doc_entity_tuples)

def save_relations(document_id, relations, cursor):
    relation_tuples = map(lambda relation: (relation['subj'], relation['obj'], relation['ddi'], relation['pred']), relations)
    cursor.executemany("INSERT INTO LEARNING_TO_NOTE.PAIRS (E1_ID, E2_ID, DDI, TYPE) VALUES (%s, %s, %s, %s)",
                        relation_tuples)

def load_document(document_id):
    cursor = connection.cursor()
    result = {}
    result['text'] = get_text(cursor, document_id)
    result['denotations'] = get_denotations(cursor, document_id)
    result['relations'] = get_relations(cursor, document_id)
    result['sourceid'] = document_id
    cursor.close()
    return respond_with(result)

def get_text(cursor, document_id):
    cursor.execute("SELECT TEXT FROM LEARNING_TO_NOTE.DOCUMENTS WHERE ID = ?", (document_id,))
    text = str(cursor.fetchone()[0].read())
    return text

def get_denotations(cursor, document_id):
    cursor.execute('SELECT E.ID, E."TYPE", O."START", O."END" FROM LEARNING_TO_NOTE.ENTITIES E \
                    JOIN LEARNING_TO_NOTE.OFFSETS O ON O.ENTITY_ID = E.ID AND E.DOC_ID = ?', (document_id,))
    denotations = []
    for result in cursor.fetchall():
        denotation = {}
        denotation['id'] = str(result[0])
        denotation['span'] = {}
        denotation['span']['begin'] = str(result[2])
        denotation['span']['end'] = str(result[3])
        denotation['obj'] = str(result[1])
        denotations.append(denotation)
    return denotations

def get_relations(cursor, document_id):
    cursor.execute("SELECT PAIRS.ID, E1_ID, E2_ID, TYPE FROM LEARNING_TO_NOTE.PAIRS \
        JOIN LEARNING_TO_NOTE.ENTITIES E1 ON PAIRS.E1_ID = E1.ID AND E1.DOC_ID = ? AND PAIRS.DDI = 1\
        JOIN LEARNING_TO_NOTE.ENTITIES E2 ON PAIRS.E2_ID = E2.ID AND E2.DOC_ID = ? AND PAIRS.DDI = 1", (document_id, document_id,))
    relations = []
    for result in cursor.fetchall():
        relation = {}
        relation['id'] = str(result[0])
        relation['subj'] = str(result[1])
        relation['obj'] = str(result[2])
        relation['pred'] = str(result[3])
        relations.append(relation)
    return relations


def respond_with(response):
    return Response(json.dumps(response), mimetype='application/json')

if __name__ == '__main__':
    init()
