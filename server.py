import json, pyhdb, os, sys

from flask import Flask, jsonify, Response
from flask.ext.cors import CORS


static_folder = "static"
if len(sys.argv) >= 2:
    static_folder = sys.argv[1]

app = Flask(__name__, static_folder=static_folder)
CORS(app)

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


@app.route('/documents/<document_id>')
def get_document(document_id):
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
    text = str(cursor.fetchone()[0])
    return text

def get_denotations(cursor, document_id):
    cursor.execute('SELECT E.ID, E."TEXT", E."TYPE", DE."START", DE."END" FROM LEARNING_TO_NOTE.DOC_ENTITIES DE \
        JOIN LEARNING_TO_NOTE.ENTITIES E ON DE.ENTITY_ID = E.ID AND DE.DOC_ID = ?', (document_id,))
    denotations = []
    for result in cursor.fetchall():
        denotation = {}
        denotation['id'] = str(result[0])
        denotation['span'] = {}
        denotation['span']['begin'] = str(result[3])
        denotation['span']['end'] = str(result[4])
        denotation['obj'] = str(result[1])
        denotation['type'] = str(result[2])
        denotations.append(denotation)
    return denotations

def get_relations(cursor, document_id):
    cursor.execute("SELECT PAIRS.ID, E1_ID, E2_ID, TYPE, DDI FROM LEARNING_TO_NOTE.PAIRS \
        JOIN LEARNING_TO_NOTE.DOC_ENTITIES DE1 ON PAIRS.E1_ID = DE1.ID AND DE1.DOC_ID = ? \
        JOIN LEARNING_TO_NOTE.DOC_ENTITIES DE2 ON PAIRS.E2_ID = DE2.ID AND DE2.DOC_ID = ?", (document_id, document_id,))
    relations = []
    for result in cursor.fetchall():
        relation = {}
        relation['id'] = str(result[0])
        relation['pred'] = str(result[3])
        relation['subj'] = str(result[1])
        relation['obj'] = str(result[2])
        relations.append(relation)
    return relations

@app.route('/sentences')
def get_sentences():
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM LEARNING_TO_NOTE.SENTENCE")
    sentences = list()
    for result in cursor.fetchall():
        sentences.append(result[0])
    cursor.close()
    return respond_with(sentences)


@app.route('/sentences/<sentence_id>')
def get_sentence(sentence_id):
    cursor = connection.cursor()
    cursor.execute("SELECT TEXT FROM LEARNING_TO_NOTE.SENTENCE WHERE SENTENCE.ID = ?", (sentence_id,))
    sentence = cursor.fetchone()[0]
    cursor.close()
    return respond_with(str(sentence))

@app.route('/sentences/<sentence_id>/entities')
def get_sentence_entities(sentence_id):
    cursor = connection.cursor()
    cursor.execute(
    'SELECT ENTITY.TEXT, OFFSET."START", OFFSET."END" '
    'FROM LEARNING_TO_NOTE.ENTITY, LEARNING_TO_NOTE.OFFSET '
    'WHERE ENTITY.ID = OFFSET.ENTITY_ID '
    'AND ENTITY.SENTENCE_ID = ?', (sentence_id,))

    entities = list()
    for result in cursor.fetchall():
        entities.append({"span":{"begin":result[1],"end":result[2]},"obj":result[0]})

    cursor.close()
    return respond_with(entities)

def respond_with(response):
    return Response(json.dumps(response), mimetype='application/json')

if __name__ == '__main__':
    init()
