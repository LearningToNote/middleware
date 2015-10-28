import json, pyhdb, os

from flask import Flask, jsonify, Response
from flask.ext.cors import CORS


app = Flask(__name__)
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
    cursor.execute("SELECT * FROM MP12015.DOCUMENT")
    documents = list()
    for result in cursor.fetchall():
        documents.append(result[0])
    cursor.close()
    return respond_with(documents)


@app.route('/documents/<id>')
def get_document(id):
    cursor = connection.cursor()
    cursor.execute("SELECT TEXT FROM MP12015.SENTENCE WHERE DOCUMENT_ID = ?", (id,))
    sentences = list()
    for result in cursor.fetchall():
        sentences.append(str(result[0]))
    cursor.close()
    return respond_with(" ".join(sentences))


@app.route('/sentences')
def get_sentences():
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM MP12015.SENTENCE")
    sentences = list()
    for result in cursor.fetchall():
        sentences.append(result[0])
    cursor.close()
    return respond_with(sentences)


@app.route('/sentences/<sentence_id>')
def get_sentence(sentence_id):
    cursor = connection.cursor()
    cursor.execute("SELECT TEXT FROM MP12015.SENTENCE WHERE SENTENCE.ID = ?", (sentence_id,))
    sentence = cursor.fetchone()[0]
    cursor.close()
    return respond_with(str(sentence))

@app.route('/sentences/<sentence_id>/entities')
def get_sentence_entities(sentence_id):
    cursor = connection.cursor()
    cursor.execute(
    'SELECT ENTITY.TEXT, OFFSET."START", OFFSET."END" '
    'FROM MP12015.ENTITY, MP12015.OFFSET '
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
