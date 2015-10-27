import json, pyhdb, os

from flask import Flask
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
    return json.dumps(documents)



@app.route('/documents/<id>')
def get_document(id):
    cursor = connection.cursor()
    cursor.execute("SELECT TEXT FROM MP12015.SENTENCE WHERE DOCUMENT_ID = ?", (id,))
    sentences = list()
    for result in cursor.fetchall():
        sentences.append(str(result[0]))
    cursor.close()
    return " ".join(sentences)


if __name__ == '__main__':
    init()