import json, pyhdb, os, sys, ssl

from flask import Flask, jsonify, Response, request
from flask.ext.cors import CORS
from flask_login import LoginManager, login_user, current_user

from user import User

static_folder = "static"
if len(sys.argv) >= 2:
    static_folder = sys.argv[1]

app = Flask(__name__, static_folder=static_folder)
CORS(app, supports_credentials=True)

SERVER_ROOT = os.path.dirname(os.path.realpath(__file__))
connection = None

SECRET_KEY = 'development key'
app.config.from_object(__name__)

context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
context.load_cert_chain('certificate.crt', 'certificate.key')

login_manager = LoginManager()
login_manager.session_protection = None
login_manager.init_app(app)

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

    app.run(host='0.0.0.0',port=8080,debug=True,ssl_context=context)

@login_manager.user_loader
def load_user(user_id):
    return User.get(user_id, connection.cursor())

@app.route('/login', methods=['POST'])
def login():
    req = request.get_json()
    if req and 'id' in req and 'password' in req:
        user = load_user(req['id'])
        if user and req['password'] == user.token:
            login_user(user)
            user.token = None
            return respond_with(user.__dict__)
    return "Not authorized", 401

@app.route('/current_user')
def get_user():
    return respond_with(current_user.__dict__)

@app.route('/documents')
def get_documents():
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM LEARNING_TO_NOTE.DOCUMENTS ORDER BY id")
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
    annotations = data['denotations']
    save_annotations(document_id, annotations)
    id_map = {}
    for annotation in annotations:
        id_map[annotation['id']] = document_id + annotation.get('originalId', annotation['id'])
    save_relations(document_id, data['relations'], id_map)

def save_annotations(document_id, annotations):
    cursor = connection.cursor()
    cursor.execute("DELETE FROM LEARNING_TO_NOTE.ENTITIES WHERE DOC_ID = ?", (document_id,))
    connection.commit()
    annotation_tuples = map(lambda annotation: (document_id + annotation.get('originalId', annotation['id']), document_id, annotation['obj']), annotations)
    cursor.executemany("INSERT INTO LEARNING_TO_NOTE.ENTITIES (ID, DOC_ID, TYPE) VALUES (?, ?, ?)", annotation_tuples)
    offset_tuples = map(lambda annotation: (annotation['span']['begin'], annotation['span']['end'], document_id + annotation.get('originalId', annotation['id'])), annotations)
    cursor.executemany("INSERT INTO LEARNING_TO_NOTE.OFFSETS VALUES (?, ?, ?)", offset_tuples)
    connection.commit()

def save_relations(document_id, relations, id_map):
    cursor = connection.cursor()
    relation_tuples = map(lambda relation: (id_map[relation['subj']], id_map[relation['obj']], 1, relation['pred']), relations)
    cursor.executemany("INSERT INTO LEARNING_TO_NOTE.PAIRS (E1_ID, E2_ID, DDI, TYPE) VALUES (?, ?, ?, ?)",
                        relation_tuples)
    connection.commit()

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
                    JOIN LEARNING_TO_NOTE.USER_DOCUMENTS UO ON E.USER_DOC_ID = UO.ID AND UO.DOCUMENT_ID = ? \
                    JOIN LEARNING_TO_NOTE.OFFSETS O ON O.ENTITY_ID = E.ID \
                    ORDER BY E.ID', (document_id,))
    denotations = []
    previous_id = None
    for result in cursor.fetchall():
        current_id = str(result[0])
        if current_id == previous_id:
            denotation = denotations[-1]
            offset = {}
            offset['begin'] = result[2]
            offset['end'] = result[3]
            denotation['span'].append(offset)
        else:
            denotation = {}
            denotation['id'] = current_id.replace(document_id, '', 1)
            denotation['obj'] = str(result[1])
            denotation['span'] = []
            offset = {}
            offset['begin'] = result[2]
            offset['end'] = result[3]
            denotation['span'].append(offset)
            denotation['originalId'] = str(result[0]).replace(document_id, '', 1)
            denotations.append(denotation)
        previous_id = str(result[0])
    return denotations

def get_relations(cursor, document_id):
    cursor.execute("SELECT P.ID, P.E1_ID, P.E2_ID, P.TYPE FROM LEARNING_TO_NOTE.PAIRS P \
        JOIN LEARNING_TO_NOTE.ENTITIES E1 ON P.E1_ID = E1.ID AND P.DDI = 1\
        JOIN LEARNING_TO_NOTE.ENTITIES E2 ON P.E2_ID = E2.ID AND P.DDI = 1\
        JOIN LEARNING_TO_NOTE.USER_DOCUMENTS UO1 ON E1.USER_DOC_ID = UO1.ID AND UO1.DOCUMENT_ID = ?\
        JOIN LEARNING_TO_NOTE.USER_DOCUMENTS UO2 ON E2.USER_DOC_ID = UO2.ID AND UO2.DOCUMENT_ID = ?", (document_id, document_id,))
    relations = []
    for result in cursor.fetchall():
        relation = {}
        relation['id'] = str(result[0])
        relation['subj'] = str(result[1]).replace(document_id, '', 1)
        relation['obj'] = str(result[2]).replace(document_id, '', 1)
        relation['pred'] = str(result[3])
        relations.append(relation)
    return relations

def respond_with(response):
    return Response(json.dumps(response), mimetype='application/json')

if __name__ == '__main__':
    init()
