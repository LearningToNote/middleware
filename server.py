import json, pyhdb, os, sys

from flask import Flask, jsonify, Response, request
from flask.ext.cors import CORS
from flask_login import LoginManager, login_user, logout_user, current_user, login_required

from collections import namedtuple
from datetime import datetime

from user import User

Entity = namedtuple('Entity', ['id', 'user_doc_id', 'type', 'start', 'end'])

static_folder = "static"
if len(sys.argv) >= 2:
    static_folder = sys.argv[1]

app = Flask(__name__, static_folder=static_folder)
CORS(app, supports_credentials=True)

SERVER_ROOT = os.path.dirname(os.path.realpath(__file__))
connection = None

SECRET_KEY = 'development key'
app.config.from_object(__name__)

context = (SERVER_ROOT + '/certificate.crt',SERVER_ROOT + '/certificate.key')

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
    if req and 'username' in req and 'password' in req:
        user = load_user(req['username'])
        if user and req['password'] == user.token:
            login_user(user, remember=True)
            user.token = None
            return respond_with(user.__dict__)
    return "Not authorized", 401

@app.route('/logout', methods=['GET', 'POST'])
def logout():
    logout_user()
    return "", 200

@app.route('/current_user')
def get_current_user():
    return respond_with(current_user.__dict__)

@app.route('/users')
def get_users():
    cursor = connection.cursor()
    users = User.all(cursor)
    cursor.close()
    return respond_with(map(lambda user: user.__dict__, users))

@app.route('/users/<user_id>')
def get_user(user_id):
    user = load_user(user_id)
    if not user:
        return "User not found", 404
    user.token = None
    return respond_with(user.__dict__)

@app.route('/user_documents/<user_id>')
def get_user_documents(user_id):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM LEARNING_TO_NOTE.USER_DOCUMENTS WHERE USER_ID = ? OR VISIBILITY > 0", (user_id,))
    user_documents = list()
    for result in cursor.fetchall():
        user_documents.append({"id": result[0], "user_id": result[1], "document_id": result[2], "visibility": result[3],
                               "created_at": str(result[4]), "updated_at": str(result[5])})
    cursor.close()
    return respond_with(user_documents)

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
@login_required
def get_document(document_id):
    if request.method == 'GET':
        return load_document(document_id)
    else:
        successful = save_document(document_id, request.get_json())
        if successful:
            return ""
        else:
            return json.dumps({ "error": "Something went wrong while saving the data. Try again later." }), 500

def load_types():
    cursor = connection.cursor()
    #id, code, group_id, group, name
    cursor.execute('SELECT CODE, NAME, GROUP_ID, "GROUP" FROM LEARNING_TO_NOTE.TYPES')
    types = list()

    for aType in cursor.fetchall():
        types.append({"code": aType[0],
                      "name": aType[1],
                      "groupId": aType[2],
                      "group": aType[3]})
    return types

def save_document(document_id, data):
    annotations = data['denotations']
    successful = True
    print "Saving document " + str(document_id) + " for user " + str(current_user.get_id())
    user_doc_id = load_user_doc_id(document_id, current_user.get_id())
    delete_annotation_data(user_doc_id)
    print "Did load user_doc_id: " + str(user_doc_id)
    successful &= save_annotations(user_doc_id, annotations)
    if successful:
        print "saved annotations successfully"
        id_map = {}
        #neccessary, as TextAE does not create "originalId"s
        for annotation in annotations:
            id_map[annotation['id']] = annotation.get('originalId', annotation['id'])
        print "saving relations"
        successful &= save_relations(user_doc_id, data['relations'], id_map)
        if successful:
            print "saved relations successfully"
        else:
            print "did not save relations successfully"
    else:
        print "did not save annotations successfully"
    return successful

def delete_annotation_data(user_doc_id):
    cursor = connection.cursor()
    print "Deleting old information..."
    print "Deleting existing pairs..."
    cursor.execute("DELETE FROM LEARNING_TO_NOTE.PAIRS WHERE USER_DOC_ID = ?", (user_doc_id,))
    connection.commit()
    print "Deleting existing offsets..."
    cursor.execute("DELETE FROM LEARNING_TO_NOTE.OFFSETS WHERE USER_DOC_ID = ?", (user_doc_id,))
    connection.commit()
    print "Deleting existing annotations..."
    cursor.execute("DELETE FROM LEARNING_TO_NOTE.ENTITIES WHERE USER_DOC_ID = ?", (user_doc_id,))
    connection.commit()

def save_annotations(user_doc_id, annotations):
    #only save annotations from the current user, defined as userId 0 at loading time
    filtered_annotations = filter(lambda annotation: annotation.get('userId', 0) == 0, annotations)
    cursor = connection.cursor()
    if not user_doc_id:
        return False
    #TODO: insert/update types
    # insert new entities and offsets
    print "inserting new annotations..."
    annotation_tuples = map(lambda annotation: (annotation.get('originalId', annotation['id']), user_doc_id, annotation['obj']), annotations)
    #TODO: handle TYPE_ID and TEXT
    cursor.executemany("INSERT INTO LEARNING_TO_NOTE.ENTITIES (ID, USER_DOC_ID, LABEL) VALUES (?, ?, ?)", annotation_tuples)
    print "inserting new offsets..."
    offset_tuples = map(lambda annotation: (annotation['span']['begin'], annotation['span']['end'], annotation.get('originalId', annotation['id']), user_doc_id), annotations)
    cursor.executemany("INSERT INTO LEARNING_TO_NOTE.OFFSETS VALUES (?, ?, ?, ?)", offset_tuples)
    connection.commit()
    return True

def save_relations(user_doc_id, relations, id_map):
    cursor = connection.cursor()
    relation_tuples = map(lambda relation: (id_map[relation['subj']], id_map[relation['obj']], user_doc_id, 1, None, relation['pred']), relations)
    cursor.executemany("INSERT INTO LEARNING_TO_NOTE.PAIRS (E1_ID, E2_ID, USER_DOC_ID, DDI, TYPE_ID, LABEL) VALUES (?, ?, ?, ?, ?, ?)",
                        relation_tuples)
    connection.commit()
    return True

def load_user_doc_id(document_id, user_id):
    cursor = connection.cursor()
    user_id = current_user.get_id()
    # get user document id
    cursor.execute("SELECT ID FROM LEARNING_TO_NOTE.USER_DOCUMENTS WHERE DOCUMENT_ID = ? AND USER_ID = ?", (document_id, user_id))
    result = cursor.fetchone()
    if result:
        return str(result[0])
    else:
        cursor.execute
    return None

def load_document(document_id):
    cursor = connection.cursor()
    result = {}
    print "Loading information for document_id: " + str(document_id) + " and user: " + str(current_user.get_id())
    result['text'] = get_text(cursor, document_id)
    result['denotations'] = get_denotations(cursor, document_id)
    result['relations'] = get_relations(cursor, document_id)
    result['sourceid'] = document_id
    result['config'] = {'entity types': load_types()}
    cursor.close()
    print result
    return respond_with(result)

def get_text(cursor, document_id):
    cursor.execute("SELECT TEXT FROM LEARNING_TO_NOTE.DOCUMENTS WHERE ID = ?", (document_id,))
    result = cursor.fetchone()
    text = None
    if result:
        text = str(result[0].read())
    return text

def get_denotations(cursor, document_id):
    cursor.execute('SELECT E.ID, T.CODE, O."START", O."END", UD.USER_ID FROM LEARNING_TO_NOTE.ENTITIES E \
                    JOIN LEARNING_TO_NOTE.USER_DOCUMENTS UD ON E.USER_DOC_ID = UD.ID AND UD.DOCUMENT_ID = ?\
                    JOIN LEARNING_TO_NOTE.OFFSETS O ON O.ENTITY_ID = E.ID AND O.USER_DOC_ID = E.USER_DOC_ID\
                    LEFT OUTER JOIN LEARNING_TO_NOTE.TYPES T ON E.TYPE_ID = T.ID \
                    WHERE UD.VISIBILITY = 1 OR UD.USER_ID = ?\
                    ORDER BY E.ID', (document_id, current_user.get_id()))
    denotations = []
    increment = 1
    previous_id = None
    #todo: handle being not logged in
    user_id_mapping = {current_user.get_id(): 0}
    for result in cursor.fetchall():
        denotation = {}
        current_id = str(result[0])
        creator = str(result[4])
        if current_id == previous_id:
            current_id += "_" + str(increment)
            increment += 1
        else:
            increment = 1
        if not user_id_mapping.get(creator):
            user_id_mapping[creator] = len(user_id_mapping)
        denotation['id'] = current_id
        denotation['obj'] = str(result[1])
        denotation['span'] = {}
        denotation['span']['begin'] = result[2]
        denotation['span']['end'] = result[3]
        #neccessary for split annotations
        denotation['originalId'] = str(result[0])
        denotation['userId'] = user_id_mapping.get(creator)
        denotations.append(denotation)
        previous_id = str(result[0])
    return denotations

def get_relations(cursor, document_id):
    current_user_id = current_user.get_id()
    cursor.execute("SELECT P.ID, P.E1_ID, P.E2_ID, P.LABEL FROM LEARNING_TO_NOTE.PAIRS P \
        JOIN LEARNING_TO_NOTE.ENTITIES E1 ON P.E1_ID = E1.ID AND P.DDI = 1 AND P.USER_DOC_ID = E1.USER_DOC_ID\
        JOIN LEARNING_TO_NOTE.ENTITIES E2 ON P.E2_ID = E2.ID AND P.DDI = 1 AND P.USER_DOC_ID = E2.USER_DOC_ID\
        JOIN LEARNING_TO_NOTE.USER_DOCUMENTS UO1 ON E1.USER_DOC_ID = UO1.ID AND UO1.DOCUMENT_ID = ? AND UO1.USER_ID = ?\
        JOIN LEARNING_TO_NOTE.USER_DOCUMENTS UO2 ON E2.USER_DOC_ID = UO2.ID AND UO2.DOCUMENT_ID = ? AND UO2.USER_ID = ?",
        (document_id, current_user_id, document_id, current_user_id))
    relations = []
    for result in cursor.fetchall():
        relation = {}
        relation['id'] = str(result[0])
        relation['subj'] = str(result[1])
        relation['obj'] = str(result[2])
        relation['pred'] = str(result[3])
        relations.append(relation)
    return relations

@app.route('/evaluate', methods=['POST'])
def return_entities():
    req = request.get_json()
    document_id = req['document_id']
    user1 = req['user1']
    user2 = req['user2']
    cursor = connection.cursor()
    e1 = sorted(get_entities_for_user_document(cursor, document_id, user1), key=lambda x: x.start)
    e2 = sorted(get_entities_for_user_document(cursor, document_id, user2), key=lambda x: x.start)
    if len(e1) < len(e2):
        shortList, longList = e1, e2
    else:
        shortList, longList = e2, e1

    p = 0
    matches, left_aligns, right_aligns, overlaps, misses, wrong_type = 0, 0, 0, 0, 0, 0

    for entity in longList:
        while shortList[p].end < entity.start:
            if p == len(shortList) - 1:
                break
            p += 1
        can_miss = True
        for candidate in shortList[p:]:
            if candidate.start > entity.end:
                if can_miss:
                    misses += 1
                break
            can_miss = False
            if candidate.start != entity.start:
                if candidate.end == entity.end:
                    if candidate.type == entity.type:
                        wrong_type += 1
                    right_aligns += 1
                else:
                    if candidate.end < entity.start:
                        misses += 1
                    else:
                        if candidate.type == entity.type:
                            wrong_type += 1
                        overlaps += 1
            else:
                if candidate.type == entity.type:
                    wrong_type += 1
                if candidate.end == entity.end:
                    matches += 1
                else:
                    left_aligns += 1
        if can_miss:
            misses += 1


    return respond_with({"matches": matches, "left-aligns": left_aligns, "right-aligns": right_aligns,
                         "overlaps": overlaps, "misses": misses, "wrong-type": wrong_type})

def get_entities_for_user_document(cursor, document_id, user_id):
    cursor.execute('SELECT E.ID, E."TYPE_ID", O."START", O."END", E.USER_DOC_ID FROM LEARNING_TO_NOTE.ENTITIES E \
                    JOIN LEARNING_TO_NOTE.USER_DOCUMENTS UD ON E.USER_DOC_ID = UD.ID AND UD.DOCUMENT_ID = ?\
                    JOIN LEARNING_TO_NOTE.OFFSETS O ON O.ENTITY_ID = E.ID \
                    WHERE UD.USER_ID = ? ORDER BY E.ID', (document_id, user_id))
    annotations = list()
    for result in cursor.fetchall():
        annotations.append(Entity(id=result[0], type=result[1], start=result[2], end=result[3], user_doc_id=result[4]))
    return annotations

@app.route('/import', methods=['POST'])
def import_document():
    if current_user.get_id() is None:
        return "No user is logged in", 401

    req = request.get_json()
    document_id = req['document_id']
    document_text = req['text']
    document_visibility = 1
    if 'visibility' in req:
        document_visibility = int(req['visibility'])
    cursor = connection.cursor()

    cursor.execute("SELECT COUNT(*) FROM LEARNING_TO_NOTE.DOCUMENTS WHERE ID = ?", (document_id,))
    result = cursor.fetchone()
    if result[0] != 0:
        return "A document with the ID '%s' already exists" % (document_id,), 409

    cursor.execute("INSERT INTO LEARNING_TO_NOTE.DOCUMENTS VALUES (?, ?)", (document_id, document_text))
    connection.commit()

    cursor.execute("INSERT INTO LEARNING_TO_NOTE.USER_DOCUMENTS VALUES (?, ?, ?, ?, ?, ?)",
                   (current_user.get_id() + '_' + document_id, current_user.get_id(), document_id,
                    document_visibility, datetime.now(), datetime.now()))
    connection.commit()
    return "Document imported", 201

def respond_with(response):
    return Response(json.dumps(response), mimetype='application/json')

if __name__ == '__main__':
    init()
