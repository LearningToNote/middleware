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

PREDICTION_USER = 'victor_predictor'


def init():
    try_reconnecting()
    app.run(host='0.0.0.0', port=8080, debug=True, ssl_context=context)


def reset_connection():
    global connection
    if connection is not None:
        try:
            connection.close()
        except Exception, e:
            print e
    connection = None


def try_reconnecting():
    try:
        global connection
        with open(SERVER_ROOT + "/secrets.json") as f:
            secrets = json.load(f)
        connection = pyhdb.connect(
            host=secrets['host'],
            port=secrets['port'],
            user=secrets['username'],
            password=secrets['password']
        )
    except Exception, e:
        print e


@login_manager.user_loader
def load_user(user_id):
    return User.get(user_id, connection.cursor())


@app.route('/login', methods=['POST'])
def login():
    if connection is None:
        try_reconnecting()
    req = request.get_json()
    if req and 'username' in req and 'password' in req:
        try:
            user = load_user(req['username'])
            if user and req['password'] == user.token:
                login_user(user, remember=True)
                user.token = None
                return respond_with(user.__dict__)
        except Exception, e:
            reset_connection()
            return str(e), 500
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


@app.route('/tasks')
def get_tasks():
    cursor = connection.cursor()
    cursor.execute('SELECT t.id, t.name, t.domain, t.author, u.name '
                   'FROM LTN_DEVELOP.TASKS t LEFT OUTER JOIN LTN_DEVELOP.USERS u ON u.id = t.author')
    tasks = list()
    for result in cursor.fetchall():
        tasks.append({'task_id': result[0], 'task_name': result[1], 'task_domain': result[2],
                      'user_id': result[3], 'user_name': result[4]})
    return respond_with(tasks)


@app.route('/tasks/<task_id>', methods=['GET', 'POST', 'DELETE'])
def manage_task(task_id):
    cursor = connection.cursor()
    if request.method == 'GET':
        cursor.execute('SELECT t.id, t.name, t.domain, t.author, u.name '
                       'FROM LTN_DEVELOP.TASKS t LEFT OUTER JOIN LTN_DEVELOP.USERS u ON u.id = t.author '
                       'WHERE t.id = ?', (task_id, ))
        result = cursor.fetchone()
        cursor.execute('SELECT d.id, count(ud.id) '
                       'FROM LTN_DEVELOP.TASKS t '
                       'JOIN LTN_DEVELOP.DOCUMENTS d ON d.task = t.id '
                       'LEFT OUTER JOIN LTN_DEVELOP.USER_DOCUMENTS ud ON ud.document_id = d.id '
                       'AND (ud.visibility = 1 OR ud.user_id = ?) '
                       'WHERE t.id = ? '
                       'GROUP BY d.id ORDER BY d.id ASC', (current_user.get_id(), task_id))
        documents = list()
        for row in cursor.fetchall():
            documents.append({'document_id': row[0], 'user_document_count': row[1]})
        return respond_with({'task_id': result[0], 'task_name': result[1], 'task_domain': result[2],
                             'user_id': result[3], 'user_name': result[4], 'documents': documents})
    elif request.method == 'POST':
        req = request.get_json()
        sql_to_prepare = 'CALL LTN_DEVELOP.update_task (?, ?, ?, ?, ?)'
        params = {
            'TASK_ID': req.get('task_id'),
            'TASK_NAME': req.get('task_name'),
            'TABLE_NAME': req.get('table_name'),
            'ER_ANALYSIS_CONFIG': req.get('config'),
            'AUTHOR': req.get('author')
        }
        psid = cursor.prepare(sql_to_prepare)
        ps = cursor.get_prepared_statement(psid)
        cursor.execute_prepared(ps, [params])
        connection.commit()
        return 'OK', 200
    elif request.method == 'DELETE':
        req = request.get_json()
        sql_to_prepare = 'CALL LTN_DEVELOP.delete_task (?)'
        params = {'TASK_ID': req.get('task_id')}
        psid = cursor.prepare(sql_to_prepare)
        ps = cursor.get_prepared_statement(psid)
        cursor.execute_prepared(ps, [params])
        connection.commit()
        return 'OK', 200


@app.route('/user_documents_for/<document_id>')
def get_document_details(document_id):
    user_documents = list()
    cursor = connection.cursor()
    cursor.execute('SELECT d.id, MIN(d.user_id), MIN(u.name), COUNT(DISTINCT e.id), COUNT(distinct p.id) '
                   'FROM LTN_DEVELOP.USER_DOCUMENTS d '
                   'JOIN LTN_DEVELOP.USERS u ON u.id = d.user_id '
                   'LEFT OUTER JOIN LTN_DEVELOP.ENTITIES e ON e.user_doc_id = d.id '
                   'LEFT OUTER JOIN LTN_DEVELOP.PAIRS p ON p.user_doc_id = d.id '
                   'WHERE d.document_id = ?'
                   'GROUP BY d.id', (document_id,))
    for row in cursor.fetchall():
        user_documents.append({'id': row[0], 'user_id': row[1], 'user_name': row[2],
                               'entities': row[3], 'pairs': row[4]})
    return respond_with(user_documents)


@app.route('/user_documents/<user_id>')
def get_user_documents(user_id):
    if user_id != current_user.get_id():
        return "Not authorized to view the documents of this user.", 401
    cursor = connection.cursor()
    cursor.execute("SELECT ID, USER_ID, DOCUMENT_ID, VISIBILITY, CREATED_AT, UPDATED_AT "
                   "FROM LTN_DEVELOP.USER_DOCUMENTS "
                   "WHERE USER_ID = ? OR VISIBILITY > 0 ORDER BY DOCUMENT_ID", (user_id,))
    user_documents = list()
    for result in cursor.fetchall():
        user_documents.append({"id": result[0], "user_id": result[1], "document_id": result[2], "visibility": result[3],
                               "created_at": result[4].strftime('%Y-%m-%d %H:%M:%S'),
                               "updated_at": result[5].strftime('%Y-%m-%d %H:%M:%S')})
    cursor.close()
    return respond_with(user_documents)


@app.route('/user_documents/<user_document_id>', methods=['DELETE'])
def manage_user_documents(user_document_id):
    if request.method == 'DELETE':
        successful = delete_user_document(user_document_id)
        if not successful:
            return 'Deletion unsuccessful.', 500
        else:
            return 'Deleted.', 200


@app.route('/documents/<document_id>', methods=['GET', 'POST', 'DELETE'])
def get_document(document_id):
    if connection is None:
        try_reconnecting()
    if request.method == 'GET':
        try:
            result = load_document(document_id, current_user.get_id())
            return respond_with(result)
        except Exception, e:
            reset_connection()
            return 'Error while loading the document.', 500
    if request.method == 'POST':
        successful = False
        try:
            user_doc_id = load_user_doc_id(document_id, current_user.get_id())
            successful = save_document(request.get_json(), user_doc_id, document_id, current_user.get_id())
        except Exception, e:
            print e
            reset_connection()
        if successful:
            return ""
        else:
            return "An error occured while saving the document.", 500
    if request.method == 'DELETE':
        successful = False
        try:
            successful = delete_document(document_id)
        except Exception, e:
            print e
            reset_connection()
        if not successful:
            return 'Deletion unsuccessful.', 500
        else:
            return 'Deleted.', 200


@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()
    document_data = load_document(data['document_id'], data['user_id'])
    user_doc_id = load_user_doc_id(data['document_id'], PREDICTION_USER)
    successful = save_document(document_data, user_doc_id, data['document_id'], PREDICTION_USER)

    predict_relations(user_doc_id)

    if successful:
        return "OK"
    else:
        return "Something went wrong.", 500


def predict_relations(user_document_id):
    cursor = connection.cursor()

    sql_to_prepare = 'CALL LTN_TRAIN.PREDICT_UD (?, ?)'
    params = {'UD_ID':user_document_id}
    psid = cursor.prepare(sql_to_prepare)
    ps = cursor.get_prepared_statement(psid)
    cursor.execute_prepared(ps, [params])
    pairs = cursor.fetchall()

    strore_predicted_relations(pairs, user_document_id)


def strore_predicted_relations(pairs, user_document_id):
    cursor = connection.cursor()
    cursor.execute("DELETE FROM LTN_DEVELOP.PAIRS WHERE USER_DOC_ID = ?", (user_document_id,))

    tuples = []
    # import pdb;pdb.set_trace()
    pairs = filter(lambda x: x[0] != -1, pairs)
    for ddi, e1_id, e2_id in pairs:
        tuples.append((e1_id, e2_id, user_document_id, 1, ddi))


    cursor.executemany(
        "INSERT INTO LTN_DEVELOP.PAIRS (E1_ID, E2_ID, USER_DOC_ID, DDI, TYPE_ID) VALUES (?, ?, ?, ?, ?)", tuples
    )
    connection.commit()


def load_types():
    cursor = connection.cursor()
    cursor.execute('SELECT CODE, NAME, GROUP_ID, "GROUP" FROM LTN_DEVELOP.TYPES ORDER BY "GROUP" DESC')
    types = list()

    for aType in cursor.fetchall():
        types.append({"code": aType[0],
                      "name": aType[1],
                      "groupId": aType[2],
                      "group": aType[3]})
    return types


def load_type_id(code):
    cursor = connection.cursor()
    cursor.execute("SELECT ID FROM LTN_DEVELOP.TYPES WHERE CODE = ?", (code,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None


def save_document(data, user_doc_id, document_id, user_id):
    annotations = data['denotations']
    successful = True
    create_user_doc_if_not_existent(user_doc_id, document_id, user_id)
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


def create_user_doc_if_not_existent(user_doc_id, document_id, user_id):
    cursor = connection.cursor()
    cursor.execute("SELECT 1 FROM LTN_DEVELOP.USER_DOCUMENTS WHERE ID = ?", (user_doc_id,))
    result = cursor.fetchone()
    if not result:
        date = datetime.now()
        cursor.execute("INSERT INTO LTN_DEVELOP.USER_DOCUMENTS VALUES (?, ?, ?, ?, ?, ?)",
            (user_doc_id, user_id, document_id, 1, date, date))
        connection.commit()


def delete_annotation_data(user_doc_id):
    cursor = connection.cursor()
    print "Deleting old information for " + str(user_doc_id) + "..."
    print "Deleting existing pairs..."
    cursor.execute("DELETE FROM LTN_DEVELOP.PAIRS WHERE USER_DOC_ID = ?", (user_doc_id,))
    connection.commit()
    print "Deleting existing offsets..."
    cursor.execute("DELETE FROM LTN_DEVELOP.OFFSETS WHERE USER_DOC_ID = ?", (user_doc_id,))
    connection.commit()
    print "Deleting existing annotations..."
    cursor.execute("DELETE FROM LTN_DEVELOP.ENTITIES WHERE USER_DOC_ID = ?", (user_doc_id,))
    connection.commit()


def save_annotations(user_doc_id, annotations):
    #only save annotations from the current user, defined as userId 0 at loading time
    filtered_annotations = filter(lambda annotation: annotation.get('userId', 0) == 0, annotations)
    cursor = connection.cursor()
    if not user_doc_id:
        return False
    print "loading type ids...."
    type_id_dict = {}
    types = set(map(lambda annotation: (annotation['obj']['code']), filtered_annotations))
    for current_type in types:
        print current_type
        type_id = load_type_id(current_type)
        if type_id is not None:
            type_id_dict[current_type] = str(type_id)
        else:
            return False
    print type_id_dict
    print "inserting new annotations..."
    print filtered_annotations
    annotation_tuples = map(lambda annotation: (annotation.get('originalId',
                                                annotation['id']),
                                                user_doc_id,
                                                type_id_dict.get(annotation['obj']['code'], None),
                                                annotation['obj'].get('label', None)),
                            filtered_annotations)
    cursor.executemany("INSERT INTO LTN_DEVELOP.ENTITIES (ID, USER_DOC_ID, TYPE_ID, LABEL) VALUES (?, ?, ?, ?)", annotation_tuples)
    print "inserting new offsets..."
    offset_tuples = map(lambda annotation: (annotation['span']['begin'], annotation['span']['end'], annotation.get('originalId', annotation['id']), user_doc_id), filtered_annotations)
    cursor.executemany("INSERT INTO LTN_DEVELOP.OFFSETS VALUES (?, ?, ?, ?)", offset_tuples)
    connection.commit()
    return True


def save_relations(user_doc_id, relations, id_map):
    cursor = connection.cursor()
    print "loading type ids...."
    type_id_dict = {}
    types = set(map(lambda relation: (relation['pred']['code']), relations))
    for current_type in types:
        print current_type
        type_id = load_type_id(current_type)
        if type_id is not None:
            type_id_dict[current_type] = str(type_id)
        else:
            return False

    relation_tuples = map(lambda relation: (id_map[relation['subj']],
                                            id_map[relation['obj']],
                                            user_doc_id, 1,
                                            type_id_dict.get(relation['pred']['code'], None),
                                            relation['pred'].get('label', None)),
                        relations)
    cursor.executemany("INSERT INTO LTN_DEVELOP.PAIRS (E1_ID, E2_ID, USER_DOC_ID, DDI, TYPE_ID, LABEL) VALUES (?, ?, ?, ?, ?, ?)",
                        relation_tuples)
    connection.commit()
    return True


def load_user_doc_id(document_id, user_id):
    cursor = connection.cursor()
    cursor.execute("SELECT ID FROM LTN_DEVELOP.USER_DOCUMENTS WHERE DOCUMENT_ID = ? AND USER_ID = ?", (document_id, user_id))
    result = cursor.fetchone()
    if result:
        return str(result[0])
    return str(user_id) + "_" + str(document_id)


def load_document(document_id, user_id):
    cursor = connection.cursor()
    result = {}
    print "Loading information for document_id: " + str(document_id) + " and user: " + str(current_user.get_id())
    default_types = load_types()
    result['text'] = get_text(cursor, document_id)
    denotations, users, annotation_id_map = get_denotations_and_users(cursor, document_id, user_id)
    result['denotations'] = denotations
    result['relations'] = get_relations(cursor, document_id, user_id, annotation_id_map)
    result['sourceid'] = document_id
    result['config'] = {'entity types':   default_types,
                        'relation types': default_types,
                        'users': users}
    cursor.close()
    return result


def get_text(cursor, document_id):
    text = None
    try:
        sql_to_prepare = 'CALL LTN_DEVELOP.get_document_content (?, ?)'
        params = {
            'DOCUMENT_ID': document_id,
            'TEXT': ''
        }
        psid = cursor.prepare(sql_to_prepare)
        ps = cursor.get_prepared_statement(psid)
        cursor.execute_prepared(ps, [params])
        result = cursor.fetchone()
        if result:
            text = str(result[0].read())
    except Exception, e:
        print 'Error: ', e
    return text


def get_denotations_and_users(cursor, document_id, user_id):
    cursor.execute('SELECT E.ID, UD.USER_ID, O."START", O."END", T.CODE, T."NAME", T.GROUP_ID, '
                   'T."GROUP", E."LABEL", U."NAME" '
                   'FROM LTN_DEVELOP.ENTITIES E '
                   'JOIN LTN_DEVELOP.USER_DOCUMENTS UD ON E.USER_DOC_ID = UD.ID AND UD.DOCUMENT_ID = ? '
                   'JOIN LTN_DEVELOP.OFFSETS O ON O.ENTITY_ID = E.ID AND O.USER_DOC_ID = E.USER_DOC_ID '
                   'JOIN LTN_DEVELOP.USERS U ON UD.USER_ID = U.ID '
                   'LEFT OUTER JOIN LTN_DEVELOP.TYPES T ON E.TYPE_ID = T.ID '
                   'WHERE UD.VISIBILITY = 1 OR UD.USER_ID = ? '
                   'ORDER BY E.ID', (document_id, user_id))
    denotations = []
    increment = 1
    previous_id = None
    # todo: handle being not logged in
    colors = ['blue', 'navy', 'brown', 'chocolate', 'orange', 'maroon', 'turquoise']
    user_id_mapping = {current_user.get_id(): 0, PREDICTION_USER: -1}
    prediction_engine_info = {'name': 'Prediction Engine', 'color': 'gray'}
    current_user_info = {'name': 'You', 'color': 'darkgreen'}
    user_info = {-1: prediction_engine_info, 0: current_user_info}
    annotation_id_map = {}
    for result in cursor.fetchall():
        denotation = {}
        current_id = str(result[0])
        creator = str(result[1])
        if current_id == previous_id:
            current_id += "_" + str(increment)
            increment += 1
            if not previous_id in annotation_id_map:
                annotation_id_map[previous_id] = {}
            annotation_id_map[previous_id][creator] = current_id
        else:
            increment = 1
        if not creator in user_id_mapping:
            new_id = len(user_id_mapping) - 1
            user_info[new_id] = {'name': str(result[9]), 'color': colors[(new_id - 1) % len(colors)]}
            user_id_mapping[creator] = new_id

        anno_info = {"code": str(result[4]),
                     "name": str(result[5]),
                     "groupId": str(result[6]),
                     "group": str(result[7]),
                     "label": str(result[8])}
        denotation['id'] = current_id
        denotation['obj'] = anno_info
        denotation['span'] = {}
        denotation['span']['begin'] = result[2]
        denotation['span']['end'] = result[3]
        # neccessary for split annotations
        denotation['originalId'] = str(result[0])
        denotation['userId'] = user_id_mapping.get(creator)
        denotations.append(denotation)
        previous_id = str(result[0])
    return denotations, user_info, annotation_id_map


def get_relations(cursor, document_id, user_id, annotation_id_map):
    cursor.execute('SELECT P.ID, P.E1_ID, P.E2_ID, P.LABEL, T.CODE, T."NAME", T.GROUP_ID, T."GROUP", UD1.USER_ID '
                   'FROM LTN_DEVELOP.PAIRS P '
                   'LEFT OUTER JOIN LTN_DEVELOP.TYPES T ON P.TYPE_ID = T.ID '
                   'JOIN LTN_DEVELOP.ENTITIES E1 ON P.E1_ID = E1.ID AND P.DDI = 1 AND P.USER_DOC_ID = E1.USER_DOC_ID '
                   'JOIN LTN_DEVELOP.ENTITIES E2 ON P.E2_ID = E2.ID AND P.DDI = 1 AND P.USER_DOC_ID = E2.USER_DOC_ID '
                   'JOIN LTN_DEVELOP.USER_DOCUMENTS UD1 ON E1.USER_DOC_ID = UD1.ID AND UD1.DOCUMENT_ID = ? '
                   'AND (UD1.USER_ID = ? OR UD1.VISIBILITY = 1) '
                   'JOIN LTN_DEVELOP.USER_DOCUMENTS UD2 ON E2.USER_DOC_ID = UD2.ID AND UD2.DOCUMENT_ID = ? '
                   'AND (UD2.USER_ID = ? OR UD2.VISIBILITY = 1)', (document_id, user_id, document_id, user_id))
    relations = []
    for result in cursor.fetchall():
        type_info = {"code":    str(result[4]),
                     "name":    str(result[5]),
                     "groupId": str(result[6]),
                     "group":   str(result[7]),
                     "label":   str(result[3])}
        relation = {}
        subj = str(result[1])
        obj = str(result[2])
        replacement_subj = annotation_id_map.get(subj)
        replacement_obj = annotation_id_map.get(obj)
        current_user_id = str(result[8])
        if replacement_subj is not None:
            if replacement_subj.get(current_user_id) is not None:
                subj = replacement_subj.get(current_user_id)
        if replacement_obj is not None:
            if replacement_obj.get(current_user_id) is not None:
                obj = replacement_obj.get(current_user_id)
        relation['id'] = str(result[0])
        relation['subj'] = subj
        relation['obj'] = obj
        relation['pred'] = type_info
        relations.append(relation)
    return relations


def delete_user_document(user_document_id):
    return delete_user_documents([user_document_id])


def delete_user_documents(user_document_ids):
    user_document_ids = "('" + "', '".join(user_document_ids) + "')"
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM LTN_DEVELOP.PAIRS WHERE USER_DOC_ID IN " + user_document_ids)
        cursor.execute("DELETE FROM LTN_DEVELOP.OFFSETS WHERE USER_DOC_ID IN  " + user_document_ids)
        cursor.execute("DELETE FROM LTN_DEVELOP.ENTITIES WHERE USER_DOC_ID IN " + user_document_ids)
        cursor.execute("DELETE FROM LTN_DEVELOP.USER_DOCUMENTS WHERE ID IN " + user_document_ids)
        connection.commit()
        return True
    except Exception, e:
        raise e


def delete_document(document_id):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT ID FROM LTN_DEVELOP.USER_DOCUMENTS WHERE DOCUMENT_ID = ?", (document_id,))
        user_document_ids = map(lambda t: t[0], cursor.fetchall())
        delete_user_documents(user_document_ids)

        sql_to_prepare = 'CALL LTN_DEVELOP.delete_document (?)'
        params = {'DOCUMENT_ID': document_id}
        psid = cursor.prepare(sql_to_prepare)
        ps = cursor.get_prepared_statement(psid)
        cursor.execute_prepared(ps, [params])
        connection.commit()

        return True
    except Exception, e:
        raise e


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
    matches, left_aligns, right_aligns, overlaps, misses, wrong_type = 0, 0, 0, 0, 0, {}

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
                    if candidate.type != entity.type:
                        wrong_type["right-aligns"] = wrong_type.get("right-aligns", 0) + 1
                    right_aligns += 1
                else:
                    if candidate.end < entity.start:
                        misses += 1
                    else:
                        if candidate.type != entity.type:
                            wrong_type["overlaps"] = wrong_type.get("overlaps", 0) + 1
                        overlaps += 1
            else:
                if candidate.end == entity.end:
                    if candidate.type != entity.type:
                        wrong_type["matches"] = wrong_type.get("matches", 0) + 1
                    matches += 1
                else:
                    if candidate.type != entity.type:
                        wrong_type["left-aligns"] = wrong_type.get("left-aligns", 0) + 1
                    left_aligns += 1
        if can_miss:
            misses += 1

    return respond_with({"matches": matches, "left-aligns": left_aligns, "right-aligns": right_aligns,
                         "overlaps": overlaps, "misses": misses, "wrong-type": wrong_type})


def get_entities_for_user_document(cursor, document_id, user_id):
    cursor.execute('SELECT E.ID, E."TYPE_ID", O."START", O."END", E.USER_DOC_ID FROM LTN_DEVELOP.ENTITIES E \
                    JOIN LTN_DEVELOP.USER_DOCUMENTS UD ON E.USER_DOC_ID = UD.ID AND UD.DOCUMENT_ID = ?\
                    JOIN LTN_DEVELOP.OFFSETS O ON O.ENTITY_ID = E.ID \
                    WHERE UD.USER_ID = ? ORDER BY E.ID', (document_id, user_id))
    annotations = list()
    for result in cursor.fetchall():
        annotations.append(Entity(id=result[0], type=result[1], start=result[2], end=result[3], user_doc_id=result[4]))
    return annotations


@app.route('/pubmed/<pubmed_id>')
def fetch_pubmed_abstract(pubmed_id):
    from metapub import PubMedFetcher
    article = PubMedFetcher(cachedir=".cache/").article_by_pmid(pubmed_id)
    return article.abstract


@app.route('/import', methods=['POST'])
def import_document():
    if current_user.get_id() is None:
        return "No user is logged in", 401

    req = request.get_json()
    document_id = req['document_id']
    document_text = req['text']
    task = req['task']
    document_visibility = 1
    if 'visibility' in req:
        document_visibility = int(req['visibility'])
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM LTN_DEVELOP.DOCUMENTS WHERE ID = ?", (document_id,))
    result = cursor.fetchone()
    if result[0] != 0:
        return "A document with the ID '%s' already exists" % (document_id,), 409

    sql_to_prepare = 'CALL LTN_DEVELOP.add_document (?, ?)'
    params = {
        'DOCUMENT_ID': document_id,
        'DOCUMENT_TEXT': document_text,
        'TASK': task
    }
    psid = cursor.prepare(sql_to_prepare)
    ps = cursor.get_prepared_statement(psid)
    cursor.execute_prepared(ps, [params])
    connection.commit()

    cursor.execute("INSERT INTO LTN_DEVELOP.USER_DOCUMENTS VALUES (?, ?, ?, ?, ?, ?)",
                   (current_user.get_id() + '_' + document_id, current_user.get_id(), document_id,
                    document_visibility, datetime.now(), datetime.now()))
    connection.commit()
    return "Document imported", 201


def respond_with(response):
    return Response(json.dumps(response), mimetype='application/json')


if __name__ == '__main__':
    init()
