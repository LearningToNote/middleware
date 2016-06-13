import json
import random
import bioc
import StringIO

from flask import Response, request, url_for, redirect
from flask_login import LoginManager, login_user, logout_user, current_user

from collections import namedtuple
from datetime import datetime

from user import User

from ltnserver import app, try_reconnecting, reset_connection, get_connection
from ltnserver.training import model_training_queue


Entity = namedtuple('Entity', ['id', 'user_doc_id', 'type', 'start', 'end'])

login_manager = LoginManager()
login_manager.session_protection = None
login_manager.init_app(app)

PREDICT_ENTITIES = 'entities'
PREDICT_RELATIONS = 'relations'

TYPE_PLAINTEXT = 'plaintext'
TYPE_BIOC = 'bioc'


@app.route('/')
def home():
    return redirect(url_for('static', filename='index.html'))


@login_manager.user_loader
def load_user(user_id):
    return User.get(user_id, get_connection().cursor())


@app.route('/login', methods=['POST'])
def login():
    if get_connection() is None:
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
            return str(e) + " Please try again later.", 500
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
    cursor = get_connection().cursor()
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
    cursor = get_connection().cursor()
    cursor.execute('SELECT t.id, t.name, t.domain, t.config, t.author, u.name '
                   'FROM LTN_DEVELOP.TASKS t LEFT OUTER JOIN LTN_DEVELOP.USERS u ON u.id = t.author ORDER BY t.id')
    tasks = list()
    for result in cursor.fetchall():
        tasks.append({'task_id': result[0], 'task_name': result[1], 'task_domain': result[2], 'task_config': result[3],
                      'user_id': result[4], 'user_name': result[5]})
    return respond_with(tasks)


@app.route('/tasks/<task_id>', methods=['GET', 'POST', 'DELETE'])
def manage_task(task_id):
    cursor = get_connection().cursor()
    if request.method == 'GET':
        cursor.execute('SELECT t.id, t.name, t.domain, t.author, u.name '
                       'FROM LTN_DEVELOP.TASKS t LEFT OUTER JOIN LTN_DEVELOP.USERS u ON u.id = t.author '
                       'WHERE t.id = ?', (task_id,))
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
        if req.get('task_id') is not None:
            sql_to_prepare = 'CALL LTN_DEVELOP.update_task (?, ?, ?, ?, ?)'
        else:
            sql_to_prepare = 'CALL LTN_DEVELOP.add_task (?, ?, ?, ?, ?)'

        params = {
            'TASK_ID': req.get('task_id'),
            'TASK_NAME': req.get('task_name'),
            'TABLE_NAME': req.get('task_domain'),
            'ER_ANALYSIS_CONFIG': req.get('task_config'),
            'NEW_AUTHOR': req.get('user_id')
        }

        if params.get('TABLE_NAME', None) is None:
            generate_table_name(params)
        if params.get('NEW_AUTHOR', None) is None:
            params['NEW_AUTHOR'] = current_user.get_id()

        psid = cursor.prepare(sql_to_prepare)
        ps = cursor.get_prepared_statement(psid)
        try:
            cursor.execute_prepared(ps, [params])
            get_connection().commit()
        except:
            pass  # Rows affected warning
        return 'OK', 200
    elif request.method == 'DELETE':
        sql_to_prepare = 'CALL LTN_DEVELOP.delete_task (?)'
        params = {'TASK_ID': task_id}
        psid = cursor.prepare(sql_to_prepare)
        ps = cursor.get_prepared_statement(psid)
        try:
            cursor.execute_prepared(ps, [params])
            get_connection().commit()
        except:
            pass  # Rows affected warning
        return 'OK', 200


def generate_table_name(task):
    task['TABLE_NAME'] = task['TASK_NAME'].replace(' ', '')[:10] + str(random.getrandbits(42))


@app.route('/tasks/<task_id>/entity_types')
def get_task_entity_types(task_id):
    return respond_with(get_task_types(task_id, relation=False))


@app.route('/tasks/<task_id>/relation_types')
def get_task_relation_types(task_id):
    return respond_with(get_task_types(task_id, relation=True))


@app.route('/user_documents_for/<document_id>')
def get_document_details(document_id):
    user_documents = list()
    cursor = get_connection().cursor()
    user_id = current_user.get_id()
    cursor.execute(
        'SELECT d.id, MIN(d.user_id), MIN(u.name), COUNT(DISTINCT e.id), COUNT(distinct p.id), MIN(d.visibility) '
        'FROM LTN_DEVELOP.USER_DOCUMENTS d '
        'JOIN LTN_DEVELOP.USERS u ON u.id = d.user_id '
        'LEFT OUTER JOIN LTN_DEVELOP.ENTITIES e ON e.user_doc_id = d.id '
        'LEFT OUTER JOIN LTN_DEVELOP.PAIRS p ON p.user_doc_id = d.id AND p.ddi = 1 '
        'WHERE d.document_id = ? AND (d.visibility = 1 OR d.user_id = ?) '
        'GROUP BY d.id', (document_id, user_id))
    for row in cursor.fetchall():
        user_documents.append({'id': row[0], 'user_id': row[1], 'user_name': row[2],
                               'entities': row[3], 'pairs': row[4],
                               'visible': bool(row[5]), 'from_current_user': row[1] == user_id})
    return respond_with(user_documents)


@app.route('/userdoc_visibility/<doc_id>', methods=['POST'])
def save_userdoc_visibility(doc_id):
    user_doc_id = load_user_doc_id(doc_id, current_user.get_id())
    visibility = request.get_json()['visible']
    cursor = get_connection().cursor()
    cursor.execute('UPDATE LTN_DEVELOP.USER_DOCUMENTS '
                   'SET VISIBILITY = ? WHERE ID = ?',
                   (visibility, user_doc_id))
    cursor.close()
    get_connection().commit()
    return "", 200


@app.route('/user_documents/<user_id>')
def get_user_documents(user_id):
    if user_id != current_user.get_id():
        return "Not authorized to view the documents of this user.", 401
    cursor = get_connection().cursor()
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
    if get_connection() is None:
        try_reconnecting()
    if request.method == 'GET':
        try:
            result = load_document(document_id, current_user.get_id())
            return respond_with(result)
        except Exception, e:
            print e
            reset_connection()
            return 'Error while loading the document.', 500
    if request.method == 'POST':
        successful = False
        try:
            user_doc_id = load_user_doc_id(document_id, current_user.get_id())
            successful = save_document(request.get_json(), user_doc_id, document_id, current_user.get_id(),
                                       request.get_json()['task_id'])
        except Exception, e:
            print e
            reset_connection()
        if successful:
            return ""
        else:
            return "An error occurred while saving the document.", 500
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


@app.route('/export/<document_id>', methods=['GET'])
def export(document_id):
    document = load_document(document_id, current_user.get_id())
    bcollection = bioc.BioCCollection()
    bdocument = create_bioc_document_from_document_json(document)
    bcollection.add_document(bdocument)
    result = bcollection.tobioc()
    response = Response(result, mimetype='text/xml')
    response.headers["Content-Disposition"] = "attachment; filename=" + document_id + ".xml"
    return response


@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()
    task_id = data['task_id']
    jobs = data.get('jobs', [PREDICT_ENTITIES])
    document_id = data['document_id']
    user_id = data.get('user_id', current_user.get_id())
    current_prediction_user = prediction_user_for_user(user_id)
    prediction_user_doc_id = load_user_doc_id(document_id, current_prediction_user)
    delete_user_document(prediction_user_doc_id)

    document_data = json.loads(data.get('current_state', None))
    if document_data is None:
        document_data = load_document(document_id, user_id)
    else:
        # the current status has to be saved first in order to disambiguate the ids of the annotations
        user_doc_id = load_user_doc_id(document_id, current_user.get_id())
        successful = save_document(document_data, user_doc_id, document_id, current_user.get_id(), task_id)
        if not successful:
            return "Could not save the document", 500

    if PREDICT_ENTITIES in jobs:
        cursor = get_connection().cursor()
        cursor.execute('INSERT INTO "LTN_DEVELOP"."USER_DOCUMENTS" '
                       'VALUES (?, ?, ?, 0, current_timestamp, current_timestamp)',
                       (prediction_user_doc_id, current_prediction_user, document_id,))
        cursor.close()
        get_connection().commit()
        predict_entities(document_id, task_id, prediction_user_doc_id)
    if PREDICT_RELATIONS in jobs:
        if PREDICT_ENTITIES not in jobs:
            save_document(document_data, prediction_user_doc_id, document_id, current_prediction_user, task_id, False)
        predicted_pairs = predict_relations(prediction_user_doc_id, task_id)
        if PREDICT_ENTITIES not in jobs:
            remove_entities_without_relations(predicted_pairs, document_data, prediction_user_doc_id)

    document_data = load_document(document_id, current_user.get_id(), True)
    return respond_with(document_data)


def remove_entities_without_relations(pairs, document_data, user_doc_id):
    used_entities = set()

    def add_entities_to_set(pair_tuple):
        used_entities.add(pair_tuple[0])
        used_entities.add(pair_tuple[1])

    map(add_entities_to_set, pairs)
    to_be_removed = map(lambda e: e['id'], filter(lambda d: d['id'] not in used_entities, document_data['denotations']))

    cursor = get_connection().cursor()
    id_string = "('" + "', '".join(to_be_removed) + "')"
    cursor.execute('DELETE FROM LTN_DEVELOP.ENTITIES WHERE ID IN ' + id_string + ' AND USER_DOC_ID = ?', (user_doc_id,))
    get_connection().commit()
    cursor.close()


def predict_entities(document_id, task_id, target_user_document_id):
    cursor = get_connection().cursor()

    cursor.execute('select "DOMAIN" from LTN_DEVELOP.tasks WHERE id = ?', (task_id,))
    table_name = cursor.fetchone()[0]
    index_name = "$TA_INDEX_" + table_name
    er_index_name = "$TA_ER_INDEX_" + table_name

    cursor.execute("""
        select distinct
          fti.ta_offset as "start",
          fti.ta_offset + length(fti.ta_token) as "end",
          fti.ta_token,
          t.code,
          t.id
        from "LTN_DEVELOP"."%s" fti
        join "LTN_DEVELOP"."TYPES" t on (t.code = fti.ta_type or
          (t.code = 'T092' and fti.ta_type like 'ORGANIZATION%%'))
        join "LTN_DEVELOP"."%s" pos on fti.document_id = pos.document_id and fti.ta_offset = pos.ta_offset
        where fti.document_id = ?
          and length(fti.ta_token) >= 3
          and pos.ta_type in ('noun', 'abbreviation', 'proper name')
        order by fti.ta_offset
    """ % (er_index_name, index_name), (document_id,))

    entities = list()
    offsets = list()

    for row in cursor.fetchall():
        entity_id = target_user_document_id + str(row[0]) + str(row[2]) + str(row[3])
        entity_id = entity_id.replace(' ', '_').replace('/', '_')
        entities.append((entity_id, target_user_document_id, int(row[4]), None, row[2]))
        offsets.append((row[0], row[1], entity_id, target_user_document_id))

    cursor.executemany('insert into "LTN_DEVELOP"."ENTITIES" VALUES (?, ?, ?, ?, ?)', entities)
    cursor.executemany('insert into "LTN_DEVELOP"."OFFSETS" VALUES (?, ?, ?, ?)', offsets)
    get_connection().commit()
    cursor.close()


def predict_relations(user_document_id, task_id):
    cursor = get_connection().cursor()

    sql_to_prepare = 'CALL LTN_DEVELOP.PREDICT_UD (?, ?, ?)'
    params = {'UD_ID': user_document_id,
              'TASK_ID': str(task_id)}
    psid = cursor.prepare(sql_to_prepare)
    ps = cursor.get_prepared_statement(psid)
    cursor.execute_prepared(ps, [params])
    pairs = cursor.fetchall()

    return store_predicted_relations(pairs, user_document_id)


def store_predicted_relations(pairs, user_document_id):
    cursor = get_connection().cursor()
    cursor.execute("DELETE FROM LTN_DEVELOP.PAIRS WHERE USER_DOC_ID = ?", (user_document_id,))

    tuples = []
    pairs = filter(lambda x: x[0] != -1, pairs)
    for ddi, e1_id, e2_id in pairs:
        tuples.append((e1_id, e2_id, user_document_id, 1, ddi))

    cursor.executemany(
        "INSERT INTO LTN_DEVELOP.PAIRS (E1_ID, E2_ID, USER_DOC_ID, DDI, TYPE_ID) VALUES (?, ?, ?, ?, ?)", tuples
    )
    get_connection().commit()
    cursor.close()
    return tuples


def get_types(document_id, relation):
    cursor = get_connection().cursor()
    relation_flag = int(relation)
    cursor.execute('''SELECT CODE, NAME, GROUP_ID, "GROUP", "LABEL", t.ID, tt.ID
                      FROM LTN_DEVELOP.TYPES t
                      JOIN LTN_DEVELOP.TASK_TYPES tt ON t.ID = tt.TYPE_ID
                      JOIN LTN_DEVELOP.DOCUMENTS d ON tt.TASK_ID = d.TASK
                      WHERE d.id = ? AND tt.RELATION = ?
                      ORDER BY "GROUP" DESC''', (document_id, relation_flag))
    types = list()
    for row in cursor.fetchall():
        types.append({"code": row[0], "name": "%s (%s)" % (row[4], row[1]), "groupId": row[2], "group": row[3],
                      "label": row[4], "type_id": row[5], "id": row[6]})
    return types


def get_entity_types(document_id):
    return get_types(document_id, relation=False)


def get_relation_types(document_id):
    return get_types(document_id, relation=True)


def get_task_types(task_id, relation):
    cursor = get_connection().cursor()
    relation_flag = int(relation)
    cursor.execute('SELECT CODE, NAME, GROUP_ID, "GROUP", "LABEL", t.ID, tt.ID '
                   'FROM LTN_DEVELOP.TYPES t '
                   'JOIN LTN_DEVELOP.TASK_TYPES tt ON t.ID = tt.TYPE_ID '
                   'WHERE tt.TASK_ID = ? AND tt.RELATION = ? '
                   'ORDER BY "GROUP" DESC', (task_id, relation_flag))
    types = list()
    for row in cursor.fetchall():
        types.append({"code": row[0], "name": row[1], "groupId": row[2], "group": row[3],
                      "label": row[4], "type_id": row[5], "id": row[6]})
    return types


def load_types():
    cursor = get_connection().cursor()
    cursor.execute('SELECT CODE, NAME, GROUP_ID, "GROUP", ID FROM LTN_DEVELOP.TYPES ORDER BY "GROUP" DESC')
    types = list()

    for aType in cursor.fetchall():
        types.append({"code": aType[0],
                      "name": aType[1],
                      "groupId": aType[2],
                      "group": aType[3],
                      "id": aType[4]})
    return types


@app.route('/base_types')
def get_base_types():
    return respond_with(load_types())


@app.route('/task_types/<type_id>', methods=['GET', 'PUT', 'DELETE'])
def manage_task_type(type_id):
    cursor = get_connection().cursor()
    if request.method == 'GET':
        cursor.execute('SELECT CODE, NAME, GROUP_ID, "GROUP", "LABEL", t.ID, tt.ID '
                       'FROM LTN_DEVELOP.TYPES t '
                       'JOIN LTN_DEVELOP.TASK_TYPES tt ON t.ID = tt.TYPE_ID '
                       'WHERE tt.ID = ?', (type_id,))
        row = cursor.fetchone()
        if row:
            return respond_with({"code": row[0], "name": row[1], "groupId": row[2], "group": row[3],
                                 "label": row[4], "type_id": row[5], "id": row[6]})
        return 'NOT FOUND', 404
    elif request.method == 'PUT':
        req = request.get_json()
        updated_type = req.get('type')
        cursor.execute('SELECT ID FROM LTN_DEVELOP.TASK_TYPES WHERE ID = ?', (type_id,))
        already_existing = cursor.fetchone()
        if already_existing:
            cursor.execute('UPDATE LTN_DEVELOP.TASK_TYPES SET ID = ?, LABEL = ?, TYPE_ID = ? '
                           'WHERE ID = ?',
                           (updated_type.get('id'), updated_type.get('label'), updated_type.get('type_id'), type_id))
            get_connection().commit()
            return 'UPDATED', 200
        else:
            task_id = req.get('task')
            is_relation = req.get('relation')
            cursor.execute('INSERT INTO LTN_DEVELOP.TASK_TYPES (LABEL, TASK_ID, TYPE_ID, RELATION) '
                           'VALUES (?, ?, ?, ?)',
                           (updated_type.get('label'), task_id, updated_type.get('type_id'), is_relation))
            get_connection().commit()
            return 'CREATED', 200
    elif request.method == 'DELETE':
        cursor.execute('DELETE FROM LTN_DEVELOP.TASK_TYPES WHERE ID = ?', (type_id,))
        get_connection().commit()
        return 'DELETED', 200


def load_type_id(code):
    cursor = get_connection().cursor()
    cursor.execute("SELECT ID FROM LTN_DEVELOP.TYPES WHERE CODE = ?", (code,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None


def save_document(data, user_doc_id, document_id, user_id, task_id, is_visible=True):
    annotations = data['denotations']
    successful = True
    create_user_doc_if_not_existent(user_doc_id, document_id, user_id, is_visible)
    delete_annotation_data(user_doc_id)
    print "Did load user_doc_id: " + str(user_doc_id)
    successful &= save_annotations(user_doc_id, annotations)
    if successful:
        print "saved annotations successfully"
        id_map = {}
        # necessary, as TextAE does not create "originalId"s
        for annotation in annotations:
            if annotation.get('userId', 0) == 0:
                id_map[annotation['id']] = annotation.get('originalId', annotation['id'])
        print "saving relations"
        successful &= save_relations(user_doc_id, data['relations'], id_map)
        if successful:
            print "saved relations successfully"
            model_training_queue.add(task_id)
        else:
            print "did not save relations successfully"
    else:
        print "did not save annotations successfully"
    return successful


def create_user_doc_if_not_existent(user_doc_id, document_id, user_id, is_visible=True):
    cursor = get_connection().cursor()
    cursor.execute("SELECT 1 FROM LTN_DEVELOP.USER_DOCUMENTS WHERE ID = ?", (user_doc_id,))
    result = cursor.fetchone()
    if not result:
        date = datetime.now()
        cursor.execute("INSERT INTO LTN_DEVELOP.USER_DOCUMENTS VALUES (?, ?, ?, ?, ?, ?)",
                       (user_doc_id, user_id, document_id, int(is_visible), date, date))
        get_connection().commit()


def delete_annotation_data(user_doc_id):
    cursor = get_connection().cursor()
    print "Deleting old information for " + str(user_doc_id) + "..."
    print "Deleting existing pairs..."
    cursor.execute("DELETE FROM LTN_DEVELOP.PAIRS WHERE USER_DOC_ID = ?", (user_doc_id,))
    get_connection().commit()
    print "Deleting existing offsets..."
    cursor.execute("DELETE FROM LTN_DEVELOP.OFFSETS WHERE USER_DOC_ID = ?", (user_doc_id,))
    get_connection().commit()
    print "Deleting existing annotations..."
    cursor.execute("DELETE FROM LTN_DEVELOP.ENTITIES WHERE USER_DOC_ID = ?", (user_doc_id,))
    get_connection().commit()


def convert_annotation(annotation, user_doc_id):
    return (annotation.get('originalId',
                           annotation['id']),
            user_doc_id,
            annotation['obj'].get('id'),
            annotation['obj'].get('label', None))


def convert_offset(annotation, user_doc_id):
    return (annotation['span']['begin'], annotation['span']['end'],
            annotation.get('originalId', annotation['id']), user_doc_id)


def save_annotations(user_doc_id, annotations):
    # only save annotations from the current user, defined as userId 0 at loading time
    filtered_annotations = filter(lambda annotation: annotation.get('userId', 0) == 0, annotations)
    if not user_doc_id:
        return False
    print "inserting new annotations..."
    annotation_tuples = map(lambda annotation: convert_annotation(annotation, user_doc_id), filtered_annotations)
    cursor = get_connection().cursor()
    cursor.executemany("INSERT INTO LTN_DEVELOP.ENTITIES (ID, USER_DOC_ID, TYPE_ID, LABEL) "
                       "VALUES (?, ?, ?, ?)", annotation_tuples)
    print "inserting new offsets..."
    offset_tuples = map(lambda annotation: convert_offset(annotation, user_doc_id), filtered_annotations)
    cursor.executemany("INSERT INTO LTN_DEVELOP.OFFSETS VALUES (?, ?, ?, ?)", offset_tuples)
    get_connection().commit()
    return True


def save_relations(user_doc_id, relations, id_map):
    relation_tuples = list()
    for relation in relations:
        if id_map.get(relation['subj']) is not None and id_map.get(relation['obj']) is not None:
            relation_tuples.append((id_map[relation['subj']],
                                    id_map[relation['obj']],
                                    user_doc_id, 1,
                                    relation['pred'].get('id'),
                                    relation['pred'].get('label', None)))

    cursor = get_connection().cursor()
    cursor.executemany("INSERT INTO LTN_DEVELOP.PAIRS (E1_ID, E2_ID, USER_DOC_ID, DDI, TYPE_ID, LABEL) "
                       "VALUES (?, ?, ?, ?, ?, ?)", relation_tuples)
    get_connection().commit()
    return True


def create_new_user_doc_id(user_id, document_id):
    return str(user_id) + '_' + str(document_id)


def load_user_doc_id(document_id, user_id):
    cursor = get_connection().cursor()
    cursor.execute("SELECT ID FROM LTN_DEVELOP.USER_DOCUMENTS WHERE DOCUMENT_ID = ? AND USER_ID = ?",
                   (document_id, user_id))
    result = cursor.fetchone()
    if result:
        return str(result[0])
    return create_new_user_doc_id(user_id, document_id)


def load_document(document_id, user_id, show_predictions=False):
    cursor = get_connection().cursor()
    result = {}
    print "Loading information for document_id: " + str(document_id) + " and user: " + str(current_user.get_id())
    result['text'] = get_text(cursor, document_id)
    denotations, users, annotation_id_map = get_denotations_and_users(cursor, document_id, user_id, show_predictions)
    result['denotations'] = denotations
    result['relations'] = get_relations(cursor, document_id, user_id, annotation_id_map, show_predictions)
    result['sourceid'] = document_id
    result['config'] = {'entity types': get_entity_types(document_id),
                        'relation types': get_relation_types(document_id),
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
            text = result[0].read()
    except Exception, e:
        print 'Error: ', e
    return text


def get_denotations_and_users(cursor, document_id, user_id, show_predictions):
    current_prediction_user = get_current_prediction_user(user_id, show_predictions)
    cursor.execute('SELECT E.ID, UD.USER_ID, O."START", O."END", T.CODE, TT."LABEL", T.GROUP_ID, '
                   'T."GROUP", E."LABEL", U."NAME", TT.ID '
                   'FROM LTN_DEVELOP.ENTITIES E '
                   'JOIN LTN_DEVELOP.USER_DOCUMENTS UD ON E.USER_DOC_ID = UD.ID AND UD.DOCUMENT_ID = ? '
                   'JOIN LTN_DEVELOP.OFFSETS O ON O.ENTITY_ID = E.ID AND O.USER_DOC_ID = E.USER_DOC_ID '
                   'LEFT OUTER JOIN LTN_DEVELOP.USERS U ON UD.USER_ID = U.ID '
                   'LEFT OUTER JOIN LTN_DEVELOP.TASK_TYPES TT ON E.TYPE_ID = TT.ID '
                   'JOIN LTN_DEVELOP.TYPES T ON TT.TYPE_ID = T.ID '
                   'WHERE UD.VISIBILITY = 1 OR UD.USER_ID = ? OR UD.USER_ID = ? '
                   'ORDER BY E.ID', (document_id, user_id, current_prediction_user))
    denotations = []
    increment = 1
    previous_id = None
    # todo: handle being not logged in
    colors = ['blue', 'navy', 'brown', 'chocolate', 'orange', 'maroon', 'turquoise']
    user_id_mapping = {current_user.get_id(): 0}
    prediction_engine_info = {'name': 'Prediction Engine', 'color': 'gray'}
    current_user_info = {'name': 'You', 'color': '#55AA55'}
    user_info = {0: current_user_info}
    annotation_id_map = {}
    user_offset = 1
    if current_prediction_user != user_id:
        user_info[-1] = prediction_engine_info
        user_id_mapping[current_prediction_user] = -1
        user_offset = 2
    for result in cursor.fetchall():
        denotation = {}
        current_id = str(result[0])
        creator = str(result[1])
        if current_id == previous_id:
            current_id += "_" + str(increment)
            increment += 1
            if previous_id not in annotation_id_map:
                annotation_id_map[previous_id] = {}
            annotation_id_map[previous_id][creator] = current_id
        else:
            increment = 1
        if creator not in user_id_mapping and creator != current_prediction_user:
            new_id = len(user_id_mapping)
            user_info[new_id] = {'name': str(result[9]), 'color': colors[(new_id - user_offset) % len(colors)]}
            user_id_mapping[creator] = new_id

        anno_info = {"code": str(result[4]),
                     "name": str(result[5]),
                     "groupId": str(result[6]),
                     "group": str(result[7]),
                     "label": str(result[8]),
                     "id": result[10]}
        denotation['id'] = current_id
        denotation['obj'] = anno_info
        denotation['span'] = {}
        denotation['span']['begin'] = result[2]
        denotation['span']['end'] = result[3]
        # necessary for split annotations
        denotation['originalId'] = str(result[0])
        denotation['userId'] = user_id_mapping.get(creator)
        denotations.append(denotation)
        previous_id = str(result[0])
    return denotations, user_info, annotation_id_map


def get_relations(cursor, document_id, user_id, annotation_id_map, show_predictions):
    current_prediction_user = get_current_prediction_user(user_id, show_predictions)
    cursor.execute('SELECT P.ID, P.E1_ID, P.E2_ID, P.LABEL, T.CODE, T."NAME", '
                   'T.GROUP_ID, T."GROUP", UD1.USER_ID, TT.ID '
                   'FROM LTN_DEVELOP.PAIRS P '
                   'LEFT OUTER JOIN LTN_DEVELOP.TASK_TYPES TT ON P.TYPE_ID = TT.ID '
                   'JOIN LTN_DEVELOP.TYPES T ON TT.TYPE_ID = T.ID '
                   'JOIN LTN_DEVELOP.ENTITIES E1 ON P.E1_ID = E1.ID AND P.DDI = 1 AND P.USER_DOC_ID = E1.USER_DOC_ID '
                   'JOIN LTN_DEVELOP.ENTITIES E2 ON P.E2_ID = E2.ID AND P.DDI = 1 AND P.USER_DOC_ID = E2.USER_DOC_ID '
                   'JOIN LTN_DEVELOP.USER_DOCUMENTS UD1 ON E1.USER_DOC_ID = UD1.ID AND UD1.DOCUMENT_ID = ? '
                   'AND (UD1.USER_ID = ? OR UD1.USER_ID = ? OR UD1.VISIBILITY = 1) '
                   'JOIN LTN_DEVELOP.USER_DOCUMENTS UD2 ON E2.USER_DOC_ID = UD2.ID AND UD2.DOCUMENT_ID = ? '
                   'AND (UD2.USER_ID = ? OR UD2.USER_ID = ? OR UD2.VISIBILITY = 1)',
                   (document_id, user_id, current_prediction_user,
                    document_id, user_id, current_prediction_user))
    relations = []
    for result in cursor.fetchall():
        type_info = {"id": result[9],
                     "code": str(result[4]),
                     "name": str(result[5]),
                     "groupId": str(result[6]),
                     "group": str(result[7]),
                     "label": str(result[3])}
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
        cursor = get_connection().cursor()
        cursor.execute("DELETE FROM LTN_DEVELOP.PAIRS WHERE USER_DOC_ID IN " + user_document_ids)
        cursor.execute("DELETE FROM LTN_DEVELOP.OFFSETS WHERE USER_DOC_ID IN  " + user_document_ids)
        cursor.execute("DELETE FROM LTN_DEVELOP.ENTITIES WHERE USER_DOC_ID IN " + user_document_ids)
        cursor.execute("DELETE FROM LTN_DEVELOP.USER_DOCUMENTS WHERE ID IN " + user_document_ids)
        get_connection().commit()
        return True
    except Exception, e:
        raise e


def delete_document(document_id):
    try:
        cursor = get_connection().cursor()
        cursor.execute("SELECT ID FROM LTN_DEVELOP.USER_DOCUMENTS WHERE DOCUMENT_ID = ?", (document_id,))
        user_document_ids = map(lambda t: t[0], cursor.fetchall())
        delete_user_documents(user_document_ids)

        sql_to_prepare = 'CALL LTN_DEVELOP.delete_document (?)'
        params = {'DOCUMENT_ID': document_id}
        psid = cursor.prepare(sql_to_prepare)
        ps = cursor.get_prepared_statement(psid)
        cursor.execute_prepared(ps, [params])
        get_connection().commit()

        return True
    except Exception, e:
        raise e


@app.route('/evaluate', methods=['POST'])
def return_entities():
    req = request.get_json()
    document_id = req['document_id']
    user1 = req['user1']
    user2 = req['user2']

    cursor = get_connection().cursor()
    predictions = sorted(get_entities_for_user_document(cursor, document_id, user1), key=lambda x: x.start)
    gold_standard = sorted(get_entities_for_user_document(cursor, document_id, user2), key=lambda x: x.start)

    p = 0
    matches, left_aligns, right_aligns, overlaps, misses, wrong_type = 0, 0, 0, 0, 0, {}

    for entity in gold_standard:
        if len(predictions) == 0:
            misses += 1
            continue
        while predictions[p].end < entity.start:
            if p == len(predictions) - 1:
                break
            p += 1
        can_miss = True
        for candidate in predictions[p:]:
            if candidate.start > entity.end:
                if can_miss:
                    misses += 1
                    can_miss = False
                break
            if candidate.end < entity.start:
                break
            can_miss = False
            if candidate.start != entity.start:
                if candidate.end == entity.end:
                    if candidate.type != entity.type:
                        wrong_type["right-aligns"] = wrong_type.get("right-aligns", 0) + 1
                    right_aligns += 1
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
    user_id = current_user.get_id()
    if user_id is None:
        return "No user is logged in", 401

    req = request.get_json()
    doc_type = req.get('type', TYPE_PLAINTEXT)
    task = req['task']

    documents = []
    if doc_type == TYPE_PLAINTEXT:
        documents.append(req)
    elif doc_type == TYPE_BIOC:
        documents = extract_documents_from_bioc(req['text'], req['document_id'])
    else:
        return "Document type not supported", 400

    for document in documents:
        document_id = document['document_id']
        message, code = create_document_in_database(document_id,
                                                    document['text'],
                                                    int(document.get('visibility', 1)),
                                                    task)
        if code == 201 and doc_type == TYPE_BIOC:
            save_document(document,
                          load_user_doc_id(document_id, user_id),
                          document_id,
                          user_id,
                          int(document.get('visibility', 1)))
        if code != 201:
            return message, code

    return "Successfully imported", 201


def extract_documents_from_bioc(bioc_text, id_prefix):
    string_doc = StringIO.StringIO(bioc_text.encode('utf-8'))
    bioc_collection = bioc.parse(string_doc)
    documents = []
    known_types = dict((t['code'], t) for t in load_types())
    for bioc_doc in bioc_collection.documents:
        doc_text = ''
        passage_count = 0
        denotations = []
        relations = []
        for passage in bioc_doc.passages:
            if passage.infons.get('type') != 'title':
                if len(passage.text) > 0:
                    doc_text += passage.text
                    prefix = 'p' + str(passage_count)
                    passage_count += 1
                    passage_denotations = extract_denotations_from_bioc_object(passage, known_types, prefix)
                    denotations_map = dict(map(lambda d: (d['id'][len(prefix):], d['id']), passage_denotations))
                    passage_relations = extract_relations_from_bioc_object(passage, known_types,
                                                                           prefix, denotations_map)
                    denotations.extend(passage_denotations)
                    relations.extend(passage_relations)
                else:
                    sentence_count = 0
                    if passage.sentences is not None:
                        sentences = passage.sentences
                    else:
                        sentences = passage
                    for sentence in sentences:
                        doc_text += sentence.text
                        prefix = 's' + str(sentence_count)
                        sentence_count += 1
                        sentence_denotations = extract_denotations_from_bioc_object(passage, known_types, prefix)
                        denotations_map = dict(map(lambda d: (d['id'][len(prefix):], d['id']), sentence_denotations))
                        sentence_relations = extract_relations_from_bioc_object(passage, known_types,
                                                                                prefix, denotations_map)
                        denotations.extend(sentence_denotations)
                        relations.extend(sentence_relations)
        document = {
            'document_id': id_prefix + '__' + bioc_doc.id,
            'text': doc_text,
            'denotations': denotations,
            'relations': relations,
        }
        documents.append(document)
    string_doc.close()
    return documents


def extract_denotations_from_bioc_object(bioc_object, known_types, id_prefix):
    denotations = []
    for annotation in bioc_object.annotations:
        denotation = {'id': id_prefix + annotation.id, 'span': {}}
        denotation['span']['begin'] = annotation.locations[0].offset
        denotation['span']['end'] = annotation.locations[0].offset + annotation.locations[0].length
        annotation_info = annotation.infons.values()
        for value in annotation_info:
            umls_type = known_types.get(value, None)
            if umls_type is not None:
                denotation['obj'] = umls_type
                break
        if denotation.get('obj') is None:
            label_guesses = filter(
                lambda x: x[0] == 'label' or (x[1] != 'None' and x[1] is not None and x[1] != 'undefined'),
                annotation.infons.iteritems())
            if len(label_guesses) > 0:
                denotation['obj'] = {'label': label_guesses[0][1]}
        denotations.append(denotation)

    return denotations


def extract_relations_from_bioc_object(bioc_object, known_types, id_prefix, denotations):
    relations = []
    for bRelation in bioc_object.relations:
        nodes = list(bRelation.nodes)
        subj_id = denotations.get(nodes[0].refid, None)
        obj_id = denotations.get(nodes[1].refid, None)
        if subj_id is not None and obj_id is not None:
            relation_type = None
            bRelationInfons = bRelation.infons.values()
            for value in bRelationInfons:
                relation_type = known_types.get(value, None)
                if relation_type is not None:
                    break
            relation = {'id': id_prefix + bRelation.id,
                        'subj': subj_id,
                        'obj': obj_id,
                        'pred': relation_type
                        }
            if relation_type is None:
                label_guesses = filter(
                    lambda x: x[0] == 'label' or (x[1] != 'None' and x[1] is not None and x[1] != 'undefined'),
                    bRelation.infons.iteritems())
                if len(label_guesses) > 0:
                    relation['pred'] = {'label': label_guesses[0][1]}
            relations.append(relation)
    return relations


def create_document_in_database(document_id, document_text, document_visibility, task):
    cursor = get_connection().cursor()
    cursor.execute("SELECT COUNT(*) FROM LTN_DEVELOP.DOCUMENTS WHERE ID = ?", (document_id,))
    result = cursor.fetchone()
    if result[0] != 0:
        return "A document with the ID '%s' already exists" % (document_id,), 409

    sql_to_prepare = 'CALL LTN_DEVELOP.add_document (?, ?, ?)'
    params = {
        'DOCUMENT_ID': document_id,
        'DOCUMENT_TEXT': document_text.replace("'", "''"),
        'TASK': task
    }
    psid = cursor.prepare(sql_to_prepare)
    ps = cursor.get_prepared_statement(psid)
    cursor.execute_prepared(ps, [params])
    get_connection().commit()

    cursor.execute("INSERT INTO LTN_DEVELOP.USER_DOCUMENTS VALUES (?, ?, ?, ?, ?, ?)",
                   (create_new_user_doc_id(current_user.get_id(), document_id), current_user.get_id(), document_id,
                    document_visibility, datetime.now(), datetime.now()))
    get_connection().commit()
    return "Successfully imported", 201


def create_bioc_document_from_document_json(document):
    b_document = bioc.BioCDocument()
    b_document.id = document['sourceid']
    passage = bioc.BioCPassage()
    passage.text = document['text']
    passage.offset = 0
    annotation_user_map = {}
    for denotation in document['denotations']:
        annotation_user_map[denotation['id']] = denotation['userId']
        if denotation['userId'] != 0:
            continue
        annotation = bioc.BioCAnnotation()
        annotation.id = denotation['id']
        location = bioc.BioCLocation(0, 0)
        location.offset = denotation['span']['begin']
        location.length = denotation['span']['end'] - denotation['span']['begin']
        annotation.locations.append(location)
        annotation.text = document['text'][denotation['span']['begin']:denotation['span']['end']]
        annotation.infons = denotation['obj']
        passage.add_annotation(annotation)
    for relation in document['relations']:
        subj_from_current_user = annotation_user_map[relation['subj']] == 0
        obj_from_current_user = annotation_user_map[relation['obj']] == 0
        if not (subj_from_current_user and obj_from_current_user):
            continue
        b_relation = bioc.BioCRelation()
        b_relation.id = relation['id']
        start_node = bioc.BioCNode('', '')
        end_node = bioc.BioCNode('', '')
        start_node.refid = relation['subj']
        end_node.refid = relation['obj']
        b_relation.add_node(start_node)
        b_relation.add_node(end_node)
        b_relation.infons = relation['pred']
        passage.add_relation(b_relation)
    b_document.add_passage(passage)
    return b_document


def prediction_user_for_user(user_id):
    return user_id + '__predictor'


def get_current_prediction_user(user_id, show_predictions):
    if show_predictions:
        return prediction_user_for_user(user_id)
    else:
        return user_id


def respond_with(response):
    return Response(json.dumps(response), mimetype='application/json')