from flask import request
from flask_login import current_user

from datetime import datetime

from ltnserver import app, try_reconnecting, reset_connection, get_connection, respond_with
from ltnserver.training import model_training_queue
from ltnserver.types import get_entity_types, get_relation_types


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
    print "Loading information for document_id: '%s' and user: '%s'" % (document_id, user_id)
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
    from ltnserver.prediction import get_current_prediction_user
    current_prediction_user = get_current_prediction_user(user_id, show_predictions)
    cursor.execute('SELECT E.ID, UD.USER_ID, O."START", O."END", T.CODE, TT."LABEL", T.GROUP_ID, '
                   'T."GROUP", E."LABEL", U."NAME", TT.ID '
                   'FROM LTN_DEVELOP.ENTITIES E '
                   'JOIN LTN_DEVELOP.USER_DOCUMENTS UD ON E.USER_DOC_ID = UD.ID AND UD.DOCUMENT_ID = ? '
                   'JOIN LTN_DEVELOP.OFFSETS O ON O.ENTITY_ID = E.ID AND O.USER_DOC_ID = E.USER_DOC_ID '
                   'LEFT OUTER JOIN LTN_DEVELOP.USERS U ON UD.USER_ID = U.ID '
                   'LEFT OUTER JOIN LTN_DEVELOP.TASK_TYPES TT ON E.TYPE_ID = TT.ID '
                   'LEFT OUTER JOIN LTN_DEVELOP.TYPES T ON TT.TYPE_ID = T.ID '
                   'WHERE UD.VISIBILITY = 1 OR UD.USER_ID = ? OR UD.USER_ID = ? '
                   'ORDER BY E.ID', (document_id, user_id, current_prediction_user))
    denotations = []
    increment = 1
    previous_id = None
    # todo: handle being not logged in
    colors = ['blue', 'navy', 'brown', 'chocolate', 'orange', 'maroon', 'turquoise']
    user_id_mapping = {user_id: 0}
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

        # the bioc library expects all attributes to be strings (and TextAE doesn't care)
        anno_info = {"code": str(result[4]),
                     "name": str(result[5]),
                     "groupId": str(result[6]),
                     "group": str(result[7]),
                     "label": str(result[8]),
                     "id": str(result[10])}
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
    from ltnserver.prediction import get_current_prediction_user
    current_prediction_user = get_current_prediction_user(user_id, show_predictions)
    cursor.execute('SELECT P.ID, P.E1_ID, P.E2_ID, P.LABEL, T.CODE, TT.LABEL, '
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
        # the bioc library expects all attributes to be strings (and TextAE doesn't care)
        type_info = {"id": str(result[9]),
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


def get_associated_users(document_id):
    cursor = get_connection().cursor()
    cursor.execute("SELECT DISTINCT ud.user_id "
                   "FROM LTN_DEVELOP.USER_DOCUMENTS ud "
                   "JOIN LTN_DEVELOP.DOCUMENTS d ON ud.document_id = d.id")
    users = map(lambda row: row[0], cursor.fetchall())
    cursor.close()
    return users


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
