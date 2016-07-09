from flask import request
from flask_login import current_user

from datetime import datetime
from collections import namedtuple

from pyhdb import DatabaseError

from ltnserver import app, reset_connection, get_connection, respond_with, execute_prepared
from ltnserver.training import model_training_queue
from ltnserver.types import get_entity_types, get_relation_types
from ltnserver.user import User

Entity = namedtuple('Entity', ['id', 'user_id', 'start', 'end', 'label', 'type_id'])
Relation = namedtuple('Relation', ['id', 'e1_id', 'e2_id', 'label', 'type_id'])


class UserDocument:

    def __init__(self, user_document_id, document_id, user_id, entities, relations, visible):
        self.document_id = document_id
        self.user_id = user_id
        self.entities = entities or dict()
        self.relations = relations or dict()
        self.visible = visible
        if user_document_id is not None:
            self.id = user_document_id
        else:
            self.id = "%s_%s" % (user_id, document_id)

    def get_summary(self):
        return {'id': self.id, 'entities': len(self.entities), 'pairs': len(self.relations), 'visible': self.visible,
                'user_id': self.user_id, 'user_name': User.get(self.user_id, get_connection().cursor()).name,
                'from_current_user': self.user_id == current_user.get_id()}

    def save(self):
        if UserDocument.exists(self.id):
            cursor = get_connection().cursor()
            cursor.execute("UPDATE LTN_DEVELOP.USER_DOCUMENTS "
                           "SET visibility = ? "
                           "WHERE id = ?", (int(self.visible), self.id))
            get_connection().commit()
        else:
            pass

    def delete(self):
        UserDocument.fail_if_not_exists(self.id)
        delete_user_document(self.id)

    def document(self):
        return Document.by_id(self.document_id)

    @classmethod
    def by_id(cls, user_document_id):
        UserDocument.fail_if_not_exists(user_document_id)
        cursor = get_connection().cursor()
        cursor.execute("SELECT user_id, document_id, visibility "
                       "FROM LTN_DEVELOP.USER_DOCUMENTS WHERE id = ?", (user_document_id,))
        user_id, document_id, visibility = cursor.fetchone()
        entities = UserDocument.get_entities(user_document_id)
        relations = UserDocument.get_relations(user_document_id)
        return UserDocument(user_document_id, document_id, user_id, entities, relations, bool(visibility))

    @classmethod
    def exists(cls, user_document_id):
        cursor = get_connection().cursor()
        cursor.execute("SELECT COUNT(*) FROM LTN_DEVELOP.USER_DOCUMENTS WHERE ID = ?", (user_document_id,))
        return cursor.fetchone()[0] != 0

    @classmethod
    def fail_if_not_exists(cls, user_document_id):
        if not UserDocument.exists(user_document_id):
            raise KeyError("UserDocument '%s' does not exist.", (user_document_id,))

    @classmethod
    def get_entities(cls, user_document_id):
        UserDocument.fail_if_not_exists(user_document_id)
        cursor = get_connection().cursor()
        cursor.execute('SELECT E.ID, UD.USER_ID, O."START", O."END", E.LABEL, TT.ID '
                       'FROM LTN_DEVELOP.ENTITIES E '
                       'JOIN LTN_DEVELOP.OFFSETS O ON O.ENTITY_ID = E.ID AND O.USER_DOC_ID = E.USER_DOC_ID '
                       'JOIN LTN_DEVELOP.USER_DOCUMENTS UD ON UD.ID = E.USER_DOC_ID '
                       'LEFT OUTER JOIN LTN_DEVELOP.TASK_TYPES TT ON E.TYPE_ID = TT.ID '
                       'WHERE UD.ID = ? '
                       'ORDER BY E.ID', (user_document_id,))
        return [Entity(*t) for t in cursor.fetchall()]

    @classmethod
    def get_relations(cls, user_document_id):
        UserDocument.fail_if_not_exists(user_document_id)
        cursor = get_connection().cursor()
        cursor.execute('SELECT P.ID, P.E1_ID, P.E2_ID, P.LABEL, TT.ID '
                       'FROM LTN_DEVELOP.PAIRS P '
                       'LEFT OUTER JOIN LTN_DEVELOP.TASK_TYPES TT ON P.TYPE_ID = TT.ID '
                       'JOIN LTN_DEVELOP.ENTITIES E1 '
                       '  ON P.E1_ID = E1.ID AND P.DDI = 1 AND P.USER_DOC_ID = E1.USER_DOC_ID '
                       'JOIN LTN_DEVELOP.ENTITIES E2 '
                       '  ON P.E2_ID = E2.ID AND P.DDI = 1 AND P.USER_DOC_ID = E2.USER_DOC_ID '
                       'JOIN LTN_DEVELOP.USER_DOCUMENTS UD1 ON E1.USER_DOC_ID = UD1.ID '
                       'JOIN LTN_DEVELOP.USER_DOCUMENTS UD2 ON E2.USER_DOC_ID = UD2.ID '
                       'WHERE UD1.ID = ?', (user_document_id,))
        return [Relation(*t) for t in cursor.fetchall()]


class Document:

    def __init__(self, document_id, task, text):
        self.id = document_id
        self.user_documents = dict()
        self.task = task
        self.text = text
        self.update_user_documents()

    def save(self):
        if Document.exists(self.id):
            raise NotImplementedError("Updating existing documents is not supported.")
        else:
            params = {
                'DOCUMENT_ID': self.id,
                'DOCUMENT_TEXT': self.text.replace("'", "''"),
                'TASK': self.task
            }
            execute_prepared('CALL LTN_DEVELOP.add_document (?, ?, ?)', params, commit=True)

    def delete(self):
        Document.fail_if_not_exists(self.id)
        self.delete_user_documents()
        execute_prepared('CALL LTN_DEVELOP.delete_document (?)', {'DOCUMENT_ID': self.id}, commit=True)

    def delete_user_documents(self):
        for user_document in self.get_user_documents():
            user_document.delete()
        self.user_documents = dict()

    def get_users(self):
        return self.user_documents.keys()

    def get_user_documents(self):
        return self.user_documents.values()

    def update_user_documents(self):
        self.user_documents = dict()
        cursor = get_connection().cursor()
        cursor.execute('SELECT id FROM LTN_DEVELOP.USER_DOCUMENTS WHERE document_id = ?', (self.id,))
        for row in cursor.fetchall():
            user_document = UserDocument.by_id(row[0])
            self.user_documents[user_document.user_id] = user_document

    @classmethod
    def by_id(cls, document_id):
        Document.fail_if_not_exists(document_id)
        cursor = get_connection().cursor()
        cursor.execute('SELECT task FROM LTN_DEVELOP.DOCUMENTS WHERE id = ?', (document_id,))
        task = cursor.fetchone()[0]
        text = Document.get_text_for(document_id)
        return Document(document_id, task, text)

    @classmethod
    def exists(cls, document_id):
        cursor = get_connection().cursor()
        cursor.execute("SELECT COUNT(*) FROM LTN_DEVELOP.DOCUMENTS WHERE ID = ?", (document_id,))
        return cursor.fetchone()[0] != 0

    @classmethod
    def fail_if_not_exists(cls, document_id):
        if not Document.exists(document_id):
            raise KeyError("Document '%s' does not exist.", (document_id,))

    @classmethod
    def get_text_for(cls, document_id):
        text = None
        params = {
            'DOCUMENT_ID': document_id,
            'TEXT': ''
        }
        result = execute_prepared('CALL LTN_DEVELOP.get_document_content (?, ?)', params).fetchone()
        if result:
            text = result[0].read()
        return text


@app.route('/user_documents_for/<document_id>')
def get_document_details(document_id):
    document = Document.by_id(document_id)
    user_documents = filter(lambda d: d.visible or d.user_id == current_user.get_id(), document.get_user_documents())
    user_documents = map(lambda d: d.get_summary(), user_documents)
    return respond_with(user_documents)


@app.route('/userdoc_visibility/<user_doc_id>', methods=['POST'])
def save_userdoc_visibility(user_doc_id):
    user_document = UserDocument.by_id(user_doc_id)
    user_document.visible = request.get_json()['visible']
    user_document.save()
    return "OK", 200


@app.route('/user_documents/<user_document_id>', methods=['DELETE'])
def manage_user_documents(user_document_id):
    if request.method == 'DELETE':
        try:
            UserDocument.by_id(user_document_id).delete()
            return 'Deleted.', 200
        except KeyError:
            return 'The user document does not exist.', 500


@app.route('/documents/<document_id>', methods=['GET', 'POST', 'DELETE'])
def get_document(document_id):
    document = Document.by_id(document_id)
    user_document = document.user_documents.get(current_user.get_id())
    if request.method == 'GET':
        try:
            result = load_document(user_document)
            return respond_with(result)
        except DatabaseError:
            reset_connection()
            return 'Error while loading the document.', 500
    if request.method == 'POST':
        try:
            save_document(request.get_json(), user_document.id, document.id, current_user.get_id(),
                          request.get_json()['task_id'])
            return "Document saved successfully.", 200
        except DatabaseError:
            reset_connection()
            return "An error occurred while saving the document.", 500
    if request.method == 'DELETE':
        try:
            document.delete()
            return 'Deleted.', 200
        except DatabaseError:
            reset_connection()
            return 'Deletion unsuccessful.', 500


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


def load_document(user_document, show_predictions=False):
    cursor = get_connection().cursor()
    result = {}
    print "Loading information for document_id: '%s' and user: '%s'" % (user_document.document_id, user_document.user_id)
    result['text'] = user_document.document().text
    denotations, users, annotation_id_map = get_denotations_and_users(cursor, user_document.document_id, user_document.user_id, show_predictions)
    result['denotations'] = denotations
    result['relations'] = get_relations(cursor, user_document.document_id, user_document.user_id, annotation_id_map, show_predictions)
    result['sourceid'] = user_document.document_id
    result['config'] = {'entity types': get_entity_types(user_document.document_id),
                        'relation types': get_relation_types(user_document.document_id),
                        'users': users}
    cursor.close()
    return result


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
