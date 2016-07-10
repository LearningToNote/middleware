from flask import request
from flask_login import current_user

from datetime import datetime
from collections import namedtuple

from pyhdb import DatabaseError

from ltnserver import app, reset_connection, get_connection, respond_with, execute_prepared
from ltnserver.training import model_training_queue
from ltnserver.types import get_entity_types, get_relation_types, TaskType
from ltnserver.user import User

Entity = namedtuple('Entity', ['id', 'user_id', 'start', 'end', 'label', 'type_id'])
Relation = namedtuple('Relation', ['id', 'user_id', 'e1_id', 'e2_id', 'ddi', 'label', 'type_id'])


class UserDocument:

    def __init__(self, user_document_id, document_id, user_id, entities, relations, visible,
                 created_at=datetime.now(), updated_at=datetime.now()):
        self.document_id = document_id
        self.user_id = user_id
        self.entities = entities or dict()
        self.relations = relations or dict()
        self.visible = visible
        self.created_at = created_at
        self.updated_at = updated_at
        if user_document_id is not None:
            self.id = user_document_id
        else:
            self.id = "%s_%s" % (user_id, document_id)

    def get_summary(self):
        return {'id': self.id, 'entities': len(self.entities), 'pairs': len(self.relations), 'visible': self.visible,
                'user_id': self.user_id, 'user_name': User.get(self.user_id).name,
                'from_current_user': self.user_id == current_user.get_id(),
                'created_at': self.created_at, 'updated_at': self.updated_at}

    def save(self, save_annotations=True):
        cursor = get_connection().cursor()
        if UserDocument.exists(self.id):
            cursor.execute("UPDATE LTN_DEVELOP.USER_DOCUMENTS "
                           "SET visibility = ?, updated_at = ?, user_id = ? "
                           "WHERE id = ?", (int(self.visible), datetime.now(), self.user_id, self.id))
        else:
            cursor.execute("INSERT INTO LTN_DEVELOPMENT.USER_DOCUMENTS VALUES (?, ?, ?, ?, ?, ?)",
                           (self.id, self.user_id, self.document_id, int(self.visible),
                            self.created_at, self.updated_at))
        get_connection().commit()
        if save_annotations:
            self.save_entities()
            self.save_relations()

    def save_entities(self):
        cursor = get_connection().cursor()
        cursor.execute("DELETE FROM LTN_DEVELOP.ENTITIES WHERE USER_DOC_ID = ?", (self.id,))
        cursor.execute("DELETE FROM LTN_DEVELOP.OFFSETS WHERE USER_DOC_ID = ?", (self.id,))

        entities, offsets = list(), list()
        for entity in self.entities:
            entities.append((entity.id, self.id, entity.type_id, entity.label))
            offsets.append((entity.start, entity.end, entity.id, self.id))

        cursor.executemany("INSERT INTO LTN_DEVELOP.ENTITIES (ID, USER_DOC_ID, TYPE_ID, LABEL) "
                           "VALUES (?, ?, ?, ?)", entities)
        cursor.executemany("INSERT INTO LTN_DEVELOP.OFFSETS VALUES (?, ?, ?, ?)", offsets)
        get_connection().commit()

    def save_relations(self):
        cursor = get_connection().cursor()
        cursor.execute("DELETE FROM LTN_DEVELOP.PAIRS WHERE USER_DOC_ID = ?", (self.id,))
        relation_tuples = list()
        for relation in self.relations:
            if TaskType.exists(relation.type_id) and relation.e1_id and relation.e2_id:
                relation_tuples.append((relation.e1_id, relation.e2_id, self.id,
                                        relation.ddi, relation.type_id, relation.label))
        cursor.executemany("INSERT INTO LTN_DEVELOP.PAIRS (E1_ID, E2_ID, USER_DOC_ID, DDI, TYPE_ID, LABEL) "
                           "VALUES (?, ?, ?, ?, ?, ?)", relation_tuples)
        get_connection().commit()

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
        cursor.execute('SELECT P.ID, UD1.USER_ID, P.E1_ID, P.E2_ID, P.DDI, P.LABEL, TT.ID '
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
    user_document.save(save_annotations=False)
    return "OK", 200


@app.route('/user_documents/<user_document_id>', methods=['DELETE'])
def manage_user_documents(user_document_id):
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
            if user_document is None:
                user_document = UserDocument(None, document_id, current_user.get_id(), [], [], False)
            return respond_with(textae_document_from(user_document))
        except DatabaseError:
            reset_connection()
            return 'Error while loading the document.', 500
    if request.method == 'POST':
        try:
            save_textae_document(request.get_json(), user_document.id, document.id, current_user.get_id(),
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


def save_textae_document(data, user_doc_id, document_id, user_id, task_id, is_visible=True):
    if not UserDocument.exists(user_doc_id):
        user_document = UserDocument(user_doc_id, document_id, user_id, [], [], is_visible)
        user_document.save(save_annotations=False)
        print "Created new UserDocument"
    else:
        user_document = UserDocument.by_id(user_doc_id)
        print "Used existing UserDocument"

    # only save annotations from the current user, defined as userId 0 at loading time
    annotations = filter(lambda a: a.get('userId', 0) == 0, data['denotations'])
    save_annotations(user_document, annotations)
    id_map = {}
    # necessary, as TextAE does not create "originalId"s
    for annotation in annotations:
        id_map[annotation.get('id')] = annotation.get('originalId', annotation.get('id'))

    save_relations(user_document, data['relations'], id_map)
    model_training_queue.add(task_id)
    return True


def save_annotations(user_document, annotations):
    entities = list()
    for annotation in annotations:
        entities.append(Entity(annotation.get('originalId', annotation.get('id')),
                               user_document.user_id,
                               annotation['span']['begin'],
                               annotation['span']['end'],
                               annotation['obj']['label'],
                               annotation['obj']['id']))
    user_document.entities = entities
    user_document.save_entities()


def save_relations(user_document, relations, id_map):
    new_relations = list()
    for relation in relations:
        r_subject, r_predicate, r_object = relation.get('subj'), relation.get('pred'), relation.get('obj')
        if id_map.get(r_subject) is not None and id_map.get(r_object) is not None:
            new_relations.append(Relation(relation.get('id'),
                                          user_document.user_id,
                                          id_map.get(r_subject),
                                          id_map.get(r_object),
                                          True,
                                          r_predicate.get('label', None),
                                          r_predicate.get('id')))
    user_document.relations = new_relations
    user_document.save_relations()


def create_new_user_doc_id(user_id, document_id):
    return str(user_id) + '_' + str(document_id)


def textae_document_from(user_document, show_predictions=False):
    result = {'text': user_document.document().text}
    denotations, users, annotation_id_map = get_denotations_and_users(user_document, show_predictions)
    result['denotations'] = denotations
    result['relations'] = get_relations(user_document, annotation_id_map, show_predictions)
    result['sourceid'] = user_document.document_id
    result['config'] = {'entity types': get_entity_types(user_document.document_id),
                        'relation types': get_relation_types(user_document.document_id),
                        'users': users}
    return result


def get_denotations_and_users(user_document, show_predictions):
    from ltnserver.prediction import get_current_prediction_user
    current_prediction_user = get_current_prediction_user(user_document.user_id, show_predictions)
    document = user_document.document()
    entities = []
    for ud in document.get_user_documents():
        if ud.visible or ud.user_id in [user_document.user_id, current_prediction_user]:
            entities.extend(ud.entities)
    denotations = []
    increment = 1
    previous_id = None
    # todo: handle being not logged in
    colors = ['blue', 'navy', 'brown', 'chocolate', 'orange', 'maroon', 'turquoise']
    user_id_mapping = {user_document.user_id: 0}
    prediction_engine_info = {'name': 'Prediction Engine', 'color': 'gray'}
    current_user_info = {'name': 'You', 'color': '#55AA55'}
    user_info = {0: current_user_info}
    annotation_id_map = {}
    user_offset = 1
    if current_prediction_user != user_document.user_id:
        user_info[-1] = prediction_engine_info
        user_id_mapping[current_prediction_user] = -1
        user_offset = 2
    for entity in entities:
        denotation = {}
        current_id = str(entity.id)
        creator = str(entity.user_id)
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
            user_info[new_id] = {'name': str(User.get(entity.user_id).name), 'color': colors[(new_id - user_offset) % len(colors)]}
            user_id_mapping[creator] = new_id

        task_type = TaskType.by_id(entity.type_id)
        # the bioc library expects all attributes to be strings (and TextAE doesn't care)
        anno_info = {"code": str(task_type.code),
                     "name": str(task_type.name),
                     "groupId": str(task_type.group_id),
                     "group": str(task_type.group),
                     "label": str(task_type.label),
                     "id": str(task_type.task_type_id)}
        denotation['id'] = current_id
        denotation['obj'] = anno_info
        denotation['span'] = {}
        denotation['span']['begin'] = entity.start
        denotation['span']['end'] = entity.end
        # necessary for split annotations
        denotation['originalId'] = str(entity.id)
        denotation['userId'] = user_id_mapping.get(creator)
        denotations.append(denotation)
        previous_id = str(entity.id)
    return denotations, user_info, annotation_id_map


def get_relations(user_document, annotation_id_map, show_predictions):
    from ltnserver.prediction import get_current_prediction_user
    current_prediction_user = get_current_prediction_user(user_document.user_id, show_predictions)
    document = user_document.document()
    annotations = []
    for ud in document.get_user_documents():
        if ud.visible or ud.user_id in [user_document.user_id, current_prediction_user]:
            annotations.extend(ud.relations)
    relations = []
    for rel in annotations:
        task_type = TaskType.by_id(rel.type_id)
        # the bioc library expects all attributes to be strings (and TextAE doesn't care)
        type_info = {"id": str(task_type.task_type_id),
                     "code": str(task_type.code),
                     "name": str(task_type.name),
                     "groupId": str(task_type.group_id),
                     "group": str(task_type.group),
                     "label": str(task_type.label)}
        relation = {}
        subj = str(rel.e1_id)
        obj = str(rel.e2_id)
        replacement_subj = annotation_id_map.get(subj)
        replacement_obj = annotation_id_map.get(obj)
        current_user_id = str(rel.user_id)
        if replacement_subj is not None:
            if replacement_subj.get(current_user_id) is not None:
                subj = replacement_subj.get(current_user_id)
        if replacement_obj is not None:
            if replacement_obj.get(current_user_id) is not None:
                obj = replacement_obj.get(current_user_id)
        relation['id'] = str(rel.id)
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
