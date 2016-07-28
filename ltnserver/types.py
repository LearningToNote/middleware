from flask import request
from ltnserver import app, respond_with, get_connection


class TaskType:

    def __init__(self, task_type_id, name, group_id, group, label, code, type_id):
        self.task_type_id = task_type_id
        self.name = name
        self.group_id = group_id
        self.group = group
        self.label = label
        self.code = code
        self.type_id = type_id

    @classmethod
    def by_id(cls, task_type_id):
        TaskType.fail_if_not_exists(task_type_id)
        cursor = get_connection().cursor()
        cursor.execute('SELECT CODE, NAME, GROUP_ID, "GROUP", "LABEL", t.ID, tt.ID '
                       'FROM LTN_DEVELOP.TYPES t '
                       'JOIN LTN_DEVELOP.TASK_TYPES tt ON t.ID = tt.TYPE_ID '
                       'WHERE tt.ID = ?', (task_type_id,))
        row = cursor.fetchone()
        return TaskType(row[6], row[1], row[2], row[3], row[4], row[0], row[5])

    @classmethod
    def exists(cls, task_type_id):
        cursor = get_connection().cursor()
        cursor.execute("SELECT COUNT(*) FROM LTN_DEVELOP.TASK_TYPES WHERE ID = ?", (task_type_id,))
        return cursor.fetchone()[0] != 0

    @classmethod
    def fail_if_not_exists(cls, task_type_id):
        if not TaskType.exists(task_type_id):
            raise KeyError("TaskType '%s' does not exist." % (task_type_id,))


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


def get_base_types():
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
def serve_base_types():
    return respond_with(get_base_types())


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


@app.route('/tasks/<task_id>/entity_types')
def get_task_entity_types(task_id):
    return respond_with(get_task_types(task_id, relation=False))


@app.route('/tasks/<task_id>/relation_types')
def get_task_relation_types(task_id):
    return respond_with(get_task_types(task_id, relation=True))


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
