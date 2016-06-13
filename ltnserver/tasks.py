import random

from flask import request
from flask_login import current_user
from ltnserver import app, respond_with, get_connection


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
