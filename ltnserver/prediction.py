import json

from flask import request
from flask_login import current_user

from ltnserver import app, get_connection, respond_with
from ltnserver.server import load_user_doc_id, delete_user_document, save_document, load_document

PREDICT_ENTITIES = 'entities'
PREDICT_RELATIONS = 'relations'


def prediction_user_for_user(user_id):
    return user_id + '__predictor'


def get_current_prediction_user(user_id, show_predictions):
    if show_predictions:
        return prediction_user_for_user(user_id)
    else:
        return user_id


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
