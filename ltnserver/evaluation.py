from collections import namedtuple
from flask import request

from ltnserver import app, get_connection, respond_with


Entity = namedtuple('Entity', ['id', 'user_doc_id', 'type', 'start', 'end'])


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
