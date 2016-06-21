import bioc
import StringIO

from datetime import datetime

from flask import request, Response
from flask_login import current_user

from metapub import PubMedFetcher
from metapub.exceptions import InvalidPMID

from ltnserver import app, get_connection, respond_with
from ltnserver.documents import create_new_user_doc_id, save_document, load_user_doc_id, load_document
from ltnserver.types import get_task_types

TYPE_PLAINTEXT = 'plaintext'
TYPE_BIOC = 'bioc'


@app.route('/pubmed/<pubmed_id>')
def fetch_pubmed_abstract(pubmed_id):
    try:
        article = PubMedFetcher(cachedir=".cache/").article_by_pmid(pubmed_id)
        return respond_with(article.abstract)
    except InvalidPMID:
        return 'Invalid PubmedID', 500


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
        documents = extract_documents_from_bioc(req['text'], req['document_id'], task)
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


def extract_documents_from_bioc(bioc_text, id_prefix, task):
    string_doc = StringIO.StringIO(bioc_text.encode('utf-8'))
    bioc_collection = bioc.parse(string_doc)
    documents = []
    known_types = dict((t['code'], t) for t in get_task_types(task, relation=False))
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
    for b_relation in bioc_object.relations:
        nodes = list(b_relation.nodes)
        subj_id = denotations.get(nodes[0].refid, None)
        obj_id = denotations.get(nodes[1].refid, None)
        if subj_id is not None and obj_id is not None:
            relation_type = None
            b_relation_infons = b_relation.infons.values()
            for value in b_relation_infons:
                relation_type = known_types.get(value, None)
                if relation_type is not None:
                    break
            relation = {'id': id_prefix + b_relation.id,
                        'subj': subj_id,
                        'obj': obj_id,
                        'pred': relation_type
                        }
            if relation_type is None:
                label_guesses = filter(
                    lambda x: x[0] == 'label' or (x[1] != 'None' and x[1] is not None and x[1] != 'undefined'),
                    b_relation.infons.iteritems())
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


@app.route('/export/<document_id>', methods=['GET'])
def export(document_id):
    user_id = request.args.get('user_id', current_user.get_id())
    document = load_document(document_id, user_id)
    bcollection = bioc.BioCCollection()
    bdocument = create_bioc_document_from_document_json(document)
    bcollection.add_document(bdocument)
    result = bcollection.tobioc()
    response = Response(result, mimetype='text/xml')
    response.headers["Content-Disposition"] = "attachment; filename=" + document_id + ".xml"
    return response
