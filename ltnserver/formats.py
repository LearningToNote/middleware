import StringIO

import bioc
from flask import request, Response
from flask_login import current_user
from metapub import PubMedFetcher
from metapub.exceptions import InvalidPMID
from pyhdb import DatabaseError

from ltnserver import app, respond_with
from ltnserver.documents import create_new_user_doc_id, save_textae_document, Document, UserDocument
from ltnserver.types import get_task_types, TaskType

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
            save_textae_document(UserDocument(None, document_id, user_id, [], [], int(document.get('visibility', 0))),
                                 document)
        if code != 201:
            return message, code

    return "Successfully imported", 201


def extract_documents_from_bioc(bioc_text, id_prefix, task):
    string_doc = StringIO.StringIO(bioc_text.encode('utf-8'))
    bioc_collection = bioc.parse(string_doc)
    documents = []
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
                    passage_denotations = extract_denotations_from_bioc_object(passage, task, prefix)
                    denotations_map = dict(map(lambda d: (d['id'][len(prefix):], d['id']), passage_denotations))
                    passage_relations = extract_relations_from_bioc_object(passage, task, prefix, denotations_map)
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
                        sentence_denotations = extract_denotations_from_bioc_object(passage, task, prefix)
                        denotations_map = dict(map(lambda d: (d['id'][len(prefix):], d['id']), sentence_denotations))
                        sentence_relations = extract_relations_from_bioc_object(passage, task, prefix, denotations_map)
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


def extract_denotations_from_bioc_object(bioc_object, task, id_prefix):
    denotations = []
    known_types = dict((t['code'], t) for t in get_task_types(task, relation=False))
    unknown_types = dict()
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
                lambda x: x[0] in ['label', 'type'] or (x[1] != 'None' and x[1] is not None and x[1] != 'undefined'),
                annotation.infons.iteritems())
            if len(label_guesses) > 0:
                label = label_guesses[0][1]
                if label in unknown_types:
                    new_type = unknown_types.get(label)
                else:
                    new_type = TaskType(TaskType.GENERATE_NEW_ID, None, None, None, label, None, None, task)
                    new_type.save()
                    unknown_types[label] = new_type
                denotation['obj'] = {'label': label, 'id': new_type.task_type_id}
        denotations.append(denotation)
    return denotations


def extract_relations_from_bioc_object(bioc_object, task, id_prefix, denotations):
    relations = []
    known_types = dict((t['code'], t) for t in get_task_types(task, relation=True))
    unknown_types = dict()
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
                    lambda x: x[0] in ['label', 'type'] or (x[1] != 'None' and x[1] is not None and x[1] != 'undefined'),
                    b_relation.infons.iteritems())
                if len(label_guesses) > 0:
                    label = label_guesses[0][1]
                    if label in unknown_types:
                        new_type = unknown_types.get(label)
                    else:
                        new_type = TaskType(TaskType.GENERATE_NEW_ID, None, None, None, label, None, None, task, True)
                        new_type.save()
                        unknown_types[label] = new_type
                    relation['pred'] = {'label': label, 'id': new_type.task_type_id}
            relations.append(relation)
    return relations


def create_document_in_database(document_id, document_text, document_visibility, task):
    document = Document(document_id, task, document_text)
    try:
        document.save()
        user_document = UserDocument(None, document_id, current_user.get_id(), None, None, document_visibility)
        user_document.save(save_annotations=False)
        return "Successfully imported", 201
    except NotImplementedError:
        return "A document with the ID '%s' already exists" % (document_id,), 409
    except DatabaseError, e:
        return "Database Error: '%s'" % (e.message,), 500


@app.route('/export/<document_id>', methods=['GET'])
def export(document_id):
    document = Document.by_id(document_id)
    user_id = request.args.get('user_id', None)
    if user_id is None:
        user_ids_to_export = document.get_users()
    else:
        user_ids_to_export = [user_id]
    return export_document(document, user_ids_to_export)


def export_document(document, users):
    bcollection = bioc.BioCCollection()
    for user_id in users:
        user_document = document.user_documents.get(user_id)
        bdocument = create_bioc_document_from(user_document)
        bcollection.add_document(bdocument)
    result = bcollection.tobioc()
    response = Response(result, mimetype='text/xml')
    response.headers["Content-Disposition"] = "attachment; filename=" + document.id + ".xml"
    return response


def create_bioc_document_from(user_document):
    b_document = bioc.BioCDocument()
    b_document.id = str(user_document.id)
    passage = create_bioc_passage_from(user_document)
    b_document.add_passage(passage)
    return b_document


def create_bioc_passage_from(user_document):
    passage = bioc.BioCPassage()
    passage.text = user_document.document().text
    passage.offset = 0
    for entity in user_document.entities:
        passage.add_annotation(create_bioc_annotation_from(entity))
    for relation in user_document.relations:
        passage.add_relation(create_bioc_relation_from(relation))
    return passage


def create_bioc_annotation_from(entity):
    annotation = bioc.BioCAnnotation()
    annotation.id = str(entity.id)
    annotation.add_location(bioc.BioCLocation(entity.start, entity.end - entity.start))
    type = TaskType.by_id(entity.type_id)
    annotation.infons = {
        "user": str(entity.user_id),
        "type": str(type.task_type_id),
        "umls": str(type.code),
        "label": str(entity.label)
    }
    return annotation


def create_bioc_relation_from(pair):
    relation = bioc.BioCRelation()
    relation.id = str(pair.id)
    relation.add_node(bioc.BioCNode(str(pair.e1_id), ''))
    relation.add_node(bioc.BioCNode(str(pair.e2_id), ''))
    type = TaskType.by_id(pair.type_id)
    relation.infons = {
        "user": str(pair.user_id),
        "type": str(type.task_type_id),
        "umls": str(type.code),
        "label": str(pair.label),
        "ddi": str(pair.ddi)
    }
    return relation
