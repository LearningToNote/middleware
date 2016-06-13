import time

from threading import Thread

from ltnserver import get_connection


should_continue = True
model_thread = None
model_training_queue = set()


def init():
    global model_thread
    model_thread = Thread(target=call_start_training)
    model_thread.start()


def call_start_training():
    global should_continue
    while True:
        if not should_continue:
            break
        try:
            global model_training_queue
            task_id = model_training_queue.pop()
        except KeyError:
            time.sleep(10)
            continue

        cursor = get_connection().cursor()
        sql_to_prepare = 'CALL LTN_DEVELOP.LTN_TRAIN (?)'
        params = {
            'TASK_ID': task_id
        }
        psid = cursor.prepare(sql_to_prepare)
        ps = cursor.get_prepared_statement(psid)

        try:
            cursor.execute_prepared(ps, [params])
            get_connection().commit()
        except Exception, e:
            print 'Error: ', e
        finally:
            cursor.close()
