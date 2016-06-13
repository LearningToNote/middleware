import sys
import os

from ltnserver import app, init, init_training, context

if __name__ == '__main__':
    debug = True
    if len(sys.argv) >= 3:
        debug = sys.argv[2] in ['true', 'True', '1', 'y', 'yes']
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        init()
    else:
        # This ensures that we can join the thread on exit
        # as flask does not wait on exit for its child processes to gracefully quit
        # unfortunately this means that changes to the code that runs in the
        # training thread cannot be reloaded with flask
        init_training()
    app.run(host='0.0.0.0', port=8080, debug=debug, ssl_context=context)
