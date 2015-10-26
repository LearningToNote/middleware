from flask import Flask
from flask.ext.cors import CORS


app = Flask(__name__)
CORS(app)


@app.route('documents/')
def get_documents():
    pass


@app.route('/documents/<id>')
def get_document(id):
    pass


if __name__ == '__main__':
    app.run(port=4242, debug=True)