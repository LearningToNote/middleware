from flask import request
from flask_login import LoginManager, login_user, logout_user, current_user

from ltnserver import app, get_connection, try_reconnecting, reset_connection, respond_with


class User:

    @classmethod
    def get(cls, user_id, cursor):
        cursor.execute("SELECT id, name, token, description, image "
                       "FROM LTN_DEVELOP.Users WHERE id = ?", (user_id,))
        row = cursor.fetchone()
        if row:
            image = row[4]
            if image is not None:
                image = image.read()
            return User(user_id, str(row[1]), str(row[2]), row[3], image)
        else:
            return None

    @classmethod
    def all(cls, cursor):
        cursor.execute("SELECT id, name FROM LTN_DEVELOP.Users")
        users = list()
        for row in cursor.fetchall():
            users.append(User(str(row[0]), str(row[1]), None, None, None))
        return users

    def __init__(self, id, name, token, description, image):
        self.id = id
        self.name = name
        self.token = token
        self.description = description
        self.image = image

    def is_authenticated(self):
        return True

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return self.id


login_manager = LoginManager()
login_manager.session_protection = None
login_manager.init_app(app)


@login_manager.user_loader
def load_user(user_id):
    return User.get(user_id, get_connection().cursor())


@app.route('/login', methods=['POST'])
def login():
    if get_connection() is None:
        try_reconnecting()
    req = request.get_json()
    if req and 'username' in req and 'password' in req:
        try:
            user = load_user(req['username'])
            if user and req['password'] == user.token:
                login_user(user, remember=True)
                user.token = None
                return respond_with(user.__dict__)
        except Exception, e:
            reset_connection()
            return str(e) + " Please try again later.", 500
    return "Not authorized", 401


@app.route('/logout', methods=['GET', 'POST'])
def logout():
    logout_user()
    return "", 200


@app.route('/current_user')
def get_current_user():
    return respond_with(current_user.__dict__)


@app.route('/users')
def get_users():
    cursor = get_connection().cursor()
    users = User.all(cursor)
    cursor.close()
    return respond_with(map(lambda user: user.__dict__, users))


@app.route('/users/<user_id>')
def get_user(user_id):
    user = load_user(user_id)
    if not user:
        return "User not found", 404
    user.token = None
    return respond_with(user.__dict__)
