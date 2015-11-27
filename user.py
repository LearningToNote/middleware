class User:

    @classmethod
    def get(cls, user_id, cursor):
        cursor.execute("SELECT id, name, token FROM LEARNING_TO_NOTE.Users WHERE id = ?", (user_id,))
        row = cursor.fetchone()
        if row:
            return User(user_id, str(row[1]), str(row[2]))
        else:
            return None

    @classmethod
    def all(cls, cursor):
        cursor.execute("SELECT id, name FROM LEARNING_TO_NOTE.Users")
        users = list()
        for row in cursor.fetchall():
            users.append(User(str(row[0]), str(row[1]), None))
        return users

    def __init__(self, id, name, token):
        self.id = id
        self.name = name
        self.token = token

    def is_authenticated(self):
        return True

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return self.id