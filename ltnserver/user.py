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
