from datetime import datetime
from nanoid import generate


class Commit:
    def __init__(self, message, hash_code=None, date_time=None):
        self.hash_code = hash_code if hash_code else generate(size=10)
        self.message = message
        self.date_time = date_time if date_time else datetime.now().isoformat()

    def __str__(self):
        return f"ID: {self.hash_code}\tMessage: {self.message}\tDate: {self.date_time}"

    def get_list_value(self):
        return [self.hash_code, self.message, self.date_time]
