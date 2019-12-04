from typing import Optional


class Tag(object):

    # noinspection PyProtectedMember
    def __get__(self, instance, owner)-> str:
        if instance is None:
            raise Exception('Cannot access tag without instance')
        return instance._data[self.name]

    # noinspection PyProtectedMember
    def __set__(self, instance, value):
        if instance is None:
            raise Exception('Cannot access tag without instance')
        if not self.null:
            assert value is not None, "Null value for not nullable tag: " + self.name
        instance._data[self.name] = value

    def __init__(self, name: Optional[str] = None, null: bool = True):
        super(Tag, self).__init__()
        self.name = name
        self.null = null

    def __add__(self, other):
        return str(self) + str(other)
