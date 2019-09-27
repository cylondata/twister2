from twister2.data.DataObject import DataObject


class TSetContext:

    def __init__(self, java_ref):
        self.__java_ref = java_ref

    def get_input(self, key):
        data_object = self.__java_ref.getInput(key)
        return DataObject(self.__java_ref.getInput(key))

    def get_index(self):
        return self.__java_ref.getIndex()
