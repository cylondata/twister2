from twister2.tset import DirectLink


class SourceTSet:

    def __init__(self, java_ref):
        self.java_ref = java_ref

    def direct(self) -> DirectLink:
        return self.java_ref.direct()
