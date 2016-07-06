
#useful to manage different aspect of the application.
class utils(object):

    INSTANCE = None
    maxRecursionLevel = 1
    actualRecursionLevel = 0

    def __init__(self):
        if self.INSTANCE is not None:
            raise ValueError("An instantiation already exist!")

    @classmethod
    def get_instance(cls):
        if cls.INSTANCE is None:
            cls.INSTANCE = utils()
        return cls.INSTANCE

    # This function is useful when considering archives. Whenever entering an archive you check the recursion level
    # reached. If you have exceeded the maximum level you stop
    def setRecursion(self):

        self.actualRecursionLevel = self.actualRecursionLevel + 1
        if self.actualRecursionLevel > self.maxRecursionLevel:
            self.actualRecursionLevel = 0
            return 0
        else:
            return 1

    # To specify a different maxRecursionLevel
    def setMaxRecursionLevel(self,level):
        self.maxRecursionLevel = level