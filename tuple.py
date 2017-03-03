class Tuple(object):
    def __init__(self, tuple_id):
        self.tuple_id = tuple_id


class BarrierTuple(Tuple):
    def __init__(self, tuple_id):
        super(BarrierTuple, self).__init__(tuple_id)

