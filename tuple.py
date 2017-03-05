class Tuple(object):
    def __init__(self, tuple_id, sent_from):
        self.tuple_id = tuple_id
        self.sent_from = sent_from


class BarrierTuple(Tuple):
    # TODO: add version?
    def __init__(self, tuple_id, sent_from, sent_to):
        super(BarrierTuple, self).__init__(tuple_id, sent_from)
