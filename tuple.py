class Tuple(object):
    def __init__(self, tuple_id, sent_from):
        self.tuple_id = tuple_id
        self.sent_from = sent_from


class BarrierTuple(Tuple):
    def __init__(self, tuple_id, sent_from, version=None):
        super(BarrierTuple, self).__init__(tuple_id, sent_from)

        # TODO: version and tuple_id
        self.version = version if version != None else tuple_id


class VersionAck(object):
    def __init__(self, sent_from, version):
        self.sent_from = sent_from
        self.version = version
        