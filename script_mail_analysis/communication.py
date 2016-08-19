# coding=utf-8
class Communication:

    # In result salvo il risultato dell analisi, cioè,per ogni giorno, il numero di mex scambiati.

    def __init__(self):
        self.incoming = []
        self.outgoing = []
        self.result = {}
        self.std_deviation = 0
        self.variance = 0

    def add_incoming(self, incoming):
        self.incoming.append(incoming)

    def add_outgoing(self, outgoing):
        self.outgoing.append(outgoing)

    def get_num_mail_exchanged(self):
        return len(self.incoming)+len(self.outgoing)
