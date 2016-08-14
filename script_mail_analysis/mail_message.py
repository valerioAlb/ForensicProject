# coding=utf-8
from datetime import datetime


class Message:

    def __init__(self, mail_id='', mail_to=[], mail_from='', cc=[], delivered_to=[], date='',
                 subject='', references=[]):

        self.mailID = mail_id
        self.mailTo = mail_to
        self.mailFrom = mail_from
        self.date = date
        self.deliveredTo = delivered_to
        self.cc = cc
        self.subject = subject
        self.references = references

        # Better to save in a nice format
        date1 = self.date.split("T")[0]
        self.date1 = datetime.strptime(date1, "%Y-%m-%d").date()

    # Redefine this method to sort messages according their date.
    def __lt__(self, other):
        return self.date1 < other.date1
