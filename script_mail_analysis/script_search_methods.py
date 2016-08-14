# coding=utf-8
import elasticsearch
import re
import datetime
import csv
import matplotlib.pyplot as plt
import numpy as np
from datetime import timedelta
from communication import Communication
from mail_message import *

es = elasticsearch.Elasticsearch("127.0.0.1:9200")


def extract_mail_owner(messages):
    for msg in messages:
        if msg.deliveredTo:
            if msg.deliveredTo[0] in msg.cc:
                owner_address = msg.deliveredTo[0]
                return owner_address


def display_messages(messages):
    i = 0
    for msg in messages:
        print 'MailID ', msg.mailID
        print 'MailFrom ', msg.mailFrom
        print 'MailTo ', msg.mailTo
        print 'Delivered-To', msg.deliveredTo
        print 'Date ', msg.date
        print 'CC ', msg.cc
        print '######'
        print
        i = i + 1
    print i, ' Messages displayed'


def find_conversations(messages, mbox_owner):

    # conversation is in the structure user-conversation, where user is the other user.
    conversations = {}

    for msg in messages:

        # If it is an outgoing message
        if msg.mailFrom == mbox_owner:
            # I have to iterate over CC and TO
            for toAddress in msg.mailTo:
                # If I have already stored a conversation with him
                if toAddress in conversations.keys():
                    conversations[toAddress].add_outgoing(msg)
                else:
                    conversations[toAddress] = Communication()
                    conversations[toAddress].add_outgoing(msg)

            for ccAddress in msg.cc:
                # If I have already stored a conversation with him
                if ccAddress in conversations.keys():
                    conversations[ccAddress].add_outgoing(msg)
                else:
                    conversations[ccAddress] = Communication()
                    conversations[ccAddress].add_outgoing(msg)

        # If it is an incoming message
        elif (mbox_owner in msg.mailTo) or (mbox_owner in msg.cc) or (mbox_owner in msg.deliveredTo):

            # I simply have to store the converation
            if msg.mailFrom in conversations.keys():
                conversations[msg.mailFrom].add_incoming(msg)
            else:
                conversations[msg.mailFrom] = Communication()
                conversations[msg.mailFrom].add_incoming(msg)

    return conversations


def extract_mails_from_interval(start, end='now'):
    mails = {}

    query = {
        # Obtain all the IDs of emails from start-date to end-date, ordered from the older to the newest.

        'sort': [
            {'normalized_date': {'order': 'asc'}}
        ],
        'query': {
            'bool': {
                'must': {
                    'match_all': {}
                },
                'filter': {
                    'range': {
                        'normalized_date': {
                            'gte': start,
                            'lte': end
                        }
                    }
                }
            }
        },
        'size': 5000
    }

    res = es.search(index="test", body=query)

    print 'There are: ', str(res['hits']['total']), ' results'
    print 'The max score is: ', str(res['hits']['max_score']), ' .'

    for hit in res['hits']['hits']:
        # print hit['_source']
        # print hit['_source']['mailID']
        ID = hit['_source']['mailID']
        date = hit['_source']['normalized_date']
        # <print 'ID ',ID,' Date: ',date

        # To avoid duplicates, if any
        if ID not in mails.keys():
            mails[ID] = date

    # Get all the emails in the specified period
    print 'Selected mail: ', len(mails), ' from ', start, ' to ', end
    return mails


def extract_relevant_information_about_mail(mails):
    messages = []
    for ID in mails.keys():

        mail_to = []
        mail_from = ''
        cc = []
        delivered_to = []

        query = {
            'query': {
                'bool': {
                    'must': {
                        'match_phrase': {
                            'mailID': ID
                        },
                    }
                }
            },
            'size': 100
        }
        res = es.search(index="test", body=query)

        for hit in res['hits']['hits']:

            key = hit['_source'].keys()[1]

            if key.lower() == 'from_address':
                mail_from = hit['_source'][key]

            if key.lower() == 'to':
                result = hit['_source'][key]
                mail_to = re.findall(r'[\w\.-]+@[\w\.-]+', result)

            if key.lower() == 'cc':
                result = hit['_source'][key]
                cc = re.findall(r'[\w\.-]+@[\w\.-]+', result)

            if key.lower() == 'delivered-to':
                result = hit['_source'][key]
                delivered_to = re.findall(r'[\w\.-]+@[\w\.-]+', result)

        msg = Message(ID, mail_to, mail_from, cc, delivered_to, mails[ID])
        messages.append(msg)
    return messages


def analyze_conversations(conversations, start, end):

    start_date = datetime.strptime(start, "%Y-%m-%d").date()
    if end != 'now':
        end_date = datetime.strptime(end, "%Y-%m-%d").date()
    else:
        end_date = datetime.now().date()

    # For every conversation extract a result
    for k in conversations:
        conversation = conversations[k]

        outgoing = conversation.outgoing
        incoming = conversation.incoming

        outgoing.sort()
        incoming.sort()

        # Initialization of result
        for single_date in date_range(start_date, end_date):
            conversation.result[single_date] = 0

        for msg in outgoing:
            conversation.result[msg.date1] = conversation.result[msg.date1] + 1

        for msg in incoming:
            conversation.result[msg.date1] = conversation.result[msg.date1] + 1

        # print sorted(conversation.result)


def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def visualize_results(conversations, threshold=10):
    for k in conversations:
        conversation = conversations[k]

        # Introduce a threshold, if number > thresold, then show graph and export csv
        if conversation.get_num_mail_exchanged() > threshold:

            result = conversation.result
            sorted_keys = sorted(result)

            x = []
            y = []

            for element in sorted_keys:
                x.append(element)
                y.append(result[element])

            # x = matplotlib.dates.date2num(x)

            plt.bar(x, y, align='center', width=0.35, color='green')

            plt.title('Communication with: '+k)
            plt.xlabel('Date')
            plt.ylabel('Number of email exchanged')
            plt.grid()
            plt.yticks(np.arange(min(y), max(y)+1, 1.0))
            plt.show()

            # Save the result on a csv file
            with open('csv/'+k + '.csv', 'w') as mycsvfile:
                data_writer = csv.writer(mycsvfile)
                data_writer.writerow(('Date', 'Mail exchanged'))
                for col1, col2 in zip(x, y):
                    data = col1.strftime("%Y/%m/%d")
                    data_writer.writerow((data, str(col2)))
