# coding=utf-8
from __future__ import division
import elasticsearch
import re
import datetime
import csv
import matplotlib.pyplot as plt
import numpy as np
from datetime import timedelta
from communication import Communication
from mail_message import *
from math import sqrt


es = elasticsearch.Elasticsearch("127.0.0.1:9200")
standard_deviation_threshold = 6


def extract_mail_owner(messages):
    addresses = []
    for msg in messages:
        if msg.deliveredTo:
            if msg.deliveredTo[0] in msg.cc:
                owner_address = msg.deliveredTo[0]
                if owner_address not in addresses:
                    addresses.append(owner_address)
    return addresses


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


def find_communications(messages, mbox_owner):

    # conversation is in the structure user-conversation, where user is the other user.
    conversations = {}

    for msg in messages:

        # If it is an outgoing message
        if msg.mailFrom in mbox_owner:
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
        elif len(list(set(mbox_owner) & set(msg.mailTo))) > 0 or len(list(set(mbox_owner) & set(msg.cc))) > 0 or len(list(set(mbox_owner) & set(msg.deliveredTo))) > 0:
            # I simply have to store the conversation
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
        'size': 10000
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


def analyze_communication(conversations, start, end):

    end_date, start_date = get_dates(end, start)
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


def get_dates(end, start):
    start_date = datetime.strptime(start, "%Y-%m-%d").date()
    if end != 'now':
        end_date = datetime.strptime(end, "%Y-%m-%d").date()
    else:
        end_date = datetime.now().date()

    return end_date, start_date


def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def communication_filter(start, end, conversations, threshold=10):

    # Convert dates-string in python date-datetime
    end_date, start_date = get_dates(end, start)

    new_conversations = {}

    for k in conversations:

        conversation = conversations[k]
        # Filter on the number of mail exchanged. If less than 10 email, exclude the communication.
        if conversation.get_num_mail_exchanged() > threshold:
            new_conversations[k] = conversation
        else:
            continue

    # Now lets see about the frequency of exchanged emails. Compute all the intervals between one email and that after.
    # Basing on the result, keep the conversation or not.

    to_be_deleted = []

    for j in new_conversations:
        # print '#################################'
        # print 'Communications with: ', j

        conv = new_conversations[j]
        conv_res = conv.result

        intervals = extract_intervals(conv_res, end_date, start_date)

        variance = get_variance(intervals)
        # print 'Variance: ', variance
        std_deviation = sqrt(variance)
        # print 'Standard deviation: ', std_deviation
        conv.std_deviation = std_deviation
        conv.variance = variance

        if std_deviation < standard_deviation_threshold:
            to_be_deleted.append(j)

    # Delete elements with low std_deviation, like newsletter.. and so on
    print 'Before first delete', len(new_conversations)

    for element in to_be_deleted:
        del new_conversations[element]

    # Filter again! Delete the communication, if only incoming messages.
    to_be_deleted = []

    print 'Before second delete', len(new_conversations)

    for j in new_conversations:
        conversation = new_conversations[j]
        if len(conversation.outgoing) == 0:
            to_be_deleted.append(j)

    for element in to_be_deleted:
        del new_conversations[element]

    print 'After second delete', len(new_conversations)

    return new_conversations


def extract_intervals(conv_res, end_date, start_date):
    previous_date = None
    intervals = []
    for single_date in date_range(start_date, end_date):

        if conv_res[single_date] and conv_res[single_date] > 0:

            if previous_date is not None:
                d = (single_date - previous_date).days
                # print 'Previous Date: ', previous_date, ' Date: ', single_date, ' Delay: ', d
                intervals.append(d)

            previous_date = single_date
    return intervals


def visualize_results(conversations):
    for k in conversations:
        conversation = conversations[k]
        print '######################'
        print 'Current std_deviation: ', conversation.std_deviation, 'for communication with: ', k

        result = conversation.result
        sorted_keys = sorted(result)

        x = []
        y = []

        for element in sorted_keys:
            x.append(element)
            y.append(result[element])

        plt.bar(x, y, align='center', width=0.35, color='green')

        plt.title('Communication with: '+k)
        plt.xlabel('Date')
        plt.ylabel('Number of email exchanged')
        plt.grid()
        plt.yticks(np.arange(min(y), max(y)+1, 1.0))
        plt.show()


def store_results(communications, mbox_owner):
    for key in communications:
        # Save to elasticsearch and to a csv file

        current_communication = communications[key]
        variance = current_communication.variance
        std_deviation = current_communication.std_deviation
        communication_with = key

        # First save to elasticsearch
        save_to_elasticsearch(communication_with, current_communication, mbox_owner, std_deviation, variance)

        # Now save on a csv file
        save_to_csv(communication_with, current_communication, key, mbox_owner, std_deviation, variance)


def save_to_elasticsearch(communication_with, current_communication, mbox_owner, std_deviation,
                          variance):
    communication_details = {}

    for element in current_communication.result:
        date = element.strftime('%Y-%m-%d')
        mail_exchanged = current_communication.result[element]

        # print 'Date: ',date, 'Mail exchanged: ',mail_exchanged
        if mail_exchanged > 0:
            communication_details[date] = mail_exchanged

    doc = {
        'communication_with': communication_with,
        'variance': variance,
        'std_deviation': std_deviation,
        'mbox_owner': mbox_owner,
    }

    doc['messages_exchanged'] = [communication_details]
    es.index(index="test", doc_type='analysis', body=doc)


def save_to_csv(communication_with, current_communication, key, mbox_owner, std_deviation, variance):
    result = current_communication.result
    sorted_keys = sorted(result)
    x = []
    y = []
    for element in sorted_keys:
        x.append(element)
        y.append(result[element])
    with open('csv/' + key + '.csv', 'w') as mycsvfile:
        data_writer = csv.writer(mycsvfile)
        data_writer.writerow(('Mail_Owner', mbox_owner))
        data_writer.writerow(('Communication_with', communication_with))
        data_writer.writerow(('Variance', variance))
        data_writer.writerow(('Standard_deviation', std_deviation))
        for col1, col2 in zip(x, y):
            data = col1.strftime("%Y/%m/%d")
            data_writer.writerow((data, str(col2)))


def get_variance(intervals):

    # To avoid corner cases in which all emails are exchanged in a single day. In this cases intervals is empty,
    # and there is a division per 0 (len(intervals)).

    if len(intervals) > 1:
        sum_intervals = 0
        temp_interval = []
        for element in intervals:
            sum_intervals = sum_intervals + element

        sample_mean = sum_intervals / len(intervals)
        for element in intervals:
            temp_value = (element-sample_mean)
            temp_interval.append((temp_value*temp_value))
        sum_intervals = 0

        for element in temp_interval:
            sum_intervals = sum_intervals + element

        sample_variance = sum_intervals/(len(intervals)-1)

        return sample_variance
    else:
        return 0
