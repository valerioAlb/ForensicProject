# coding=utf-8
import elasticsearch
from container import Container
from mail_message import *
import re

es = elasticsearch.Elasticsearch("127.0.0.1:9200")
p = re.compile('([\[\(] *)?.*(RE?S?|FWD?|re\[\d+\]?) *([-:;)\]][ :;\])-]*|$)|\]+ *$', re.IGNORECASE)
p_match_ID = re.compile(ur'[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+', re.MULTILINE | re.IGNORECASE)



def search_messages_informations(mails):
    messages = []
    for ID in mails.keys():

        mail_to = []
        mail_from = ''
        subject = ''
        cc = []
        delivered_to = []
        temp_in_reply_to = []

        # merged contains in_reply_to + references
        merged = []

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

            if key.lower() == 'references':
                result = hit['_source'][key]
                references = re.findall(p_match_ID, result)
                merged.extend(references)

            if key.lower() == 'in-reply-to':
                result = hit['_source'][key]
                in_reply_to = re.findall(p_match_ID, result)
                # in-reply-to should be added only at the end of merged! after references
                if len(merged) > 0:
                    for reply in in_reply_to:
                        if reply not in merged:
                            merged.extend(in_reply_to)
                else:
                    temp_in_reply_to = in_reply_to

            if key.lower() == 'subject':
                subject = hit['_source'][key]

        if temp_in_reply_to:
            for temp in temp_in_reply_to:
                if temp not in merged:
                    merged.extend(temp_in_reply_to)

        # ID[1:-1] to remove '<' and '>' from the ID
        msg = Message(ID[1:-1], mail_to, mail_from, cc, delivered_to, mails[ID], subject, merged)
        messages.append(msg)

    return messages


def thread(messages):

    # 1)
    print 'Phase 1'
    id_table = {}
    for msg in messages:
        msg_container = id_table.get(msg.mailID)
        # 1.A if ID table contain an empty Container for this ID
        if msg_container is not None:
            # Store this message in the Container's message slot
            msg_container.message = msg
        # else Create a new container holding this message and index the new container by message-ID in id_table
        else:
            msg_container = Container()
            msg_container.message = msg
            id_table[msg.mailID] = msg_container

        last_reference = None

        # 1.B For each element in the message's reference field:
        # find a container object for the given message_id; if there is one in id_table, use that, otherwise,
        # make (and index) one with a null message.

        for reference in msg.references:
            # return None if not in the dictionary.
            rfr_container = id_table.get(reference)
            if rfr_container is None:
                rfr_container = Container()
                id_table[reference] = rfr_container

                # Link the references field's Containers together in the order implied by References header.
                # If they are already linked, don't change the existing links.
                # Do not add a link if adding that link would introduce a loop. that is, before asserting
                # A -> B, search down the children of B to see if A is reachable, and also search down the children of
                # A, to see if B is reachable. If either is already reachable as a child of the other, don't add the
                # link.

            if last_reference is not None:
                # Check if create loops
                if last_reference.introduce_loop(rfr_container):
                    continue
                if msg_container is rfr_container:
                    continue
                # Create the link! the reference container has a child which is the message container.
                last_reference.add_child(rfr_container)

            # To keep track of the previous created container.
            last_reference = rfr_container

        # Set the parent of the current message to be the last element in References.
        if last_reference is not None:
            last_reference.add_child(msg_container)

    '''for element in id_table:
        print element,' ------------> ',id_table[element]'''

    # 2) Find the root set
    # walk over the elements of id_table, and gather a list of the container objects that have no parents.
    print 'Phase 2'

    root_set = []
    for value in id_table:
        if id_table[value].parent is None:
            root_set.append(id_table[value])

    print 'root found: ', len(root_set)

    # 3) Discard the id_table
    print 'Phase 3'

    del id_table

    # 4) Prune empty containers.
    print 'Phase 4'

    temp_root_set = []
    for container in root_set:
        containers = prune_container(container)
        temp_root_set.extend(containers)

    root_set = temp_root_set

    print 'Phase 5'
    # 5) Group root_set by subject.
    '''If any two members of the root_set have the same subject, merge them. This is so that messages which don't have
        references headers at all still get threaded.
        5.A) Construct a new table, subject_table, which associates subjects strings with container objects.'''

    subject_table = {}

    """For each container in the root_set:
        - Find the subject of that sub-tree"""
    for container in root_set:
        # If there is no message in the container, then the container will have at least one child Container,
        # and that container will have a message. Use the subject of that message
        if container.message is None:
            subject = container.children[0].message.subject
        else:
            #  If there is a message in the Container, the subject is the subject of that message.
            subject = container.message.subject

        # To remove 'Re' from subject. p is defined at the top.
        subject = p.sub('', subject).strip()

        # If the subject is now '', give up with this container.
        if subject == '':
            continue

        '''Add this container to the subject table if:
            1) There is no container in the table with this subject
            2) This is an empty container and the old one is not: the empty container is more interesting as a root
                So put it in the table instead.
            3) The container in the table has a 'Re' version of the subject, and this container has a non 'Re' version
                of the subject. The non re-version of the subject is the more interesting of the two. '''

        subject_container = subject_table.get(subject)

        if (subject_container is None or (subject_container.message is not None and container.message is None) or (subject_container.message is not None and container.message is not None and len(subject_container.message.subject) > len(container.message.subject))):

            subject_table[subject] = container

    # 5.C
    # Now the subject table is populated with one entry for each subject which occurs in the root set.
    # Now iterate on the root set and gather together the difference.

    ''' For each container in the root set:
        - Find the subject of this container;
        - Look up the Container of that subject in the table;
        - If it is null, or if it is this container, continue.'''

    for container in root_set:

        i = 0
        # As above

        # If there is a message in the Container, the subject is the subject of that message.
        # If there is no message in the container, then the container will have at least one child Container,
        # and that container will have a message. Use the subject of that message

        if container.message is None:
            subject = container.children[0].message.subject
        else:
            subject = container.message.subject

        subject = p.sub('', subject).strip()

        subject_container = subject_table.get(subject)

        # If it is null, or if it is this container, continue
        if subject_container is container or subject_container is None:
            continue

        ######################### Check that there have not been changes in interlocutors ############################

        if container.message is None:
            mail_to = container.children[0].message.mailTo
            mail_from = container.children[0].message.mailFrom
        else:
            mail_to = container.message.mailTo
            mail_from = container.message.mailFrom

        if subject_container.message is None:
            mail_to_subject = subject_container.children[0].message.mailTo
            mail_from_subject = subject_container.children[0].message.mailFrom
        else:
            mail_to_subject = subject_container.message.mailTo
            mail_from_subject = subject_container.message.mailFrom

        addresses_container = []
        addresses_subject_container = []

        addresses_container.append(mail_from)
        addresses_container.extend(mail_to)

        addresses_subject_container.append(mail_from_subject)
        addresses_subject_container.extend(mail_to_subject)

        # less or equal than one, because one is always present: the email address of the mailbox owner.
        to_append = True

        if len(list(set(addresses_container) & set(addresses_subject_container))) <= 1:
            # In this case there is a change in interlocutors, also if the subject is the same.
            # In this case to not merge the two conversations.
            to_append = False
            i = i + 1


        ############################################################################################################

            # Otherwise we want to group together this container and the one in the table. There are a few
            # possibilities:
            # If both are dummies, append one's children to the other, and remove the non empty container.

        if not to_append:
            # Case where we don't want to merge conversations
            subject_new = subject + '(' + str(i) + ')'
            subject_table[subject_new] = container

        elif subject_container.message is None and container.message is None:
            for child in container.children:
                subject_container.add_child(child)

        # If one container is a empty and the other is not, make the non empty one be a child of the empty,
        # and a sibling of the other 'real' message with the same subject.

        elif subject_container.message is None or container.message is None:
            if subject_container.message is None:
                subject_container.add_child(container)
            else:
                container.add_child(subject_container)

        # If that container is a non empty, and that message's subject does not begin with 'Re', but this
        # message subject does not, then make that be a child of this one -- they where mis-ordered.

        elif len(subject_container.message.subject) > len(container.message.subject):
            container.add_child(subject_container)

        elif len(subject_container.message.subject) < len(container.message.subject):
            subject_container.add_child(container)

        # Otherwise make a new empty container and make both messages be a child of it.
        # this catches the both-and-replies and neither-are-replies cases, and makes them be a
        # siblings instead of asserting a hierarchical relationship which might not be true.
        else:
            new_container = Container()
            new_container.add_child(subject_container)
            new_container.add_child(container)
            subject_table[subject] = new_container

    return subject_table


def print_element(element):

    if element.message is None:
        print 'Root element'
    else:
        print '--> ', element.message.subject, 'Date: ', element.message.date
    for child in element.children:
        print_element(child)


def prune_container(container):

    #   Recursively walk all containers under the root set.
    #   for each container:
    #   -> if it is an empty container with no children nuke it.
    #   -> if the container has no message but does have children, remove this container but promote its children
    #           to this level. Do not promote the children if doing so would promote
    #           them to root set.(unless there is only one child, in which case, do)

    temp_children = []

    for child in container.children[:]:
        containers = prune_container(child)
        temp_children.extend(containers)
        container.remove_child(child)

    for child in temp_children:
        container.add_child(child)

    if len(container.children) == 0 and container.message is None:
        # 4.A: delete empty containers
        return []

    elif container.message is None and (len(container.children) == 1 or container.parent is not None):
        # 4.B: promote children
        containers = container.children[:]
        for c in containers:
            container.remove_child(c)
        return containers
    else:
        # Don't do anything
        return [container]


def refine_result(subject_table,communications):
    # Now, for each entry of subject_table I have a possible conversation. For each of these,
    # I have to verify that it is really only one conversation.

    temp_subject_table = {}

    # First filter over addresses (founded before while looking only for 'good' communications).

    addresses_to_care = communications.keys()

    print 'Addresses to care: ',addresses_to_care

    for subject in subject_table:
        subject_root_container = subject_table[subject]

        if subject_root_container.message is None:
            message = subject_root_container.children[0].message
        else:
            message = subject_root_container.message

        if message.mailFrom in addresses_to_care or len(list(set(addresses_to_care) & set(message.mailTo))) > 0:
            temp_subject_table[subject] = subject_root_container

    # Second filter, we remove all the elements that are not conversations. This is the case of conversations
    # with only one message.

    subject_table = temp_subject_table
    temp_subject_table = {}
    for subject in subject_table:

        subject_root_container = subject_table[subject]

        if subject_root_container.message is not None and len(subject_root_container.children) == 0:
            continue
        else:
            temp_subject_table[subject] = subject_root_container

    subject_table = temp_subject_table

    return subject_table




