# coding=utf-8

from script_search_methods import *
import threading_methods


# mboxOwner = 'VALERIO ALBINI <valerio.albini01@universitadipavia.it>'

# Specify the period to take in consideration for the analysis
#start = '2016-01-01'
start = '2011-01-01'
end = 'now'

# With this method, extract all the ID of the mails in the timebox [start date - end date]
mails = extract_mails_from_interval(start, end)
print 'len of mails: ', len(mails)
# With this method retrieve all the info relevant for building statistic, mail from, mail to, cc, delivered to...
messages = threading_methods.search_messages_informations(mails)
print 'len of messages: ', len(messages)


print
print '#####################'
# Un-Comment to see all the information retrieved
# displayMessages(messages)
print

print 'Now try to guess the mail owner address'

# Retrieve the email address of the mbox owner. Return a list, since at an user can correspond different addresses.
mbox_owner = extract_mail_owner(messages)
# mbox_owner = 'valerio.albini01@universitadipavia.it'

print 'Owner of the mailbox founded: ', mbox_owner

print
print '#####################'
print

# Sort messages according their dates
messages.sort()
# displayMessages(messages)
print 'Now extract conversations'
communications = find_communications(messages, mbox_owner)
print 'Conversations founded: ', len(communications)
print 'Addresses: ', communications.keys()

print
print '#####################'
print

print 'Analyzing conversations.'
analyze_communication(communications, start, end)

print
print '#####################'
print

print 'Filtering real conversations..'
communications = communication_filter(start, end, communications, 10)

print
print '#####################'
print
print 'Storing results into elasticsearch Database'
# store_results(communications, mbox_owner)

print
# visualize_results(communications)

print 'Starting threading alghorithm'

messages = threading_methods.search_messages_informations(mails)

messages.sort()

subject_table = threading_methods.thread(messages)

subject_table = threading_methods.refine_result(subject_table,communications)

for element in subject_table:
    print '########################'
    threading_methods.print_element(subject_table[element])