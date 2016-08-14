# coding=utf-8

from script_search_methods import *


# mboxOwner = 'VALERIO ALBINI <valerio.albini01@universitadipavia.it>'

# Specify the period to take in consideration for the analysis
start = '2016-01-01'
end = 'now'

# With this method, extract all the ID of the mails in the timebox [start date - end date]
mails = extract_mails_from_interval(start, end)

# With this method retrieve all the info relevant for building statistic, mail from, mail to, cc, delivered to...
messages = extract_relevant_information_about_mail(mails)


print
print '#####################'
# Un-Comment to see all the information retrieved
# displayMessages(messages)
print

print 'Now try to guess the mail owner address'

# Retrieve the email address of the mbox owner
mbox_owner = extract_mail_owner(messages)

print 'Owner of the mailbox founded: ', mbox_owner

print
print '#####################'
print

# Sort messages according their dates
messages.sort()
# displayMessages(messages)
print 'Now extract conversations'
conversations = find_conversations(messages, mbox_owner)
print 'Conversations founded: ', len(conversations)
print 'Addresses: ', conversations.keys()
print
print '#####################'
print

print 'Analyzing conversations.'
analyze_conversations(conversations, start, end)

visualize_results(conversations, 10)

print 'Program Terminated'
