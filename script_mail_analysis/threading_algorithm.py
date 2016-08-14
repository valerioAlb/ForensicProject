import threading_methods
import script_search_methods

print 'Starting of the program..'
print

print 'Initialization of the messages'

start = '2014-09-09'
end = 'now'

mails = script_search_methods.extract_mails_from_interval(start, end)

messages = threading_methods.search_messages_informations(mails)

messages.sort()
'''print '############'
for msg in messages:
    print 'Message: ',msg
    print 'References: ',msg.references
    print '#############'''''


subject_table = threading_methods.thread(messages)
'''
# Now you are done threading!
# Now sort the siblings.

values = subject_table.items()
values.sort()

for subject,container in values:
    threading_methods.print_container(container)
'''

#subject_table = threading_methods.thread2(messages)

for element in subject_table:
    print '########################'
    threading_methods.print_element(subject_table[element])