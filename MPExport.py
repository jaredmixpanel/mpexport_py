from Tkinter import *
from ttk import Progressbar
from tkMessageBox import askyesno
from tkFileDialog import askdirectory
import sys
import os
import tempfile
import hashlib
import time
import urllib
import unicodecsv as csv
import json
import codecs
import cStringIO
from functools import partial
import threading
from idlelib.WidgetRedirector import WidgetRedirector
import eventlet
from eventlet.green import urllib2
import base64

API_ENDPOINT = 'http://mixpanel.com/api'
EXPORT_ENDPOINT = 'http://data.mixpanel.com/api'
API_VERSION = '2.0'


def get_sub_keys(list_of_dicts):
    sub_keys = set()
    for event_dict in list_of_dicts:
        if event_dict[u'properties']:
            sub_keys.update(set(event_dict[u'properties'].keys()))
        else:
            pass
    return sub_keys


class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class Mixpanel(object):

    def __init__(self, api_key, api_secret, endpoint, project_token=''):
        self.api_key = api_key
        self.api_secret = api_secret
        self.endpoint = endpoint
        self.project_token = project_token

    def request(self, methods, params):
        params['api_key']=self.api_key
        params['expire'] = int(time.time())+600 # 600 is ten minutes from now
        if 'sig' in params: del params['sig']
        params['sig'] = self.hash_args(params)

        if methods[0] == 'export':
            request_url = '/'.join([EXPORT_ENDPOINT, str(API_VERSION)] + methods) + '/?'
        else:
            request_url = '/'.join([API_ENDPOINT, str(API_VERSION)] + methods) + '/?'

        request_url = request_url + self.unicode_urlencode(params)
        print request_url
        request = urllib.urlopen(request_url)
        data = request.read()

        return data

    def hash_args(self, args, secret=None):
        '''Hash dem arguments in the proper way
        join keys - values and append a secret -> md5 it'''

        for a in args:
            if isinstance(args[a], list): args[a] = json.dumps(args[a])

        args_joined = ''
        for a in sorted(args.keys()):
            if isinstance(a, unicode):
                args_joined += a.encode('utf-8')
            else:
                args_joined += str(a)

            args_joined += "="

            if isinstance(args[a], unicode):
                args_joined += args[a].encode('utf-8')
            else:
                args_joined += str(args[a])

        hash = hashlib.md5(args_joined)

        if secret:
            hash.update(secret)
        elif self.api_secret:
            hash.update(self.api_secret)
        return hash.hexdigest()

    def unicode_urlencode(self, params):
        ''' Convert stuff to json format and correctly handle unicode url parameters'''

        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        result = urllib.urlencode([(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params])
        return result

    def people_json_to_csv(self, outfilename, fname):
        """
        takes the json and returns a csv file
        """
        subkeys = set()
        with open(fname, 'rb') as r:
            with open(outfilename, 'wb') as w:
                # Get all properties (will use this to create the header)
                for line in r:
                    try:
                        subkeys.update(set(json.loads(line)['$properties'].keys()))
                    except:
                        pass

                # Create the header
                header = ['$distinct_id']

                for key in subkeys:
                    header.append(key)

                # Create the writer and write the header
                writer = csv.writer(w)
                writer.writerow(header)

                #Return to the top of the file, then write the events out, one per row
                r.seek(0, 0)
                for line in r:
                    entry = json.loads(line)
                    row = []
                    try:
                        row.append(entry['$distinct_id'])
                    except:
                        row.append('')

                    for subkey in subkeys:
                        try:
                            row.append((entry['$properties'][subkey]).encode('utf-8'))
                        except AttributeError:
                            row.append(entry['$properties'][subkey])
                        except KeyError:
                            row.append("")
                    writer.writerow(row)
                print 'CSV saved to ' + w.name
                w.close()

    def event_json_to_csv(self, outfileName, data):
        """
        takes mixpanel export API json and returns a csv file
        """
        event_raw = data.split('\n')
        try:
            result = '\nAPI ERROR! - ' + json.loads(event_raw[0])['error'] + '\n'
            print result
            return
        except KeyError:
            pass

        '''remove the lost line, which is a newline'''
        event_raw.pop()

        event_list = []
        jsonfile = outfileName[:-4] + '.json'
        with open(jsonfile,'w') as j:
            j.write('[')
            i = 0
            event_count = len(event_raw)
            for event in event_raw:
                j.write(event)
                i += 1
                if i != event_count:
                    j.write(',')
                else:
                    j.write(']')
                event_json = json.loads(event)
                event_list.append(event_json)
            print 'JSON saved to ' + j.name
            j.close()

        subkeys = get_sub_keys(event_list)

        #open the file
        f = open(outfileName, 'w')
        writer = UnicodeWriter(f)

        #write the file header
        f.write(codecs.BOM_UTF8)

        #writer the top row
        header = [u'event']
        for key in subkeys:
            header.append(key)
        writer.writerow(header)

        #write all the data rows
        for event in event_list:
            line = []
            #get the event name
            try:
                line.append(event[u'event'])
            except KeyError:
                line.append("")
            #get each property value
            for subkey in subkeys:
                try:
                    line.append(unicode(event[u'properties'][subkey]))
                except KeyError:
                    line.append("")
            #write the line
            writer.writerow(line)

        print 'CSV saved to ' + f.name
        f.close()

    def update(self, userlist, uparams):
        url = "http://api.mixpanel.com/engage/"
        batch = []
        for user in userlist:
            distinctid = user['$distinct_id']
            tempparams = {
                    'token': self.project_token,
                    '$distinct_id': distinctid,
                    '$ignore_alias': True
                    }
            tempparams.update(uparams)
            batch.append(tempparams)

        payload = {"data":base64.b64encode(json.dumps(batch)), "verbose":1,"api_key":self.api_key}

        response = urllib2.urlopen(url, urllib.urlencode(payload))
        message = response.read()

        '''if something goes wrong, this will say what'''
        if json.loads(message)['status'] != 1:
            print message

    def batch_update(self, users, params):
        pool = eventlet.GreenPool(size=10) # increase the pool size if you have more memory (e.g., a server)
        while len(users):
            batch = users[:50]
            pool.spawn(self.update, batch, params)
            users = users[50:]
        pool.waitall()
        print "Done!"


class StdRedirector(object):
    def __init__(self, widget):
        self.widget = widget

    def write(self, string):
        self.widget.insert(END, string + '\n')
        self.widget.see(END)


class ReadOnlyText(Text):
    def __init__(self, *args, **kwargs):
        Text.__init__(self, *args, **kwargs)
        self.redirector = WidgetRedirector(self)
        self.insert = \
            self.redirector.register("insert", lambda *args, **kw: "break")
        self.delete = \
            self.redirector.register("delete", lambda *args, **kw: "break")


class MPExportApp(object):
    """docstring for MPExportApp"""
    def __init__(self, master):
        super(MPExportApp, self).__init__()
        self.master = master
        self.export_type = StringVar()

        master.columnconfigure(1, weight=1)

        export_type_label = Label(master, text="Data Type: ")
        export_type_label.grid(row=0, column=0, sticky=E)

        radio_group_frame = Frame(master)
        radio_group_frame.grid(row=0, column=1, sticky=W)

        self.events_radio_button = Radiobutton(radio_group_frame, text='Events', value='events', variable=self.export_type,
                                               command=self.radio_button_changed)

        self.events_radio_button.select()
        self.events_radio_button.grid(row=0, column=0)

        self.people_radio_button = Radiobutton(radio_group_frame, text='People', value='people', variable=self.export_type,
                                               command=self.radio_button_changed)
        self.people_radio_button.grid(row=0, column=1)

        api_key_label = Label(master, text="API Key: ")
        api_key_label.grid(row=1, column=0, sticky=E)

        self.api_key_entry = Entry(master)
        self.api_key_entry.grid(row=1, column=1, columnspan=2, sticky=(E,W))

        api_secret_label = Label(master, text="API Secret: ")
        api_secret_label.grid(row=2, column=0, sticky=E)

        self.api_secret_entry = Entry(master)
        self.api_secret_entry.grid(row=2, column=1, columnspan=2, sticky=(E, W))

        self.project_token_label = Label(master, text="Project Token: ", state=DISABLED)
        self.project_token_label.grid(row=3, column=0, sticky=E)

        self.project_token_entry = Entry(master, state=DISABLED)
        self.project_token_entry.grid(row=3, column=1, columnspan=2, sticky=(E, W))

        self.events_label = Label(master, text="Events: ")
        self.events_label.grid(row=4, column=0, sticky=E)

        self.events_entry = Entry(master)
        self.events_entry.insert(0, 'Event A,Event B,Event C')
        self.events_entry.bind('<FocusIn>', partial(self.clear_placeholder_text, self.events_entry))
        self.events_entry.grid(row=4, column=1, columnspan=2, sticky=(E, W))

        where_label = Label(master, text="Where: ")
        where_label.grid(row=5, column=0, sticky=E)
        self.where_entry = Entry(master)
        self.where_entry.grid(row=5, column=1, columnspan=2, sticky=(E, W))

        self.from_date_label = Label(master, text="From Date: ")
        self.from_date_label.grid(row=6, column=0, sticky=E)

        self.from_date_entry = Entry(master)
        self.from_date_entry.insert(0, 'YYYY-MM-DD')
        self.from_date_entry.bind('<FocusIn>', partial(self.clear_placeholder_text, self.from_date_entry))
        self.from_date_entry.grid(row=6, column=1, columnspan=2, sticky=(E, W))

        self.to_date_label = Label(master, text="To Date: ")
        self.to_date_label.grid(row=7, column=0, sticky=E)

        self.to_date_entry = Entry(master)
        self.to_date_entry.insert(0, 'YYYY-MM-DD')
        self.to_date_entry.bind('<FocusIn>', partial(self.clear_placeholder_text, self.to_date_entry))
        self.to_date_entry.grid(row=7, column=1, columnspan=2, sticky=(E, W))

        export_button = Button(master, text="EXPORT", fg="green", command=self.export)
        export_button.grid(row=8, column=1, sticky=(E, W))

        self.delete_button = Button(master, text="DELETE", fg='red', state=DISABLED,
                                    command=lambda: threading.Thread(target=self.delete_people).start())
        self.delete_button.grid(row=9, column=1, sticky=(E, W))

        self.progress_bar = Progressbar(master)
        self.progress_bar_value = IntVar()
        self.progress_bar.config(mode='determinate', orient='horizontal', variable=self.progress_bar_value)
        self.progress_bar.grid(row=10, column=0, columnspan=3, sticky=(E, W))


    def clear_placeholder_text(self, entry, event):
        entry_text = entry.get()
        if entry_text == 'Event A,Event B,Event C' or entry_text == 'YYYY-MM-DD':
            entry.delete(0, END)

    def make_events_string(self, events):
        events = events.replace(', ', ',')
        events = events.split(',')
        events_string = '['
        for x in range(0, len(events)):
            events_string += '"' + events[x] + '"'
            if x != len(events)-1:
                events_string += ','
            else:
                events_string += ']'
        return events_string

    def radio_button_changed(self):
        if self.export_type.get() == 'people':
            self.project_token_label.config(state=NORMAL)
            self.project_token_entry.config(state=NORMAL)
            self.events_label.config(state=DISABLED)
            self.events_entry.config(state=DISABLED)
            self.from_date_label.config(state=DISABLED)
            self.from_date_entry.config(state=DISABLED)
            self.to_date_label.config(state=DISABLED)
            self.to_date_entry.config(state=DISABLED)
            self.delete_button.config(state=NORMAL)
        elif self.export_type.get() == 'events':
            self.project_token_label.config(state=DISABLED)
            self.project_token_entry.config(state=DISABLED)
            self.events_label.config(state=NORMAL)
            self.events_entry.config(state=NORMAL)
            self.from_date_label.config(state=NORMAL)
            self.from_date_entry.config(state=NORMAL)
            self.to_date_label.config(state=NORMAL)
            self.to_date_entry.config(state=NORMAL)
            self.delete_button.config(state=DISABLED)

    def export(self):
        if self.api_key_entry.get() == '':
            print 'API Key Required!'
            return
        elif self.api_secret_entry.get() == '':
            print 'API Secret Required!'
            return

        self.output_dir = askdirectory(title='Choose output directory', mustexist=True, parent=self.master)
        print 'Output directory is ' + self.output_dir

        self.progress_bar.start()
        if self.export_type.get() == 'people':
            self.export_thread = threading.Thread(target=self.export_people)
        elif self.export_type.get() == 'events':
            self.export_thread = threading.Thread(target=self.export_events)

        self.export_thread.start()

    def export_people(self):

        mixpanel = Mixpanel(
            api_key=self.api_key_entry.get(),
            api_secret=self.api_secret_entry.get(),
            endpoint=API_ENDPOINT
            )

        '''Here is the place to define your selector to target only the users that you're after'''
        '''parameters = {'selector':'(properties["$email"] == "Albany") or (properties["$city"] == "Alexandria")'}'''
        parameters = {'selector': self.where_entry.get()}
        response = mixpanel.request(['engage'], parameters)

        try:
            result = '\nAPI ERROR! - ' + json.loads(response)['error'] + '\n'
            if result:
                print result
                return
        except KeyError, e:
            pass

        parameters['session_id'] = json.loads(response)['session_id']
        parameters['page'] = 0
        global_total = json.loads(response)['total']
        if global_total == 0:
            print 'Query returned 0 profiles!'
            self.progress_bar.stop()
            self.progress_bar_value.set(0)
            return

        print "Session id is %s \n" % parameters['session_id']
        print "Here are the # of people %d" % global_total
        filename = self.output_dir + "/people_export_"+str(int(time.time()))
        jsonfile = filename + ".json"
        csvfile = filename + ".csv"
        has_results = True
        total = 0
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            with open(jsonfile, 'w') as j:
                j.write('[')
                while has_results:
                    responser = json.loads(response)['results']
                    response_count = len(responser)
                    total += response_count
                    has_results = response_count == 1000
                    i = 0
                    for data in responser:
                        dump = json.dumps(data)
                        temp.write(dump + '\n')
                        j.write(dump)
                        i += 1
                        if i != response_count:
                            j.write(',')

                    print "%d / %d" % (total,global_total)
                    parameters['page'] += 1
                    if has_results:
                        j.write(',')
                        response = mixpanel.request(['engage'], parameters)
                    else:
                        j.write(']')

                print 'JSON saved to ' + j.name
                j.close()

        mixpanel.people_json_to_csv(csvfile, temp.name)
        temp.close()
        os.remove(temp.name)
        self.progress_bar.stop()
        self.progress_bar_value.set(0)

    def export_events(self):

        if self.from_date_entry.get() == '':
            print 'From Date Required!'
            self.progress_bar.stop()
            self.progress_bar_value.set(0)
            return
        elif self.to_date_entry.get() == '':
            print 'To Date Required!'
            self.progress_bar.stop()
            self.progress_bar_value.set(0)
            return

        mixpanel = Mixpanel(
            api_key=self.api_key_entry.get(),
            api_secret=self.api_secret_entry.get(),
            endpoint=EXPORT_ENDPOINT
        )

        params = {'from_date': self.from_date_entry.get(), 'to_date': self.to_date_entry.get()}
        if self.events_entry.get():
            params['event'] = self.make_events_string(self.events_entry.get())

        if self.where_entry.get():
            params['where'] = self.where_entry.get()

        json_data = mixpanel.request(['export'], params)
        if json_data:
            mixpanel.event_json_to_csv(self.output_dir + "/event_export_"+str(int(time.time()))+".csv", json_data)
        else:
            print 'Export returned 0 events!'

        self.progress_bar.stop()
        self.progress_bar_value.set(0)

    def delete_people(self):

        if self.project_token_entry.get() == '':
            print 'Project Token Required!'
            return

        self.output_dir = askdirectory(title='Choose backup directory', mustexist=True, parent=self.master)

        self.progress_bar.start()
        mixpanel = Mixpanel(
            api_key=self.api_key_entry.get(),
            api_secret=self.api_secret_entry.get(),
            project_token=self.project_token_entry.get(),
            endpoint=API_ENDPOINT
        )

        parameters = {}
        '''Here is the place to define your selector to target only the users that you're after'''
        if self.where_entry.get():
            parameters['where'] = self.where_entry.get()

        response = mixpanel.request(['engage'], parameters)

        parameters.update({
                    'session_id': json.loads(response)['session_id'],
                    'page': 0
                    })
        global_total = json.loads(response)['total']

        if global_total == 0:
            print "Query returned 0 profiles!"
            self.progress_bar.stop()
            self.progress_bar_value.set(0)
            return

        print "Here are the # of people %d" % global_total
        fname = self.output_dir + "/backup-" + str(int(time.time())) + ".json"
        has_results = True
        total = 0
        print "BACKUP FILE saved to " + fname
        f = open(fname, 'w')

        self.progress_bar.stop()

        if askyesno('Delete People', 'You are about to delete ' + str(global_total) + ' users.\n\nDo you want to proceed?'):
            self.progress_bar.start()
            f.write('[')
            while has_results:
                responser = json.loads(response)['results']
                response_count = len(responser)
                total += response_count
                has_results = response_count == 1000
                i = 0
                for data in responser:
                    f.write(json.dumps(data))
                    i += 1
                    if i != response_count:
                        f.write(',')

                print "%d / %d" % (total,global_total)
                parameters['page'] += 1
                if has_results:
                    f.write(',')
                    response = mixpanel.request(['engage'], parameters)
                else:
                    f.write(']')
            print 'Done! - ' + str(global_total) + ' users deleted'
            self.progress_bar.stop()

        self.progress_bar_value.set(0)

if __name__ == '__main__':

    root = Tk()
    root.title('MPExport')
    text_output = ReadOnlyText(root)
    text_output.grid(row=11, column=0, columnspan=3, sticky=(N, S, E, W))
    sys.stdout = StdRedirector(text_output)
    app = MPExportApp(root)
    root.geometry("1024x768")
    root.minsize(width=480, height=480)
    root.mainloop()


