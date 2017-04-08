#!/usr/bin/env python

"""
main.py -- Udacity conference server-side Python App Engine
    HTTP controller handlers for memcache & task queue access

$Id$

created by wesc on 2014 may 24

"""

__author__ = 'wesc+api@google.com (Wesley Chun)'

import webapp2
from google.appengine.api import app_identity
from google.appengine.api import mail
from conference import ConferenceApi
import requests
from requests_toolbelt.adapters import appengine
import json
from models import Topic
from conference import Tag
from conference import Task
from datetime import datetime
from google.appengine.ext import ndb
from google.appengine.api import memcache


appengine.monkeypatch()


class SetAnnouncementHandler(webapp2.RequestHandler):
    def get(self):
        """Set Announcement in Memcache."""
        ConferenceApi._cacheAnnouncement()
        self.response.set_status(204)


class SendConfirmationEmailHandler(webapp2.RequestHandler):
    def post(self):
        """Send email confirming Conference creation."""
        mail.send_mail(
            'noreply@%s.appspotmail.com' % (
                app_identity.get_application_id()),     # from
            self.request.get('email'),                  # to
            'You created a new Conference!',            # subj
            'Hi, you have created a following '         # body
            'conference:\r\n\r\n%s' % self.request.get(
                'conferenceInfo')
        )


class TopicSnapshot(ndb.Model):
    task = ndb.KeyProperty(kind=Task)
    topic = ndb.KeyProperty(kind=Topic) # to be commented out
    vote = ndb.IntegerProperty()
    comment = ndb.IntegerProperty()
    time = ndb.DateTimeProperty(auto_now_add=True)


class CollectTopicTagHandler(webapp2.RequestHandler):
    # @ndb.transactional_async(xg=True)
    # def add_topic(self, topics, snapshots):
    #     ndb.put_multi_async(topics)
    #     ndb.put_multi_async(snapshots)

    def post(self):
        tag = self.request.get('tag')
        urlsafe = self.request.get('task_key')
        task_key = ndb.Key(urlsafe=urlsafe)
        task = task_key.get()
        task_id = str(task_key.id())
        item_mc_key = '{}:item'.format(task_id)
        last_id_mc_key = '{}:last_id'.format(task_id)
        counting_mc_key = '{}:counting'.format(task_id)
                
        counting = memcache.get(counting_mc_key)
        if not counting:
            counting = task.counting
            memcache.set(counting_mc_key, counting)

        url = 'https://pantip.com/forum/topic/ajax_json_all_topic_tag'
        headers = {'User-Agent': 'grit.intelligence@gmail.com',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'x-requested-with': 'XMLHttpRequest'}
        payload = [('last_id_current_page', '0'),
                    ('dataSend[tag]', tag),
                    ('dataSend[topic_type][type]', '0'),
                    ('dataSend[topic_type][default_type]', '1'),
                    ('thumbnailview', 'false'),
                    ('current_page', '1')]
        last_id = memcache.get(last_id_mc_key)
        if last_id:
            payload[0] = (payload[0][0], last_id)
        else:
            if task.last_id != '0':
                memcache.set(tag, task.last_id)
                payload[0] = (payload[0][0], task.last_id)
        item = memcache.get(item_mc_key)
        if not item:
            res = requests.post(url, payload, headers=headers)
            j = res.json()
            item = j['item']
        elif last_id and int(last_id) <= item['last_id_current_page']:
            payload[0] = (payload[0][0], last_id)
            res = requests.post(url, payload, headers=headers)
            j = res.json()
            item = j['item']
        while len(item['topic']) > 0:
            memcache.set(item_mc_key, item)
            topics = []
            topic_keys = []
            snapshots = []
            """ for likely high topic-redundancy task, unfinished implemented
            for t in item['topic']:
                if '_id' not in t.keys():
                    continue
                topic_keys.append(ndb.Key(Topic, str(t['_id'])))
                counting += 1
            topics = ndb.get_multi_async(topic_keys)
            """
            for t in item['topic']:
                if '_id' not in t.keys():
                    continue
                tags = []
                if isinstance(t['tags'], list):
                    for tt in t['tags']:
                        tags.append(ndb.Key(Tag, tt['tag']))
                top_key = ndb.Key(Topic, str(t['_id']))
                # topic = top_key.get()
                """ fuck! premature optimization is root of all evil!!!!!!!
                if topic:
                    if topic.tags !=  tags:
                        topic.tags = tags # won't update without snapshot
                    if topic.vote < t['votes'] or topic.comment < t['comments']:
                        topic.vote = t['votes']
                        topic.comment = t['comments']
                        snapshot = TopicSnapshot(task=task_key,
                                                 topic=top_key,
                                                 vote=t['votes'],
                                                 comment=t['comments'])
                        topics.append(topic)
                        snapshots.append(snapshot)
                """
                topic = Topic(key = top_key,
                              top_id = str(t['_id']),
                              vote = t['votes'],
                              comment = t['comments'],
                              author = t['author'],
                              disp_topic = t['disp_topic'],
                              topic_type = str(t['topic_type']),
                              utime = datetime.strptime(t['utime'], '%m/%d/%Y %H:%M:%S'),
                              tags = tags)
                # parent_key = ndb.Key(Topic, topic.top_id)
                """ # try without ancestor first
                s_id = TopicSnapshot.allocate_ids(size=1, parent=parent_key)[0]
                s_key = ndb.Key(TopicSnapshot, s_id, parent=parent_key)
                """
                snapshot = TopicSnapshot(# key=s_key,
                                         task=task_key,
                                         topic=top_key, # to be commented out
                                         vote=t['votes'],
                                         comment=t['comments'])
                topics.append(topic)
                snapshots.append(snapshot)
                counting += 1
            # self.add_topic(topics, snapshots)
            ndb.put_multi_async(topics)
            ndb.put_multi_async(snapshots)
            memcache.set(counting_mc_key, counting)
            # memcache.set(last_id_mc_key, str(item['last_id_current_page']))
            task.last_id = str(item['last_id_current_page'])
            task.counting = counting
            task.put_async()
            # page += 1 # not implement yet, counting is hard
            payload[0] = (payload[0][0], task.last_id)
            # payload[-1] = (payload[-1][0], page)
            res = requests.post(url, payload, headers=headers)
            j = res.json()
            item = j['item']
        # counted = memcache.get(counting_mc_key)
        # tag_key = ndb.Key(Tag, tag)
        # tag = tag_key.get()
        # tag.counting = counted
        # tag.put()
        # memcache.delete(counting_mc_key)
        memcache.delete(last_id_mc_key)
        memcache.delete(item_mc_key)


app = ndb.toplevel(
    webapp2.WSGIApplication([
    ('/crons/set_announcement', SetAnnouncementHandler),
    ('/tasks/send_confirmation_email', SendConfirmationEmailHandler),
    ('/tasks/collect_topic_tag', CollectTopicTagHandler)
], debug=True))
