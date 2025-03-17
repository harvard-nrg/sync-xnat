import os
import sys
import json
import logging
import tempfile
import requests
from urllib.parse import urlparse
from requests import codes as codes

logger = logging.getLogger(__name__)

class XNAT:
    def __init__(self, auth, cache_dir=None, keep_cache=False, verify=True):
        self.auth = auth
        self.keep_cache = keep_cache
        self.cache_dir = cache_dir
        self.verify = verify
        if not self.cache_dir:
            self.cache_dir = os.path.expanduser(f'~/.cache/syncxnat')

    def create_project(self, project):
        url = self.auth.url.rstrip('/')
        url = f'{url}/data/projects'
        label = project.ID
        body = {
            'xnat:projectData/ID': project.ID,
            'xnat:projectData/secondary_ID': project.get('secondary_ID', None),
            'xnat:projectData/name': project.get('name', None),
            'xnat:projectData/description': project.get('description', None),
            'xnat:projectData/keywords': project.get('keywords', None),
            'xnat:projectData/alias': project.get('alias', None),
            'xnat:projectData/PI/firstname': project.PI.get('firstname', None),
            'xnat:projectData/PI/lastname': project.PI.get('lastname', None)
        }
        logger.info(f'creating project {project.ID}')
        logger.info(f'POST to {url} with data={body}')
        #input('press enter to continue')
        r = requests.post(
            url,
            data=body,
            auth=(self.auth.username, self.auth.password),
            verify=self.verify
        )
        if r.status_code in (codes.ok, codes.created):
            logger.info(f'put {label} HTTP/{r.status_code}')
        elif r.status_code == codes.conflict:
            logger.info(f'{label} exists HTTP/{r.status_code}')
        elif r.status_code == codes.expectation_failed:
            logger.info(f'{label} exists HTTP/{r.status_code}')            
        else:
            raise Exception(f'unexpected response status HTTP/{r.status_code}')

    def create_subject(self, subject):
        project = subject.data_fields['project']
        label = subject.data_fields['label']
        url = self.auth.url.rstrip('/')
        url = f'{url}/data/projects/{project}/subjects/{label}'
        body = {
            'xnat:subjectData/group': subject.data_fields.get('group', None)
        }
        logger.info(f'creating subject {label}')
        logger.info(f'PUT url={url} data={body}')
        #input('press enter to continue')
        r = requests.put(
            url,
            data=body,
            auth=(self.auth.username, self.auth.password),
            verify=self.verify
        ) 
        if r.status_code in (codes.ok, codes.created):
            logger.info(f'put {label} HTTP/{r.status_code}')
        elif r.status_code == codes.conflict:
            logger.info(f'{label} exists HTTP/{r.status_code}')
        else:
            raise Exception(f'unexpected response status HTTP/{r.status_code}')

    def experiment_exists(self, subject, experiment):
        project = experiment.data_fields['project']
        subject = subject.data_fields['label']
        label = experiment.data_fields['label']
        url = self.auth.url.rstrip('/')
        url = f'{url}/data/projects/{project}/subjects/{subject}/experiments/{label}'
        r = requests.head(
            url,
            auth=(self.auth.username, self.auth.password),
            verify=self.verify
        )
        if r.status_code in (codes.ok, codes.created):
            return True
        return False

    def create_experiment(self, subject, experiment):
        project = experiment.data_fields['project']
        subject = subject.data_fields['label']
        label = experiment.data_fields['label']
        url = self.auth.url.rstrip('/')
        url = f'{url}/data/projects/{project}/subjects/{subject}/experiments/{label}'
        body = {
            'xnat:mrSessionData/dcmPatientBirthDate': experiment.data_fields.get('dcmPatientBirthDate', None),
            'xnat:mrSessionData/dcmPatientId': experiment.data_fields.get('dcmPatientId', None),
            'xnat:mrSessionData/dcmPatientWeight': experiment.data_fields.get('dcmPatientWeight', None),
            'xnat:mrSessionData/date': experiment.data_fields.get('date', None),
            'xnat:mrSessionData/prearchivePath': experiment.data_fields.get('prearchivePath', None),
            'xnat:mrSessionData/scanner/model': experiment.data_fields.get('scanner/model', None),
            'xnat:mrSessionData/scanner/manufacturer': experiment.data_fields.get('scanner/manufacturer', None),
            'xnat:mrSessionData/operator': experiment.data_fields.get('operator', None),
            'xnat:mrSessionData/acquisition_site': experiment.data_fields.get('acquisition_site', None),
            'xnat:mrSessionData/dcmPatientName': experiment.data_fields.get('dcmPatientName', None),
            'xnat:mrSessionData/UID': experiment.data_fields.get('UID', None),
            'xnat:mrSessionData/scanner': experiment.data_fields.get('scanner', None),
            'xnat:mrSessionData/fieldStrength': experiment.data_fields.get('fieldStrength', None),
            'xnat:mrSessionData/session_type': experiment.data_fields.get('session_type', None),
            'xnat:mrSessionData/time': experiment.data_fields.get('time', None),
            'xnat:mrSessionData/note': experiment.data_fields.get('note', None)
        }
        logger.info(f'creating {label}')
        logger.info(f'PUT url={url} data={body}')
        #input('press enter to continue')
        r = requests.put(
            url,
            data=body,
            auth=(self.auth.username, self.auth.password),
            verify=self.verify
        )
        if r.status_code in (codes.ok, codes.created):
            logger.info(f'put {label} HTTP/{r.status_code}')
        elif r.status_code == codes.conflict:
            logger.info(f'{label} exists HTTP/{r.status_code}')
        else:
            raise Exception(f'unexpected response status HTTP/{r.status_code}')

    def create_scan(self, subject, experiment, scan):
        plabel = experiment.data_fields['project']
        slabel = subject.data_fields['label']
        elabel = experiment.data_fields['label']
        label = scan.ID
        url = self.auth.url.rstrip('/')
        url = f'{url}/data/projects/{plabel}/subjects/{slabel}/experiments/{elabel}/scans/{scan.ID}'
        data = dict()
        for key,value in iter(scan.data_fields.items()):
            data[f'xnat:mrScandata/{key}'] = value
        logger.info(f'creating {scan.ID}')
        logger.info(f'PUT url={url} data={data}')
        #input('press enter to continue')
        r = requests.put(
            url,
            data=data,
            auth=(self.auth.username, self.auth.password),
            verify=self.verify
        )
        if r.status_code in (codes.ok, codes.created):
            logger.info(f'put {label} HTTP/{r.status_code}')
        elif r.status_code == codes.conflict:
            logger.info(f'{label} exists HTTP/{r.status_code}')
        else:
            raise Exception(f'unexpected response status HTTP/{r.status_code}')

    def download_scan(self, project, subject, experiment, scan):
        fname = f'{project.ID}-{subject.label}-{experiment.label}-{scan.ID}.zip'
        fullfile = os.path.join(self.cache_dir, fname)
        os.makedirs(self.cache_dir, exist_ok=True)
        if os.path.exists(fullfile):
            logger.info(f'scan archive is {fullfile}')
            return
        url = self.auth.url.rstrip('/')
        url = f'{url}/data/projects/{project.ID}/subjects/{subject.label}/experiments/{experiment.label}/scans/{scan.ID}/resources/{scan.label}/files'
        params = {
            'format': 'zip'
        }
        logger.info('downloading %s', url)
        r = requests.get(
            url,
            params=params,
            auth=(self.auth.username, self.auth.password),
            verify=self.verify
        )
        if r.status_code == codes.ok:
            with open(fullfile, 'wb') as fo:
                logger.info(f'writing scan to {fullfile}')
                fo.write(r.content)

    def upload_scan(self, project, subject, experiment, scan):
        label = scan.ID
        # create resource
        url = self.auth.url.rstrip('/')
        url = f'{url}/data/projects/{project.ID}/subjects/{subject.label}/experiments/{experiment.label}/scans/{scan.ID}/resources/{scan.label}'
        params = {
            'format': scan.format,
            'content': scan.content
        }
        logger.info(f'creating resource {url}')
        logger.info(f'PUT {url} with params={params}')
        #input('press enter to continue')
        r = requests.put(
            url,
            params=params,
            auth=(self.auth.username, self.auth.password),
            verify=self.verify
        )
        if r.status_code in (codes.ok, codes.created):
            logger.info(f'put {label} HTTP/{r.status_code}')
        elif r.status_code == codes.conflict:
            logger.info(f'{label} exists HTTP/{r.status_code}')
        else:
            raise Exception(f'unexpected response status HTTP/{r.status_code}')

        # upload files
        url = self.auth.url.rstrip('/')
        url = f'{url}/data/projects/{project.ID}/subjects/{subject.label}/experiments/{experiment.label}/scans/{scan.ID}/files'
        fname = f'{project.ID}-{subject.label}-{experiment.label}-{scan.ID}.zip'
        fullfile = os.path.join(self.cache_dir, fname)
        params = {
            'extract': 'true'
        }
        files = {
            'file.zip': open(fullfile, 'rb')
        }
        logger.info(f'uploading {fullfile} to {url}')
        #input('press enter to continue')
        r = requests.post(
            url,
            params=params,
            files=files,
            auth=(self.auth.username, self.auth.password),
            verify=self.verify
        )
        if r.status_code in (codes.ok, codes.created):
            logger.info(f'put {label} HTTP/{r.status_code}')
        elif r.status_code == codes.conflict:
            logger.info(f'{label} exists HTTP/{r.status_code}')
        else:
            raise Exception(f'unexpected response status HTTP/{r.status_code}')
        if not self.keep_cache:
            os.remove(fullfile)

    def project(self, pid):
        url = self.auth.url.rstrip('/')
        url = f'{url}/data/projects/{pid}?format=json'
        r = requests.get(
            url,
            auth=(self.auth.username, self.auth.password),
            verify=self.verify
        )
        project = Project()
        project.load(r.json())
        return project

    def subjects(self, project):
        url = self.auth.url.rstrip('/')
        url = f'{url}/data/projects/{project.ID}/subjects?format=json'
        r = requests.get(
            url,
            auth=(self.auth.username, self.auth.password),
            verify=self.verify
        )
        for subject in r.json()['ResultSet']['Result']:
            yield self.subject(subject['ID'])

    def subject(self, sid):
        url = self.auth.url.rstrip('/')
        url = f'{url}/data/subjects/{sid}?format=json'
        r = requests.get(
            url,
            auth=(self.auth.username, self.auth.password),
            verify=self.verify
        )
        subject = Subject()
        subject.load(r.json())
        return subject

    def experiments(self, subject):
        for eid in subject.experiments:
            yield self.experiment(eid)

    def experiment(self, eid):
        url = self.auth.url.rstrip('/')
        url = f'{url}/data/experiments/{eid}?format=json'
        r = requests.get(
            url,
            auth=(self.auth.username, self.auth.password),
            verify=self.verify
        )
        experiment = Experiment()
        experiment.load(r.json())
        return experiment

    def scans(self, experiment):
        url = self.auth.url.rstrip('/')
        url = f'{url}/data/experiments/{experiment.ID}/scans'
        params = {
            'format': 'json',
            'columns': 'ID'
        }
        r = requests.get(
            url,
            params=params,
            auth=(self.auth.username, self.auth.password),
            verify=self.verify
        )
        js = r.json()
        for item in js['ResultSet']['Result']:
            scan = Scan()
            scan.load(item)
            yield scan

class Scan(dict):
    def __init__(self):
        self.data_fields = dict()
        self.format = None
        self.content = None
        self.label = None

    @property
    def ID(self):
        return self.data_fields.get('ID')

    def load(self, js):
        self.data_fields = js['data_fields']
        self.format = js['children'][0]['items'][0]['data_fields']['format']
        self.content = js['children'][0]['items'][0]['data_fields']['content']
        self.label = js['children'][0]['items'][0]['data_fields']['label']

    def __str__(self):
        return json.dumps({
            'data_fields': self.data_fields,
            'format': self.format,
            'content': self.content,
            'label': self.label
        }, indent=2)

    def download(self):
        url = self.auth.url.rstrip('/')
        url = f'{url}/data/experiments/{experiment.ID}/scans?format=json'

class Experiment(dict):
    def __init__(self):
        self.data_fields = dict()
        self.scans = list()

    @property
    def label(self):
        return self.data_fields['label']

    @property
    def ID(self):
        return self.data_fields.get('ID')

    def load(self, js):
        self.data_fields = js['items'][0]['data_fields']
        self._scans(js)

    def _scans(self, js):
        for child in js['items'][0]['children']:
            if child['field'] != 'scans/scan':
                continue
            for item in child['items']:
                scan = Scan()
                scan.load(item)
                self.scans.append(scan)

    def __str__(self):
        return json.dumps({
            'data_fields': self.data_fields
        }, indent=2)

class Subject(dict):
    def __init__(self):
        self.data_fields = dict()
        self.experiments = list()

    @property
    def label(self):
        return self.data_fields.get('label')

    @property
    def ID(self):
        return self.data_fields.get('ID')

    def load(self, js):
        self.data_fields = js['items'][0]['data_fields']
        self._experiments(js)

    def _experiments(self, js):
        for child in js['items'][0]['children']:
            if child['field'] == 'experiments/experiment':
                for item in child['items']:
                    if item['meta']['xsi:type'] != 'xnat:mrSessionData':
                        continue
                    ID = item['data_fields']['ID']
                    self.experiments.append(ID)

    def __str__(self):
        return json.dumps({
            'data_fields': self.data_fields,
            'experiments': self.experiments
        }, indent=2)

class Project(dict):
    def __init__(self):
        self.data_fields = dict()
        self.PI = dict()

    @property
    def ID(self):
        return self.data_fields.get('ID')

    def load(self, js):
        self.data_fields = js['items'][0]['data_fields']
        for child in js['items'][0]['children']:
            if child['field'] == 'PI':
                self.PI = child['items'][0]['data_fields']

    def keys(self):
        return self.data_fields.keys()

    def __getitem__(self, key):
        return self.data_fields[key]

    def __str__(self):
        return json.dumps({
            'data_fields': self.data_fields,
            'PI': self.PI
        }, indent=2)

