#!/usr/bin/env python

import sys
import yaxil
import logging
import argparse as ap
from xsync.models import XNAT

logger = logging.getLogger(__name__)

def main():
    parser = ap.ArgumentParser()
    parser.add_argument('--source')
    parser.add_argument('--destination')
    parser.add_argument('--project')
    parser.add_argument('--subject')
    parser.add_argument('--session')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level)

    source = XNAT(yaxil.auth(args.source))
    dest = XNAT(yaxil.auth(args.destination))

    project = source.project(args.project)
    logger.info(f'Mirroring Project {project.ID}')
    dest.create_project(project)
    for subject in source.subjects(project):
        if args.subject and subject.label != args.subject:
            continue
        logger.info(f'Mirroring Subject {subject.label}')
        dest.create_subject(subject)
        for experiment in source.experiments(subject):
            if args.session and experiment.label != args.session:
                continue
            if not dest.experiment_exists(subject, experiment):
                logger.info(f'Mirroring Experiment {experiment.label}')
                dest.create_experiment(subject, experiment)
                for scan in experiment.scans:
                    scanid = scan.data_fields['ID']
                    dest.create_scan(subject, experiment, scan)
                    source.download_scan(project, subject, experiment, scan)
                    logger.info(f'Mirroring Scan {scanid}')
                    dest.upload_scan(project, subject, experiment, scan)
            else:
                logger.info(f'Experiment Exists {experiment.label}')

if __name__ == '__main__':
    main()
