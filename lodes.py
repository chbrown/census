import os
# import requests
import urllib

datadir = 'downloads'
root = 'http://lehd.did.census.gov/onthemap/{Version}/{State}'
states = [
    ('al', 'Alabama'),
    ('ak', 'Alaska'),
    ('az', 'Arizona'),
    ('ar', 'Arkansas'),
    ('ca', 'California'),
    ('co', 'Colorado'),
    ('ct', 'Connecticut'),
    ('de', 'Delaware'),
    ('dc', 'District of Columbia'),
    ('fl', 'Florida'),
    ('ga', 'Georgia'),
    ('hi', 'Hawaii'),
    ('id', 'Idaho'),
    ('il', 'Illinois'),
    ('in', 'Indiana'),
    ('ia', 'Iowa'),
    ('ks', 'Kansas'),
    ('ky', 'Kentucky'),
    ('la', 'Louisiana'),
    ('me', 'Maine'),
    ('md', 'Maryland'),
    ('ma', 'Massachusetts'),
    ('mi', 'Michigan'),
    ('mn', 'Minnesota'),
    ('ms', 'Mississippi'),
    ('mo', 'Missouri'),
    ('mt', 'Montana'),
    ('ne', 'Nebraska'),
    ('nv', 'Nevada'),
    ('nh', 'New Hampshire'),
    ('nj', 'New Jersey'),
    ('nm', 'New Mexico'),
    ('ny', 'New York'),
    ('nc', 'North Carolina'),
    ('nd', 'North Dakota'),
    ('oh', 'Ohio'),
    ('ok', 'Oklahoma'),
    ('or', 'Oregon'),
    ('pa', 'Pennsylvania'),
    ('pr', 'Puerto Rico'),
    ('ri', 'Rhode Island'),
    ('sc', 'South Carolina'),
    ('sd', 'South Dakota'),
    ('tn', 'Tennessee'),
    ('tx', 'Texas'),
    ('ut', 'Utah'),
    ('vt', 'Vermont'),
    ('va', 'Virginia'),
    ('wa', 'Washington'),
    ('wv', 'West Virginia'),
    ('wi', 'Wisconsin'),
    ('wy', 'Wyoming')]
# versions = ['LODES5', 'LODES7']
versions = ['LODES7']


def download(url, verbose=True):
    filepath = os.path.join(datadir, os.path.basename(url))
    if os.path.exists(filepath):
        if verbose:
            print 'Local file already exists:', filepath
        return filepath
    else:
        filename, headers = urllib.urlretrieve(url, filepath)
        if verbose:
            print 'Downloaded: ' + url
            print '>', filename
            print '!', headers
        return filename


def main():
    if not os.path.exists(datadir):
        os.mkdir(datadir)

    for version in versions:
        for state, state_name in states:
            args = dict(Version=version, State=state)
            print 'Starting {StateName} with {Version}'.format(StateName=state_name, Version=version)

            metadata_filename = download((root + '/version.txt').format(**args))
            md5sum_filename = download((root + '/lodes_{State}.md5sum').format(**args))
            xwalk_filename = download((root + '/{State}_xwalk.csv.gz').format(**args))

            with open(md5sum_filename) as md5sum_fp:
                for line in md5sum_fp:
                    md5sum, filename = line.split()
                    if 'xwalk' not in filename:
                        _, Type, _ = filename.split('_', 2)
                        download((root + '/{Type}/{filename}.gz').format(Type=Type, filename=filename, **args))


if __name__ == '__main__':
    main()
