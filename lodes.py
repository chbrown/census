import os
import urllib
import subprocess

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


def check(url, dirpath, gzip_test=False, verbose=True):
    filepath = os.path.join(dirpath, os.path.basename(url))
    if os.path.exists(filepath):
        print 'Local file already exists:', filepath
        if gzip_test:
            print 'Testing gzip:', filepath
            gzip_t = subprocess.call(['gzip', '-t', filepath])
            if gzip_t != 0:
                print 'Deleting corrupt gzip:', filepath
                os.remove(filepath)
                download(url, filepath, verbose)
    else:
        download(url, filepath, verbose)

    return filepath


def download(url, filepath, verbose):
    filename, headers = urllib.urlretrieve(url, filepath)
    if verbose:
        print 'Downloaded: ' + url
        print '>', filename
        print '!', headers


def loop(dirpath, versions):
    root = 'http://lehd.did.census.gov/onthemap/{Version}/{State}'

    if not os.path.exists(dirpath):
        os.mkdir(dirpath)

    for version in versions:
        for state, state_name in states:
            args = dict(Version=version, State=state)
            print 'Starting {StateName} with {Version}'.format(StateName=state_name, Version=version)

            check((root + '/version.txt').format(**args), dirpath)
            check((root + '/{State}_xwalk.csv.gz').format(**args), dirpath, gzip_test=True)
            md5sum_filename = check((root + '/lodes_{State}.md5sum').format(**args), dirpath)

            with open(md5sum_filename) as md5sum_fp:
                for line in md5sum_fp:
                    md5sum, filename = line.split()
                    if 'xwalk' not in filename:
                        _, Type, _ = filename.split('_', 2)
                        url = (root + '/{Type}/{filename}.gz').format(Type=Type, filename=filename, **args)
                        check(url, dirpath, gzip_test=True)


def main(dirpath, versions, nattempts=100):
    # Retry until we run without raising an exception
    for i in range(nattempts):
        print 'Attempt #%d' % i
        try:
            loop(dirpath, versions)
        except Exception, exc:
            print exc
        else:
            break
    else:
        print 'Tried the maximum amount of times (%d)' % nattempts


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Download all LODES census data')
    parser.add_argument('dirpath', help='directory to hold all files, without hierarchy')
    parser.add_argument('--versions', nargs='+', default=['LODES7'])
    opts = parser.parse_args()

    main(opts.dirpath, opts.versions)
